//! `pbx_command_v1` command handling for one authenticated RWI session.

use super::control::{Fence, MediaRequest, SnapshotFilter, VboxControl};
use super::{CONTRACT, MEDIA_FRAMING, OPERATIONS};
use crate::proxy::active_call_registry::ActiveProxyCallRegistry;
use crate::rwi::RwiGatewayRef;
use crate::rwi::bounded::BoundedSender;
use crate::rwi::processor::{CommandError, RwiCommandProcessor};
use crate::rwi::session::{OriginateRequest, OwnershipMode, RwiCommandPayload};
use serde_json::{Value, json};
use std::sync::Arc;
use tokio_util::sync::CancellationToken;

/// Everything a contract command needs from its RWI session.
pub struct ContractContext<'a> {
    pub session_id: &'a str,
    pub gateway: &'a RwiGatewayRef,
    pub processor: &'a Arc<RwiCommandProcessor>,
    pub call_registry: &'a Arc<ActiveProxyCallRegistry>,
    pub events: &'a BoundedSender<Value>,
    pub cancel: &'a CancellationToken,
}

/// The correlated reply, then events that must follow it on the wire.
pub struct ContractReply {
    pub reply: Value,
    pub follow_up: Vec<Value>,
}

impl ContractReply {
    fn only(reply: Value) -> Self {
        Self {
            reply,
            follow_up: Vec::new(),
        }
    }
}

struct Envelope<'v> {
    command_id: &'v str,
    operation: &'v str,
    owner_epoch: Option<u64>,
    call_id: Option<&'v str>,
    binding_id: Option<&'v str>,
    payload: &'v Value,
    value: &'v Value,
}

impl<'v> Envelope<'v> {
    /// A field from the payload, falling back to the envelope.
    fn field(&self, key: &str) -> Option<&'v str> {
        self.payload
            .get(key)
            .and_then(Value::as_str)
            .or_else(|| self.value.get(key).and_then(Value::as_str))
            .filter(|s| !s.is_empty())
    }
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn error(control: &VboxControl, env_command_id: &str, operation: &str, code: &str, reason: impl Into<String>) -> Value {
    json!({
        "type": "command_error",
        "schema_version": CONTRACT,
        "command_id": env_command_id,
        "operation": operation,
        "error_code": code,
        "reason": reason.into(),
        "pbx_boot_epoch": control.pbx_boot_epoch(),
        "current_owner_epoch": control.high_water_epoch(),
    })
}

fn ack(control: &VboxControl, env: &Envelope, pbx_call_id: Option<&str>, result: Value) -> Value {
    json!({
        "type": "command_ack",
        "schema_version": CONTRACT,
        "command_id": env.command_id,
        "operation": env.operation,
        "call_id": env.call_id,
        "binding_id": env.binding_id,
        "pbx_call_id": pbx_call_id,
        "pbx_leg_id": pbx_call_id,
        "pbx_boot_epoch": control.pbx_boot_epoch(),
        "pbx_instance_id": control.pbx_instance_id(),
        "owner_epoch": control.high_water_epoch(),
        "state_revision": 1,
        "result": result,
    })
}

fn native_error(control: &VboxControl, env: &Envelope, code: &str, err: CommandError) -> Value {
    match err {
        CommandError::CallNotFound(id) => error(control, env.command_id, env.operation, "call_not_found", id),
        CommandError::CommandFailed(msg) => error(control, env.command_id, env.operation, code, msg),
    }
}

/// `/ws/rustpbx/<call_id>` with an optional query: the only media target a
/// command may name. The host always comes from configuration.
fn valid_media_path(path: &str, call_id: &str) -> bool {
    let prefix = format!("/ws/rustpbx/{}", urlencoding::encode(call_id));
    match path.strip_prefix(&prefix) {
        Some(rest) => rest.is_empty() || rest.starts_with('?'),
        None => false,
    }
}

/// Handle one normalized frame. Never falls through to native dispatch.
pub async fn handle(value: &Value, ctx: ContractContext<'_>) -> ContractReply {
    let control = ctx.gateway.read().vbox.clone();
    control.set_call_registry(ctx.call_registry.clone());
    if !control.is_contract_session(ctx.session_id) {
        control.register_session(ctx.session_id, ctx.events.clone(), ctx.cancel.clone());
        // Native call events are not part of the normalized contract; the
        // controller receives normalized events only.
        ctx.gateway.write().suppress_native_events(&ctx.session_id.to_string());
    }

    let command_id = value.get("command_id").and_then(Value::as_str).unwrap_or("");
    let operation = value.get("operation").and_then(Value::as_str).unwrap_or("");
    if command_id.is_empty() {
        return ContractReply::only(error(&control, "", operation, "missing_command_id", "command_id is required"));
    }
    if value.get("schema_version").and_then(Value::as_str) != Some(CONTRACT) {
        return ContractReply::only(error(&control, command_id, operation, "unsupported_schema_version", "expected pbx_command_v1"));
    }
    if value.get("type").and_then(Value::as_str) != Some("command") {
        return ContractReply::only(error(&control, command_id, operation, "invalid_command_type", "type must be command"));
    }
    if !OPERATIONS.contains(&operation) {
        return ContractReply::only(error(&control, command_id, operation, "unsupported_contract_operation", operation));
    }
    if let Some(deadline) = value.get("deadline_at").and_then(Value::as_i64)
        && deadline < now_ms()
    {
        // A command that arrives after its deadline is never executed; the
        // sender already treats the outcome as unknown.
        return ContractReply::only(error(&control, command_id, operation, "deadline_exceeded", "deadline_at has passed"));
    }
    let null = Value::Null;
    let env = Envelope {
        command_id,
        operation,
        owner_epoch: value.get("owner_epoch").and_then(Value::as_u64),
        call_id: value.get("call_id").and_then(Value::as_str).filter(|s| !s.is_empty()),
        binding_id: value.get("binding_id").and_then(Value::as_str).filter(|s| !s.is_empty()),
        payload: value.get("payload").unwrap_or(&null),
        value,
    };

    // Read-only discovery operations do not take or require ownership.
    match operation {
        "capabilities" => return ContractReply::only(ack(&control, &env, None, control.capabilities())),
        "health" => return ContractReply::only(ack(&control, &env, None, control.health())),
        _ => {}
    }

    let Some(owner_epoch) = env.owner_epoch else {
        return ContractReply::only(error(&control, command_id, operation, "invalid_command_envelope", "owner_epoch is required"));
    };
    match control.fence(ctx.session_id, owner_epoch) {
        Fence::Stale { current_owner_epoch } | Fence::Fenced { current_owner_epoch } => {
            let mut e = error(&control, command_id, operation, "stale_owner_epoch", "owner_epoch is below the current controller epoch");
            e["current_owner_epoch"] = json!(current_owner_epoch);
            return ContractReply::only(e);
        }
        Fence::Accepted { became_controller } => {
            if became_controller {
                let mut gw = ctx.gateway.write();
                for pbx_call_id in control.active_pbx_call_ids() {
                    let _ = gw.claim_call_ownership(&ctx.session_id.to_string(), pbx_call_id, OwnershipMode::Control);
                }
            }
        }
    }

    match operation {
        "list_owned_calls" => {
            let since = env
                .payload
                .get("include_terminated_since")
                .or_else(|| value.get("include_terminated_since"))
                .and_then(Value::as_u64)
                .unwrap_or(0);
            ContractReply::only(control.snapshot(command_id, None, since))
        }
        "get_call" => {
            let filter = SnapshotFilter {
                pbx_call_id: env.field("pbx_call_id").map(str::to_string),
                call_id: env.field("call_id").map(str::to_string),
                binding_id: env.field("binding_id").map(str::to_string),
                command_id: env.payload.get("command_id").and_then(Value::as_str).map(str::to_string),
            };
            if filter.pbx_call_id.is_none() && filter.call_id.is_none() && filter.binding_id.is_none() && filter.command_id.is_none() {
                return ContractReply::only(error(&control, command_id, operation, "missing_required_field", "get_call needs a call identity"));
            }
            ContractReply::only(control.snapshot(command_id, Some(&filter), 0))
        }
        "claim_call" => claim_call(&control, &env, &ctx, owner_epoch),
        "attach_media" => attach_media(&control, &env),
        "answer" => answer(&control, &env, &ctx).await,
        "reject" => reject(&control, &env, &ctx).await,
        "hangup" => hangup(&control, &env, &ctx).await,
        "transfer" => transfer(&control, &env, &ctx).await,
        "originate" => originate(&control, &env, &ctx, owner_epoch).await,
        _ => ContractReply::only(error(&control, command_id, operation, "unsupported_contract_operation", operation)),
    }
}

fn resolve_pbx_call_id(control: &VboxControl, env: &Envelope) -> Option<String> {
    env.field("pbx_call_id")
        .map(str::to_string)
        .or_else(|| control.find_pbx_call_id(env.call_id, env.binding_id))
}

fn claim_call(control: &VboxControl, env: &Envelope, ctx: &ContractContext<'_>, owner_epoch: u64) -> ContractReply {
    let (Some(call_id), Some(binding_id)) = (env.call_id, env.binding_id) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "missing_required_field", "call_id and binding_id are required"));
    };
    let Some(pbx_call_id) = env.field("pbx_call_id") else {
        return ContractReply::only(error(control, env.command_id, env.operation, "missing_required_field", "pbx_call_id is required"));
    };
    if control.binding_summary(pbx_call_id).is_none() {
        if ctx.call_registry.get_handle(pbx_call_id).is_none() {
            return ContractReply::only(match control.tombstone_for(pbx_call_id) {
                Some(t) => {
                    let mut e = error(control, env.command_id, env.operation, "already_terminal", t.hangup_reason.clone());
                    e["final_state"] = json!(t.final_state);
                    e
                }
                None => error(control, env.command_id, env.operation, "call_not_found", pbx_call_id),
            });
        }
        if !control.is_discovered(pbx_call_id) {
            // Only calls offered in the vbox context may be claimed.
            return ContractReply::only(error(control, env.command_id, env.operation, "not_offered", pbx_call_id));
        }
    }
    match control.bind_inbound(pbx_call_id, call_id, binding_id, owner_epoch) {
        Ok(_) => {
            let _ = ctx.gateway.write().claim_call_ownership(
                &ctx.session_id.to_string(),
                pbx_call_id.to_string(),
                OwnershipMode::Control,
            );
            ContractReply::only(ack(control, env, Some(pbx_call_id), json!({"state": "claimed"})))
        }
        Err(code) => ContractReply::only(error(control, env.command_id, env.operation, code, pbx_call_id)),
    }
}

fn media_request(control: &VboxControl, env: &Envelope, call_id: &str) -> Result<Option<MediaRequest>, Value> {
    let Some(path) = env.field("ws_url") else {
        return Ok(None);
    };
    if let Some(framing) = env.field("outbound_framing")
        && framing != MEDIA_FRAMING
    {
        return Err(error(control, env.command_id, env.operation, "unsupported_media_framing", framing));
    }
    if !valid_media_path(path, call_id) {
        return Err(error(control, env.command_id, env.operation, "invalid_media_path", "ws_url must be /ws/rustpbx/<call_id>"));
    }
    if !control.media_configured() {
        return Err(error(control, env.command_id, env.operation, "media_not_configured", "rwi.vbox.media_base_url is not set"));
    }
    let Some(media_session_id) = env.field("media_session_id") else {
        return Err(error(control, env.command_id, env.operation, "missing_required_field", "media_session_id is required"));
    };
    Ok(Some(MediaRequest {
        media_session_id: media_session_id.to_string(),
        path: path.to_string(),
    }))
}

fn attach_media(control: &VboxControl, env: &Envelope) -> ContractReply {
    let Some(pbx_call_id) = resolve_pbx_call_id(control, env) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", "unknown call"));
    };
    let Some((call_id, _, _)) = control.binding_summary(&pbx_call_id) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_owned", pbx_call_id));
    };
    let request = match media_request(control, env, &call_id) {
        Ok(Some(r)) => r,
        Ok(None) => {
            return ContractReply::only(error(control, env.command_id, env.operation, "missing_required_field", "ws_url is required"));
        }
        Err(e) => return ContractReply::only(e),
    };
    let media_session_id = request.media_session_id.clone();
    match control.request_media(&pbx_call_id, request) {
        Ok(Some(connect)) => {
            if !control.dispatch_media_connect(connect) {
                return ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", pbx_call_id));
            }
            ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"media_session_id": media_session_id, "media_state": "connecting"})))
        }
        Ok(None) => ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"media_session_id": media_session_id, "media_state": "pending_answer"}))),
        Err(code) => ContractReply::only(error(control, env.command_id, env.operation, code, pbx_call_id)),
    }
}

async fn answer(control: &VboxControl, env: &Envelope<'_>, ctx: &ContractContext<'_>) -> ContractReply {
    let Some(pbx_call_id) = resolve_pbx_call_id(control, env) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", "unknown call"));
    };
    if control.binding_summary(&pbx_call_id).is_none() {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_owned", pbx_call_id));
    }
    match ctx.processor.process_command(RwiCommandPayload::Answer { call_id: pbx_call_id.clone() }).await {
        Ok(_) => ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"state": "answering"}))),
        Err(e) => ContractReply::only(native_error(control, env, "answer_failed", e)),
    }
}

async fn reject(control: &VboxControl, env: &Envelope<'_>, ctx: &ContractContext<'_>) -> ContractReply {
    let Some(pbx_call_id) = resolve_pbx_call_id(control, env) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", "unknown call"));
    };
    if control.binding_summary(&pbx_call_id).is_none() && !control.is_discovered(&pbx_call_id) {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_owned", pbx_call_id));
    }
    let reason = env.field("reason").map(str::to_string);
    if let Some(r) = &reason {
        control.set_terminal_reason(&pbx_call_id, r);
    }
    match ctx
        .processor
        .process_command(RwiCommandPayload::Reject { call_id: pbx_call_id.clone(), reason })
        .await
    {
        Ok(_) => ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"state": "rejecting"}))),
        Err(e) => ContractReply::only(native_error(control, env, "reject_failed", e)),
    }
}

async fn hangup(control: &VboxControl, env: &Envelope<'_>, ctx: &ContractContext<'_>) -> ContractReply {
    let pbx_call_id = resolve_pbx_call_id(control, env);
    let already_terminal = |pbx: &str| -> Option<ContractReply> {
        control.tombstone_for(pbx).map(|t| {
            ContractReply::only(ack(
                control,
                env,
                Some(pbx),
                json!({"already_terminal": true, "final_state": t.final_state, "hangup_reason": t.hangup_reason}),
            ))
        })
    };
    let Some(pbx_call_id) = pbx_call_id else {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", "unknown call"));
    };
    let owned = control.binding_summary(&pbx_call_id).is_some() || control.is_discovered(&pbx_call_id);
    if !owned {
        return already_terminal(&pbx_call_id)
            .unwrap_or_else(|| ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", pbx_call_id.clone())));
    }
    let reason = env.field("reason").map(str::to_string);
    if let Some(r) = &reason {
        control.set_terminal_reason(&pbx_call_id, r);
    }
    match ctx
        .processor
        .process_command(RwiCommandPayload::Hangup {
            call_id: pbx_call_id.clone(),
            reason,
            code: None,
        })
        .await
    {
        Ok(_) => ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"state": "ending"}))),
        Err(CommandError::CallNotFound(_)) => already_terminal(&pbx_call_id)
            .unwrap_or_else(|| ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", pbx_call_id.clone()))),
        Err(e) => ContractReply::only(native_error(control, env, "hangup_failed", e)),
    }
}

/// Transfer target ring limit bounds (`max_ring_secs` / `transfer_timeout_ms`).
pub(crate) const MIN_TRANSFER_RING_MS: u64 = 1_000;
pub(crate) const MAX_TRANSFER_RING_MS: u64 = 120_000;
pub(crate) const DEFAULT_TRANSFER_RING_MS: u64 = 30_000;

async fn transfer(control: &VboxControl, env: &Envelope<'_>, ctx: &ContractContext<'_>) -> ContractReply {
    let Some(pbx_call_id) = resolve_pbx_call_id(control, env) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_found", "unknown call"));
    };
    let Some(target) = env.field("target") else {
        return ContractReply::only(error(control, env.command_id, env.operation, "missing_required_field", "target is required"));
    };
    // How long the target may ring: vbox's per-target `max_ring_secs`, else
    // the legacy `transfer_timeout_ms`; clamped to 1-120 s, 30 s when absent.
    // A real phone needs well over the former 4 s cap to answer.
    let timeout_ms = match (env.payload.get("max_ring_secs"), env.payload.get("transfer_timeout_ms")) {
        (Some(value), _) => match value.as_u64() {
            Some(secs) => secs.clamp(MIN_TRANSFER_RING_MS / 1000, MAX_TRANSFER_RING_MS / 1000) * 1000,
            None => return ContractReply::only(error(control, env.command_id, env.operation,
                "invalid_max_ring_secs", "max_ring_secs must be a non-negative integer")),
        },
        (None, Some(value)) => match value.as_u64() {
            Some(ms) => ms.clamp(MIN_TRANSFER_RING_MS, MAX_TRANSFER_RING_MS),
            None => return ContractReply::only(error(control, env.command_id, env.operation,
                "invalid_transfer_timeout", "transfer_timeout_ms must be a non-negative integer")),
        },
        (None, None) => DEFAULT_TRANSFER_RING_MS,
    };
    if !control.set_pending_transfer(&pbx_call_id, env.command_id, target, timeout_ms) {
        return ContractReply::only(error(control, env.command_id, env.operation, "call_not_owned", pbx_call_id));
    }
    match ctx
        .processor
        .process_command(RwiCommandPayload::Transfer {
            call_id: pbx_call_id.clone(),
            target: target.to_string(),
        })
        .await
    {
        Ok(_) => ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"state": "transferring", "target": target}))),
        Err(e) => {
            control.clear_pending_transfer(&pbx_call_id);
            ContractReply::only(native_error(control, env, "transfer_failed", e))
        }
    }
}

fn originate_destination(control: &VboxControl, env: &Envelope) -> Option<String> {
    if let Some(dest) = env.field("destination") {
        return Some(dest.to_string());
    }
    let to = env.field("to")?;
    if to.starts_with("sip:") || to.starts_with("sips:") {
        return Some(to.to_string());
    }
    let domain = control
        .config()
        .outbound_domain
        .unwrap_or_else(|| "localhost".to_string());
    Some(format!("sip:{to}@{domain}"))
}

async fn originate(control: &VboxControl, env: &Envelope<'_>, ctx: &ContractContext<'_>, owner_epoch: u64) -> ContractReply {
    let (Some(call_id), Some(binding_id)) = (env.call_id, env.binding_id) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "missing_required_field", "call_id and binding_id are required"));
    };
    // The same command again (a duplicated frame) never dials twice.
    if let Some(pbx_call_id) = control.originate_for_command(env.command_id) {
        return ContractReply::only(ack(control, env, Some(&pbx_call_id), json!({"state": "dialing", "duplicate": true})));
    }
    let Some(destination) = originate_destination(control, env) else {
        return ContractReply::only(error(control, env.command_id, env.operation, "missing_required_field", "to or destination is required"));
    };
    let media = match media_request(control, env, call_id) {
        Ok(m) => m,
        Err(e) => return ContractReply::only(e),
    };
    let pbx_call_id = format!("vbx-{}", uuid::Uuid::new_v4().simple());
    let trunk = env
        .field("trunk")
        .map(str::to_string)
        .or_else(|| control.config().outbound_trunk);
    let timeout_secs = env
        .payload
        .get("ring_timeout_secs")
        .and_then(Value::as_u64)
        .map(|s| s as u32);
    control.bind_outbound(&pbx_call_id, call_id, binding_id, env.command_id, owner_epoch, media);
    let _ = ctx.gateway.write().claim_call_ownership(
        &ctx.session_id.to_string(),
        pbx_call_id.clone(),
        OwnershipMode::Control,
    );
    let request = OriginateRequest {
        call_id: pbx_call_id.clone(),
        destination,
        caller_id: env.field("from").map(str::to_string),
        timeout_secs,
        extra_headers: Default::default(),
        trunk,
        route_originated_calls: None,
        record: None,
    };
    match ctx.processor.process_command(RwiCommandPayload::Originate(request)).await {
        Ok(_) => {
            let follow_up = control
                .binding_event_for(&pbx_call_id, "dialing")
                .into_iter()
                .collect();
            ContractReply {
                reply: ack(control, env, Some(&pbx_call_id), json!({"state": "dialing"})),
                follow_up,
            }
        }
        Err(e) => {
            ctx.gateway
                .write()
                .release_call_ownership(&ctx.session_id.to_string(), &pbx_call_id);
            control.unbind_failed_originate(&pbx_call_id, "originate_failed");
            ContractReply::only(native_error(control, env, "originate_failed", e))
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_path_must_name_the_bound_call() {
        assert!(valid_media_path("/ws/rustpbx/call-1", "call-1"));
        assert!(valid_media_path("/ws/rustpbx/a%2Fb%20c%3F?token=x", "a/b c?"));
        assert!(valid_media_path("/ws/rustpbx/call-1?token=abc", "call-1"));
        assert!(!valid_media_path("/ws/rustpbx/call-10?token=abc", "call-1"));
        assert!(!valid_media_path("wss://evil.example/ws/rustpbx/call-1", "call-1"));
        assert!(!valid_media_path("/ws/rustpbx/call-1/../x", "call-1"));
        assert!(!valid_media_path("/other/call-1", "call-1"));
    }
}
