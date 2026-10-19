//! Per-PBX vbox controller state: ownership epochs, call bindings, terminal
//! tombstones, orphan hold and media attachments.
//!
//! Lock discipline: `inner` is a short in-memory critical section. It may be
//! taken while the gateway lock is held (event observation runs inside
//! gateway dispatch), so nothing here takes the gateway lock or awaits.

use super::{IMPLEMENTED_CAPABILITIES, OPERATIONS, REQUIRED_CAPABILITIES, VboxContractConfig};
use crate::call::vbox_media::{CloseRequest, VboxMediaStart};
use crate::proxy::active_call_registry::ActiveProxyCallRegistry;
use crate::rwi::bounded::BoundedSender;
use crate::rwi::event::RwiEvent;
use serde_json::{Value, json};
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::sync::mpsc;
use tokio_util::sync::CancellationToken;

pub type SessionId = String;

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Direction {
    Inbound,
    Outbound,
}

impl Direction {
    fn as_str(&self) -> &'static str {
        match self {
            Direction::Inbound => "inbound",
            Direction::Outbound => "outbound",
        }
    }
}

/// What vbox asked to attach for a call.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaRequest {
    pub media_session_id: String,
    /// Path and query from `attach_media` / `originate` (`/ws/rustpbx/<call_id>?token=…`).
    pub path: String,
}

#[derive(Debug)]
struct ActiveMedia {
    media_session_id: String,
    close: mpsc::Sender<CloseRequest>,
}

#[derive(Debug)]
pub struct Binding {
    pub pbx_call_id: String,
    pub call_id: String,
    pub binding_id: String,
    pub command_id: Option<String>,
    pub direction: Direction,
    pub state: &'static str,
    pub owner_epoch: u64,
    pub answered_at_ms: Option<i64>,
    pub created_at_ms: i64,
    media: Option<MediaRequest>,
    /// `media_session_id` already handed to the call session for connect.
    media_requested: Option<String>,
    active_media: Option<ActiveMedia>,
    pending_transfer: Option<(String, String)>,
    transfer_timeout_ms: Option<u64>,
    terminal_hint: Option<&'static str>,
    terminal_reason: Option<String>,
}

#[derive(Debug, Clone)]
pub struct Tombstone {
    pub seq: u64,
    pub pbx_call_id: String,
    pub call_id: Option<String>,
    pub binding_id: Option<String>,
    pub command_id: Option<String>,
    pub direction: Option<&'static str>,
    pub sip_status: Option<u16>,
    pub answered_at_ms: Option<i64>,
    pub ended_at_ms: i64,
    pub hangup_reason: String,
    pub final_state: &'static str,
    recorded: Instant,
}

#[derive(Debug, Clone)]
struct Discovered {
    from: String,
    to: String,
    discovered_at_ms: i64,
}

struct SessionEntry {
    epoch: Option<u64>,
    events: BoundedSender<Value>,
    cancel: CancellationToken,
    fenced: bool,
}

#[derive(Default)]
struct Inner {
    high_water_epoch: u64,
    controller: Option<SessionId>,
    sessions: HashMap<SessionId, SessionEntry>,
    bindings: HashMap<String, Binding>,
    by_command: HashMap<String, String>,
    discovered: HashMap<String, Discovered>,
    tombstones: VecDeque<Tombstone>,
    tombstone_seq: u64,
    /// Tombstones dropped (count bound or retention) since this boot. While
    /// zero, every call that ended in this boot epoch is still answerable.
    tombstones_evicted: u64,
    orphan_generation: u64,
    orphan_since: Option<Instant>,
    call_registry: Option<Arc<ActiveProxyCallRegistry>>,
}

/// Outcome of presenting an `owner_epoch` with a command.
#[derive(Debug, PartialEq)]
pub enum Fence {
    /// The session is (still or now) the controller.
    Accepted { became_controller: bool },
    /// `owner_epoch` below the high-water epoch.
    Stale { current_owner_epoch: u64 },
    /// The session was fenced by a newer controller and may not act again.
    Fenced { current_owner_epoch: u64 },
}

/// A timer the caller must arm after a controller disconnect.
#[derive(Debug, PartialEq)]
pub struct OrphanHold {
    pub generation: u64,
    pub hold: Duration,
}

/// Call to finish because the orphan hold expired.
pub struct OrphanExpiry {
    pub pbx_call_id: String,
    pub media_close: Option<mpsc::Sender<CloseRequest>>,
}

/// A media connect the caller must hand to the call session.
#[derive(Debug, Clone)]
pub struct MediaConnect {
    pub pbx_call_id: String,
    pub start: VboxMediaStart,
    /// A previous attachment to close first (`media_reattach`).
    pub supersede: Option<mpsc::Sender<CloseRequest>>,
}

pub struct VboxControl {
    config: parking_lot::RwLock<VboxContractConfig>,
    pbx_boot_epoch: String,
    inner: parking_lot::Mutex<Inner>,
}

impl std::fmt::Debug for VboxControl {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("VboxControl")
            .field("pbx_boot_epoch", &self.pbx_boot_epoch)
            .finish()
    }
}

impl Default for VboxControl {
    fn default() -> Self {
        Self::new(VboxContractConfig::default())
    }
}

impl VboxControl {
    pub fn new(config: VboxContractConfig) -> Self {
        let boot = format!(
            "boot_{}_{}",
            now_ms(),
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        Self {
            config: parking_lot::RwLock::new(config),
            pbx_boot_epoch: boot,
            inner: parking_lot::Mutex::new(Inner::default()),
        }
    }

    pub fn set_config(&self, config: VboxContractConfig) {
        *self.config.write() = config;
    }

    pub fn config(&self) -> VboxContractConfig {
        self.config.read().clone()
    }

    pub fn pbx_boot_epoch(&self) -> &str {
        &self.pbx_boot_epoch
    }

    pub fn pbx_instance_id(&self) -> String {
        self.config
            .read()
            .pbx_instance_id
            .clone()
            .unwrap_or_else(|| "rustpbx".to_string())
    }

    pub fn context(&self) -> String {
        self.config.read().context.clone()
    }

    pub fn set_call_registry(&self, registry: Arc<ActiveProxyCallRegistry>) {
        let mut inner = self.inner.lock();
        if inner.call_registry.is_none() {
            inner.call_registry = Some(registry);
        }
    }

    fn registry(&self) -> Option<Arc<ActiveProxyCallRegistry>> {
        self.inner.lock().call_registry.clone()
    }

    // ── Sessions and ownership ────────────────────────────────────────────

    /// Record a session that speaks the normalized contract. Idempotent.
    pub fn register_session(
        &self,
        session_id: &str,
        events: BoundedSender<Value>,
        cancel: CancellationToken,
    ) {
        let mut inner = self.inner.lock();
        inner
            .sessions
            .entry(session_id.to_string())
            .or_insert(SessionEntry {
                epoch: None,
                events,
                cancel,
                fenced: false,
            });
    }

    pub fn is_contract_session(&self, session_id: &str) -> bool {
        self.inner.lock().sessions.contains_key(session_id)
    }

    /// Receiver-enforced ownership fencing (`I10`). An epoch below the
    /// high-water mark is rejected; a higher epoch, or the same epoch from a
    /// replacement session, takes control and closes the previous controller.
    pub fn fence(&self, session_id: &str, owner_epoch: u64) -> Fence {
        let mut inner = self.inner.lock();
        let high = inner.high_water_epoch;
        if let Some(entry) = inner.sessions.get(session_id)
            && entry.fenced
        {
            return Fence::Fenced {
                current_owner_epoch: high,
            };
        }
        if owner_epoch < high {
            return Fence::Stale {
                current_owner_epoch: high,
            };
        }
        let already = inner.controller.as_deref() == Some(session_id)
            && inner
                .sessions
                .get(session_id)
                .and_then(|e| e.epoch)
                == Some(owner_epoch);
        if already {
            return Fence::Accepted {
                became_controller: false,
            };
        }
        inner.high_water_epoch = owner_epoch;
        if let Some(previous) = inner.controller.take()
            && previous != session_id
            && let Some(entry) = inner.sessions.get_mut(&previous)
        {
            entry.fenced = true;
            entry.cancel.cancel();
        }
        // Any other session still holding an older epoch is fenced too.
        for (id, entry) in inner.sessions.iter_mut() {
            if id != session_id && entry.epoch.is_some_and(|e| e < owner_epoch) && !entry.fenced {
                entry.fenced = true;
                entry.cancel.cancel();
            }
        }
        if let Some(entry) = inner.sessions.get_mut(session_id) {
            entry.epoch = Some(owner_epoch);
        }
        inner.controller = Some(session_id.to_string());
        inner.orphan_since = None;
        inner.orphan_generation += 1;
        for binding in inner.bindings.values_mut() {
            binding.owner_epoch = owner_epoch;
        }
        Fence::Accepted {
            became_controller: true,
        }
    }

    pub fn high_water_epoch(&self) -> u64 {
        self.inner.lock().high_water_epoch
    }

    pub fn controller(&self) -> Option<SessionId> {
        self.inner.lock().controller.clone()
    }

    /// Pbx call ids of active bindings (for re-claiming native ownership
    /// after a controller change).
    pub fn active_pbx_call_ids(&self) -> Vec<String> {
        self.inner.lock().bindings.keys().cloned().collect()
    }

    /// A contract session's WebSocket closed. Returns an orphan hold to arm
    /// when it was the live controller (not fenced by a successor).
    pub fn session_closed(&self, session_id: &str) -> Option<OrphanHold> {
        let mut inner = self.inner.lock();
        let entry = inner.sessions.remove(session_id)?;
        if inner.controller.as_deref() != Some(session_id) || entry.fenced {
            return None;
        }
        inner.controller = None;
        inner.orphan_generation += 1;
        inner.orphan_since = Some(Instant::now());
        let hold = Duration::from_secs(self.config.read().orphan_hold_secs);
        Some(OrphanHold {
            generation: inner.orphan_generation,
            hold,
        })
    }

    /// New inbound calls are offered only while a controller is connected.
    pub fn accepting_inbound(&self) -> bool {
        self.inner.lock().controller.is_some()
    }

    /// The orphan hold expired. Returns the calls to hang up, or nothing if a
    /// controller reconnected meanwhile.
    pub fn orphan_expired(&self, generation: u64) -> Vec<OrphanExpiry> {
        let mut inner = self.inner.lock();
        if inner.controller.is_some() || inner.orphan_generation != generation {
            return Vec::new();
        }
        inner.orphan_since = None;
        inner
            .bindings
            .values_mut()
            .map(|binding| {
                binding.terminal_reason = Some("rwi_orphan_hold_expired".to_string());
                OrphanExpiry {
                    pbx_call_id: binding.pbx_call_id.clone(),
                    media_close: binding.active_media.as_ref().map(|m| m.close.clone()),
                }
            })
            .collect()
    }

    /// Unclaimed discovered calls cannot be held either; they are rejected
    /// on orphan expiry.
    pub fn take_unclaimed_discoveries(&self) -> Vec<String> {
        let mut inner = self.inner.lock();
        inner.discovered.drain().map(|(id, _)| id).collect()
    }

    // ── Event emission ────────────────────────────────────────────────────

    fn emit_locked(inner: &Inner, value: Value) {
        if let Some(controller) = inner.controller.as_ref()
            && let Some(entry) = inner.sessions.get(controller)
        {
            let _ = entry.events.send(value);
        }
    }

    fn event_base(&self, kind: &str, epoch: u64) -> Value {
        json!({
            "type": kind,
            "event": kind,
            "schema_version": super::CONTRACT,
            "event_id": uuid::Uuid::new_v4().to_string(),
            "pbx_instance_id": self.pbx_instance_id(),
            "pbx_boot_epoch": self.pbx_boot_epoch,
            "owner_epoch": epoch,
            "occurred_at": now_ms(),
        })
    }

    fn binding_event(&self, kind: &str, binding: &Binding) -> Value {
        let mut v = self.event_base(kind, binding.owner_epoch);
        v["call_id"] = json!(binding.call_id);
        v["binding_id"] = json!(binding.binding_id);
        v["pbx_call_id"] = json!(binding.pbx_call_id);
        v["pbx_leg_id"] = json!(binding.pbx_call_id);
        if let Some(cmd) = &binding.command_id {
            v["command_id"] = json!(cmd);
        }
        v["direction"] = json!(binding.direction.as_str());
        v
    }

    /// Emit an event built by the command handler to the controller.
    pub fn emit(&self, value: Value) {
        let inner = self.inner.lock();
        Self::emit_locked(&inner, value);
    }

    /// Event for a bound call, if the binding still exists.
    pub fn binding_event_for(&self, pbx_call_id: &str, kind: &str) -> Option<Value> {
        let inner = self.inner.lock();
        inner
            .bindings
            .get(pbx_call_id)
            .map(|b| self.binding_event(kind, b))
    }

    // ── Bindings ──────────────────────────────────────────────────────────

    pub fn is_discovered(&self, pbx_call_id: &str) -> bool {
        self.inner.lock().discovered.contains_key(pbx_call_id)
    }

    pub fn binding_summary(&self, pbx_call_id: &str) -> Option<(String, String, &'static str)> {
        let inner = self.inner.lock();
        inner
            .bindings
            .get(pbx_call_id)
            .map(|b| (b.call_id.clone(), b.binding_id.clone(), b.state))
    }

    pub fn find_pbx_call_id(&self, call_id: Option<&str>, binding_id: Option<&str>) -> Option<String> {
        let inner = self.inner.lock();
        inner
            .bindings
            .values()
            .find(|b| {
                call_id.is_some_and(|c| c == b.call_id) || binding_id.is_some_and(|x| x == b.binding_id)
            })
            .map(|b| b.pbx_call_id.clone())
    }

    /// Bind a discovered inbound call to a vbox call. Idempotent for the same
    /// binding; a different binding for the same PBX call is a conflict.
    pub fn bind_inbound(
        &self,
        pbx_call_id: &str,
        call_id: &str,
        binding_id: &str,
        owner_epoch: u64,
    ) -> Result<bool, &'static str> {
        let mut inner = self.inner.lock();
        if let Some(existing) = inner.bindings.get(pbx_call_id) {
            return if existing.binding_id == binding_id && existing.call_id == call_id {
                Ok(false)
            } else {
                Err("binding_conflict")
            };
        }
        inner.discovered.remove(pbx_call_id);
        inner.bindings.insert(
            pbx_call_id.to_string(),
            Binding {
                pbx_call_id: pbx_call_id.to_string(),
                call_id: call_id.to_string(),
                binding_id: binding_id.to_string(),
                command_id: None,
                direction: Direction::Inbound,
                state: "claimed",
                owner_epoch,
                answered_at_ms: None,
                created_at_ms: now_ms(),
                media: None,
                media_requested: None,
                active_media: None,
                pending_transfer: None,
                transfer_timeout_ms: None,
                terminal_hint: None,
                terminal_reason: None,
            },
        );
        Ok(true)
    }

    /// Previously recorded originate for a command id (idempotent retry).
    pub fn originate_for_command(&self, command_id: &str) -> Option<String> {
        self.inner.lock().by_command.get(command_id).cloned()
    }

    /// Record an outbound binding before the INVITE is sent so early events
    /// correlate even if they precede the command reply (`I3`).
    pub fn bind_outbound(
        &self,
        pbx_call_id: &str,
        call_id: &str,
        binding_id: &str,
        command_id: &str,
        owner_epoch: u64,
        media: Option<MediaRequest>,
    ) {
        let mut inner = self.inner.lock();
        inner
            .by_command
            .insert(command_id.to_string(), pbx_call_id.to_string());
        inner.bindings.insert(
            pbx_call_id.to_string(),
            Binding {
                pbx_call_id: pbx_call_id.to_string(),
                call_id: call_id.to_string(),
                binding_id: binding_id.to_string(),
                command_id: Some(command_id.to_string()),
                direction: Direction::Outbound,
                state: "dialing",
                owner_epoch,
                answered_at_ms: None,
                created_at_ms: now_ms(),
                media,
                media_requested: None,
                active_media: None,
                pending_transfer: None,
                transfer_timeout_ms: None,
                terminal_hint: None,
                terminal_reason: None,
            },
        );
    }

    /// Undo an outbound binding whose INVITE was never sent.
    pub fn unbind_failed_originate(&self, pbx_call_id: &str, reason: &str) {
        let mut inner = self.inner.lock();
        if let Some(binding) = inner.bindings.remove(pbx_call_id) {
            let tombstone = self.make_tombstone(&mut inner, &binding, None, reason.to_string(), "not_sent");
            self.push_tombstone(&mut inner, tombstone);
        }
    }

    pub fn set_pending_transfer(&self, pbx_call_id: &str, command_id: &str, target: &str, timeout_ms: u64) -> bool {
        let mut inner = self.inner.lock();
        match inner.bindings.get_mut(pbx_call_id) {
            Some(b) => {
                b.pending_transfer = Some((command_id.to_string(), target.to_string()));
                b.transfer_timeout_ms = Some(timeout_ms);
                b.state = "transferring";
                true
            }
            None => false,
        }
    }

    /// True while a contract `transfer` for this call awaits its outcome.
    pub fn transfer_pending(&self, pbx_call_id: &str) -> bool {
        self.inner
            .lock()
            .bindings
            .get(pbx_call_id)
            .is_some_and(|b| b.pending_transfer.is_some())
    }

    pub fn transfer_timeout_ms(&self, pbx_call_id: &str) -> Option<u64> {
        let inner = self.inner.lock();
        let binding = inner.bindings.get(pbx_call_id)?;
        binding.pending_transfer.as_ref()?;
        binding.transfer_timeout_ms
    }

    pub fn clear_pending_transfer(&self, pbx_call_id: &str) {
        let mut inner = self.inner.lock();
        if let Some(b) = inner.bindings.get_mut(pbx_call_id) {
            b.pending_transfer = None;
            if b.state == "transferring" {
                b.state = if b.answered_at_ms.is_some() { "answered" } else { "claimed" };
            }
        }
    }

    pub fn set_terminal_reason(&self, pbx_call_id: &str, reason: &str) {
        let mut inner = self.inner.lock();
        if let Some(b) = inner.bindings.get_mut(pbx_call_id)
            && b.terminal_reason.is_none()
        {
            b.terminal_reason = Some(reason.to_string());
        }
    }

    // ── Media ─────────────────────────────────────────────────────────────

    fn media_url(&self, path: &str) -> Option<String> {
        let base = self.config.read().media_base_url.clone()?;
        Some(format!("{}{}", base.trim_end_matches('/'), path))
    }

    fn media_start(&self, binding: &Binding, request: &MediaRequest) -> Option<VboxMediaStart> {
        let config = self.config.read();
        let base = config.media_base_url.clone()?;
        Some(VboxMediaStart {
            url: format!("{}{}", base.trim_end_matches('/'), request.path),
            call_id: binding.call_id.clone(),
            binding_id: binding.binding_id.clone(),
            media_session_id: request.media_session_id.clone(),
            pbx_call_id: binding.pbx_call_id.clone(),
            pbx_instance_id: config
                .pbx_instance_id
                .clone()
                .unwrap_or_else(|| "rustpbx".to_string()),
            max_buffer_ms: config.max_playout_buffer_ms,
            connect_timeout_ms: config.media_connect_timeout_ms,
        })
    }

    pub fn media_configured(&self) -> bool {
        self.config.read().media_base_url.is_some()
    }

    /// Record what vbox wants attached. Returns a connect to perform now when
    /// the call is answered; otherwise the connect happens on answer.
    pub fn request_media(&self, pbx_call_id: &str, request: MediaRequest) -> Result<Option<MediaConnect>, &'static str> {
        let mut inner = self.inner.lock();
        let binding = inner.bindings.get_mut(pbx_call_id).ok_or("call_not_found")?;
        binding.media = Some(request);
        Ok(self.connect_if_ready(binding))
    }

    fn connect_if_ready(&self, binding: &mut Binding) -> Option<MediaConnect> {
        if binding.answered_at_ms.is_none() {
            return None;
        }
        let request = binding.media.clone()?;
        if binding.media_requested.as_deref() == Some(request.media_session_id.as_str()) {
            return None;
        }
        let start = self.media_start(binding, &request)?;
        binding.media_requested = Some(request.media_session_id.clone());
        let supersede = binding
            .active_media
            .as_ref()
            .filter(|m| m.media_session_id != request.media_session_id)
            .map(|m| m.close.clone());
        Some(MediaConnect {
            pbx_call_id: binding.pbx_call_id.clone(),
            start,
            supersede,
        })
    }

    /// The call session connected a media socket.
    pub fn media_attached(&self, pbx_call_id: &str, media_session_id: &str, close: mpsc::Sender<CloseRequest>) {
        let mut inner = self.inner.lock();
        if let Some(binding) = inner.bindings.get_mut(pbx_call_id) {
            binding.active_media = Some(ActiveMedia {
                media_session_id: media_session_id.to_string(),
                close,
            });
        }
    }

    /// The media socket for `media_session_id` ended. A superseded socket
    /// does not clear the current attachment.
    pub fn media_detached(&self, pbx_call_id: &str, media_session_id: &str) {
        let mut inner = self.inner.lock();
        if let Some(binding) = inner.bindings.get_mut(pbx_call_id) {
            if binding
                .active_media
                .as_ref()
                .is_some_and(|m| m.media_session_id == media_session_id)
            {
                binding.active_media = None;
            }
            if binding.media_requested.as_deref() == Some(media_session_id) {
                binding.media_requested = None;
                if binding.media.as_ref().is_some_and(|m| m.media_session_id == media_session_id) {
                    binding.media = None;
                }
            }
        }
    }

    pub fn is_active_media(&self, pbx_call_id: &str, media_session_id: &str) -> bool {
        self.inner
            .lock()
            .bindings
            .get(pbx_call_id)
            .and_then(|b| b.active_media.as_ref())
            .is_some_and(|m| m.media_session_id == media_session_id)
    }

    /// Close the active media socket of a call (if any).
    pub fn media_close_handle(&self, pbx_call_id: &str) -> Option<mpsc::Sender<CloseRequest>> {
        self.inner
            .lock()
            .bindings
            .get(pbx_call_id)
            .and_then(|b| b.active_media.as_ref().map(|m| m.close.clone()))
    }

    // ── Native event observation ──────────────────────────────────────────

    /// Observe a native gateway event. Runs inside gateway dispatch; returns
    /// a media connect for the caller to deliver to the call session.
    pub fn observe(&self, event: &RwiEvent) -> Option<MediaConnect> {
        let call_id = event.call_id.as_deref()?;
        let leg_id = event.payload.get("leg_id").and_then(Value::as_str);
        match event.event_type {
            "call_created" => {
                let context = event.payload.get("context").and_then(Value::as_str);
                if context != Some(self.config.read().context.as_str()) {
                    return None;
                }
                let mut inner = self.inner.lock();
                if inner.controller.is_none() || inner.bindings.contains_key(call_id) {
                    return None;
                }
                let from_uri = event.payload.get("caller").and_then(Value::as_str).unwrap_or("");
                let to_uri = event.payload.get("callee").and_then(Value::as_str).unwrap_or("");
                let from = sip_user(from_uri);
                let to = sip_user(to_uri);
                inner.discovered.insert(
                    call_id.to_string(),
                    Discovered {
                        from: from.clone(),
                        to: to.clone(),
                        discovered_at_ms: now_ms(),
                    },
                );
                let mut v = self.event_base("incoming_call", inner.high_water_epoch);
                v["pbx_call_id"] = json!(call_id);
                v["pbx_leg_id"] = json!(call_id);
                v["from"] = json!(from);
                v["to"] = json!(to);
                v["from_uri"] = json!(from_uri);
                v["to_uri"] = json!(to_uri);
                v["sip_headers"] = event
                    .payload
                    .get("sip_headers")
                    .cloned()
                    .unwrap_or_else(|| json!({}));
                v["direction"] = json!("inbound");
                v["provider_connection_id"] = json!(self.config.read().provider_connection_id);
                Self::emit_locked(&inner, v);
                None
            }
            "call_ringing" if leg_id.is_none() || self.is_outbound(call_id) => {
                let early = event.payload.get("early_media").and_then(Value::as_bool).unwrap_or(false);
                let mut inner = self.inner.lock();
                let binding = inner.bindings.get_mut(call_id)?;
                if binding.direction != Direction::Outbound || binding.answered_at_ms.is_some() {
                    return None;
                }
                let kind = if early { "early_media" } else { "ringing" };
                if binding.state == kind {
                    return None;
                }
                binding.state = kind;
                let v = self.binding_event(kind, binding);
                Self::emit_locked(&inner, v);
                None
            }
            "call_answered" if leg_id.is_none() => self.mark_answered(call_id),
            "call_busy" => {
                let mut inner = self.inner.lock();
                if let Some(b) = inner.bindings.get_mut(call_id) {
                    b.terminal_hint = Some("busy");
                }
                None
            }
            "call_no_answer" => {
                let mut inner = self.inner.lock();
                if let Some(b) = inner.bindings.get_mut(call_id) {
                    b.terminal_hint = Some("no_answer");
                }
                None
            }
            "call_transferred" => {
                let mut inner = self.inner.lock();
                let binding = inner.bindings.get(call_id)?;
                let (_, target) = binding.pending_transfer.clone()?;
                let binding = inner.bindings.remove(call_id)?;
                let mut v = self.binding_event("transfer_completed", &binding);
                if let Some((cmd, _)) = &binding.pending_transfer {
                    v["command_id"] = json!(cmd);
                }
                v["transfer_target"] = json!(target);
                v["replacement_pbx_leg_id"] = json!(format!("{call_id}:transfer"));
                if let Some(media) = &binding.active_media {
                    let _ = media.close.try_send(CloseRequest {
                        code: crate::call::vbox_media::CLOSE_NORMAL,
                        reason: "transferred".into(),
                    });
                }
                let tombstone = self.make_tombstone(&mut inner, &binding, None, "transferred".into(), "transferred");
                self.push_tombstone(&mut inner, tombstone);
                Self::emit_locked(&inner, v);
                None
            }
            "call_transfer_failed" => {
                let mut inner = self.inner.lock();
                let binding = inner.bindings.get_mut(call_id)?;
                let (cmd, target) = binding.pending_transfer.take()?;
                binding.state = if binding.answered_at_ms.is_some() { "answered" } else { "claimed" };
                let mut v = self.binding_event("command_error", binding);
                v["command_id"] = json!(cmd);
                v["operation"] = json!("transfer");
                v["error_code"] = json!("transfer_failed");
                v["reason"] = json!(event
                    .payload
                    .get("reason")
                    .and_then(Value::as_str)
                    .unwrap_or("transfer_failed"));
                v["sip_status"] = event.payload.get("sip_status").cloned().unwrap_or(Value::Null);
                v["transfer_target"] = json!(target);
                Self::emit_locked(&inner, v);
                None
            }
            "call_hangup" if leg_id.is_none() => {
                let reason = event.payload.get("reason").and_then(Value::as_str).map(str::to_string);
                let sip_status = event
                    .payload
                    .get("sip_status")
                    .and_then(Value::as_u64)
                    .map(|s| s as u16);
                self.finish(call_id, reason, sip_status);
                None
            }
            _ => None,
        }
    }

    /// The telephony leg is answered: emit `answered` once and start media
    /// if vbox already asked for it. Fed by the session-level
    /// `call_answered` event and by the caller-leg answer of an app-anchored
    /// call (which upstream deliberately does not report as `call_answered`).
    pub fn mark_answered(&self, pbx_call_id: &str) -> Option<MediaConnect> {
        let mut inner = self.inner.lock();
        let binding = inner.bindings.get_mut(pbx_call_id)?;
        if binding.answered_at_ms.is_some() {
            return None;
        }
        let answered_at = now_ms();
        binding.answered_at_ms = Some(answered_at);
        binding.state = "answered";
        let mut v = self.binding_event("answered", binding);
        v["answered_at"] = json!(answered_at);
        let connect = self.connect_if_ready(binding);
        Self::emit_locked(&inner, v);
        connect
    }

    fn is_outbound(&self, pbx_call_id: &str) -> bool {
        self.inner
            .lock()
            .bindings
            .get(pbx_call_id)
            .is_some_and(|b| b.direction == Direction::Outbound)
    }

    /// The native session is gone (gateway `call_finished`). Backstop for a
    /// session that ended without a session-level hangup event.
    pub fn call_finished(&self, pbx_call_id: &str) {
        self.finish(pbx_call_id, None, None);
    }

    fn finish(&self, pbx_call_id: &str, reason: Option<String>, sip_status: Option<u16>) {
        let mut inner = self.inner.lock();
        if let Some(binding) = inner.bindings.remove(pbx_call_id) {
            if let Some(cmd) = &binding.pending_transfer {
                // The call ended while a transfer was pending: report it.
                let mut v = self.binding_event("command_error", &binding);
                v["command_id"] = json!(cmd.0);
                v["operation"] = json!("transfer");
                v["error_code"] = json!("transfer_failed");
                v["reason"] = json!("call_ended");
                Self::emit_locked(&inner, v);
            }
            let hangup_reason = binding
                .terminal_reason
                .clone()
                .or(reason)
                .unwrap_or_else(|| "normal_clearing".to_string());
            let final_state = match (binding.answered_at_ms, binding.terminal_hint) {
                (Some(_), _) => "completed",
                (None, Some(hint)) => hint,
                (None, None) => match binding.direction {
                    Direction::Outbound => "failed",
                    Direction::Inbound => "rejected",
                },
            };
            let tombstone = self.make_tombstone(&mut inner, &binding, sip_status, hangup_reason.clone(), final_state);
            let mut v = self.binding_event("call_ended", &binding);
            v["reason"] = json!(hangup_reason);
            v["hangup_reason"] = json!(hangup_reason);
            v["sip_status"] = json!(tombstone.sip_status.unwrap_or(if binding.answered_at_ms.is_some() { 200 } else { 0 }));
            v["final_state"] = json!(final_state);
            v["ended_at"] = json!(tombstone.ended_at_ms);
            self.push_tombstone(&mut inner, tombstone);
            Self::emit_locked(&inner, v);
        } else if inner.discovered.remove(pbx_call_id).is_some() {
            // Offered but never claimed (caller CANCEL, or rejected).
            let reason = reason.unwrap_or_else(|| "cancelled".to_string());
            let tombstone = Tombstone {
                seq: 0,
                pbx_call_id: pbx_call_id.to_string(),
                call_id: None,
                binding_id: None,
                command_id: None,
                direction: Some("inbound"),
                sip_status,
                answered_at_ms: None,
                ended_at_ms: now_ms(),
                hangup_reason: reason.clone(),
                final_state: "cancelled",
                recorded: Instant::now(),
            };
            let mut v = self.event_base("call_ended", inner.high_water_epoch);
            v["pbx_call_id"] = json!(pbx_call_id);
            v["reason"] = json!(reason);
            v["sip_status"] = json!(sip_status.unwrap_or(0));
            v["final_state"] = json!("cancelled");
            self.push_tombstone(&mut inner, tombstone);
            Self::emit_locked(&inner, v);
        }
    }

    // ── Tombstones and snapshots ──────────────────────────────────────────

    fn make_tombstone(
        &self,
        _inner: &mut Inner,
        binding: &Binding,
        sip_status: Option<u16>,
        hangup_reason: String,
        final_state: &'static str,
    ) -> Tombstone {
        Tombstone {
            seq: 0,
            pbx_call_id: binding.pbx_call_id.clone(),
            call_id: Some(binding.call_id.clone()),
            binding_id: Some(binding.binding_id.clone()),
            command_id: binding.command_id.clone(),
            direction: Some(binding.direction.as_str()),
            sip_status,
            answered_at_ms: binding.answered_at_ms,
            ended_at_ms: now_ms(),
            hangup_reason,
            final_state,
            recorded: Instant::now(),
        }
    }

    fn push_tombstone(&self, inner: &mut Inner, mut tombstone: Tombstone) {
        inner.tombstone_seq += 1;
        tombstone.seq = inner.tombstone_seq;
        inner.tombstones.push_back(tombstone);
        self.prune_locked(inner);
    }

    fn prune_locked(&self, inner: &mut Inner) {
        let config = self.config.read();
        let retention = Duration::from_secs(config.tombstone_retention_secs);
        while inner.tombstones.len() > config.max_tombstones {
            inner.tombstones.pop_front();
            inner.tombstones_evicted += 1;
        }
        while inner
            .tombstones
            .front()
            .is_some_and(|t| t.recorded.elapsed() > retention)
        {
            inner.tombstones.pop_front();
            inner.tombstones_evicted += 1;
        }
    }

    pub fn tombstone_for(&self, pbx_call_id: &str) -> Option<Tombstone> {
        let mut inner = self.inner.lock();
        self.prune_locked(&mut inner);
        inner
            .tombstones
            .iter()
            .rev()
            .find(|t| t.pbx_call_id == pbx_call_id)
            .cloned()
    }

    fn tombstone_json(&self, t: &Tombstone) -> Value {
        json!({
            "seq": t.seq,
            "pbx_call_id": t.pbx_call_id,
            "pbx_leg_id": t.pbx_call_id,
            "call_id": t.call_id,
            "binding_id": t.binding_id,
            "command_id": t.command_id,
            "direction": t.direction,
            "sip_status": t.sip_status.unwrap_or(if t.answered_at_ms.is_some() { 200 } else { 0 }),
            "answered_at": t.answered_at_ms,
            "ended_at": t.ended_at_ms,
            "hangup_reason": t.hangup_reason,
            "final_state": t.final_state,
            "pbx_boot_epoch": self.pbx_boot_epoch,
        })
    }

    fn active_json(&self, b: &Binding) -> Value {
        json!({
            "pbx_call_id": b.pbx_call_id,
            "pbx_leg_id": b.pbx_call_id,
            "call_id": b.call_id,
            "binding_id": b.binding_id,
            "command_id": b.command_id,
            "direction": b.direction.as_str(),
            "state": b.state,
            "owner_epoch": b.owner_epoch,
            "answered_at": b.answered_at_ms,
            "media_session_id": b.active_media.as_ref().map(|m| m.media_session_id.clone()),
            "pbx_boot_epoch": self.pbx_boot_epoch,
        })
    }

    /// Snapshot of active bindings and retained tombstones. `filter` limits
    /// it to one call identified by any of its ids.
    pub fn snapshot(&self, command_id: &str, filter: Option<&SnapshotFilter>, terminated_since: u64) -> Value {
        let mut inner = self.inner.lock();
        self.prune_locked(&mut inner);
        let matches_binding = |b: &Binding| match filter {
            None => true,
            Some(f) => f.matches(&b.pbx_call_id, Some(&b.call_id), Some(&b.binding_id), b.command_id.as_deref()),
        };
        let matches_tomb = |t: &Tombstone| match filter {
            None => t.seq > terminated_since,
            Some(f) => f.matches(&t.pbx_call_id, t.call_id.as_deref(), t.binding_id.as_deref(), t.command_id.as_deref()),
        };
        let mut active: Vec<Value> = inner
            .bindings
            .values()
            .filter(|b| matches_binding(b))
            .map(|b| self.active_json(b))
            .collect();
        // Calls offered to vbox but not claimed are PBX-active too; a booting
        // controller must see them to clean them up.
        active.extend(
            inner
                .discovered
                .iter()
                .filter(|(pbx, _)| match filter {
                    None => true,
                    Some(f) => f.matches(pbx, None, None, None),
                })
                .map(|(pbx, d)| {
                    json!({
                        "pbx_call_id": pbx,
                        "pbx_leg_id": pbx,
                        "direction": "inbound",
                        "state": "offered",
                        "from": d.from,
                        "to": d.to,
                        "offered_at": d.discovered_at_ms,
                        "owner_epoch": inner.high_water_epoch,
                        "pbx_boot_epoch": self.pbx_boot_epoch,
                    })
                }),
        );
        let tombstones: Vec<Value> = inner
            .tombstones
            .iter()
            .filter(|t| matches_tomb(t))
            .map(|t| self.tombstone_json(t))
            .collect();
        json!({
            "type": "snapshot_response",
            "schema_version": super::CONTRACT,
            "command_id": command_id,
            "pbx_instance_id": self.pbx_instance_id(),
            "pbx_boot_epoch": self.pbx_boot_epoch,
            "owner_epoch": inner.high_water_epoch,
            "active_calls": active,
            "terminal_tombstones": tombstones,
            "terminated_cursor": inner.tombstone_seq,
            "snapshot_at_ms": now_ms(),
            "tombstone_retention_secs": self.config.read().tombstone_retention_secs,
            "tombstone_retention_ms": self.config.read().tombstone_retention_secs * 1000,
            // True while no tombstone of this boot epoch has been dropped, so
            // a call absent from the snapshot provably never reached the PBX.
            "tombstone_window_covers": inner.tombstones_evicted == 0,
        })
    }

    pub fn capabilities(&self) -> Value {
        let capabilities: Vec<&str> = IMPLEMENTED_CAPABILITIES.to_vec();
        let missing: Vec<&str> = REQUIRED_CAPABILITIES
            .iter()
            .copied()
            .filter(|c| !capabilities.contains(c))
            .collect();
        let config = self.config.read();
        json!({
            "contract": super::CONTRACT,
            "integration_ready": missing.is_empty() && config.media_base_url.is_some(),
            "capabilities": capabilities,
            "missing_capabilities": missing,
            "supported_operations": OPERATIONS,
            "media_framing": super::MEDIA_FRAMING,
            "media_configured": config.media_base_url.is_some(),
            "orphan_hold_secs": config.orphan_hold_secs,
            "tombstone_retention_secs": config.tombstone_retention_secs,
            "max_tombstones": config.max_tombstones,
            "pbx_boot_epoch": self.pbx_boot_epoch,
            "pbx_instance_id": config.pbx_instance_id.clone().unwrap_or_else(|| "rustpbx".into()),
            "owner_epoch_fencing": "receiver_enforced",
            "native_contract": "rwi-v1-action-params",
        })
    }

    pub fn health(&self) -> Value {
        let mut inner = self.inner.lock();
        self.prune_locked(&mut inner);
        json!({
            "pbx_boot_epoch": self.pbx_boot_epoch,
            "pbx_instance_id": self.pbx_instance_id(),
            "owner_epoch": inner.high_water_epoch,
            "controller_connected": inner.controller.is_some(),
            "orphan_hold_elapsed_ms": inner.orphan_since.map(|s| s.elapsed().as_millis() as u64),
            "active_calls": inner.bindings.len(),
            "unclaimed_calls": inner.discovered.len(),
            "tombstones": inner.tombstones.len(),
        })
    }

    pub fn media_url_for(&self, path: &str) -> Option<String> {
        self.media_url(path)
    }

    /// Hang up through the call registry (orphan expiry path).
    pub fn hangup_native(&self, pbx_call_id: &str, reason: &str) -> bool {
        let Some(registry) = self.registry() else {
            return false;
        };
        let Some(handle) = registry.get_handle(pbx_call_id) else {
            return false;
        };
        handle
            .send_command(crate::call::domain::CallCommand::Hangup(
                crate::call::domain::HangupCommand {
                    leg_id: None,
                    cascade: crate::call::domain::HangupCascade::All,
                    initiator: crate::call::domain::HangupInitiator::System {
                        reason: crate::call::domain::SystemHangupReason::InternalError,
                        details: Some(reason.to_string()),
                    },
                    reason: Some(crate::callrecord::CallRecordHangupReason::BySystem),
                    code: Some(503),
                    rtp_timeout_side: None,
                },
            ))
            .is_ok()
    }

    /// Deliver a media connect to the call session.
    pub fn dispatch_media_connect(&self, connect: MediaConnect) -> bool {
        if let Some(previous) = connect.supersede {
            let _ = previous.try_send(CloseRequest {
                code: crate::call::vbox_media::CLOSE_NORMAL,
                reason: "superseded".into(),
            });
        }
        let Some(registry) = self.registry() else {
            return false;
        };
        let Some(handle) = registry.get_handle(&connect.pbx_call_id) else {
            return false;
        };
        handle
            .send_command(crate::call::domain::CallCommand::VboxMediaAttach(connect.start))
            .is_ok()
    }
}

/// The user part of a SIP/tel URI (`"Bob" <sip:+1555@h;x=y>` → `+1555`), so
/// vbox routes on the dialled number rather than PBX host details.
pub fn sip_user(uri: &str) -> String {
    let inner = match (uri.find('<'), uri.rfind('>')) {
        (Some(a), Some(b)) if b > a => &uri[a + 1..b],
        _ => uri,
    };
    let inner = inner.trim();
    let rest = inner
        .strip_prefix("sips:")
        .or_else(|| inner.strip_prefix("sip:"))
        .or_else(|| inner.strip_prefix("tel:"))
        .unwrap_or(inner);
    let user = rest.split('@').next().unwrap_or(rest);
    user.split(';').next().unwrap_or(user).to_string()
}

/// Identifies one call in `get_call`.
#[derive(Debug, Default, Clone)]
pub struct SnapshotFilter {
    pub pbx_call_id: Option<String>,
    pub call_id: Option<String>,
    pub binding_id: Option<String>,
    pub command_id: Option<String>,
}

impl SnapshotFilter {
    fn matches(&self, pbx: &str, call_id: Option<&str>, binding_id: Option<&str>, command_id: Option<&str>) -> bool {
        self.pbx_call_id.as_deref().is_some_and(|x| x == pbx)
            || (self.call_id.is_some() && self.call_id.as_deref() == call_id)
            || (self.binding_id.is_some() && self.binding_id.as_deref() == binding_id)
            || (self.command_id.is_some() && self.command_id.as_deref() == command_id)
    }
}

/// Arm the orphan-hold timer for `control` (`I7`).
pub fn spawn_orphan_timer(control: Arc<VboxControl>, hold: OrphanHold) {
    crate::utils::spawn(async move {
        tokio::time::sleep(hold.hold).await;
        let expired = control.orphan_expired(hold.generation);
        for call in expired {
            if let Some(close) = call.media_close {
                let _ = close
                    .send(CloseRequest {
                        code: crate::call::vbox_media::CLOSE_RWI_ORPHAN_TIMEOUT,
                        reason: "rwi_orphan_timeout".into(),
                    })
                    .await;
            }
            if !control.hangup_native(&call.pbx_call_id, "rwi_orphan_hold_expired") {
                // The session is already gone; record the terminal state now.
                control.call_finished(&call.pbx_call_id);
            }
        }
        if !control.accepting_inbound() {
            for pbx_call_id in control.take_unclaimed_discoveries() {
                control.hangup_native(&pbx_call_id, "rwi_orphan_hold_expired");
            }
        }
    });
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::rwi::bounded;

    fn session(control: &VboxControl, id: &str) -> (mpsc::Receiver<Value>, CancellationToken) {
        let cancel = CancellationToken::new();
        let (tx, rx) = bounded::channel(64, cancel.clone());
        control.register_session(id, tx, cancel.clone());
        (rx, cancel)
    }

    fn native(event_type: &'static str, call_id: &str, payload: Value) -> RwiEvent {
        let mut payload = payload;
        payload["event_type"] = json!(event_type);
        payload["call_id"] = json!(call_id);
        RwiEvent {
            event_type,
            call_id: Some(call_id.to_string()),
            payload,
        }
    }

    fn drain(rx: &mut mpsc::Receiver<Value>) -> Vec<Value> {
        let mut out = Vec::new();
        while let Ok(v) = rx.try_recv() {
            out.push(v);
        }
        out
    }

    #[test]
    fn stale_epoch_is_rejected_and_newer_epoch_fences_the_old_controller() {
        let control = VboxControl::default();
        let (_rx_a, cancel_a) = session(&control, "a");
        let (_rx_b, cancel_b) = session(&control, "b");
        assert_eq!(control.fence("a", 4), Fence::Accepted { became_controller: true });
        assert_eq!(control.fence("a", 4), Fence::Accepted { became_controller: false });
        assert_eq!(control.fence("b", 3), Fence::Stale { current_owner_epoch: 4 });
        assert!(!cancel_a.is_cancelled());
        // Standby takes over with E + 1 while A's socket is still open.
        assert_eq!(control.fence("b", 5), Fence::Accepted { became_controller: true });
        assert!(cancel_a.is_cancelled(), "old controller socket is closed");
        assert!(!cancel_b.is_cancelled());
        assert_eq!(control.fence("a", 5), Fence::Fenced { current_owner_epoch: 5 });
        assert_eq!(control.fence("a", 6), Fence::Fenced { current_owner_epoch: 5 });
        assert_eq!(control.controller().as_deref(), Some("b"));
        // A fenced session closing does not start an orphan hold.
        assert_eq!(control.session_closed("a"), None);
        assert!(control.accepting_inbound());
    }

    #[test]
    fn same_epoch_from_a_replacement_session_takes_over() {
        let control = VboxControl::default();
        let (_rx_a, cancel_a) = session(&control, "a");
        let (_rx_b, _cancel_b) = session(&control, "b");
        control.fence("a", 2);
        assert_eq!(control.fence("b", 2), Fence::Accepted { became_controller: true });
        assert!(cancel_a.is_cancelled());
    }

    #[test]
    fn inbound_discovery_claim_answer_and_hangup_produce_events_and_tombstone() {
        let control = VboxControl::default();
        let (mut rx, _c) = session(&control, "a");
        control.fence("a", 1);
        control.observe(&native("call_created", "pbx-1", json!({"context":"vbox","caller":"sip:+1555@carrier","callee":"sip:+1666@pbx:5060"})));
        let events = drain(&mut rx);
        assert_eq!(events[0]["type"], "incoming_call");
        assert_eq!(events[0]["pbx_call_id"], "pbx-1");
        assert_eq!(events[0]["from"], "+1555");
        assert_eq!(events[0]["to"], "+1666");
        assert_eq!(events[0]["pbx_boot_epoch"], control.pbx_boot_epoch());
        // Another context is not offered.
        control.observe(&native("call_created", "pbx-x", json!({"context":"default"})));
        assert!(drain(&mut rx).is_empty());

        assert_eq!(control.bind_inbound("pbx-1", "call-1", "bind-1", 1), Ok(true));
        assert_eq!(control.bind_inbound("pbx-1", "call-1", "bind-1", 1), Ok(false));
        assert_eq!(control.bind_inbound("pbx-1", "call-2", "bind-2", 1), Err("binding_conflict"));
        control.observe(&native("call_answered", "pbx-1", json!({})));
        let answered = drain(&mut rx);
        assert_eq!(answered[0]["type"], "answered");
        assert_eq!(answered[0]["call_id"], "call-1");
        assert_eq!(answered[0]["binding_id"], "bind-1");
        // Leg-level hangups do not end the call.
        control.observe(&native("call_hangup", "pbx-1", json!({"leg_id":"callee","reason":"x"})));
        assert!(drain(&mut rx).is_empty());
        control.observe(&native("call_hangup", "pbx-1", json!({"reason":"by_caller","sip_status":200})));
        let ended = drain(&mut rx);
        assert_eq!(ended[0]["type"], "call_ended");
        assert_eq!(ended[0]["reason"], "by_caller");
        assert_eq!(ended[0]["final_state"], "completed");
        let snap = control.snapshot("cmd-s", None, 0);
        assert_eq!(snap["type"], "snapshot_response");
        assert_eq!(snap["pbx_instance_id"], "rustpbx");
        assert_eq!(snap["tombstone_window_covers"], true);
        assert_eq!(snap["active_calls"].as_array().unwrap().len(), 0);
        let tombs = snap["terminal_tombstones"].as_array().unwrap();
        assert_eq!(tombs.len(), 1);
        assert_eq!(tombs[0]["call_id"], "call-1");
        assert_eq!(tombs[0]["hangup_reason"], "by_caller");
        assert!(tombs[0]["answered_at"].as_i64().is_some());
        // Repeated finish is a no-op.
        control.call_finished("pbx-1");
        assert_eq!(control.snapshot("c", None, 0)["terminal_tombstones"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn unclaimed_cancelled_call_reports_call_ended_and_tombstone() {
        let control = VboxControl::default();
        let (mut rx, _c) = session(&control, "a");
        control.fence("a", 1);
        control.observe(&native("call_created", "pbx-2", json!({"context":"vbox"})));
        drain(&mut rx);
        control.observe(&native("call_hangup", "pbx-2", json!({"reason":"canceled"})));
        let ended = drain(&mut rx);
        assert_eq!(ended[0]["type"], "call_ended");
        assert_eq!(ended[0]["final_state"], "cancelled");
        assert_eq!(control.tombstone_for("pbx-2").unwrap().final_state, "cancelled");
    }

    #[test]
    fn outbound_events_correlate_before_reply_and_busy_is_recorded() {
        let control = VboxControl::default();
        let (mut rx, _c) = session(&control, "a");
        control.fence("a", 7);
        control.bind_outbound("pbx-o", "call-o", "bind-o", "cmd-o", 7, None);
        control.observe(&native("call_ringing", "pbx-o", json!({"early_media": false})));
        control.observe(&native("call_ringing", "pbx-o", json!({"early_media": true})));
        control.observe(&native("call_busy", "pbx-o", json!({})));
        control.observe(&native("call_hangup", "pbx-o", json!({"reason":"busy","sip_status":486})));
        let events = drain(&mut rx);
        let kinds: Vec<&str> = events.iter().map(|e| e["type"].as_str().unwrap()).collect();
        assert_eq!(kinds, vec!["ringing", "early_media", "call_ended"]);
        assert_eq!(events[0]["command_id"], "cmd-o");
        assert_eq!(events[0]["owner_epoch"], 7);
        assert_eq!(events[2]["final_state"], "busy");
        assert_eq!(events[2]["sip_status"], 486);
        assert_eq!(control.originate_for_command("cmd-o").as_deref(), Some("pbx-o"));
        let snap = control.snapshot("s", Some(&SnapshotFilter { command_id: Some("cmd-o".into()), ..Default::default() }), 0);
        assert_eq!(snap["terminal_tombstones"][0]["final_state"], "busy");
    }

    #[test]
    fn controller_disconnect_starts_orphan_hold_and_reconnect_cancels_it() {
        let mut config = VboxContractConfig::default();
        config.orphan_hold_secs = 5;
        let control = VboxControl::new(config);
        let (_rx, _c) = session(&control, "a");
        control.fence("a", 1);
        control.bind_inbound("pbx-1", "call-1", "bind-1", 1).unwrap();
        let hold = control.session_closed("a").expect("orphan hold armed");
        assert_eq!(hold.hold, Duration::from_secs(5));
        assert!(!control.accepting_inbound(), "new inbound calls are refused while orphaned");
        // Reconnect within the hold.
        let (_rx2, _c2) = session(&control, "b");
        control.fence("b", 1);
        assert!(control.orphan_expired(hold.generation).is_empty(), "stale timer is ignored");
        assert_eq!(control.active_pbx_call_ids(), vec!["pbx-1".to_string()]);
    }

    #[test]
    fn orphan_expiry_marks_calls_for_hangup_with_reason() {
        let control = VboxControl::default();
        let (_rx, _c) = session(&control, "a");
        control.fence("a", 1);
        control.bind_inbound("pbx-1", "call-1", "bind-1", 1).unwrap();
        let (close_tx, _close_rx) = mpsc::channel(1);
        control.media_attached("pbx-1", "media-1", close_tx);
        let hold = control.session_closed("a").unwrap();
        let expired = control.orphan_expired(hold.generation);
        assert_eq!(expired.len(), 1);
        assert!(expired[0].media_close.is_some());
        control.call_finished("pbx-1");
        let t = control.tombstone_for("pbx-1").unwrap();
        assert_eq!(t.hangup_reason, "rwi_orphan_hold_expired");
    }

    #[test]
    fn tombstones_are_bounded_by_count() {
        let mut config = VboxContractConfig::default();
        config.max_tombstones = 3;
        let control = VboxControl::new(config);
        let (_rx, _c) = session(&control, "a");
        control.fence("a", 1);
        for i in 0..5 {
            let pbx = format!("pbx-{i}");
            control.bind_inbound(&pbx, &format!("call-{i}"), &format!("b-{i}"), 1).unwrap();
            control.call_finished(&pbx);
        }
        let snap = control.snapshot("s", None, 0);
        let tombs = snap["terminal_tombstones"].as_array().unwrap();
        assert_eq!(tombs.len(), 3);
        assert_eq!(tombs[0]["pbx_call_id"], "pbx-2");
        assert_eq!(snap["terminated_cursor"], 5);
        assert_eq!(snap["tombstone_window_covers"], false, "evicted tombstones void coverage");
        assert_eq!(snap["tombstone_retention_ms"], 3_600_000);
        assert!(snap["snapshot_at_ms"].as_i64().unwrap() > 0);
        // Cursor filtering returns only newer tombstones.
        let newer = control.snapshot("s", None, 4);
        assert_eq!(newer["terminal_tombstones"].as_array().unwrap().len(), 1);
    }

    #[test]
    fn media_connect_waits_for_answer_and_reattach_supersedes() {
        let mut config = VboxContractConfig::default();
        config.media_base_url = Some("ws://vbox.test:4000/".into());
        let control = VboxControl::new(config);
        let (_rx, _c) = session(&control, "a");
        control.fence("a", 1);
        control.bind_inbound("pbx-1", "call-1", "bind-1", 1).unwrap();
        let req = MediaRequest { media_session_id: "m1".into(), path: "/ws/rustpbx/call-1?token=t1".into() };
        assert!(matches!(control.request_media("pbx-1", req), Ok(None)), "not answered yet");
        let connect = control.observe(&native("call_answered", "pbx-1", json!({}))).expect("connect on answer");
        assert_eq!(connect.start.url, "ws://vbox.test:4000/ws/rustpbx/call-1?token=t1");
        assert_eq!(connect.start.media_session_id, "m1");
        assert!(connect.supersede.is_none());
        let (close_tx, _close_rx) = mpsc::channel(1);
        control.media_attached("pbx-1", "m1", close_tx);
        // Same media session again is not reconnected.
        let same = MediaRequest { media_session_id: "m1".into(), path: "/ws/rustpbx/call-1?token=t1".into() };
        assert!(matches!(control.request_media("pbx-1", same), Ok(None)));
        // Reattach with a new media session closes the old one.
        let next = MediaRequest { media_session_id: "m2".into(), path: "/ws/rustpbx/call-1?token=t2".into() };
        let connect = control.request_media("pbx-1", next).unwrap().expect("reattach connect");
        assert!(connect.supersede.is_some());
        // The superseded socket closing leaves the pending reattach in place.
        control.media_detached("pbx-1", "m1");
        assert!(control.media_close_handle("pbx-1").is_none());
        let again = MediaRequest { media_session_id: "m2".into(), path: "/ws/rustpbx/call-1?token=t2".into() };
        assert!(matches!(control.request_media("pbx-1", again), Ok(None)), "m2 connect already requested");
    }

    #[test]
    fn sip_user_extracts_the_dialled_number() {
        assert_eq!(sip_user("sip:vboxline@127.0.0.1:5060"), "vboxline");
        assert_eq!(sip_user("\"Bob\" <sip:+15550100@carrier.example;user=phone>"), "+15550100");
        assert_eq!(sip_user("tel:+15550199"), "+15550199");
        assert_eq!(sip_user("+15550199"), "+15550199");
        assert_eq!(sip_user("sips:1001@pbx;transport=tls"), "1001");
    }

    #[test]
    fn capabilities_are_derived_from_implemented_set() {
        // Without a media endpoint the contract cannot be served.
        let control = VboxControl::default();
        let caps = control.capabilities();
        assert_eq!(caps["contract"], "pbx_command_v1");
        assert_eq!(caps["integration_ready"], false);
        assert_eq!(caps["media_configured"], false);
        assert_eq!(caps["missing_capabilities"], json!([]));
        assert_eq!(caps["supported_operations"].as_array().unwrap().len(), OPERATIONS.len());
        let mut config = VboxContractConfig::default();
        config.media_base_url = Some("wss://vbox.internal:4000".into());
        let ready = VboxControl::new(config).capabilities();
        assert_eq!(ready["integration_ready"], true);
        for required in REQUIRED_CAPABILITIES {
            assert!(ready["capabilities"].as_array().unwrap().iter().any(|c| c == required), "{required}");
        }
    }
}
