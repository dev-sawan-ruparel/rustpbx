//! Normalized contract discovery at the real authenticated RWI boundary.
//!
//! Do not translate call commands into upstream actions until all required
//! ownership, reconciliation and media semantics are implemented. Discovery
//! explicitly advertises the missing capabilities so admission remains closed.
use serde_json::{Value, json};

pub(super) fn reply(value: &Value) -> Option<Value> {
    if value.get("operation").is_none()
        && value.get("schema_version").and_then(Value::as_str) != Some("pbx_command_v1")
    {
        return None;
    }
    let command_id = value
        .get("command_id")
        .and_then(Value::as_str)
        .unwrap_or("");
    let operation = value.get("operation").and_then(Value::as_str).unwrap_or("");
    let error = if command_id.is_empty() {
        Some("missing_command_id")
    } else if value.get("schema_version").and_then(Value::as_str) != Some("pbx_command_v1") {
        Some("unsupported_schema_version")
    } else if value.get("type").and_then(Value::as_str) != Some("command") {
        Some("invalid_command_type")
    } else if operation != "capabilities" {
        Some("unsupported_contract_operation")
    } else {
        None
    };
    Some(match error {
        Some(code) => json!({
            "type": "command_error",
            "schema_version": "pbx_command_v1",
            "command_id": command_id,
            "operation": operation,
            "error_code": code,
        }),
        None => json!({
            "type": "command_ack",
            "schema_version": "pbx_command_v1",
            "command_id": command_id,
            "operation": "capabilities",
            "result": {
                "contract": "pbx_command_v1",
                "integration_ready": false,
                "capabilities": ["capabilities"],
                "supported_operations": ["capabilities"],
                "missing_capabilities": ["owner_epoch_fencing", "list_owned_calls",
                    "terminal_tombstones", "generation_u64_pcm16le"],
                "native_contract": "rwi-v1-action-params",
            },
        }),
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn normalized_capabilities_are_correlated_and_truthful() {
        let response = reply(&json!({"type":"command", "schema_version":"pbx_command_v1",
            "operation":"capabilities", "command_id":"command-42"}))
        .unwrap();
        assert_eq!(response["type"], "command_ack");
        assert_eq!(response["command_id"], "command-42");
        assert_eq!(response["result"]["contract"], "pbx_command_v1");
        assert_eq!(response["result"]["integration_ready"], false);
        assert_eq!(response["result"]["capabilities"], json!(["capabilities"]));
        assert_eq!(
            response["result"]["missing_capabilities"]
                .as_array()
                .unwrap()
                .len(),
            4
        );
    }

    #[test]
    fn call_commands_are_rejected_instead_of_partially_executed() {
        for operation in [
            "originate",
            "answer",
            "claim_call",
            "hangup",
            "list_owned_calls",
        ] {
            let response = reply(&json!({"type":"command", "schema_version":"pbx_command_v1",
                "operation":operation, "command_id":"call-command"}))
            .unwrap();
            assert_eq!(response["type"], "command_error");
            assert_eq!(response["command_id"], "call-command");
            assert_eq!(response["error_code"], "unsupported_contract_operation");
        }
    }

    #[test]
    fn invalid_normalized_commands_cannot_fall_through_to_native_processor() {
        assert_eq!(
            reply(&json!({"operation":"capabilities", "command_id":"x"})).unwrap()["error_code"],
            "unsupported_schema_version"
        );
        assert_eq!(
            reply(&json!({"schema_version":"pbx_command_v1", "operation":"capabilities"})).unwrap()
                ["error_code"],
            "missing_command_id"
        );
    }

    #[test]
    fn native_commands_still_reach_native_processor() {
        assert_eq!(
            reply(&json!({"action":"session.list_calls", "action_id":"native-1"})),
            None
        );
    }
}
