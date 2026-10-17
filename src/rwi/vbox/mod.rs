//! vbox call-control contract (`pbx_command_v1`) at the authenticated RWI
//! boundary.
//!
//! A vbox controller speaks normalized `operation`/`payload` commands; this
//! module translates them into native RWI actions and native call events back
//! into normalized events, and adds what the native protocol lacks:
//!
//! - receiver-enforced monotonic `owner_epoch` fencing (`I10`),
//! - a PBX boot identity (`pbx_boot_epoch`) and bounded terminal call
//!   tombstones returned by `get_call` / `list_owned_calls` (`I8`),
//! - an RWI orphan hold: established calls survive a controller disconnect
//!   for `orphan_hold_secs`, new inbound calls are rejected while no
//!   controller is connected, and on expiry the calls are hung up and their
//!   media sockets closed with `4001` (`I7`),
//! - media attachment to `/ws/rustpbx/:call_id` with the generation-prefixed
//!   framing in [`crate::call::vbox_media`].
//!
//! Native RWI clients are unaffected: a frame without `schema_version` /
//! `operation` still goes to the native processor.

pub mod command;
pub mod control;

pub use control::VboxControl;

use serde::{Deserialize, Serialize};

pub const CONTRACT: &str = "pbx_command_v1";
pub const MEDIA_FRAMING: &str = "generation_u64_pcm16le";

/// Operations a vbox controller may send.
pub const OPERATIONS: &[&str] = &[
    "claim_call",
    "originate",
    "answer",
    "reject",
    "attach_media",
    "hangup",
    "transfer",
    "get_call",
    "list_owned_calls",
    "capabilities",
    "health",
];

/// Capabilities vbox requires before it opens admission. Each one is listed
/// in [`IMPLEMENTED_CAPABILITIES`] only when this build implements it and a
/// test exercises it; `integration_ready` is derived, never configured.
pub const REQUIRED_CAPABILITIES: &[&str] = &[
    "owner_epoch_fencing",
    "list_owned_calls",
    "terminal_tombstones",
    "generation_u64_pcm16le",
    "clear",
    "mark",
    "dtmf",
    "close_4000_hangup",
    "close_4001_orphan",
    "media_reattach",
    "non_blocking_clear",
];

pub const IMPLEMENTED_CAPABILITIES: &[&str] = &[
    "capabilities",
    "owner_epoch_fencing",
    "list_owned_calls",
    "terminal_tombstones",
];

/// `[rwi.vbox]` configuration.
#[derive(Debug, Clone, Serialize, Deserialize)]
pub struct VboxContractConfig {
    /// Routing context whose calls are offered to the vbox controller
    /// (`app = "rwi"`, `params.context = <context>`).
    #[serde(default = "default_context")]
    pub context: String,
    /// Base URL of the vbox media endpoint, e.g. `wss://vbox.internal:4000`.
    /// `attach_media` only supplies a path; the PBX never connects to a host
    /// chosen by a command.
    #[serde(default)]
    pub media_base_url: Option<String>,
    #[serde(default)]
    pub pbx_instance_id: Option<String>,
    /// How long established calls survive a controller disconnect.
    #[serde(default = "default_orphan_hold_secs")]
    pub orphan_hold_secs: u64,
    #[serde(default = "default_tombstone_retention_secs")]
    pub tombstone_retention_secs: u64,
    #[serde(default = "default_max_tombstones")]
    pub max_tombstones: usize,
    /// Bound on queued agent audio per call.
    #[serde(default = "default_max_playout_buffer_ms")]
    pub max_playout_buffer_ms: u64,
    /// Trunk used for `originate` when the command names none.
    #[serde(default)]
    pub outbound_trunk: Option<String>,
    /// Host part for `originate` destinations given as bare numbers.
    #[serde(default)]
    pub outbound_domain: Option<String>,
    /// Media WebSocket connect deadline.
    #[serde(default = "default_media_connect_timeout_ms")]
    pub media_connect_timeout_ms: u64,
}

fn default_context() -> String {
    "vbox".to_string()
}
fn default_orphan_hold_secs() -> u64 {
    30
}
fn default_tombstone_retention_secs() -> u64 {
    3600
}
fn default_max_tombstones() -> usize {
    1024
}
fn default_max_playout_buffer_ms() -> u64 {
    10_000
}
fn default_media_connect_timeout_ms() -> u64 {
    3_000
}

impl Default for VboxContractConfig {
    fn default() -> Self {
        Self {
            context: default_context(),
            media_base_url: None,
            pbx_instance_id: None,
            orphan_hold_secs: default_orphan_hold_secs(),
            tombstone_retention_secs: default_tombstone_retention_secs(),
            max_tombstones: default_max_tombstones(),
            max_playout_buffer_ms: default_max_playout_buffer_ms(),
            outbound_trunk: None,
            outbound_domain: None,
            media_connect_timeout_ms: default_media_connect_timeout_ms(),
        }
    }
}

/// True when a frame is addressed to the normalized contract (and must never
/// fall through to the native action dispatcher).
pub fn is_contract_frame(value: &serde_json::Value) -> bool {
    value.get("operation").is_some() || value.get("schema_version").is_some()
}
