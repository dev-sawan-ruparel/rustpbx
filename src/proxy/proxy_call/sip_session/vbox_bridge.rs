//! Session wiring for vbox media (`/ws/rustpbx/:call_id`).
//!
//! The protocol core lives in [`crate::call::vbox_media`]; this module
//! connects it to the call: the caller leg's decoded PCM feeds the uplink
//! (resampled to 16 kHz), the leg egress plays the generation-aware queue
//! through [`VboxPlayoutSource`], caller DTMF is relayed, and the attachment
//! outcome comes back as `CallCommand::VboxMediaClosed`.

use super::session::SipSession;
use crate::call::domain::{CallCommand, HangupCascade, HangupCommand, HangupInitiator, LegId};
use crate::call::vbox_media::{
    CloseRequest, MediaIo, MediaLimits, MediaOutput, Playout, SAMPLE_RATE, SAMPLES_PER_20MS,
    SharedPlayout, VboxMediaStart, VboxPlayoutSource, run_media_session, MAX_MEDIA_MESSAGE_BYTES,
};
use crate::proxy::proxy_call::media_state::{ExternalBridgeHandle, ExternalBridgeKind};
use std::sync::Arc;
use std::time::Duration;
use tokio::sync::mpsc;
use tracing::{info, warn};

impl SipSession {
    fn vbox_control(&self) -> Option<Arc<crate::rwi::vbox::VboxControl>> {
        self.server
            .rwi_gateway
            .as_ref()
            .map(|gw| gw.read().vbox.clone())
    }

    /// A vbox-bound call's caller leg was answered by command. Upstream emits
    /// no session `call_answered` while an app holds the call, so tell the
    /// vbox controller state directly (no-op for calls vbox does not own).
    pub(crate) fn notify_vbox_caller_answered(&self) {
        if let Some(control) = self.vbox_control()
            && let Some(connect) = control.mark_answered(&self.context.session_id)
        {
            control.dispatch_media_connect(connect);
        }
    }

    /// `CallCommand::VboxMediaAttach` — connect the caller leg to vbox media.
    /// A re-attach (new `media_session_id`) replaces the playout source; the
    /// previous socket was already asked to close by the controller state.
    pub(crate) async fn handle_vbox_media_attach(&mut self, start: VboxMediaStart) -> anyhow::Result<()> {
        let control = self
            .vbox_control()
            .ok_or_else(|| anyhow::anyhow!("vbox media: RWI gateway not configured"))?;
        let leg = self
            .media_leg(&LegId::from("caller"))
            .ok_or_else(|| anyhow::anyhow!("vbox media: no caller media leg"))?;

        let config = tokio_tungstenite::tungstenite::protocol::WebSocketConfig::default()
            .max_message_size(Some(MAX_MEDIA_MESSAGE_BYTES))
            .max_frame_size(Some(MAX_MEDIA_MESSAGE_BYTES));
        let connect = tokio_tungstenite::connect_async_with_config(start.url.as_str(), Some(config), false);
        let ws = match tokio::time::timeout(Duration::from_millis(start.connect_timeout_ms), connect).await {
            Ok(Ok((ws, _))) => ws,
            Ok(Err(e)) => {
                control.media_detached(&start.pbx_call_id, &start.media_session_id);
                return Err(anyhow::anyhow!("vbox media: connect failed: {e}"));
            }
            Err(_) => {
                control.media_detached(&start.pbx_call_id, &start.media_session_id);
                return Err(anyhow::anyhow!("vbox media: connect timed out"));
            }
        };

        // One cancel scope per attachment so a superseded attachment can end
        // without touching its successor.
        let cancel = self.cancel_token.child_token();
        let playout: SharedPlayout = Arc::new(parking_lot::Mutex::new(Playout::new(start.max_buffer_ms)));
        let (playout_events_tx, playout_events) = mpsc::unbounded_channel();
        leg.play(
            Box::new(VboxPlayoutSource::new(playout.clone(), playout_events_tx)),
            false,
            None,
        )
        .await?;

        // ── Uplink: caller PCM → 16 kHz 20 ms frames. Never blocks RTP: a
        //    full uplink queue drops the frame. ────────────────────────────
        let (uplink_tx, uplink) = mpsc::channel::<Vec<i16>>(50);
        match leg.pcm_stream(cancel.child_token()) {
            Ok(mut stream) => {
                let pump_cancel = cancel.clone();
                crate::utils::spawn(async move {
                    let mut pending: Vec<i16> = Vec::with_capacity(SAMPLES_PER_20MS * 2);
                    loop {
                        let frame = tokio::select! {
                            _ = pump_cancel.cancelled() => break,
                            frame = stream.recv() => match frame {
                                Some(f) => f,
                                None => break,
                            },
                        };
                        let rate = frame.frame.sample_rate;
                        if rate == SAMPLE_RATE {
                            pending.extend_from_slice(&frame.frame.samples);
                        } else {
                            pending.extend(crate::call::runtime::conference_media_bridge::resample_linear(
                                &frame.frame.samples,
                                rate,
                                SAMPLE_RATE,
                            ));
                        }
                        while pending.len() >= SAMPLES_PER_20MS {
                            let chunk: Vec<i16> = pending.drain(..SAMPLES_PER_20MS).collect();
                            let _ = uplink_tx.try_send(chunk);
                        }
                    }
                });
            }
            Err(e) => warn!(session_id = %self.id, error = %e, "vbox media: caller PCM tap unavailable"),
        }

        // ── Caller DTMF (session-level, de-duplicated) ────────────────────
        let (dtmf_json_tx, mut dtmf_json_rx) = mpsc::unbounded_channel::<String>();
        let (caller_dtmf_tx, caller_dtmf) = mpsc::unbounded_channel::<String>();
        *self.bridge_dtmf_tx.write() = Some(dtmf_json_tx);
        {
            let relay_cancel = cancel.clone();
            crate::utils::spawn(async move {
                loop {
                    let json = tokio::select! {
                        _ = relay_cancel.cancelled() => break,
                        json = dtmf_json_rx.recv() => match json { Some(j) => j, None => break },
                    };
                    if let Ok(v) = serde_json::from_str::<serde_json::Value>(&json)
                        && let Some(d) = v.get("digit").and_then(|d| d.as_str())
                        && caller_dtmf_tx.send(d.to_string()).is_err()
                    {
                        break;
                    }
                }
            });
        }

        let (close_tx, close_requests) = mpsc::channel::<CloseRequest>(4);
        let (outputs_tx, mut outputs) = mpsc::unbounded_channel::<MediaOutput>();
        control.media_attached(&start.pbx_call_id, &start.media_session_id, close_tx);

        let io = MediaIo {
            playout,
            playout_events,
            uplink,
            caller_dtmf,
            close_requests,
            outputs: outputs_tx,
            cancel: cancel.clone(),
        };
        crate::utils::spawn(run_media_session(ws, start.attachment(), io, MediaLimits::default()));

        // ── Outcomes back into the session loop ───────────────────────────
        {
            let cmd_tx = self.cmd_tx.clone();
            let media_session_id = start.media_session_id.clone();
            let consumer_cancel = cancel.clone();
            crate::utils::spawn(async move {
                while let Some(output) = outputs.recv().await {
                    match output {
                        MediaOutput::Ready => {}
                        MediaOutput::SendDtmf { digits } => {
                            if let Some(tx) = &cmd_tx {
                                let _ = tx
                                    .send(CallCommand::SendDtmf {
                                        leg_id: LegId::from("caller"),
                                        digits,
                                    })
                                    .await;
                            }
                        }
                        end @ (MediaOutput::AgentHangup { .. } | MediaOutput::Closed { .. }) => {
                            consumer_cancel.cancel();
                            if let Some(tx) = &cmd_tx {
                                let _ = tx
                                    .send(CallCommand::VboxMediaClosed {
                                        media_session_id: media_session_id.clone(),
                                        outcome: end,
                                    })
                                    .await;
                            }
                            break;
                        }
                    }
                }
            });
        }

        self.external_bridge = Some(ExternalBridgeHandle {
            cancel,
            kind: ExternalBridgeKind::Vbox,
            hangup_on_disconnect: false,
        });
        info!(
            session_id = %self.id,
            media_session_id = %start.media_session_id,
            "vbox media: attached"
        );
        // The call is now driven by the agent; keep RTP watchdogs normal.
        self.meta.transfer_in_progress = false;
        self.sync_rtp_timeout_pause();
        Ok(())
    }

    /// `CallCommand::VboxMediaClosed` — an attachment ended. A `4000` close
    /// hangs the SIP leg up immediately (agent hangup even without RWI).
    pub(crate) async fn handle_vbox_media_closed(&mut self, media_session_id: String, outcome: MediaOutput) {
        let Some(control) = self.vbox_control() else {
            return;
        };
        let pbx_call_id = self.context.session_id.clone();
        let current = control.is_active_media(&pbx_call_id, &media_session_id);
        control.media_detached(&pbx_call_id, &media_session_id);
        info!(session_id = %self.id, %media_session_id, ?outcome, current, "vbox media: attachment ended");
        if current && self.external_bridge.as_ref().is_some_and(|h| h.kind == ExternalBridgeKind::Vbox) {
            self.teardown_external_bridge("vbox media closed");
            *self.bridge_dtmf_tx.write() = None;
            if let Some(leg) = self.media_leg(&LegId::from("caller")) {
                let _ = leg.stop_playback().await;
            }
        }
        if let MediaOutput::AgentHangup { reason } = outcome {
            control.set_terminal_reason(&pbx_call_id, "agent_hangup_ws_4000");
            self.handle_hangup(&HangupCommand {
                leg_id: None,
                cascade: HangupCascade::All,
                initiator: HangupInitiator::System {
                    reason: crate::call::domain::SystemHangupReason::InternalError,
                    details: Some(format!("vbox media closed with 4000: {reason}")),
                },
                reason: Some(crate::callrecord::CallRecordHangupReason::BySystem),
                code: None,
                rtp_timeout_side: None,
            })
            .await;
        }
    }
}
