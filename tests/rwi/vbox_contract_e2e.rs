//! E2E: vbox `pbx_command_v1` controller and `/ws/rustpbx/:call_id` media
//! against the real SIP stack, real RTP and real WebSockets.
//!
//! The PBX side is production code: `rwi_ws_handler`, the SIP server with
//! the RWI gateway wired in, the `rwi` routing app, the MediaBridge leg and
//! its RTP egress. The test plays vbox (an RWI WebSocket client and a media
//! WebSocket server) and a SIP caller that sends a µ-law tone over RTP and
//! timestamps what it receives.
//!
//! Run: `cargo test --test e2e_core -- vbox_contract`

use crate::common::audio_analysis::{goertzel_power, mulaw_payload_to_samples, rms};
use crate::common::e2e_test_server::{E2eTestServer, E2eTestServerInject};
use crate::common::rtp_utils::{RtpPacket, extract_media_endpoint};
use crate::common::test_helpers::make_sdp;
use crate::common::test_ua::{TestUa, TestUaEvent};
use axum::{
    Extension,
    extract::{Query, ws::WebSocketUpgrade},
    http::HeaderMap,
    routing::get,
};
use futures::{SinkExt, StreamExt};
use rsipstack::dialog::DialogId;
use rustpbx::call::vbox_media::{encode_generation_frame, SAMPLES_PER_20MS};
use rustpbx::config::{MediaProxyMode, ProxyConfig};
use rustpbx::proxy::routing::{MatchConditions, RouteAction, RouteRule};
use rustpbx::rwi::vbox::VboxContractConfig;
use rustpbx::rwi::{
    RwiAuth, RwiAuthRef, RwiGateway, RwiGatewayRef,
    auth::{RwiConfig, RwiTokenConfig},
    handler::rwi_ws_handler,
};
use serde_json::{Value, json};
use std::collections::HashMap;
use std::net::SocketAddr;
use std::sync::Arc;
use std::time::{Duration, Instant};
use tokio::net::{TcpListener, TcpStream, UdpSocket};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::client::IntoClientRequest;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::{MaybeTlsStream, WebSocketStream};

const TOKEN: &str = "vbox-e2e-token";
const VBOX_DID: &str = "vboxline";
const CALLER_TONE_HZ: f64 = 700.0;
const AGENT_TONE_HZ: f64 = 1000.0;

// ── Harness ────────────────────────────────────────────────────────────────

struct Harness {
    server: E2eTestServer,
    gateway: RwiGatewayRef,
    rwi_url: String,
    media_rx: mpsc::UnboundedReceiver<(String, WebSocketStream<TcpStream>)>,
    http_cancel: tokio_util::sync::CancellationToken,
}

impl Drop for Harness {
    fn drop(&mut self) {
        self.http_cancel.cancel();
    }
}

async fn harness(orphan_hold_secs: u64) -> Harness {
    harness_with_modules(orphan_hold_secs, Vec::new()).await
}
async fn harness_with_modules(orphan_hold_secs: u64, transaction_modules: Vec<(String, rustpbx::proxy::FnCreateProxyModule)>) -> Harness {
    harness_with(orphan_hold_secs, transaction_modules, false).await
}
/// `carrier`: also listen on SIP/TCP and treat 127.0.0.1 as a trunk (no
/// digest auth), so a raw SIP carrier can call in like Telnyx does.
async fn harness_with(
    orphan_hold_secs: u64,
    transaction_modules: Vec<(String, rustpbx::proxy::FnCreateProxyModule)>,
    carrier: bool,
) -> Harness {
    let _ = tracing_subscriber::fmt::try_init();
    // Fake vbox media endpoint: records the request path of each socket.
    let media_listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let media_addr = media_listener.local_addr().unwrap();
    let (media_tx, media_rx) = mpsc::unbounded_channel();
    tokio::spawn(async move {
        loop {
            let Ok((stream, _)) = media_listener.accept().await else { break };
            let tx = media_tx.clone();
            tokio::spawn(async move {
                let path = Arc::new(parking_lot::Mutex::new(String::new()));
                let path_cb = path.clone();
                let ws = tokio_tungstenite::accept_hdr_async(
                    stream,
                    move |req: &tokio_tungstenite::tungstenite::handshake::server::Request,
                          resp: tokio_tungstenite::tungstenite::handshake::server::Response| {
                        *path_cb.lock() = req.uri().to_string();
                        Ok(resp)
                    },
                )
                .await
                .unwrap();
                let p = path.lock().clone();
                let _ = tx.send((p, ws));
            });
        }
    });

    let gateway: RwiGatewayRef = Arc::new(parking_lot::RwLock::new(RwiGateway::new()));
    gateway.read().vbox.set_config(VboxContractConfig {
        media_base_url: Some(format!("ws://{media_addr}")),
        pbx_instance_id: Some("pbx-e2e".into()),
        orphan_hold_secs,
        ..Default::default()
    });

    let mut config = ProxyConfig {
        media_proxy: MediaProxyMode::All,
        routes: Some(vec![RouteRule {
            name: "vbox_inbound".to_string(),
            priority: 10,
            match_conditions: MatchConditions {
                to_user: Some(VBOX_DID.to_string()),
                ..Default::default()
            },
            action: RouteAction {
                app: Some("rwi".to_string()),
                app_params: Some(json!({"context": "vbox"})),
                auto_answer: false,
                ..Default::default()
            },
            ..Default::default()
        }]),
        ..Default::default()
    };
    if carrier {
        config.tcp_port = Some(0);
        config.trunks.insert(
            "carrier".to_string(),
            rustpbx::proxy::routing::TrunkConfig {
                dest: "sip:127.0.0.1:5060".to_string(),
                inbound_hosts: vec!["127.0.0.1".to_string()],
                ..Default::default()
            },
        );
    }
    let server = E2eTestServer::start_with_inject(
        config,
        E2eTestServerInject {
            rwi_gateway: Some(gateway.clone()),
            transaction_modules,
            ..Default::default()
        },
    )
    .await
    .expect("PBX starts");

    let auth: RwiAuthRef = Arc::new(tokio::sync::RwLock::new(RwiAuth::new(&RwiConfig {
        tokens: vec![RwiTokenConfig {
            token: TOKEN.to_string(),
            scopes: vec!["call.control".to_string()],
        }],
        ..Default::default()
    })));
    let (a, g, r, s) = (
        auth.clone(),
        gateway.clone(),
        server.registry.clone(),
        Some(server.server_ref.clone()),
    );
    let router = axum::Router::new().route(
        "/rwi/v1",
        get(
            move |client_addr: rustpbx::handler::middleware::clientaddr::ClientAddr,
                  ws: WebSocketUpgrade,
                  Query(params): Query<HashMap<String, String>>,
                  headers: HeaderMap| {
                let (a, g, r, s) = (a.clone(), g.clone(), r.clone(), s.clone());
                async move {
                    rwi_ws_handler(client_addr, ws, Query(params), Extension(a), Extension(g), Extension(r), Extension(s), headers)
                        .await
                }
            },
        ),
    );
    let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let http_port = listener.local_addr().unwrap().port();
    let http_cancel = tokio_util::sync::CancellationToken::new();
    let ct = http_cancel.clone();
    tokio::spawn(async move {
        axum::serve(listener, router.into_make_service_with_connect_info::<SocketAddr>())
            .with_graceful_shutdown(async move { ct.cancelled().await })
            .await
            .unwrap();
    });
    Harness {
        server,
        gateway,
        rwi_url: format!("ws://127.0.0.1:{http_port}/rwi/v1"),
        media_rx,
        http_cancel,
    }
}

impl Harness {
    async fn next_media(&mut self) -> (String, WebSocketStream<TcpStream>) {
        tokio::time::timeout(Duration::from_secs(5), self.media_rx.recv())
            .await
            .expect("PBX connects vbox media in time")
            .expect("media server alive")
    }
}

// ── vbox controller (RWI client) ────────────────────────────────────────────

struct Controller {
    ws: WebSocketStream<MaybeTlsStream<TcpStream>>,
    events: Vec<Value>,
}

impl Controller {
    async fn connect(url: &str) -> Self {
        let mut req = url.into_client_request().unwrap();
        req.headers_mut()
            .insert("authorization", format!("Bearer {TOKEN}").parse().unwrap());
        req.headers_mut()
            .insert("sec-websocket-protocol", "rwi-v1".parse().unwrap());
        let (ws, _) = tokio_tungstenite::connect_async(req).await.expect("RWI connects");
        Self { ws, events: Vec::new() }
    }

    async fn read_frame(&mut self, timeout: Duration) -> Option<Value> {
        loop {
            let msg = tokio::time::timeout(timeout, self.ws.next()).await.ok()??;
            match msg {
                Ok(Message::Text(t)) => return serde_json::from_str(&t).ok(),
                Ok(Message::Close(_)) | Err(_) => return None,
                _ => continue,
            }
        }
    }

    async fn command(&mut self, operation: &str, owner_epoch: u64, fields: Value) -> Value {
        let command_id = format!("cmd-{}", uuid::Uuid::new_v4().simple());
        let mut frame = json!({
            "rwi_version": "1.0",
            "type": "command",
            "schema_version": "pbx_command_v1",
            "command_id": command_id,
            "operation": operation,
            "owner_epoch": owner_epoch,
            "deadline_at": chrono::Utc::now().timestamp_millis() + 5000,
        });
        if let Value::Object(extra) = fields {
            for (k, v) in extra {
                frame[k] = v;
            }
        }
        self.send(frame).await;
        self.reply_for(&command_id).await
    }

    async fn send(&mut self, frame: Value) {
        self.ws.send(Message::Text(frame.to_string().into())).await.unwrap();
    }

    async fn reply_for(&mut self, command_id: &str) -> Value {
        let deadline = Instant::now() + Duration::from_secs(10);
        while Instant::now() < deadline {
            let frame = self
                .read_frame(Duration::from_secs(10))
                .await
                .expect("reply before the socket closes");
            let kind = frame["type"].as_str().unwrap_or("");
            let is_reply = matches!(kind, "command_ack" | "command_error" | "snapshot_response");
            if is_reply && frame["command_id"] == command_id {
                return frame;
            }
            self.events.push(frame);
        }
        panic!("no reply for {command_id}");
    }

    async fn event(&mut self, kind: &str, timeout: Duration) -> Value {
        if let Some(i) = self.events.iter().position(|e| e["type"] == kind) {
            return self.events.remove(i);
        }
        let deadline = Instant::now() + timeout;
        loop {
            let left = deadline.saturating_duration_since(Instant::now());
            let frame = self
                .read_frame(left)
                .await
                .unwrap_or_else(|| panic!("event {kind} not received; buffered: {:?}", self.events));
            if frame["type"] == kind {
                return frame;
            }
            self.events.push(frame);
        }
    }

    /// True once the PBX has closed this controller's socket.
    async fn closed_within(&mut self, timeout: Duration) -> bool {
        let deadline = Instant::now() + timeout;
        while Instant::now() < deadline {
            match tokio::time::timeout(Duration::from_millis(200), self.ws.next()).await {
                Ok(None) | Ok(Some(Err(_))) | Ok(Some(Ok(Message::Close(_)))) => return true,
                Ok(Some(Ok(Message::Text(t)))) => {
                    if let Ok(v) = serde_json::from_str(&t) {
                        self.events.push(v);
                    }
                }
                _ => {}
            }
        }
        false
    }
}

// ── Caller RTP: µ-law tone out, timestamped capture in ─────────────────────

fn linear_to_mulaw(sample: i16) -> u8 {
    const BIAS: i32 = 0x84;
    const CLIP: i32 = 32635;
    let mut s = sample as i32;
    let sign = if s < 0 { 0x80 } else { 0 };
    if s < 0 {
        s = -s;
    }
    s = s.min(CLIP) + BIAS;
    let mut exponent = 7;
    let mut mask = 0x4000;
    while exponent > 0 && s & mask == 0 {
        exponent -= 1;
        mask >>= 1;
    }
    let mantissa = (s >> (exponent + 3)) & 0x0F;
    !(sign | (exponent << 4) | mantissa) as u8
}

fn tone(freq: f64, rate: u32, samples: usize, offset: usize, amplitude: f64) -> Vec<i16> {
    (0..samples)
        .map(|i| {
            let t = (offset + i) as f64 / rate as f64;
            (amplitude * (2.0 * std::f64::consts::PI * freq * t).sin()) as i16
        })
        .collect()
}

#[derive(Clone)]
struct CallerRtp {
    socket: Arc<UdpSocket>,
    captured: Arc<parking_lot::Mutex<Vec<(Instant, Vec<i16>)>>>,
    telephone_events: Arc<parking_lot::Mutex<Vec<Vec<u8>>>>,
    cancel: tokio_util::sync::CancellationToken,
}

impl CallerRtp {
    async fn bind() -> Self {
        let socket = Arc::new(UdpSocket::bind("127.0.0.1:0").await.unwrap());
        let me = Self {
            socket,
            captured: Arc::new(parking_lot::Mutex::new(Vec::new())),
            telephone_events: Arc::new(parking_lot::Mutex::new(Vec::new())),
            cancel: tokio_util::sync::CancellationToken::new(),
        };
        let (s, c, cancel) = (me.socket.clone(), me.captured.clone(), me.cancel.clone());
        let te = me.telephone_events.clone();
        tokio::spawn(async move {
            let mut buf = vec![0u8; 1500];
            loop {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    r = s.recv_from(&mut buf) => {
                        let Ok((n, _)) = r else { break };
                        if let Ok(p) = RtpPacket::decode(&buf[..n]) {
                            if p.payload_type == 0 {
                                c.lock().push((Instant::now(), mulaw_payload_to_samples(&p.payload)));
                            } else if p.payload_type == 101 {
                                te.lock().push(p.payload.clone());
                            }
                        }
                    }
                }
            }
        });
        me
    }

    fn port(&self) -> u16 {
        self.socket.local_addr().unwrap().port()
    }

    /// Paced 20 ms µ-law frames of a tone (or silence when `freq` is 0).
    fn send_tone(&self, target: SocketAddr, freq: f64, seconds: u64) {
        let socket = self.socket.clone();
        let cancel = self.cancel.clone();
        tokio::spawn(async move {
            let mut seq: u16 = 1;
            let mut ts: u32 = 0;
            let mut offset = 0usize;
            let mut interval = tokio::time::interval(Duration::from_millis(20));
            let end = Instant::now() + Duration::from_secs(seconds);
            while Instant::now() < end {
                tokio::select! {
                    _ = cancel.cancelled() => break,
                    _ = interval.tick() => {}
                }
                let pcm = if freq > 0.0 { tone(freq, 8000, 160, offset, 9000.0) } else { vec![0; 160] };
                offset += 160;
                let payload: Vec<u8> = pcm.iter().map(|s| linear_to_mulaw(*s)).collect();
                let packet = RtpPacket::new(0, seq, ts, 0x1234_5678, payload);
                seq = seq.wrapping_add(1);
                ts = ts.wrapping_add(160);
                let _ = socket.send_to(&packet.encode(), target).await;
            }
        });
    }

    /// Received 20 ms frames carrying `freq` (clearly above silence).
    fn tone_frames(&self, freq: f64) -> Vec<Instant> {
        self.captured
            .lock()
            .iter()
            .filter(|(_, s)| is_tone(s, 8000, freq))
            .map(|(t, _)| *t)
            .collect()
    }
}

impl Drop for CallerRtp {
    fn drop(&mut self) {
        if Arc::strong_count(&self.socket) <= 2 {
            self.cancel.cancel();
        }
    }
}

fn is_tone(samples: &[i16], rate: u32, freq: f64) -> bool {
    if samples.is_empty() || rms(samples) < 800.0 {
        return false;
    }
    let target = goertzel_power(samples, rate, freq);
    let other = goertzel_power(samples, rate, if freq > 850.0 { 500.0 } else { 1300.0 });
    target > other * 8.0
}

fn pcm16le_to_samples(bytes: &[u8]) -> Vec<i16> {
    bytes.chunks_exact(2).map(|b| i16::from_le_bytes([b[0], b[1]])).collect()
}

// ── Fake vbox media helpers ────────────────────────────────────────────────

async fn media_text(ws: &mut WebSocketStream<TcpStream>, kind: &str, timeout: Duration) -> Value {
    let deadline = Instant::now() + timeout;
    loop {
        let left = deadline.saturating_duration_since(Instant::now());
        let msg = tokio::time::timeout(left, ws.next())
            .await
            .unwrap_or_else(|_| panic!("media control {kind} not received"))
            .expect("media socket open")
            .expect("valid frame");
        if let Message::Text(t) = msg {
            let v: Value = serde_json::from_str(&t).unwrap();
            if v["type"] == kind {
                return v;
            }
        }
    }
}

async fn send_agent_audio(ws: &mut WebSocketStream<TcpStream>, generation: u64, freq: f64, frames: usize) {
    for i in 0..frames {
        let pcm = tone(freq, 16_000, SAMPLES_PER_20MS, i * SAMPLES_PER_20MS, 9000.0);
        ws.send(Message::Binary(encode_generation_frame(generation, &pcm).into()))
            .await
            .unwrap();
    }
}

async fn caller_dialog(
    caller: tokio::task::JoinHandle<anyhow::Result<DialogId>>,
) -> DialogId {
    tokio::time::timeout(Duration::from_secs(10), caller)
        .await
        .expect("call answered in time")
        .expect("caller task")
        .expect("call established")
}

async fn wait_for_ua_event<F: Fn(&TestUaEvent) -> bool>(ua: &TestUa, timeout: Duration, pred: F) -> Option<TestUaEvent> {
    let deadline = Instant::now() + timeout;
    while Instant::now() < deadline {
        for e in ua.process_dialog_events().await.unwrap_or_default() {
            if pred(&e) {
                return Some(e);
            }
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    None
}

/// Claim, attach and answer an offered inbound call; returns (pbx_call_id,
/// caller dialog, PBX RTP address, media socket).
async fn establish_inbound(
    h: &mut Harness,
    ctl: &mut Controller,
    alice: &Arc<TestUa>,
    rtp: &CallerRtp,
    epoch: u64,
    call_id: &str,
    media_session_id: &str,
) -> (String, DialogId, SocketAddr, WebSocketStream<TcpStream>) {
    let caller = tokio::spawn({
        let ua = alice.clone();
        let sdp = make_sdp(rtp.port());
        async move { ua.make_call(VBOX_DID, Some(sdp)).await }
    });
    let offered = ctl.event("incoming_call", Duration::from_secs(5)).await;
    let pbx_call_id = offered["pbx_call_id"].as_str().unwrap().to_string();
    assert_eq!(offered["to"], VBOX_DID);
    assert_eq!(offered["pbx_instance_id"], "pbx-e2e");
    let binding_id = format!("bind-{call_id}");
    let claim = ctl
        .command("claim_call", epoch, json!({"call_id": call_id, "binding_id": binding_id, "payload": {"pbx_call_id": pbx_call_id}}))
        .await;
    assert_eq!(claim["type"], "command_ack", "{claim}");
    let token = format!("tok-{media_session_id}");
    let attach = ctl
        .command(
            "attach_media",
            epoch,
            json!({"call_id": call_id, "binding_id": binding_id, "payload": {
                "pbx_call_id": pbx_call_id,
                "media_session_id": media_session_id,
                "ws_url": format!("/ws/rustpbx/{call_id}?token={token}"),
                "token": token,
                "outbound_framing": "generation_u64_pcm16le",
            }}),
        )
        .await;
    assert_eq!(attach["type"], "command_ack", "{attach}");
    assert_eq!(attach["result"]["media_state"], "pending_answer");
    let answer = ctl
        .command("answer", epoch, json!({"call_id": call_id, "binding_id": binding_id, "payload": {"pbx_call_id": pbx_call_id}}))
        .await;
    assert_eq!(answer["type"], "command_ack", "{answer}");
    let answered = ctl.event("answered", Duration::from_secs(5)).await;
    assert_eq!(answered["call_id"], call_id);
    assert_eq!(answered["binding_id"], binding_id.as_str());
    let dialog = caller_dialog(caller).await;
    let answer_sdp = alice.get_negotiated_answer_sdp(&dialog).await.expect("answer SDP");
    let pbx_rtp = extract_media_endpoint(&answer_sdp).expect("PBX media address");

    let (path, mut media) = h.next_media().await;
    assert_eq!(path, format!("/ws/rustpbx/{call_id}?token={token}"));
    let start = media_text(&mut media, "start", Duration::from_secs(3)).await;
    assert_eq!(start["call_id"], call_id);
    assert_eq!(start["binding_id"], binding_id.as_str());
    assert_eq!(start["media_session_id"], media_session_id);
    assert_eq!(start["pbx_call_id"], pbx_call_id.as_str());
    assert_eq!(start["outbound_framing"], "generation_u64_pcm16le");
    media
        .send(Message::Text(json!({"type": "ready", "media_session_id": media_session_id}).to_string().into()))
        .await
        .unwrap();
    (pbx_call_id, dialog, pbx_rtp, media)
}

// ── Tests ──────────────────────────────────────────────────────────────────

/// Inbound call: discovery, claim, attach, answer, two-way audio with
/// distinguishable tones, mark acknowledged at playout, clear under 4 s of
/// queued audio, caller DTMF, and a `4000` close hanging the SIP leg up.
#[tokio::test]
async fn vbox_contract_inbound_full_duplex_clear_mark_dtmf_and_agent_hangup() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    let caps = ctl.command("capabilities", 0, json!({})).await;
    assert_eq!(caps["type"], "command_ack");
    assert_eq!(caps["result"]["contract"], "pbx_command_v1");
    assert_eq!(caps["result"]["integration_ready"], true, "{caps}");
    assert_eq!(caps["result"]["missing_capabilities"], json!([]));
    let snap = ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    assert_eq!(snap["type"], "snapshot_response");
    assert_eq!(snap["active_calls"], json!([]));
    assert_eq!(snap["pbx_instance_id"], "pbx-e2e");
    assert_eq!(snap["tombstone_window_covers"], true);
    assert!(snap["snapshot_at_ms"].as_i64().unwrap() > 0);

    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, dialog, pbx_rtp, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-in-1", "m1").await;

    // Caller → vbox: raw 640-byte 16 kHz frames carrying the caller tone.
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
    let mut caller_frames = 0;
    let mut caller_tone_frames = 0;
    let deadline = Instant::now() + Duration::from_secs(5);
    while Instant::now() < deadline && caller_tone_frames < 25 {
        match tokio::time::timeout(Duration::from_secs(2), media.next()).await {
            Ok(Some(Ok(Message::Binary(b)))) => {
                assert_eq!(b.len(), 640, "caller frames are 20 ms of 16 kHz PCM16 LE");
                caller_frames += 1;
                if is_tone(&pcm16le_to_samples(&b), 16_000, CALLER_TONE_HZ) {
                    caller_tone_frames += 1;
                }
            }
            Ok(Some(Ok(_))) => {}
            _ => break,
        }
    }
    assert!(caller_tone_frames >= 25, "caller tone reached vbox ({caller_tone_frames}/{caller_frames} frames)");

    // vbox → caller: 2 s of agent tone, then a mark. The mark must not be
    // acknowledged before that audio has played out over RTP.
    let sent = Instant::now();
    send_agent_audio(&mut media, 1, AGENT_TONE_HZ, 100).await;
    media
        .send(Message::Text(json!({"type":"mark","mark_id":"mark-1","generation":1,"turn_id":"turn-1","response_id":"resp-1"}).to_string().into()))
        .await
        .unwrap();
    let played = media_text(&mut media, "mark_played", Duration::from_secs(5)).await;
    let mark_after = sent.elapsed();
    assert_eq!(played["mark_id"], "mark-1");
    assert_eq!(played["turn_id"], "turn-1");
    assert!(
        mark_after >= Duration::from_millis(1800),
        "mark_played after {mark_after:?}: acknowledged before 2 s of audio played"
    );
    let agent_frames = rtp.tone_frames(AGENT_TONE_HZ);
    assert!(agent_frames.len() >= 80, "agent tone reached the caller ({} frames)", agent_frames.len());

    // Barge-in: queue 4 s of generation 2, let it play, then clear(3).
    send_agent_audio(&mut media, 2, AGENT_TONE_HZ, 200).await;
    media
        .send(Message::Text(json!({"type":"mark","mark_id":"mark-2","generation":2,"turn_id":"turn-2"}).to_string().into()))
        .await
        .unwrap();
    tokio::time::sleep(Duration::from_millis(600)).await;
    let clear_at = Instant::now();
    media
        .send(Message::Text(json!({"type":"clear","generation":3,"sent_at_ms":0}).to_string().into()))
        .await
        .unwrap();
    let mark_cleared = media_text(&mut media, "mark_cleared", Duration::from_secs(2)).await;
    assert_eq!(mark_cleared["mark_id"], "mark-2");
    let cleared = media_text(&mut media, "cleared", Duration::from_secs(2)).await;
    let ack_latency = clear_at.elapsed();
    assert_eq!(cleared["generation"], 3);
    let discarded = cleared["discarded_ms"].as_u64().unwrap();
    assert!((3000..=3500).contains(&discarded), "discarded {discarded} ms of the queued 4 s");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    // The check is meaningful only if old-generation audio was reaching the
    // caller right up to the clear.
    let playing_before = rtp
        .tone_frames(AGENT_TONE_HZ)
        .into_iter()
        .filter(|t| *t >= clear_at - Duration::from_millis(500) && *t < clear_at)
        .count();
    assert!(playing_before >= 15, "agent audio was playing before clear ({playing_before} frames in 500 ms)");
    let after_clear = rtp
        .tone_frames(AGENT_TONE_HZ)
        .into_iter()
        .filter(|t| *t >= clear_at)
        .count();
    eprintln!("clear: {playing_before} tone frames in the 500 ms before clear, {after_clear} after");
    let last_old = rtp
        .tone_frames(AGENT_TONE_HZ)
        .into_iter()
        .filter(|t| *t >= clear_at - Duration::from_millis(100))
        .max();
    let caller_stop = last_old.map(|t| t.saturating_duration_since(clear_at)).unwrap_or_default();
    eprintln!("clear: ack {ack_latency:?}, last old-generation audio at caller +{caller_stop:?}");
    assert!(caller_stop < Duration::from_millis(300), "caller still heard cleared audio {caller_stop:?} after clear");
    // Stale generation after clear is not played.
    send_agent_audio(&mut media, 2, AGENT_TONE_HZ, 25).await;
    let before = rtp.tone_frames(AGENT_TONE_HZ).len();
    tokio::time::sleep(Duration::from_millis(700)).await;
    assert_eq!(rtp.tone_frames(AGENT_TONE_HZ).len(), before, "stale generation frames are dropped");

    // Caller DTMF reaches vbox.
    alice.send_dtmf_info(&dialog, "5").await.unwrap();
    let dtmf = media_text(&mut media, "dtmf", Duration::from_secs(3)).await;
    assert_eq!(dtmf["digit"], "5");
    assert!(dtmf["event_id"].as_str().is_some());

    // Agent DTMF goes to the caller as RFC 2833 telephone-events.
    media
        .send(Message::Text(json!({"type": "dtmf", "digit": "7"}).to_string().into()))
        .await
        .unwrap();
    let deadline = Instant::now() + Duration::from_secs(3);
    while Instant::now() < deadline && !rtp.telephone_events.lock().iter().any(|p| p.first() == Some(&7)) {
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    assert!(
        rtp.telephone_events.lock().iter().any(|p| p.first() == Some(&7)),
        "caller received telephone-event 7"
    );

    // Agent hangup: close 4000 → SIP BYE to the caller, call_ended, tombstone.
    media
        .send(Message::Close(Some(CloseFrame {
            code: 4000.into(),
            reason: "agent_hangup".into(),
        })))
        .await
        .unwrap();
    let ended = ctl.event("call_ended", Duration::from_secs(5)).await;
    assert_eq!(ended["call_id"], "call-in-1");
    assert_eq!(ended["reason"], "agent_hangup_ws_4000");
    assert_eq!(ended["final_state"], "completed");
    let bye = wait_for_ua_event(&alice, Duration::from_secs(5), |e| matches!(e, TestUaEvent::CallTerminated(_))).await;
    assert!(bye.is_some(), "caller received BYE after 4000 close");
    let snap = ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    assert_eq!(snap["active_calls"], json!([]));
    let tomb = &snap["terminal_tombstones"][0];
    assert_eq!(tomb["pbx_call_id"], pbx_call_id.as_str());
    assert_eq!(tomb["call_id"], "call-in-1");
    assert_eq!(tomb["hangup_reason"], "agent_hangup_ws_4000");
    assert!(tomb["answered_at"].as_i64().is_some());
    assert_eq!(tomb["pbx_boot_epoch"], snap["pbx_boot_epoch"]);
    let get = ctl.command("get_call", 1, json!({"call_id": "call-in-1", "payload": {}})).await;
    assert_eq!(get["terminal_tombstones"][0]["call_id"], "call-in-1");
    // Repeating hangup on a terminal call is safe.
    let again = ctl
        .command("hangup", 1, json!({"call_id": "call-in-1", "payload": {"pbx_call_id": pbx_call_id, "reason": "agent_hangup"}}))
        .await;
    assert_eq!(again["type"], "command_ack", "{again}");
    assert_eq!(again["result"]["already_terminal"], true);
    rtp.cancel.cancel();
    h.server.stop();
}

/// Receiver-enforced ownership: a higher epoch closes the old controller's
/// still-open socket and a stale epoch is rejected.
#[tokio::test]
async fn vbox_contract_owner_epoch_takeover_closes_stale_controller() {
    let h = harness(30).await;
    let mut a = Controller::connect(&h.rwi_url).await;
    let snap = a.command("list_owned_calls", 4, json!({"payload": {"include_terminated_since": 0}})).await;
    assert_eq!(snap["type"], "snapshot_response");
    let mut b = Controller::connect(&h.rwi_url).await;
    let snap = b.command("list_owned_calls", 5, json!({"payload": {"include_terminated_since": 0}})).await;
    assert_eq!(snap["owner_epoch"], 5);
    assert!(a.closed_within(Duration::from_secs(3)).await, "old controller socket closed on takeover");
    let mut c = Controller::connect(&h.rwi_url).await;
    let stale = c.command("list_owned_calls", 4, json!({"payload": {"include_terminated_since": 0}})).await;
    assert_eq!(stale["type"], "command_error");
    assert_eq!(stale["error_code"], "stale_owner_epoch");
    assert_eq!(stale["current_owner_epoch"], 5);
    // Discovery stays available to any authenticated session.
    let caps = c.command("capabilities", 0, json!({})).await;
    assert_eq!(caps["type"], "command_ack");
    let health = b.command("health", 5, json!({})).await;
    assert_eq!(health["result"]["controller_connected"], true);
    drop(h);
}

/// RWI outage: inside the hold the call and its media survive and new
/// inbound calls get 503; past the hold the call is hung up, its media socket
/// closed with 4001 and a tombstone records `rwi_orphan_hold_expired`.
#[tokio::test]
async fn vbox_contract_orphan_hold_survives_then_expires_with_4001() {
    let mut h = harness(2).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _dialog, pbx_rtp, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-orphan", "m-orphan").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);

    // Controller drops (not fenced): orphan hold starts.
    drop(ctl);
    tokio::time::sleep(Duration::from_millis(300)).await;
    let charlie = Arc::new(h.server.create_ua("charlie").await.unwrap());
    let charlie_rtp = CallerRtp::bind().await;
    let refused = tokio::time::timeout(
        Duration::from_secs(5),
        charlie.make_call(VBOX_DID, Some(make_sdp(charlie_rtp.port()))),
    )
    .await
    .expect("refusal in time");
    assert!(refused.is_err(), "inbound call refused while no controller is connected");

    // Media keeps flowing inside the hold.
    let mut frames = 0;
    let until = Instant::now() + Duration::from_millis(800);
    while Instant::now() < until {
        if let Ok(Some(Ok(Message::Binary(_)))) = tokio::time::timeout(Duration::from_millis(200), media.next()).await {
            frames += 1;
        }
    }
    assert!(frames > 10, "media survives inside the orphan hold ({frames} frames)");

    // Past the hold: 4001 close on the media socket and BYE to the caller.
    let mut close_code = None;
    let until = Instant::now() + Duration::from_secs(5);
    while Instant::now() < until {
        match tokio::time::timeout(Duration::from_millis(500), media.next()).await {
            Ok(Some(Ok(Message::Close(frame)))) => {
                close_code = frame.map(|f| u16::from(f.code));
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            _ => {}
        }
    }
    assert_eq!(close_code, Some(4001), "media closed with rwi_orphan_timeout");
    let bye = wait_for_ua_event(&alice, Duration::from_secs(5), |e| matches!(e, TestUaEvent::CallTerminated(_))).await;
    assert!(bye.is_some(), "orphaned SIP leg hung up");

    let mut ctl2 = Controller::connect(&h.rwi_url).await;
    let snap = ctl2.command("list_owned_calls", 2, json!({"payload": {"include_terminated_since": 0}})).await;
    let tomb = snap["terminal_tombstones"]
        .as_array()
        .unwrap()
        .iter()
        .find(|t| t["pbx_call_id"] == pbx_call_id.as_str())
        .cloned()
        .expect("tombstone for the orphaned call");
    assert_eq!(tomb["hangup_reason"], "rwi_orphan_hold_expired");
    assert_eq!(tomb["call_id"], "call-orphan");
    rtp.cancel.cancel();
    charlie_rtp.cancel.cancel();
    h.server.stop();
}

/// Reconnecting inside the hold keeps the call; the new controller sees it.
#[tokio::test]
async fn vbox_contract_reconnect_inside_orphan_hold_keeps_the_call() {
    let mut h = harness(3).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _dialog, pbx_rtp, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-keep", "m-keep").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
    drop(ctl);
    tokio::time::sleep(Duration::from_millis(500)).await;
    let mut ctl2 = Controller::connect(&h.rwi_url).await;
    let snap = ctl2.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    assert_eq!(snap["active_calls"][0]["pbx_call_id"], pbx_call_id.as_str());
    assert_eq!(snap["active_calls"][0]["call_id"], "call-keep");
    assert_eq!(snap["active_calls"][0]["media_session_id"], "m-keep");
    // Wait past the original hold: nothing is torn down.
    let until = Instant::now() + Duration::from_secs(4);
    let mut frames = 0;
    while Instant::now() < until {
        match tokio::time::timeout(Duration::from_millis(300), media.next()).await {
            Ok(Some(Ok(Message::Binary(_)))) => frames += 1,
            Ok(Some(Ok(Message::Close(f)))) => panic!("media closed after reconnect: {f:?}"),
            _ => {}
        }
    }
    assert!(frames > 100, "media still flowing ({frames})");
    let hangup = ctl2
        .command("hangup", 1, json!({"call_id": "call-keep", "payload": {"pbx_call_id": pbx_call_id, "reason": "agent_hangup"}}))
        .await;
    assert_eq!(hangup["type"], "command_ack");
    let ended = ctl2.event("call_ended", Duration::from_secs(5)).await;
    assert_eq!(ended["reason"], "agent_hangup");
    rtp.cancel.cancel();
    h.server.stop();
}

/// Agent hangup of an answered inbound (`rwi` app) call: contract `hangup`
/// — and vbox's follow-up `4000` close of the media socket, which arrives
/// while the PBX is already tearing the call down — BYE the caller promptly.
/// Live 2026-10-19: the caller dialog was never BYEd ("Timed out waiting for
/// cleanup hangups") and the caller sat on silence until it hung up.
#[tokio::test]
async fn vbox_contract_inbound_hangup_byes_the_caller() {
    for close_media_with_4000 in [false, true] {
        let mut h = harness(30).await;
        let mut ctl = Controller::connect(&h.rwi_url).await;
        ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
        let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
        let rtp = CallerRtp::bind().await;
        let call_id = format!("call-hup-{close_media_with_4000}");
        let (pbx_call_id, dialog, pbx_rtp, mut media) =
            establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, &call_id, "m-hup").await;
        rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
        let until = Instant::now() + Duration::from_millis(300);
        while Instant::now() < until {
            let _ = tokio::time::timeout(Duration::from_millis(50), media.next()).await;
        }

        let sent = Instant::now();
        let hangup = ctl
            .command("hangup", 1, json!({"call_id": call_id, "payload": {"pbx_call_id": pbx_call_id, "reason": "agent_hangup"}}))
            .await;
        assert_eq!(hangup["type"], "command_ack", "{hangup}");
        if close_media_with_4000 {
            let _ = media
                .send(Message::Close(Some(CloseFrame { code: 4000.into(), reason: "agent_hangup".into() })))
                .await;
        }
        let bye = wait_for_ua_event(&alice, Duration::from_secs(3), |e| {
            matches!(e, TestUaEvent::CallTerminated(d) if *d == dialog)
        })
        .await;
        let after = sent.elapsed();
        assert!(bye.is_some(), "caller received no BYE within 3 s of contract hangup (4000 close: {close_media_with_4000})");
        assert!(after < Duration::from_secs(1), "caller BYE took {after:?} (4000 close: {close_media_with_4000})");
        let ended = ctl.event("call_ended", Duration::from_secs(5)).await;
        assert_eq!(ended["call_id"], call_id.as_str());
        rtp.cancel.cancel();
        h.server.stop();
    }
}

// ── Raw SIP/TCP carrier (Telnyx-shaped inbound leg) ───────────────────────

/// One SIP message (head + body) from a stream connection.
async fn read_sip(stream: &mut TcpStream, buf: &mut Vec<u8>, timeout: Duration) -> Option<String> {
    use tokio::io::AsyncReadExt;
    let deadline = Instant::now() + timeout;
    loop {
        // Skip CRLF keep-alives.
        while buf.starts_with(b"\r\n") {
            buf.drain(..2);
        }
        if let Some(end) = buf.windows(4).position(|w| w == b"\r\n\r\n") {
            let head = String::from_utf8_lossy(&buf[..end]).to_string();
            let len = head
                .lines()
                .find_map(|l| {
                    let (k, v) = l.split_once(':')?;
                    (k.eq_ignore_ascii_case("content-length") || k == "l").then(|| v.trim().parse::<usize>().ok())?
                })
                .unwrap_or(0);
            if buf.len() >= end + 4 + len {
                let msg = String::from_utf8_lossy(&buf[..end + 4 + len]).to_string();
                buf.drain(..end + 4 + len);
                return Some(msg);
            }
        }
        let left = deadline.checked_duration_since(Instant::now())?;
        let mut chunk = [0u8; 8192];
        match tokio::time::timeout(left, stream.read(&mut chunk)).await {
            Ok(Ok(n)) if n > 0 => buf.extend_from_slice(&chunk[..n]),
            _ => return None,
        }
    }
}

fn sip_header<'a>(msg: &'a str, name: &str) -> Option<&'a str> {
    msg.lines().find_map(|l| {
        let (k, v) = l.split_once(':')?;
        k.trim().eq_ignore_ascii_case(name).then(|| v.trim())
    })
}

async fn send_sip(stream: &mut TcpStream, msg: &str) {
    use tokio::io::AsyncWriteExt;
    stream.write_all(msg.as_bytes()).await.unwrap();
}

/// Inbound call from a record-routing carrier proxy over a reliable
/// transport, shaped like Telnyx's 2026-10-19 INVITE: two Record-Route
/// entries (the proxy's own address first) and a Contact without a transport
/// parameter naming the carrier's internal media server. The agent hangs up
/// with contract `hangup`; the BYE must go to the first route with the
/// Contact unchanged as Request-URI and a Via naming the transport it is
/// sent on (RFC 3261 §12.2.1.1, §18.1.1). Live, the BYE was
/// `BYE sip:mod_sofia@…:6000;transport=TLS` with `Via: SIP/2.0/UDP …:5060`;
/// Telnyx's proxy tried TLS to its UDP media server, answered 408 and the
/// caller stayed up.
#[tokio::test]
async fn vbox_contract_record_routed_inbound_hangup_bye_keeps_contact_and_via_transport() {
    // `acl` identifies the trunk by source address (as in production).
    let acl: (String, rustpbx::proxy::FnCreateProxyModule) = ("acl".to_string(), rustpbx::proxy::acl::AclModule::create);
    let mut h = harness_with(30, vec![acl], true).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;

    // The carrier proxy listens where its Record-Route points.
    let proxy = TcpListener::bind("127.0.0.1:0").await.unwrap();
    let proxy_port = proxy.local_addr().unwrap().port();
    let pbx_port = h.server.port;
    let rtp = CallerRtp::bind().await;
    let sdp = make_sdp(rtp.port());
    let call_id = format!("carrier-{}", uuid::Uuid::new_v4().simple());
    let contact = "<sip:mod_sofia@10.239.42.224:6000>";
    let mut conn = TcpStream::connect(("127.0.0.1", pbx_port)).await.expect("PBX SIP/TCP");
    let invite = format!(
        "INVITE sip:{VBOX_DID}@127.0.0.1:{pbx_port} SIP/2.0\r\n\
         Record-Route: <sip:127.0.0.1:{proxy_port};transport=tcp;r2=on;lr;ftag=ft1>\r\n\
         Record-Route: <sip:10.255.0.1;r2=on;lr;ftag=ft1>\r\n\
         Via: SIP/2.0/TCP 127.0.0.1:{proxy_port};branch=z9hG4bKinv{call_id}\r\n\
         Max-Forwards: 69\r\n\
         From: <sip:+15550100001@127.0.0.1>;tag=ft1\r\n\
         To: <sip:{VBOX_DID}@127.0.0.1>\r\n\
         Call-ID: {call_id}\r\n\
         CSeq: 1 INVITE\r\n\
         Contact: {contact}\r\n\
         Content-Type: application/sdp\r\n\
         Content-Length: {}\r\n\r\n{sdp}",
        sdp.len()
    );
    send_sip(&mut conn, &invite).await;

    let offered = ctl.event("incoming_call", Duration::from_secs(5)).await;
    let pbx_call_id = offered["pbx_call_id"].as_str().unwrap().to_string();
    let vbox_call = "call-carrier";
    let binding_id = "bind-carrier";
    let claim = ctl
        .command("claim_call", 1, json!({"call_id": vbox_call, "binding_id": binding_id, "payload": {"pbx_call_id": pbx_call_id}}))
        .await;
    assert_eq!(claim["type"], "command_ack", "{claim}");
    let attach = ctl
        .command(
            "attach_media",
            1,
            json!({"call_id": vbox_call, "binding_id": binding_id, "payload": {
                "pbx_call_id": pbx_call_id,
                "media_session_id": "m-carrier",
                "ws_url": format!("/ws/rustpbx/{vbox_call}?token=tok-carrier"),
                "token": "tok-carrier",
                "outbound_framing": "generation_u64_pcm16le",
            }}),
        )
        .await;
    assert_eq!(attach["type"], "command_ack", "{attach}");
    let answer = ctl
        .command("answer", 1, json!({"call_id": vbox_call, "binding_id": binding_id, "payload": {"pbx_call_id": pbx_call_id}}))
        .await;
    assert_eq!(answer["type"], "command_ack", "{answer}");

    // 200 OK on the carrier's connection, then the carrier ACKs.
    let mut buf = Vec::new();
    let ok = loop {
        let msg = read_sip(&mut conn, &mut buf, Duration::from_secs(5)).await.expect("final response to INVITE");
        if msg.starts_with("SIP/2.0 200") {
            break msg;
        }
        assert!(msg.starts_with("SIP/2.0 1"), "unexpected response: {msg}");
    };
    let to = sip_header(&ok, "To").expect("To").to_string();
    let pbx_contact = sip_header(&ok, "Contact").expect("PBX Contact");
    let pbx_target = pbx_contact.trim_start_matches('<').split('>').next().unwrap().to_string();
    send_sip(
        &mut conn,
        &format!(
            "ACK {pbx_target} SIP/2.0\r\n\
             Via: SIP/2.0/TCP 127.0.0.1:{proxy_port};branch=z9hG4bKack{call_id}\r\n\
             Max-Forwards: 69\r\n\
             From: <sip:+15550100001@127.0.0.1>;tag=ft1\r\n\
             To: {to}\r\n\
             Call-ID: {call_id}\r\n\
             CSeq: 1 ACK\r\n\
             Content-Length: 0\r\n\r\n"
        ),
    )
    .await;
    ctl.event("answered", Duration::from_secs(5)).await;
    let (_, mut media) = h.next_media().await;
    media_text(&mut media, "start", Duration::from_secs(3)).await;
    media
        .send(Message::Text(json!({"type": "ready", "media_session_id": "m-carrier"}).to_string().into()))
        .await
        .unwrap();

    let hangup = ctl
        .command("hangup", 1, json!({"call_id": vbox_call, "payload": {"pbx_call_id": pbx_call_id, "reason": "agent_hangup"}}))
        .await;
    assert_eq!(hangup["type"], "command_ack", "{hangup}");

    // The BYE reaches the first route (the carrier proxy), on whichever
    // connection the PBX picks: a new one to the route or the inbound flow.
    let mut route_conn = None;
    let bye = tokio::select! {
        accepted = proxy.accept() => {
            let (mut s, _) = accepted.unwrap();
            let mut b = Vec::new();
            let msg = read_sip(&mut s, &mut b, Duration::from_secs(3)).await;
            route_conn = Some(s);
            msg
        }
        msg = read_sip(&mut conn, &mut buf, Duration::from_secs(3)) => msg,
    };
    let bye = bye.expect("carrier proxy received a BYE");
    let bye_conn = route_conn.as_mut().unwrap_or(&mut conn);
    let request_line = bye.lines().next().unwrap();
    assert_eq!(
        request_line, "BYE sip:mod_sofia@10.239.42.224:6000 SIP/2.0",
        "BYE Request-URI must be the INVITE Contact unchanged:\n{bye}"
    );
    let via = sip_header(&bye, "Via").expect("Via");
    assert!(
        via.starts_with("SIP/2.0/TCP 127.0.0.1:"),
        "BYE Via must name the transport it was sent on (TCP): {via}\n{bye}"
    );
    let routes: Vec<&str> = bye.lines().filter(|l| l.to_ascii_lowercase().starts_with("route:")).collect();
    assert_eq!(routes.len(), 2, "{bye}");
    assert!(routes[0].contains(&format!("127.0.0.1:{proxy_port}")), "{bye}");
    let mut reply = String::from("SIP/2.0 200 OK\r\n");
    for name in ["Via", "From", "To", "Call-ID", "CSeq"] {
        reply.push_str(&format!("{name}: {}\r\n", sip_header(&bye, name).unwrap()));
    }
    reply.push_str("Content-Length: 0\r\n\r\n");
    send_sip(bye_conn, &reply).await;
    let ended = ctl.event("call_ended", Duration::from_secs(5)).await;
    assert_eq!(ended["call_id"], vbox_call);
    rtp.cancel.cancel();
    h.server.stop();
}

/// Outbound: originate with pre-assigned identity, early correlation,
/// answer → media attach, duplicate command id never dials twice, hangup.
#[tokio::test]
async fn vbox_contract_outbound_originate_answer_media_and_hangup() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 3, json!({"payload": {"include_terminated_since": 0}})).await;
    let bob = h.server.create_ua("bob").await.unwrap();
    let bob_rtp = CallerRtp::bind().await;
    let destination = format!("sip:bob@127.0.0.1:{}", bob.local_port());
    let command_id = format!("cmd-orig-{}", uuid::Uuid::new_v4().simple());
    let originate = json!({
        "rwi_version": "1.0",
        "type": "command",
        "schema_version": "pbx_command_v1",
        "command_id": command_id,
        "operation": "originate",
        "owner_epoch": 3,
        "request_id": "req-1",
        "tenant_id": "tenant-a",
        "call_id": "call-out-1",
        "binding_id": "bind-out-1",
        "deadline_at": chrono::Utc::now().timestamp_millis() + 5000,
        "payload": {
            "call_id": "call-out-1",
            "binding_id": "bind-out-1",
            "media_session_id": "m-out-1",
            "ws_url": "/ws/rustpbx/call-out-1?token=tok-out",
            "token": "tok-out",
            "destination": destination,
            "to": "+15550199",
            "from": "+15550100",
            "outbound_framing": "generation_u64_pcm16le",
        }
    });
    ctl.send(originate.clone()).await;
    let ack = ctl.reply_for(&command_id).await;
    assert_eq!(ack["type"], "command_ack", "{ack}");
    let pbx_call_id = ack["pbx_call_id"].as_str().unwrap().to_string();
    let dialing = ctl.event("dialing", Duration::from_secs(3)).await;
    assert_eq!(dialing["call_id"], "call-out-1");
    assert_eq!(dialing["command_id"], command_id.as_str());

    // The same command again is answered from the record, not dialed again.
    ctl.send(originate).await;
    let dup = ctl.reply_for(&command_id).await;
    assert_eq!(dup["pbx_call_id"], pbx_call_id.as_str());
    assert_eq!(dup["result"]["duplicate"], true);

    let incoming = wait_for_ua_event(&bob, Duration::from_secs(5), |e| matches!(e, TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(bob_dialog, offer)) = incoming else { panic!("bob was not called") };
    let pbx_rtp = extract_media_endpoint(offer.as_deref().expect("originate offer")).expect("offer address");
    bob.answer_call(&bob_dialog, Some(make_sdp(bob_rtp.port()))).await.unwrap();
    let answered = ctl.event("answered", Duration::from_secs(5)).await;
    assert_eq!(answered["call_id"], "call-out-1");
    assert_eq!(answered["binding_id"], "bind-out-1");
    let (path, mut media) = h.next_media().await;
    assert_eq!(path, "/ws/rustpbx/call-out-1?token=tok-out");
    let start = media_text(&mut media, "start", Duration::from_secs(3)).await;
    assert_eq!(start["media_session_id"], "m-out-1");

    // Two-way audio on the outbound leg.
    bob_rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 10);
    send_agent_audio(&mut media, 1, AGENT_TONE_HZ, 75).await;
    let mut callee_tone = 0;
    let until = Instant::now() + Duration::from_secs(4);
    while Instant::now() < until && callee_tone < 20 {
        if let Ok(Some(Ok(Message::Binary(b)))) = tokio::time::timeout(Duration::from_millis(500), media.next()).await
            && is_tone(&pcm16le_to_samples(&b), 16_000, CALLER_TONE_HZ)
        {
            callee_tone += 1;
        }
    }
    assert!(callee_tone >= 20, "callee audio reached vbox ({callee_tone})");
    tokio::time::sleep(Duration::from_millis(1600)).await;
    assert!(bob_rtp.tone_frames(AGENT_TONE_HZ).len() >= 50, "agent audio reached the callee");

    let hangup = ctl
        .command("hangup", 3, json!({"call_id": "call-out-1", "payload": {"pbx_call_id": pbx_call_id, "reason": "agent_hangup"}}))
        .await;
    assert_eq!(hangup["type"], "command_ack");
    let ended = ctl.event("call_ended", Duration::from_secs(5)).await;
    assert_eq!(ended["call_id"], "call-out-1");
    assert_eq!(ended["final_state"], "completed");
    let bye = wait_for_ua_event(&bob, Duration::from_secs(5), |e| matches!(e, TestUaEvent::CallTerminated(_))).await;
    assert!(bye.is_some(), "callee received BYE");
    let get = ctl.command("get_call", 3, json!({"payload": {"command_id": command_id}})).await;
    assert_eq!(get["terminal_tombstones"][0]["command_id"], command_id.as_str());
    assert_eq!(get["terminal_tombstones"][0]["direction"], "outbound");
    bob_rtp.cancel.cancel();
    h.server.stop();
}

/// Rejecting an offered call before answer ends it with a tombstone.
#[tokio::test]
async fn vbox_contract_reject_before_answer() {
    let h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let caller = tokio::spawn({
        let ua = alice.clone();
        let sdp = make_sdp(rtp.port());
        async move { ua.make_call(VBOX_DID, Some(sdp)).await }
    });
    let offered = ctl.event("incoming_call", Duration::from_secs(5)).await;
    let pbx_call_id = offered["pbx_call_id"].as_str().unwrap().to_string();
    let claim = ctl
        .command("claim_call", 1, json!({"call_id": "call-rej", "binding_id": "bind-rej", "payload": {"pbx_call_id": pbx_call_id}}))
        .await;
    assert_eq!(claim["type"], "command_ack");
    let reject = ctl
        .command("reject", 1, json!({"call_id": "call-rej", "payload": {"pbx_call_id": pbx_call_id, "reason": "capacity_exceeded"}}))
        .await;
    assert_eq!(reject["type"], "command_ack", "{reject}");
    let outcome = tokio::time::timeout(Duration::from_secs(5), caller).await.expect("caller sees the rejection");
    assert!(outcome.unwrap().is_err(), "call was rejected");
    let ended = ctl.event("call_ended", Duration::from_secs(5)).await;
    assert_eq!(ended["call_id"], "call-rej");
    assert_eq!(ended["final_state"], "rejected");
    let tomb = ctl.command("get_call", 1, json!({"call_id": "call-rej", "payload": {}})).await;
    assert_eq!(tomb["terminal_tombstones"][0]["hangup_reason"], "capacity_exceeded");
    rtp.cancel.cancel();
    drop(h);
}

/// Malformed, unsupported, late and unauthorized-scope commands never reach
/// the native dispatcher; an oversized frame closes only its own session.
#[tokio::test]
async fn vbox_contract_rejects_invalid_commands_and_isolates_oversized_frames() {
    let h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.send(json!({"type":"command","schema_version":"pbx_command_v1","operation":"capabilities"})).await;
    let missing = ctl.read_frame(Duration::from_secs(2)).await.unwrap();
    assert_eq!(missing["error_code"], "missing_command_id");
    ctl.send(json!({"type":"command","schema_version":"v0","operation":"capabilities","command_id":"x1"})).await;
    assert_eq!(ctl.reply_for("x1").await["error_code"], "unsupported_schema_version");
    ctl.send(json!({"type":"command","schema_version":"pbx_command_v1","operation":"conference_create","command_id":"x2","owner_epoch":1})).await;
    assert_eq!(ctl.reply_for("x2").await["error_code"], "unsupported_contract_operation");
    ctl.send(json!({"type":"command","schema_version":"pbx_command_v1","operation":"list_owned_calls","command_id":"x3","owner_epoch":1,
        "deadline_at": chrono::Utc::now().timestamp_millis() - 1, "payload": {"include_terminated_since": 0}})).await;
    assert_eq!(ctl.reply_for("x3").await["error_code"], "deadline_exceeded");
    let unknown = ctl.command("claim_call", 1, json!({"call_id":"c","binding_id":"b","payload":{"pbx_call_id":"no-such-call"}})).await;
    assert_eq!(unknown["error_code"], "call_not_found");
    let bad_path = ctl.command("attach_media", 1, json!({"call_id":"c","payload":{"pbx_call_id":"no-such-call","ws_url":"wss://evil/ws/rustpbx/c","media_session_id":"m"}})).await;
    assert_eq!(bad_path["type"], "command_error");

    let mut victim = Controller::connect(&h.rwi_url).await;
    victim.ws.send(Message::Text("x".repeat(64 * 1024 + 1).into())).await.unwrap();
    assert!(victim.closed_within(Duration::from_secs(3)).await, "oversized sender closed");
    let caps = ctl.command("capabilities", 0, json!({})).await;
    assert_eq!(caps["type"], "command_ack", "unaffected session still answers");
    drop(h);
}

/// `media_reattach`: a new `attach_media` replaces the live socket; the old
/// one is closed `1000 superseded` and audio continues on the new one.
#[tokio::test]
async fn vbox_contract_media_reattach_supersedes_the_old_socket() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _dialog, pbx_rtp, mut old) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-re", "m-a").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
    send_agent_audio(&mut old, 1, AGENT_TONE_HZ, 25).await;
    tokio::time::sleep(Duration::from_millis(700)).await;
    let before = rtp.tone_frames(AGENT_TONE_HZ).len();
    assert!(before >= 15, "audio on the first attachment ({before})");

    let attach = ctl
        .command(
            "attach_media",
            1,
            json!({"call_id": "call-re", "binding_id": "bind-call-re", "payload": {
                "pbx_call_id": pbx_call_id,
                "media_session_id": "m-b",
                "ws_url": "/ws/rustpbx/call-re?token=tok-m-b",
                "outbound_framing": "generation_u64_pcm16le",
            }}),
        )
        .await;
    assert_eq!(attach["type"], "command_ack", "{attach}");
    assert_eq!(attach["result"]["media_state"], "connecting");
    let mut close = None;
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        match tokio::time::timeout(Duration::from_millis(300), old.next()).await {
            Ok(Some(Ok(Message::Close(f)))) => {
                close = f.map(|f| (u16::from(f.code), f.reason.to_string()));
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            _ => {}
        }
    }
    assert_eq!(close, Some((1000, "superseded".to_string())));
    let (path, mut new) = h.next_media().await;
    assert_eq!(path, "/ws/rustpbx/call-re?token=tok-m-b");
    let start = media_text(&mut new, "start", Duration::from_secs(3)).await;
    assert_eq!(start["media_session_id"], "m-b");
    // Deterministically deliver the old worker's delayed 4000 outcome after
    // successor attachment. This exercises the real session command handler;
    // subsequent SIP/RTP assertions prove it cannot tear down the successor.
    h.server.registry.get_handle(&pbx_call_id).unwrap().send_command(
        rustpbx::call::domain::CallCommand::VboxMediaClosed {
            media_session_id: "m-a".into(),
            outcome: rustpbx::call::vbox_media::MediaOutput::AgentHangup { reason: "delayed_old_4000".into() },
        }
    ).unwrap();
    // vbox resumes above its high-water generation and clears first.
    new.send(Message::Text(json!({"type":"clear","generation":5}).to_string().into())).await.unwrap();
    let cleared = media_text(&mut new, "cleared", Duration::from_secs(2)).await;
    assert_eq!(cleared["generation"], 5);
    send_agent_audio(&mut new, 5, AGENT_TONE_HZ, 50).await;
    tokio::time::sleep(Duration::from_millis(1300)).await;
    let after = rtp.tone_frames(AGENT_TONE_HZ).len();
    assert!(after >= before + 40, "audio continues on the new attachment ({before} -> {after})");
    let mut caller_frames = 0;
    let until = Instant::now() + Duration::from_secs(2);
    while Instant::now() < until && caller_frames < 10 {
        if let Ok(Some(Ok(Message::Binary(_)))) = tokio::time::timeout(Duration::from_millis(300), new.next()).await {
            caller_frames += 1;
        }
    }
    assert!(caller_frames >= 10, "caller audio arrives on the new attachment");
    let snap = ctl.command("get_call", 1, json!({"call_id": "call-re", "payload": {}})).await;
    assert_eq!(snap["active_calls"][0]["media_session_id"], "m-b");
    rtp.cancel.cancel();
    h.server.stop();
}

/// Blind transfer to a SIP target: success detaches vbox media and reports
/// `transfer_completed`; the caller is then connected to the target.
#[tokio::test]
async fn vbox_contract_transfer_completes_and_detaches_media() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let bob = h.server.create_ua("bob").await.unwrap();
    let rtp = CallerRtp::bind().await;
    let bob_rtp = CallerRtp::bind().await;
    let (pbx_call_id, _dialog, pbx_rtp, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-xfer", "m-x").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);

    let target = format!("sip:bob@127.0.0.1:{}", bob.local_port());
    let xfer = ctl
        .command("transfer", 1, json!({"call_id": "call-xfer", "payload": {"pbx_call_id": pbx_call_id, "target": target, "policy": "blind"}}))
        .await;
    assert_eq!(xfer["type"], "command_ack", "{xfer}");
    let xfer_cmd = xfer["command_id"].as_str().unwrap().to_string();
    let incoming = wait_for_ua_event(&bob, Duration::from_secs(8), |e| matches!(e, TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(bob_dialog, offer)) = incoming else { panic!("transfer target was not called") };
    let bob_pbx_rtp = extract_media_endpoint(offer.as_deref().expect("offer")).expect("offer address");
    bob.answer_call(&bob_dialog, Some(make_sdp(bob_rtp.port()))).await.unwrap();
    bob_rtp.send_tone(bob_pbx_rtp, AGENT_TONE_HZ, 10);

    let done = ctl.event("transfer_completed", Duration::from_secs(8)).await;
    assert_eq!(done["call_id"], "call-xfer");
    assert_eq!(done["command_id"], xfer_cmd.as_str());
    let mut close = None;
    let until = Instant::now() + Duration::from_secs(3);
    while Instant::now() < until {
        match tokio::time::timeout(Duration::from_millis(300), media.next()).await {
            Ok(Some(Ok(Message::Close(f)))) => {
                close = f.map(|f| u16::from(f.code));
                break;
            }
            Ok(None) | Ok(Some(Err(_))) => break,
            _ => {}
        }
    }
    assert_eq!(close, Some(1000), "vbox media detached on transfer");
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(bob_rtp.tone_frames(CALLER_TONE_HZ).len() >= 20, "caller now reaches the transfer target");
    let snap = ctl.command("get_call", 1, json!({"call_id": "call-xfer", "payload": {}})).await;
    assert_eq!(snap["active_calls"], json!([]));
    assert_eq!(snap["terminal_tombstones"][0]["final_state"], "transferred");
    rtp.cancel.cancel();
    bob_rtp.cancel.cancel();
    h.server.stop();
}

/// A failed transfer leaves the caller with the agent and reports
/// `command_error` / `transfer_failed` for the transfer command.
#[tokio::test]
async fn vbox_contract_failed_transfer_keeps_the_agent_attached() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {"include_terminated_since": 0}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let bob = h.server.create_ua("bob").await.unwrap();
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _dialog, pbx_rtp, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-xf", "m-xf").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
    let target = format!("sip:bob@127.0.0.1:{}", bob.local_port());
    let xfer = ctl
        .command("transfer", 1, json!({"call_id": "call-xf", "payload": {"pbx_call_id": pbx_call_id, "target": target}}))
        .await;
    assert_eq!(xfer["type"], "command_ack", "{xfer}");
    let xfer_cmd = xfer["command_id"].as_str().unwrap().to_string();
    let incoming = wait_for_ua_event(&bob, Duration::from_secs(8), |e| matches!(e, TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(bob_dialog, _)) = incoming else { panic!("transfer target was not called") };
    bob.reject_call(&bob_dialog).await.unwrap();
    let failed = ctl.event("command_error", Duration::from_secs(8)).await;
    assert_eq!(failed["error_code"], "transfer_failed");
    assert_eq!(failed["command_id"], xfer_cmd.as_str());
    assert_eq!(failed["call_id"], "call-xf");
    // The agent still talks to the caller over the same attachment.
    send_agent_audio(&mut media, 9, AGENT_TONE_HZ, 50).await;
    let before = rtp.tone_frames(AGENT_TONE_HZ).len();
    tokio::time::sleep(Duration::from_millis(1300)).await;
    assert!(rtp.tone_frames(AGENT_TONE_HZ).len() >= before + 40, "agent audio still reaches the caller");
    let snap = ctl.command("get_call", 1, json!({"call_id": "call-xf", "payload": {}})).await;
    assert_eq!(snap["active_calls"][0]["state"], "answered");
    rtp.cancel.cancel();
    h.server.stop();
}

/// Cross-language milestone: the controller/media server is the real Erlang
/// application, started by scripts/rustpbx_interop.py. No Rust RWI client or
/// fake media server participates in these calls.
#[tokio::test]
#[ignore = "requires the external Erlang runtime and disposable PostgreSQL"]
async fn vbox_erlang_interop() {
    let dir = std::path::PathBuf::from(std::env::var("VBOX_INTEROP_DIR").expect("VBOX_INTEROP_DIR required"));
    async fn wait_file(dir: &std::path::Path, name: &str) -> Value {
        let until = Instant::now() + Duration::from_secs(90);
        while Instant::now() < until {
            if let Ok(bytes) = std::fs::read(dir.join(name)) {
                if let Ok(v) = serde_json::from_slice(&bytes) { return v; }
            }
            tokio::time::sleep(Duration::from_millis(50)).await;
        }
        panic!("timed out waiting for {name}");
    }
    let vbox = wait_file(&dir, "vbox.json").await;
    let h = harness(30).await;
    h.gateway.read().vbox.set_config(VboxContractConfig {
        media_base_url: Some(vbox["media_base_url"].as_str().unwrap().into()),
        pbx_instance_id: Some("pbx-e2e".into()),
        provider_connection_id: Some("rustpbx-local".into()),
        ..Default::default()
    });
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let bob = h.server.create_ua("bob").await.unwrap();
    std::fs::write(dir.join("pbx.json"), json!({"rwi_url": h.rwi_url,
        "callee": format!("sip:bob@127.0.0.1:{}", bob.local_port()), "token": TOKEN}).to_string()).unwrap();
    wait_file(&dir, "ready.json").await;
    let rtp = CallerRtp::bind().await;
    let dialog = tokio::time::timeout(Duration::from_secs(30), alice.make_call(VBOX_DID, Some(make_sdp(rtp.port()))))
        .await.expect("inbound answer timeout").expect("inbound established");
    let sdp = alice.get_negotiated_answer_sdp(&dialog).await.unwrap();
    rtp.send_tone(extract_media_endpoint(&sdp).unwrap(), CALLER_TONE_HZ, 20);
    wait_file(&dir, "inbound-audio.json").await;
    let inbound_frames = rtp.tone_frames(AGENT_TONE_HZ).len();
    assert!(inbound_frames >= 40, "inbound caller received agent tone: {inbound_frames}");
    std::fs::write(dir.join("inbound-capture.json"), json!({"tone_frames": inbound_frames}).to_string()).unwrap();
    assert!(wait_for_ua_event(&alice, Duration::from_secs(15), |e| matches!(e, TestUaEvent::CallTerminated(_))).await.is_some(), "inbound BYE");
    rtp.cancel.cancel();
    std::fs::write(dir.join("inbound-ended.json"), "{}").unwrap();
    let bob_rtp = CallerRtp::bind().await;
    let incoming = wait_for_ua_event(&bob, Duration::from_secs(30), |e| matches!(e, TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(dialog, offer)) = incoming else { panic!("outbound INVITE missing") };
    let addr = extract_media_endpoint(offer.as_deref().unwrap()).unwrap();
    bob.answer_call(&dialog, Some(make_sdp(bob_rtp.port()))).await.unwrap();
    bob_rtp.send_tone(addr, CALLER_TONE_HZ, 20);
    wait_file(&dir, "outbound-audio.json").await;
    let outbound_frames = bob_rtp.tone_frames(AGENT_TONE_HZ).len();
    assert!(outbound_frames >= 40, "outbound callee received agent tone: {outbound_frames}");
    std::fs::write(dir.join("outbound-capture.json"), json!({"tone_frames": outbound_frames}).to_string()).unwrap();
    assert!(wait_for_ua_event(&bob, Duration::from_secs(15), |e| matches!(e, TestUaEvent::CallTerminated(_))).await.is_some(), "outbound BYE");
    bob_rtp.cancel.cancel();
    std::fs::write(dir.join("outbound-ended.json"), "{}").unwrap();
    if std::env::var("VBOX_INTEROP_BULK").as_deref() == Ok("1") {
        real_faults::erlang_bulk(&h, &dir).await;
    }
    wait_file(&dir, "done.json").await;
    h.server.stop();
}

#[path = "vbox_real_faults.rs"]
mod real_faults;

/// A real provisional SIP target never answers. Failure is authoritative only
/// after CANCEL/final response; the original attachment still carries RTP.
#[tokio::test]
async fn vbox_contract_transfer_deadline_cancels_target_preserves_agent() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let bob = h.server.create_ua("bob").await.unwrap();
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _, _, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-x-timeout", "m-timeout").await;
    let target = format!("sip:bob@127.0.0.1:{}", bob.local_port());
    let started = Instant::now();
    let xfer = ctl.command("transfer", 1, json!({"call_id":"call-x-timeout", "payload":{
        "pbx_call_id":pbx_call_id,"target":target,"transfer_timeout_ms":500}})).await;
    assert_eq!(xfer["type"], "command_ack", "{xfer}");
    let incoming = wait_for_ua_event(&bob, Duration::from_secs(3), |e| matches!(e,TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(dialog, _)) = incoming else { panic!("target not invited") };
    bob.send_ringing(&dialog, None).await.unwrap();
    let failed = ctl.event("command_error", Duration::from_secs(3)).await;
    assert_eq!(failed["error_code"], "transfer_failed");
    assert_eq!(failed["command_id"], xfer["command_id"]);
    assert!(started.elapsed() < Duration::from_secs(3));
    assert!(wait_for_ua_event(&bob, Duration::from_secs(2), |e| matches!(e,TestUaEvent::CallTerminated(_))).await.is_some(), "target received actual CANCEL termination");
    let before = rtp.tone_frames(AGENT_TONE_HZ).len();
    send_agent_audio(&mut media, 11, AGENT_TONE_HZ, 50).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(rtp.tone_frames(AGENT_TONE_HZ).len() >= before + 40);
    let snap = ctl.command("get_call",1,json!({"call_id":"call-x-timeout","payload":{}})).await;
    assert_eq!(snap["active_calls"][0]["state"], "answered");
    ctl.command("hangup",1,json!({"call_id":"call-x-timeout","payload":{"reason":"test_cleanup"}})).await;
    assert!(wait_for_ua_event(&alice, Duration::from_secs(3), |e| matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
    rtp.cancel.cancel(); h.server.stop();
}

fn transfer_peer_response(request: &str, status: &str, contact: SocketAddr, body: &str) -> String {
    let mut response = format!("SIP/2.0 {status}\r\n");
    for line in request.split("\r\n").skip(1) {
        let Some((name, value)) = line.split_once(':') else { continue; };
        if ["via", "from", "to", "call-id", "cseq"].contains(&name.to_ascii_lowercase().as_str()) {
            let tag = if name.eq_ignore_ascii_case("to") && !value.contains(";tag=") { ";tag=late-transfer-target" } else { "" };
            response.push_str(&format!("{name}:{value}{tag}\r\n"));
        }
    }
    response.push_str(&format!("Contact: <sip:latepeer@{contact}>\r\nContent-Type: application/sdp\r\nContent-Length: {}\r\n\r\n{body}", body.len()));
    response
}

/// A wire 200 crosses the actual CANCEL. It must be ACKed/BYE'd, never
/// published as LegConnected or mislabeled as authoritative transfer failure.
#[tokio::test]
async fn vbox_contract_transfer_deadline_crossed_200_cannot_bridge() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let (_, _, _, mut media) = establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-x-late", "m-late").await;
    let target = tokio::net::UdpSocket::bind("127.0.0.1:0").await.unwrap();
    let address = target.local_addr().unwrap();
    let target_rtp = CallerRtp::bind().await;
    let sdp = make_sdp(target_rtp.port());
    let target_task = tokio::spawn(async move {
        let mut bytes = vec![0u8;65536];
        let (n,pbx) = tokio::time::timeout(Duration::from_secs(3),target.recv_from(&mut bytes)).await.unwrap().unwrap();
        let invite = String::from_utf8(bytes[..n].to_vec()).unwrap();
        assert!(invite.starts_with("INVITE "));
        target.send_to(transfer_peer_response(&invite,"180 Ringing",address,"").as_bytes(),pbx).await.unwrap();
        let until = Instant::now()+Duration::from_secs(5);
        let mut cancelled = false;
        let mut acked = false;
        while Instant::now()<until {
            let (n,from) = tokio::time::timeout(Duration::from_secs(2),target.recv_from(&mut bytes)).await.unwrap().unwrap();
            let request = String::from_utf8(bytes[..n].to_vec()).unwrap();
            if request.starts_with("CANCEL ") {
                cancelled = true;
                target.send_to(transfer_peer_response(&request,"200 OK",address,"").as_bytes(),from).await.unwrap();
                target.send_to(transfer_peer_response(&invite,"200 OK",address,&sdp).as_bytes(),pbx).await.unwrap();
            } else if request.starts_with("ACK ") { acked=true; }
            else if request.starts_with("BYE ") {
                target.send_to(transfer_peer_response(&request,"200 OK",address,"").as_bytes(),from).await.unwrap();
                assert!(cancelled && acked, "late 200 must be ACKed and BYE'd after CANCEL");
                return;
            }
        }
        panic!("late answered target was not cleaned up");
    });
    let xfer=ctl.command("transfer",1,json!({"call_id":"call-x-late","payload":{
        "target":format!("sip:latepeer@{address}"),"transfer_timeout_ms":500}})).await;
    assert_eq!(xfer["type"],"command_ack","{xfer}");
    tokio::time::timeout(Duration::from_secs(6),target_task).await.unwrap().unwrap();
    let before=rtp.tone_frames(AGENT_TONE_HZ).len();
    send_agent_audio(&mut media,12,AGENT_TONE_HZ,50).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(rtp.tone_frames(AGENT_TONE_HZ).len()>=before+40,"original agent media remains selected");
    let snapshot=ctl.command("get_call",1,json!({"call_id":"call-x-late","payload":{}})).await;
    assert_eq!(snapshot["active_calls"][0]["state"],"transferring");
    assert!(!ctl.events.iter().any(|e|e["type"]=="transfer_completed" || (e["type"]=="command_error" && e["command_id"]==xfer["command_id"])));
    ctl.command("hangup",1,json!({"call_id":"call-x-late","payload":{"reason":"test_uncertain_cleanup"}})).await;
    assert!(wait_for_ua_event(&alice,Duration::from_secs(3),|e|matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
    rtp.cancel.cancel(); target_rtp.cancel.cancel(); h.server.stop();
}

/// vbox's per-target ring limit (`max_ring_secs`) replaces the former 4 s
/// cap: a real phone answering after 6 s within a 10 s limit is bridged.
#[tokio::test]
async fn vbox_contract_transfer_target_answering_after_6s_is_bridged() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let bob = h.server.create_ua("bob").await.unwrap();
    let rtp = CallerRtp::bind().await;
    let bob_rtp = CallerRtp::bind().await;
    let (pbx_call_id, _, pbx_rtp, _media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-x-slow", "m-slow").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
    let target = format!("sip:bob@127.0.0.1:{}", bob.local_port());
    let xfer = ctl.command("transfer", 1, json!({"call_id":"call-x-slow", "payload":{
        "pbx_call_id":pbx_call_id,"target":target,"max_ring_secs":10}})).await;
    assert_eq!(xfer["type"], "command_ack", "{xfer}");
    let incoming = wait_for_ua_event(&bob, Duration::from_secs(3), |e| matches!(e,TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(dialog, offer)) = incoming else { panic!("target not invited") };
    let invited = Instant::now();
    bob.send_ringing(&dialog, None).await.unwrap();
    tokio::time::sleep(Duration::from_secs(6)).await;
    // Buffer whatever the PBX reported while the target rang: neither a
    // failure nor a premature `transfer_completed`.
    assert!(!ctl.closed_within(Duration::from_millis(300)).await);
    assert!(
        !ctl.events.iter().any(|e| e["type"] == "command_error" || e["type"] == "transfer_completed"),
        "transfer settled before the target answered: {:?}", ctl.events
    );
    bob.answer_call(&dialog, Some(make_sdp(bob_rtp.port()))).await.expect("target answers after 6 s");
    let bob_pbx_rtp = extract_media_endpoint(offer.as_deref().expect("offer")).expect("offer address");
    bob_rtp.send_tone(bob_pbx_rtp, AGENT_TONE_HZ, 10);
    let done = ctl.event("transfer_completed", Duration::from_secs(5)).await;
    assert_eq!(done["command_id"], xfer["command_id"]);
    assert!(invited.elapsed() >= Duration::from_secs(6));
    tokio::time::sleep(Duration::from_millis(1500)).await;
    assert!(bob_rtp.tone_frames(CALLER_TONE_HZ).len() >= 20, "caller reaches the late-answering target");
    rtp.cancel.cancel(); bob_rtp.cancel.cancel(); h.server.stop();
}

/// A transfer target that the route table resolves to an application on the
/// caller's own session (here the vbox DID itself, `app = "rwi"`) bridges no
/// leg: RustPBX must refuse it as `transfer_failed` and keep the caller with
/// the attached agent. Live 2026-10-19 it restarted the rwi app, closed the
/// agent's media "transferred" and reported `transfer_completed`; the caller
/// sat in silence.
#[tokio::test]
async fn vbox_contract_transfer_to_application_route_fails_and_keeps_agent() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _, pbx_rtp, mut media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-x-app", "m-app").await;
    rtp.send_tone(pbx_rtp, CALLER_TONE_HZ, 20);
    let xfer = ctl.command("transfer", 1, json!({"call_id":"call-x-app", "payload":{
        "pbx_call_id":pbx_call_id,"target":VBOX_DID,"max_ring_secs":10}})).await;
    assert_eq!(xfer["type"], "command_ack", "{xfer}");
    let failed = ctl.event("command_error", Duration::from_secs(5)).await;
    assert_eq!(failed["command_id"], xfer["command_id"], "{failed}");
    assert_eq!(failed["error_code"], "transfer_failed");
    assert_eq!(failed["reason"], "target_resolved_to_application", "{failed}");
    assert!(!ctl.events.iter().any(|e| e["type"] == "transfer_completed"), "{:?}", ctl.events);
    // The agent's media stays attached and still reaches the caller.
    let until = Instant::now() + Duration::from_millis(800);
    while Instant::now() < until {
        if let Ok(Some(Ok(Message::Close(f)))) = tokio::time::timeout(Duration::from_millis(100), media.next()).await {
            panic!("agent media closed after a refused transfer: {f:?}");
        }
    }
    let before = rtp.tone_frames(AGENT_TONE_HZ).len();
    send_agent_audio(&mut media, 21, AGENT_TONE_HZ, 50).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(rtp.tone_frames(AGENT_TONE_HZ).len() >= before + 40, "agent audio still reaches the caller");
    let snap = ctl.command("get_call", 1, json!({"call_id":"call-x-app","payload":{}})).await;
    assert_eq!(snap["active_calls"][0]["state"], "answered", "{snap}");
    ctl.command("hangup",1,json!({"call_id":"call-x-app","payload":{"reason":"test_cleanup"}})).await;
    assert!(wait_for_ua_event(&alice, Duration::from_secs(3), |e| matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
    rtp.cancel.cancel(); h.server.stop();
}

/// A target still ringing past the payload's `max_ring_secs` is cancelled
/// and the transfer fails authoritatively at that limit, not at a default.
#[tokio::test]
async fn vbox_contract_transfer_fails_at_payload_ring_limit() {
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {}})).await;
    let alice = Arc::new(h.server.create_ua("alice").await.unwrap());
    let bob = h.server.create_ua("bob").await.unwrap();
    let rtp = CallerRtp::bind().await;
    let (pbx_call_id, _, _, _media) =
        establish_inbound(&mut h, &mut ctl, &alice, &rtp, 1, "call-x-limit", "m-limit").await;
    let target = format!("sip:bob@127.0.0.1:{}", bob.local_port());
    let started = Instant::now();
    let xfer = ctl.command("transfer", 1, json!({"call_id":"call-x-limit", "payload":{
        "pbx_call_id":pbx_call_id,"target":target,"max_ring_secs":2}})).await;
    assert_eq!(xfer["type"], "command_ack", "{xfer}");
    let incoming = wait_for_ua_event(&bob, Duration::from_secs(3), |e| matches!(e,TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(dialog, _)) = incoming else { panic!("target not invited") };
    bob.send_ringing(&dialog, None).await.unwrap();
    let failed = ctl.event("command_error", Duration::from_secs(5)).await;
    let after = started.elapsed();
    assert_eq!(failed["error_code"], "transfer_failed");
    assert_eq!(failed["command_id"], xfer["command_id"]);
    assert!(after >= Duration::from_millis(1900) && after < Duration::from_millis(3500), "failed after {after:?}, limit 2 s");
    assert!(wait_for_ua_event(&bob, Duration::from_secs(2), |e| matches!(e,TestUaEvent::CallTerminated(_))).await.is_some(), "target cancelled");
    ctl.command("hangup",1,json!({"call_id":"call-x-limit","payload":{"reason":"test_cleanup"}})).await;
    rtp.cancel.cancel(); h.server.stop();
}

/// The actual UAS must preserve the requested 503, not generic hangup's 487.
#[tokio::test]
async fn vbox_contract_disconnected_controller_rejects_new_invite_with_503() {
    let h = harness(1).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls", 1, json!({"payload": {}})).await;
    drop(ctl);
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.gateway.read().vbox.accepting_inbound() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("actual controller disconnect");
    tokio::time::sleep(Duration::from_millis(1200)).await;
    let alice = h.server.create_ua("alice").await.unwrap();
    let rtp = CallerRtp::bind().await;
    let outcome = tokio::time::timeout(Duration::from_secs(3),
        alice.make_call(VBOX_DID, Some(make_sdp(rtp.port())))).await.unwrap();
    let error = outcome.expect_err("controllerless INVITE must fail").to_string();
    assert_eq!(error, "Call failed: 503 Service Unavailable");
    rtp.cancel.cancel();
    alice.stop();
    drop(h);
}
