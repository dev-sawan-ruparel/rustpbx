//! vbox media contract (`rustpbx_media_v1`) — the PBX end of
//! `/ws/rustpbx/:call_id`.
//!
//! Wire format, from vbox to the PBX:
//! - binary: `<<Generation:64/unsigned-little, Pcm16k/binary>>`, mono PCM16
//!   little endian at 16 kHz; normally 648 bytes per 20 ms.
//! - text: versioned JSON controls `clear`, `mark`, `dtmf` (agent-sent
//!   digits), `ready`, `stop`.
//!
//! From the PBX to vbox:
//! - binary: raw 640-byte PCM16 LE 16 kHz caller audio frames (generation 0).
//! - text: `start`, `cleared`, `mark_played`, `mark_cleared`,
//!   `flow_control`, `dtmf` (caller digits).
//! - close codes: `1000` normal, `4001` RWI orphan hold expired. A `4000`
//!   close *from vbox* asks the PBX to hang the SIP leg up immediately.
//!
//! The WebSocket reader never waits on RTP pacing: it only takes the
//! [`Playout`] mutex for in-memory queue operations, so a `clear` is applied
//! the moment it is read even while seconds of audio are queued. Audio is
//! consumed one tick at a time by [`VboxPlayoutSource`] from the leg egress,
//! and a `mark` is acknowledged only after every sample queued before it has
//! been successfully written by the RTP transport (not proof of remote hearing).

use crate::media::audio_source::AudioSource;
use futures::{Sink, SinkExt, Stream, StreamExt};
use serde_json::{Value, json};
use std::collections::VecDeque;
use std::sync::Arc;
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tokio_tungstenite::tungstenite::protocol::CloseFrame;
use tokio_tungstenite::tungstenite::protocol::frame::coding::CloseCode;
use tokio_util::sync::CancellationToken;

pub const MEDIA_SCHEMA: &str = "rustpbx_media_v1";
pub const OUTBOUND_FRAMING: &str = "generation_u64_pcm16le";
pub const GENERATION_HEADER_BYTES: usize = 8;
pub const MAX_PCM_PAYLOAD_BYTES: usize = 6400;
pub const SAMPLE_RATE: u32 = 16_000;
pub const SAMPLES_PER_20MS: usize = 320;
pub const CLOSE_NORMAL: u16 = 1000;
pub const CLOSE_AGENT_HANGUP: u16 = 4000;
pub const CLOSE_RWI_ORPHAN_TIMEOUT: u16 = 4001;
/// Upper bound on any inbound WebSocket message from vbox. A text control is
/// a few hundred bytes; the largest binary frame is 8 + 6400 bytes.
pub const MAX_MEDIA_MESSAGE_BYTES: usize = 64 * 1024;
/// Bounded reader replies and RTP completion events. Overflow terminates the
/// attachment rather than dropping a confirmation/control acknowledgement.
pub const MEDIA_CONTROL_CAPACITY: usize = 128;
pub const MAX_PENDING_MARKS: usize = 128;
pub const MEDIA_WRITE_TIMEOUT_MS: u64 = 1000;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FrameError {
    TooShort,
    OddPcmLength,
    Oversized,
}

impl FrameError {
    pub fn as_str(&self) -> &'static str {
        match self {
            FrameError::TooShort => "frame_too_short",
            FrameError::OddPcmLength => "odd_pcm_length",
            FrameError::Oversized => "frame_oversized",
        }
    }
}

/// Decode one generation-prefixed binary frame from vbox.
pub fn decode_generation_frame(raw: &[u8]) -> Result<(u64, Vec<i16>), FrameError> {
    if raw.len() < GENERATION_HEADER_BYTES + 2 {
        return Err(FrameError::TooShort);
    }
    let pcm = &raw[GENERATION_HEADER_BYTES..];
    if pcm.len() > MAX_PCM_PAYLOAD_BYTES {
        return Err(FrameError::Oversized);
    }
    if pcm.len() % 2 != 0 {
        return Err(FrameError::OddPcmLength);
    }
    let mut header = [0u8; GENERATION_HEADER_BYTES];
    header.copy_from_slice(&raw[..GENERATION_HEADER_BYTES]);
    let generation = u64::from_le_bytes(header);
    let samples = pcm
        .chunks_exact(2)
        .map(|b| i16::from_le_bytes([b[0], b[1]]))
        .collect();
    Ok((generation, samples))
}

/// Encode one generation-prefixed frame (used by tests and fixtures).
pub fn encode_generation_frame(generation: u64, samples: &[i16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(GENERATION_HEADER_BYTES + samples.len() * 2);
    out.extend_from_slice(&generation.to_le_bytes());
    for s in samples {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// Raw PCM16 LE bytes for caller audio sent to vbox.
pub fn encode_pcm16le(samples: &[i16]) -> Vec<u8> {
    let mut out = Vec::with_capacity(samples.len() * 2);
    for s in samples {
        out.extend_from_slice(&s.to_le_bytes());
    }
    out
}

/// A `mark` request from vbox, echoed back verbatim in `mark_played` /
/// `mark_cleared` so vbox can match `{mark_id, generation, turn_id}` exactly.
#[derive(Debug, Clone, PartialEq)]
pub struct Mark {
    pub mark_id: String,
    pub generation: u64,
    pub turn_id: Value,
    pub response_id: Value,
}

#[derive(Debug, Clone, PartialEq)]
pub enum MediaEvent {
    MarkPlayed { mark: Mark, played_at_ms: i64 },
    MarkCleared { mark: Mark },
    Cleared { generation: u64, discarded_frames: u64, discarded_ms: u64 },
    FlowControl { level: &'static str, buffered_ms: u64 },
}

impl MediaEvent {
    pub fn to_json(&self) -> Value {
        match self {
            MediaEvent::MarkPlayed { mark, played_at_ms } => json!({
                "schema_version": MEDIA_SCHEMA,
                "type": "mark_played",
                "mark_id": mark.mark_id,
                "generation": mark.generation,
                "turn_id": mark.turn_id,
                "response_id": mark.response_id,
                "played_at_ms": played_at_ms,
            }),
            MediaEvent::MarkCleared { mark } => json!({
                "schema_version": MEDIA_SCHEMA,
                "type": "mark_cleared",
                "mark_id": mark.mark_id,
                "generation": mark.generation,
                "turn_id": mark.turn_id,
                "response_id": mark.response_id,
            }),
            MediaEvent::Cleared {
                generation,
                discarded_frames,
                discarded_ms,
            } => json!({
                "schema_version": MEDIA_SCHEMA,
                "type": "cleared",
                "generation": generation,
                "discarded_frames": discarded_frames,
                "discarded_ms": discarded_ms,
            }),
            MediaEvent::FlowControl { level, buffered_ms } => json!({
                "schema_version": MEDIA_SCHEMA,
                "type": "flow_control",
                "level": level,
                "buffered_ms": buffered_ms,
            }),
        }
    }
}

#[derive(Debug)]
enum Item {
    Pcm {
        generation: u64,
        samples: Vec<i16>,
        offset: usize,
    },
    Mark(Mark),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PushOutcome {
    Queued,
    Stale,
    Overflow,
}

/// Generation-aware playout queue shared by the WebSocket reader (producer)
/// and the egress tick (consumer).
#[derive(Debug)]
pub struct Playout {
    active_generation: u64,
    queue: VecDeque<Item>,
    queued_samples: usize,
    max_samples: usize,
    high_water: bool,
    pub stale_frames: u64,
    pub overflow_frames: u64,
    pub played_samples: u64,
    closed: bool,
    playback_stopped: CancellationToken,
    read_marks: Vec<Mark>,
    in_flight: VecDeque<(u32, std::time::Instant, Vec<Mark>)>,
}

fn now_ms() -> i64 {
    chrono::Utc::now().timestamp_millis()
}

fn samples_to_ms(samples: usize) -> u64 {
    (samples as u64 * 1000) / SAMPLE_RATE as u64
}

impl Playout {
    /// `max_buffer_ms` bounds queued audio. Overflow closes the attachment:
    /// dropped audio must never be followed by a successful confirmation.
    pub fn new(max_buffer_ms: u64) -> Self {
        Self {
            active_generation: 0,
            queue: VecDeque::new(),
            queued_samples: 0,
            max_samples: (max_buffer_ms as usize * SAMPLE_RATE as usize) / 1000,
            high_water: false,
            stale_frames: 0,
            overflow_frames: 0,
            played_samples: 0,
            closed: false,
            playback_stopped: CancellationToken::new(),
            read_marks: Vec::new(),
            in_flight: VecDeque::new(),
        }
    }

    pub fn active_generation(&self) -> u64 {
        self.active_generation
    }

    pub fn buffered_ms(&self) -> u64 {
        samples_to_ms(self.queued_samples)
    }

    pub fn close(&mut self) {
        self.closed = true;
        self.read_marks.clear();
        self.in_flight.clear();
        self.queue.clear();
        self.queued_samples = 0;
    }

    pub fn is_closed(&self) -> bool {
        self.closed
    }

    fn watermark(&mut self, out: &mut Vec<MediaEvent>) {
        let high = self.max_samples * 8 / 10;
        let low = self.max_samples / 2;
        if !self.high_water && self.queued_samples >= high {
            self.high_water = true;
            out.push(MediaEvent::FlowControl {
                level: "high_water",
                buffered_ms: self.buffered_ms(),
            });
        } else if self.high_water && self.queued_samples <= low {
            self.high_water = false;
            out.push(MediaEvent::FlowControl {
                level: "low_water",
                buffered_ms: self.buffered_ms(),
            });
        }
    }

    pub fn push_pcm(&mut self, generation: u64, samples: Vec<i16>, out: &mut Vec<MediaEvent>) -> PushOutcome {
        if self.closed {
            return PushOutcome::Overflow;
        }
        if generation < self.active_generation {
            self.stale_frames += 1;
            return PushOutcome::Stale;
        }
        if self.queued_samples + samples.len() > self.max_samples {
            self.overflow_frames += 1;
            self.close();
            out.push(MediaEvent::FlowControl {
                level: "overflow",
                buffered_ms: self.buffered_ms(),
            });
            return PushOutcome::Overflow;
        }
        self.queued_samples += samples.len();
        self.queue.push_back(Item::Pcm {
            generation,
            samples,
            offset: 0,
        });
        self.watermark(out);
        PushOutcome::Queued
    }

    /// Queue a mark after everything already queued. A mark for a cleared
    /// generation is reported `mark_cleared` at once. Even an empty-queue
    /// mark waits for a transport receipt, fencing previously in-flight audio.
    pub fn push_mark(&mut self, mark: Mark, out: &mut Vec<MediaEvent>) {
        if self.closed {
            return;
        }
        if self.queue.iter().filter(|item| matches!(item, Item::Mark(_))).count() + self.read_marks.len() + self.in_flight.iter().map(|(_, _, marks)| marks.len()).sum::<usize>() >= MAX_PENDING_MARKS {
            self.close();
            out.clear();
            return;
        }
        if mark.generation < self.active_generation {
            out.push(MediaEvent::MarkCleared { mark });
            return;
        }
        // A full 20 ms of zero input drains the pinned 24-tap FIR and codec
        // lookahead before the receipt boundary. It shares the audio byte cap;
        // failure to reserve the guard closes the attachment, never confirms.
        if self.push_pcm(mark.generation, vec![0; SAMPLES_PER_20MS], out) != PushOutcome::Queued { return; }
        self.queue.push_back(Item::Mark(mark));
    }

    /// Barge-in: raise the active generation and drop everything older.
    pub fn clear(&mut self, generation: u64, out: &mut Vec<MediaEvent>) {
        self.active_generation = self.active_generation.max(generation);
        let active = self.active_generation;
        let mut discarded_samples = 0usize;
        let mut cleared_marks = Vec::new();
        for marks in std::iter::once(&mut self.read_marks).chain(self.in_flight.iter_mut().map(|(_, _, marks)| marks)) {
            marks.retain(|mark| {
                if mark.generation < active { cleared_marks.push(mark.clone()); false } else { true }
            });
        }
        self.queue.retain(|item| match item {
            Item::Pcm {
                generation,
                samples,
                offset,
            } if *generation < active => {
                discarded_samples += samples.len() - offset;
                false
            }
            Item::Mark(mark) if mark.generation < active => {
                cleared_marks.push(mark.clone());
                false
            }
            _ => true,
        });
        self.queued_samples -= discarded_samples;
        for mark in cleared_marks {
            out.push(MediaEvent::MarkCleared { mark });
        }
        out.push(MediaEvent::Cleared {
            generation: active,
            discarded_frames: discarded_samples.div_ceil(SAMPLES_PER_20MS) as u64,
            discarded_ms: samples_to_ms(discarded_samples),
        });
        self.watermark(out);
    }

    /// Fill one egress tick. Returns the number of samples written (the full
    /// buffer, zero-padded when the response ends mid-tick) or `0` on
    /// underrun. Reached marks wait for the corresponding RTP write receipt.
    pub fn read(&mut self, buffer: &mut [i16], out: &mut Vec<MediaEvent>) -> usize {
        if self.in_flight.front().is_some_and(|(_, at, _)| at.elapsed() >= std::time::Duration::from_secs(1)) {
            self.close();
        }
        if self.closed { return 0; }
        let mut filled = 0usize;
        loop {
            match self.queue.front_mut() {
                Some(Item::Mark(_)) => {
                    if let Some(Item::Mark(mark)) = self.queue.pop_front() {
                        self.read_marks.push(mark);
                    }
                    // Audio before the mark has been consumed; finish this
                    // tick with silence padding so the mark is not delayed.
                    if filled > 0 {
                        break;
                    }
                }
                Some(Item::Pcm { samples, offset, .. }) => {
                    if filled == buffer.len() {
                        break;
                    }
                    let available = samples.len() - *offset;
                    let take = available.min(buffer.len() - filled);
                    buffer[filled..filled + take]
                        .copy_from_slice(&samples[*offset..*offset + take]);
                    *offset += take;
                    filled += take;
                    self.queued_samples -= take;
                    self.played_samples += take as u64;
                    if *offset == samples.len() {
                        self.queue.pop_front();
                    }
                }
                None => break,
            }
        }
        // Emit marks that now sit at the head: their audio is fully consumed.
        while let Some(Item::Mark(_)) = self.queue.front() {
            if let Some(Item::Mark(mark)) = self.queue.pop_front() {
                self.read_marks.push(mark);
            }
        }
        self.watermark(out);
        if filled == 0 {
            return 0;
        }
        if filled < buffer.len() {
            buffer[filled..].fill(0);
        }
        buffer.len()
    }
    fn prepare_rtp(&mut self, timestamp: u32) {
        if self.closed { return; }
        if self.in_flight.len() >= 128 || self.in_flight.iter().any(|(ts, _, _)| *ts == timestamp) {
            self.close(); return;
        }
        self.in_flight.push_back((timestamp, std::time::Instant::now(), std::mem::take(&mut self.read_marks)));
    }

    fn transmitted(&mut self, timestamp: u32, sent: bool, out: &mut Vec<MediaEvent>) {
        if self.closed { return; }
        let Some(index) = self.in_flight.iter().position(|(ts, _, _)| *ts == timestamp) else { return; };
        // The normal sender is serial. Missing/reordered receipts cannot prove the prefix.
        if !sent || index != 0 { self.close(); return; }
        let (_, _, marks) = self.in_flight.pop_front().unwrap();
        for mark in marks {
            if mark.generation < self.active_generation { out.push(MediaEvent::MarkCleared { mark }); }
            else { out.push(MediaEvent::MarkPlayed { mark, played_at_ms: now_ms() }); }
        }
    }

}

pub type SharedPlayout = Arc<parking_lot::Mutex<Playout>>;

/// Egress-side consumer. Reads one tick at a time from the shared playout
/// queue; events produced on the RTP side (marks reached, watermarks) are
/// forwarded to the media writer.
pub struct VboxPlayoutSource {
    playout: SharedPlayout,
    events: mpsc::Sender<MediaEvent>,
    cancel: CancellationToken,
    scratch: Vec<MediaEvent>,
}

impl VboxPlayoutSource {
    pub fn new(playout: SharedPlayout, events: mpsc::Sender<MediaEvent>, cancel: CancellationToken) -> Self {
        Self { playout, events, cancel, scratch: Vec::new() }
    }
    fn flush(&mut self) {
        for event in self.scratch.drain(..) {
            if self.events.try_send(event).is_err() { self.playout.lock().close(); self.cancel.cancel(); break; }
        }
        if self.playout.lock().is_closed() { self.cancel.cancel(); }
    }
}

/// Only the post-write callback can release a mark. The expected audio SSRC
/// rejects unrelated video/relay packets; the timestamp identifies a queued frame.
pub struct VboxTransmissionObserver {
    pub playout: std::sync::Weak<parking_lot::Mutex<Playout>>,
    pub events: mpsc::Sender<MediaEvent>,
    pub cancel: CancellationToken,
    pub ssrc: u32,
    pub payload_type: u8,
}
impl rustrtc::peer_connection::RtpObserver for VboxTransmissionObserver {
    fn on_egress_result(&self, packet: &rustrtc::rtp::RtpPacket, sent: bool) {
        if packet.header.ssrc != self.ssrc || packet.header.payload_type != self.payload_type || self.cancel.is_cancelled() { return; }
        let Some(playout) = self.playout.upgrade() else { return; };
        let mut p = playout.lock();
        let mut events = Vec::new();
        p.transmitted(packet.header.timestamp, sent, &mut events);
        for event in events { if self.events.try_send(event).is_err() { p.close(); break; } }
        if p.is_closed() && !p.playback_stopped.is_cancelled() { self.cancel.cancel(); }
    }
}

impl Drop for VboxPlayoutSource {
    fn drop(&mut self) {
        let mut playout = self.playout.lock();
        let failed = playout.is_closed();
        playout.close();
        if failed { self.cancel.cancel(); return; }
        // Source replacement is a normal lifecycle boundary (e.g. transfer).
        // Revoke marks immediately, then let the writer send a bounded close.
        // Transport/queue failures still use the hard cancellation token.
        playout.playback_stopped.cancel();
    }
}

impl AudioSource for VboxPlayoutSource {
    fn read_samples(&mut self, buffer: &mut [i16]) -> usize {
        let n = self.playout.lock().read(buffer, &mut self.scratch);
        self.flush();
        n
    }
    fn prepare_rtp(&mut self, timestamp: u32) {
        self.playout.lock().prepare_rtp(timestamp);
        self.flush();
    }
    fn rtp_enqueue_failed(&mut self) {
        self.playout.lock().close();
        self.cancel.cancel();
    }

    fn sample_rate(&self) -> u32 {
        SAMPLE_RATE
    }

    fn channels(&self) -> u16 {
        1
    }

    fn has_data(&self) -> bool {
        !self.playout.lock().is_closed()
    }

    fn reset(&mut self) -> anyhow::Result<()> {
        Ok(())
    }
}

/// A request to attach a call to vbox media (`CallCommand::VboxMediaAttach`).
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub struct VboxMediaStart {
    /// Full WebSocket URL: configured base plus the `/ws/rustpbx/<call_id>`
    /// path from the command.
    pub url: String,
    pub call_id: String,
    pub binding_id: String,
    pub media_session_id: String,
    pub pbx_call_id: String,
    pub pbx_instance_id: String,
    pub max_buffer_ms: u64,
    pub connect_timeout_ms: u64,
}

impl VboxMediaStart {
    pub fn attachment(&self) -> MediaAttachment {
        MediaAttachment {
            call_id: self.call_id.clone(),
            binding_id: self.binding_id.clone(),
            media_session_id: self.media_session_id.clone(),
            pbx_call_id: self.pbx_call_id.clone(),
            pbx_instance_id: self.pbx_instance_id.clone(),
        }
    }
}

/// Identity of one media attachment, echoed in `start`.
#[derive(Debug, Clone, PartialEq)]
pub struct MediaAttachment {
    pub call_id: String,
    pub binding_id: String,
    pub media_session_id: String,
    pub pbx_call_id: String,
    pub pbx_instance_id: String,
}

pub fn start_message(att: &MediaAttachment) -> Value {
    json!({
        "schema_version": MEDIA_SCHEMA,
        "type": "start",
        "event": "start",
        "call_id": att.call_id,
        "binding_id": att.binding_id,
        "media_session_id": att.media_session_id,
        "pbx_call_id": att.pbx_call_id,
        "pbx_instance_id": att.pbx_instance_id,
        "outbound_framing": OUTBOUND_FRAMING,
        "media_format": {
            "encoding": "pcm_s16le",
            "sample_rate": SAMPLE_RATE,
            "channels": 1,
            "outbound_framing": OUTBOUND_FRAMING,
            "inbound_framing": "raw_pcm16le",
        },
        "capabilities": ["clear", "mark", "dtmf", "close_4000_hangup", "close_4001_orphan"],
    })
}

/// What the media session reports to the call that owns it.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize)]
pub enum MediaOutput {
    /// vbox sent `ready` for this attachment.
    Ready,
    /// vbox asked to send DTMF to the caller.
    SendDtmf { digits: String },
    /// vbox closed with `4000`: hang the SIP leg up now.
    AgentHangup { reason: String },
    /// The socket ended for any other reason.
    Closed { code: Option<u16>, reason: String },
}

/// Ask the media session to close the socket from the PBX side.
#[derive(Debug, Clone, PartialEq)]
pub struct CloseRequest {
    pub code: u16,
    pub reason: String,
}

#[derive(Debug, Clone, Copy)]
pub struct MediaLimits {
    pub max_malformed_frames: u32,
}

impl Default for MediaLimits {
    fn default() -> Self {
        Self {
            max_malformed_frames: 25,
        }
    }
}

/// Channels the media session runs with.
pub struct MediaIo {
    pub playout: SharedPlayout,
    /// Events emitted on the RTP side by [`VboxPlayoutSource`].
    pub playout_events: mpsc::Receiver<MediaEvent>,
    /// Caller PCM at 16 kHz, one 20 ms frame per message.
    pub uplink: mpsc::Receiver<Vec<i16>>,
    /// Caller DTMF digits (already de-duplicated by the session).
    pub caller_dtmf: mpsc::Receiver<String>,
    pub close_requests: mpsc::Receiver<CloseRequest>,
    pub outputs: mpsc::Sender<MediaOutput>,
    pub cancel: CancellationToken,
}

fn mark_from(value: &Value) -> Option<Mark> {
    Some(Mark {
        mark_id: value.get("mark_id")?.as_str()?.to_string(),
        generation: value.get("generation")?.as_u64()?,
        turn_id: value.get("turn_id").cloned().unwrap_or(Value::Null),
        response_id: value.get("response_id").cloned().unwrap_or(Value::Null),
    })
}

fn close_message(code: u16, reason: &str) -> Message {
    Message::Close(Some(CloseFrame {
        code: CloseCode::from(code),
        reason: reason.to_string().into(),
    }))
}

/// A blocked WebSocket write cannot outlive cancellation or its deadline.
async fn send_media<S>(sink: &mut S, message: Message, cancel: &CancellationToken) -> bool
where
    S: Sink<Message, Error = tokio_tungstenite::tungstenite::Error> + Unpin,
{
    tokio::select! {
        biased;
        _ = cancel.cancelled() => false,
        result = tokio::time::timeout(
            std::time::Duration::from_millis(MEDIA_WRITE_TIMEOUT_MS), sink.send(message)
        ) => matches!(result, Ok(Ok(()))),
    }
}

// Watermarks originate on both the WebSocket reader and RTP task. Their
// notifications can arrive out of order after releasing the playout lock.
// Serialize the current hysteresis latch, not a stale producer snapshot.
fn current_control_event(event: MediaEvent, playout: &SharedPlayout) -> MediaEvent {
    match event {
        MediaEvent::FlowControl { level: "high_water" | "low_water", .. } => {
            let p = playout.lock();
            MediaEvent::FlowControl {
                level: if p.high_water { "high_water" } else { "low_water" },
                buffered_ms: p.buffered_ms(),
            }
        }
        other => other,
    }
}

/// Run one attachment until the socket closes, the call cancels it, or the
/// PBX asks it to close. Reader and writer run as separate tasks so a slow
/// vbox reader never delays `clear` handling, and RTP pacing never delays
/// either.
pub async fn run_media_session<S>(ws: S, att: MediaAttachment, io: MediaIo, limits: MediaLimits)
where
    S: Stream<Item = Result<Message, tokio_tungstenite::tungstenite::Error>>
        + Sink<Message, Error = tokio_tungstenite::tungstenite::Error>
        + Unpin
        + Send
        + 'static,
{
    let MediaIo {
        playout,
        mut playout_events,
        mut uplink,
        mut caller_dtmf,
        mut close_requests,
        outputs,
        cancel,
    } = io;
    let (mut ws_write, mut ws_read) = ws.split();
    // Reader → writer control replies (`cleared`, `mark_*`, `flow_control`).
    let (reply_tx, mut reply_rx) = mpsc::channel::<MediaEvent>(MEDIA_CONTROL_CAPACITY);
    let session_cancel = cancel.child_token();

    let reader = {
        let playout = playout.clone();
        let outputs = outputs.clone();
        let session_cancel = session_cancel.clone();
        let call_id = att.call_id.clone();
        tokio::spawn(async move {
            let mut malformed = 0u32;
            let mut scratch = Vec::new();
            let end: MediaOutput = 'reader: loop {
                let msg = tokio::select! {
                    biased;
                    _ = session_cancel.cancelled() => break MediaOutput::Closed { code: None, reason: "cancelled".into() },
                    msg = ws_read.next() => msg,
                };
                match msg {
                    Some(Ok(Message::Binary(data))) => match decode_generation_frame(&data) {
                        Ok((generation, samples)) => {
                            playout.lock().push_pcm(generation, samples, &mut scratch);
                        }
                        Err(error) => {
                            malformed += 1;
                            tracing::debug!(%call_id, reason = error.as_str(), "vbox media: rejected frame");
                            if malformed >= limits.max_malformed_frames {
                                break MediaOutput::Closed {
                                    code: Some(1007),
                                    reason: "too_many_malformed_frames".into(),
                                };
                            }
                        }
                    },
                    Some(Ok(Message::Text(text))) => {
                        let Ok(value) = serde_json::from_str::<Value>(&text) else {
                            malformed += 1;
                            if malformed >= limits.max_malformed_frames {
                                break MediaOutput::Closed {
                                    code: Some(1007),
                                    reason: "too_many_malformed_frames".into(),
                                };
                            }
                            continue;
                        };
                        let kind = value
                            .get("type")
                            .or_else(|| value.get("event"))
                            .and_then(Value::as_str)
                            .unwrap_or("");
                        match kind {
                            "clear" => {
                                let generation = value.get("generation").and_then(Value::as_u64).unwrap_or(0);
                                playout.lock().clear(generation, &mut scratch);
                            }
                            "mark" => match mark_from(&value) {
                                Some(mark) => playout.lock().push_mark(mark, &mut scratch),
                                None => malformed += 1,
                            },
                            "dtmf" => {
                                if let Some(digits) = value.get("digit").and_then(Value::as_str) {
                                    if outputs.capacity() <= 1 || outputs.try_send(MediaOutput::SendDtmf { digits: digits.to_string() }).is_err() {
                                        break MediaOutput::Closed { code: Some(1011), reason: "output_queue_overflow".into() };
                                    }
                                }
                            }
                            "ready" => {
                                if outputs.capacity() <= 1 || outputs.try_send(MediaOutput::Ready).is_err() {
                                    break MediaOutput::Closed { code: Some(1011), reason: "output_queue_overflow".into() };
                                }
                            }
                            "stop" => {
                                break MediaOutput::Closed { code: Some(CLOSE_NORMAL), reason: "stop".into() };
                            }
                            _ => {}
                        }
                    }
                    Some(Ok(Message::Close(frame))) => {
                        let code = frame.as_ref().map(|f| u16::from(f.code));
                        let reason = frame.as_ref().map(|f| f.reason.to_string()).unwrap_or_default();
                        if code == Some(CLOSE_AGENT_HANGUP) {
                            break MediaOutput::AgentHangup { reason };
                        }
                        break MediaOutput::Closed { code, reason };
                    }
                    Some(Ok(_)) => {}
                    Some(Err(error)) => {
                        break MediaOutput::Closed { code: None, reason: format!("read_error: {error}") };
                    }
                    None => break MediaOutput::Closed { code: None, reason: "eof".into() },
                }
                if playout.lock().is_closed() {
                    let stopped = playout.lock().playback_stopped.is_cancelled();
                    if stopped {
                        session_cancel.cancelled().await;
                        break MediaOutput::Closed { code: None, reason: "cancelled".into() };
                    }
                    break MediaOutput::Closed { code: Some(1011), reason: "playout_queue_overflow".into() };
                }
                for event in scratch.drain(..) {
                    if reply_tx.try_send(event).is_err() {
                        break 'reader MediaOutput::Closed { code: Some(1011), reason: "control_queue_overflow".into() };
                    }
                }
            };
            // Never flush pending acknowledgements after failure/cancellation.
            end
        })
    };

    // Each write is deadline- and cancellation-aware, including start/close.
    // Close requests outrank controls so a busy peer cannot starve teardown.
    let start = start_message(&att).to_string();
    let writer_cancel = session_cancel.clone();
    let playback_stopped = playout.lock().playback_stopped.clone();
    let writer_playout = playout.clone();
    let writer = tokio::spawn(async move {
        if !send_media(&mut ws_write, Message::Text(start.into()), &writer_cancel).await {
            return None;
        }
        let mut pending_close: Option<CloseRequest> = None;
        loop {
            let message = tokio::select! {
                biased;
                _ = writer_cancel.cancelled() => break,
                Some(request) = close_requests.recv() => {
                    let message = close_message(request.code, &request.reason);
                    pending_close = Some(request);
                    message
                }
                _ = playback_stopped.cancelled() => {
                    let request = CloseRequest { code: CLOSE_NORMAL, reason: "playback_stopped".into() };
                    let message = close_message(request.code, &request.reason);
                    pending_close = Some(request);
                    message
                }
                Some(event) = reply_rx.recv() => Message::Text(current_control_event(event, &writer_playout).to_json().to_string().into()),
                Some(event) = playout_events.recv() => Message::Text(current_control_event(event, &writer_playout).to_json().to_string().into()),
                Some(digit) = caller_dtmf.recv() => {
                    let payload = json!({
                        "schema_version": MEDIA_SCHEMA,
                        "type": "dtmf",
                        "digit": digit,
                        "direction": "inbound",
                        "event_id": uuid::Uuid::new_v4().to_string(),
                        "duration_ms": 100,
                    });
                    Message::Text(payload.to_string().into())
                }
                Some(samples) = uplink.recv() => Message::Binary(encode_pcm16le(&samples).into()),
                else => break,
            };
            if !send_media(&mut ws_write, message, &writer_cancel).await {
                return None;
            }
            if pending_close.is_some() {
                break;
            }
        }
        // Dropping the sink closes the transport; a graceful close must not
        // wait forever for a peer that stopped reading.
        tokio::select! {
            biased;
            _ = writer_cancel.cancelled() => {}
            _ = tokio::time::timeout(std::time::Duration::from_millis(MEDIA_WRITE_TIMEOUT_MS), ws_write.close()) => {}
        }
        pending_close
    });

    let mut reader = reader;
    let mut writer = writer;
    let output = tokio::select! {
        result = &mut reader => {
            let end = result.unwrap_or(MediaOutput::Closed { code: None, reason: "reader_panicked".into() });
            session_cancel.cancel();
            let _ = writer.await;
            end
        }
        result = &mut writer => {
            session_cancel.cancel();
            let _ = reader.await;
            match result.ok().flatten() {
                Some(request) => MediaOutput::Closed { code: Some(request.code), reason: request.reason },
                None => MediaOutput::Closed { code: None, reason: "write_failed".into() },
            }
        }
    };
    playout.lock().close();
    // The reader always reserves one slot for this terminal outcome.
    let _ = outputs.try_send(output);
}

#[cfg(test)]
mod tests {
    use super::*;

    fn tone(value: i16, samples: usize) -> Vec<i16> {
        vec![value; samples]
    }

    fn mark(id: &str, generation: u64) -> Mark {
        Mark {
            mark_id: id.to_string(),
            generation,
            turn_id: json!("turn-1"),
            response_id: json!("resp-1"),
        }
    }

    #[test]
    fn frame_codec_round_trips_and_rejects_bad_input() {
        let samples = vec![1i16, -2, 0x1234, i16::MIN];
        let raw = encode_generation_frame(7, &samples);
        assert_eq!(&raw[..8], &7u64.to_le_bytes());
        assert_eq!(&raw[8..10], &[1, 0]);
        assert_eq!(decode_generation_frame(&raw).unwrap(), (7, samples));
        assert_eq!(decode_generation_frame(&raw[..9]), Err(FrameError::TooShort));
        assert_eq!(decode_generation_frame(&raw[..11]), Err(FrameError::OddPcmLength));
        let oversized = encode_generation_frame(1, &vec![0; MAX_PCM_PAYLOAD_BYTES / 2 + 1]);
        assert_eq!(decode_generation_frame(&oversized), Err(FrameError::Oversized));
        let full = encode_generation_frame(u64::MAX, &vec![0; SAMPLES_PER_20MS]);
        assert_eq!(full.len(), 648);
        assert_eq!(decode_generation_frame(&full).unwrap().0, u64::MAX);
    }

    #[test]
    fn clear_purges_older_generations_and_reports_counts() {
        let mut p = Playout::new(10_000);
        let mut out = Vec::new();
        for _ in 0..200 {
            assert_eq!(p.push_pcm(1, tone(100, 320), &mut out), PushOutcome::Queued);
        }
        p.push_mark(mark("m1", 1), &mut out);
        assert_eq!(p.buffered_ms(), 4020);
        // Play two ticks first.
        let mut buf = vec![0i16; 320];
        assert_eq!(p.read(&mut buf, &mut out), 320);
        assert_eq!(p.read(&mut buf, &mut out), 320);
        out.clear();
        p.clear(2, &mut out);
        assert_eq!(p.buffered_ms(), 0);
        assert_eq!(p.active_generation(), 2);
        assert_eq!(out[0], MediaEvent::MarkCleared { mark: mark("m1", 1) });
        assert!(matches!(
            out[1],
            MediaEvent::Cleared { generation: 2, discarded_frames: 199, discarded_ms: 3980 }
        ));
        // Stale frames after clear are rejected; current generation plays.
        assert_eq!(p.push_pcm(1, tone(100, 320), &mut out), PushOutcome::Stale);
        assert_eq!(p.push_pcm(2, tone(200, 320), &mut out), PushOutcome::Queued);
        assert_eq!(p.read(&mut buf, &mut out), 320);
        assert!(buf.iter().all(|s| *s == 200));
    }

    #[test]
    fn clear_keeps_frames_of_the_new_generation() {
        let mut p = Playout::new(10_000);
        let mut out = Vec::new();
        p.push_pcm(3, tone(3, 320), &mut out);
        p.push_pcm(4, tone(4, 320), &mut out);
        p.clear(4, &mut out);
        let mut buf = vec![0i16; 320];
        assert_eq!(p.read(&mut buf, &mut out), 320);
        assert!(buf.iter().all(|s| *s == 4));
    }

    #[test]
    fn higher_generation_without_clear_does_not_cut_previous_response() {
        let mut p = Playout::new(10_000);
        let mut out = Vec::new();
        p.push_pcm(1, tone(1, 320), &mut out);
        p.push_pcm(2, tone(2, 320), &mut out);
        let mut buf = vec![0i16; 320];
        p.read(&mut buf, &mut out);
        assert!(buf.iter().all(|s| *s == 1));
        p.read(&mut buf, &mut out);
        assert!(buf.iter().all(|s| *s == 2));
    }

    #[test]
    fn mark_is_played_only_after_successful_transmission() {
        let mut p = Playout::new(10_000);
        let mut out = Vec::new();
        p.push_pcm(1, tone(9, 640), &mut out);
        p.push_mark(mark("m1", 1), &mut out);
        assert!(out.is_empty(), "mark must not be acknowledged before playout");
        let mut buf = vec![0i16; 320];
        p.read(&mut buf, &mut out);
        p.prepare_rtp(100);
        p.transmitted(100, true, &mut out);
        assert!(out.is_empty(), "half the audio is still queued");
        p.read(&mut buf, &mut out);
        p.prepare_rtp(200);
        p.transmitted(200, true, &mut out);
        assert!(out.is_empty(), "response tail still needs its guard packet");
        p.read(&mut buf, &mut out);
        p.prepare_rtp(300);
        assert!(out.is_empty(), "consumption does not prove transmission");
        p.transmitted(300, true, &mut out);
        assert!(matches!(&out[0], MediaEvent::MarkPlayed { mark: m, .. } if m.mark_id == "m1"));
    }

    #[test]
    fn mark_on_short_tail_pads_the_tick_instead_of_waiting() {
        let mut p = Playout::new(10_000);
        let mut out = Vec::new();
        p.push_pcm(1, tone(5, 100), &mut out);
        p.push_mark(mark("tail", 1), &mut out);
        p.push_pcm(2, tone(6, 320), &mut out);
        let mut buf = vec![0i16; 320];
        assert_eq!(p.read(&mut buf, &mut out), 320);
        assert!(buf[..100].iter().all(|s| *s == 5));
        assert!(buf[100..].iter().all(|s| *s == 0));
        p.prepare_rtp(100);
        p.transmitted(100, true, &mut out);
        assert!(out.is_empty());
        p.read(&mut buf, &mut out);
        p.prepare_rtp(200);
        p.transmitted(200, true, &mut out);
        assert!(matches!(&out[0], MediaEvent::MarkPlayed { mark: m, .. } if m.mark_id == "tail"));
        assert_eq!(p.read(&mut buf, &mut out), 320);
        assert!(buf.iter().all(|s| *s == 6));
    }

    #[test]
    fn mark_with_nothing_queued_is_played_and_stale_mark_is_cleared() {
        let mut p = Playout::new(10_000);
        let mut out = Vec::new();
        p.clear(5, &mut out);
        out.clear();
        p.push_mark(mark("now", 5), &mut out);
        p.push_mark(mark("old", 4), &mut out);
        assert_eq!(out.pop().unwrap(), MediaEvent::MarkCleared { mark: mark("old", 4) });
        assert!(out.is_empty());
        p.read(&mut [0; 320], &mut out);
        p.prepare_rtp(100);
        p.transmitted(100, true, &mut out);
        assert!(matches!(&out[0], MediaEvent::MarkPlayed { mark: m, .. } if m.mark_id == "now"));
    }

    #[test]
    fn bounded_buffer_reports_watermarks_and_overflow() {
        let mut p = Playout::new(100); // 1600 samples
        let mut out = Vec::new();
        for _ in 0..4 {
            p.push_pcm(1, tone(1, 320), &mut out);
        }
        assert!(out.iter().any(|e| matches!(e, MediaEvent::FlowControl { level: "high_water", .. })));
        p.push_pcm(1, tone(1, 320), &mut out);
        assert_eq!(p.push_pcm(1, tone(1, 320), &mut out), PushOutcome::Overflow);
        assert_eq!(p.overflow_frames, 1);
        out.clear();
        let mut buf = vec![0i16; 320];
        for _ in 0..3 {
            p.read(&mut buf, &mut out);
        }
        assert!(p.is_closed(), "overflow cannot silently drop audio then confirm a mark");
        assert!(out.is_empty());
    }

    #[test]
    fn underrun_returns_zero_so_egress_sends_silence() {
        let mut p = Playout::new(1000);
        let mut out = Vec::new();
        let mut buf = vec![7i16; 320];
        assert_eq!(p.read(&mut buf, &mut out), 0);
    }


    #[tokio::test]
    async fn real_transport_success_failure_and_clear_gate_marks() {
        use rustrtc::transports::{ice::{conn::IceConn, IceSocketWrapper}, rtp::RtpTransport};
        use tokio::{net::UdpSocket, sync::watch};
        for mode in 0..3 {
            let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
            let (_socket_tx, socket_rx) = watch::channel(Some(IceSocketWrapper::Udp(Arc::new(socket))));
            let transport = RtpTransport::new(IceConn::new(socket_rx, peer.local_addr().unwrap(), None), mode == 1);
            let playout = Arc::new(parking_lot::Mutex::new(Playout::new(1000)));
            let (events, mut rx) = mpsc::channel(128);
            let cancel = CancellationToken::new();
            transport.add_observer(Arc::new(VboxTransmissionObserver { playout: Arc::downgrade(&playout), events: events.clone(), cancel: cancel.clone(), ssrc: 42, payload_type: 0 }));
            let mut out = Vec::new();
            playout.lock().push_pcm(1, tone(4, 320), &mut out);
            playout.lock().push_mark(mark("wire", 1), &mut out);
            let mut source = VboxPlayoutSource::new(playout.clone(), events, cancel.clone());
            source.read_samples(&mut [0; 320]);
            source.read_samples(&mut [0; 320]);
            source.prepare_rtp(160);
            assert!(rx.try_recv().is_err(), "reading and packet enqueue never confirm");
            if mode == 2 { playout.lock().clear(2, &mut out); }
            let packet = rustrtc::rtp::RtpPacket::new(rustrtc::rtp::RtpHeader::new(0, 1, 160, 42), vec![9;160]);
            let result = transport.send_rtp(packet).await;
            if mode == 1 {
                assert!(result.is_err(), "SRTP without keys fails actual transport");
                assert!(cancel.is_cancelled());
                assert!(rx.try_recv().is_err());
            } else {
                assert!(result.unwrap() > 0);
                let mut bytes = [0; 1500];
                assert!(tokio::time::timeout(std::time::Duration::from_secs(1), peer.recv(&mut bytes)).await.unwrap().unwrap() > 0);
                if mode == 0 { assert!(matches!(rx.try_recv().unwrap(), MediaEvent::MarkPlayed { .. })); }
                else { assert!(rx.try_recv().is_err(), "clear fences late successful send"); }
            }
        }
    }

    #[tokio::test]
    async fn late_mark_flushes_fir_tail_over_real_pcmu_rtp_and_ignores_dtmf_collision() {
        use rustrtc::transports::{ice::{conn::IceConn, IceSocketWrapper}, rtp::RtpTransport};
        use tokio::{net::UdpSocket, sync::watch};
        use crate::media::audio_source::ResamplingAudioSource;
        let socket = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let peer = UdpSocket::bind("127.0.0.1:0").await.unwrap();
        let (_tx, rx) = watch::channel(Some(IceSocketWrapper::Udp(Arc::new(socket))));
        let transport = RtpTransport::new(IceConn::new(rx, peer.local_addr().unwrap(), None), false);
        let playout = Arc::new(parking_lot::Mutex::new(Playout::new(1000)));
        let (events, mut receipts) = mpsc::channel(128);
        let cancel = CancellationToken::new();
        transport.add_observer(Arc::new(VboxTransmissionObserver { playout: Arc::downgrade(&playout), events: events.clone(), cancel: cancel.clone(), ssrc: 42, payload_type: 0 }));
        let raw = VboxPlayoutSource::new(playout.clone(), events, cancel);
        let mut source = ResamplingAudioSource::new(Box::new(raw), 8000);
        let mut encoder = audio_codec::create_encoder(audio_codec::CodecType::PCMU);
        let mut decoder = audio_codec::create_decoder(audio_codec::CodecType::PCMU);
        let mut impulse = vec![0; 320];
        impulse[319] = 30000;
        playout.lock().push_pcm(1, impulse, &mut Vec::new());
        let mut pcm = [0; 160];
        assert_eq!(source.read_samples(&mut pcm), 160);
        source.prepare_rtp(160);
        let packet = rustrtc::rtp::RtpPacket::new(rustrtc::rtp::RtpHeader::new(0, 1, 160, 42), encoder.encode(&pcm).to_vec());
        transport.send_rtp(packet).await.unwrap();
        assert!(receipts.try_recv().is_err());
        // The mark arrives after the preceding audio's write has completed.
        playout.lock().push_mark(mark("late", 1), &mut Vec::new());
        assert_eq!(source.read_samples(&mut pcm), 160);
        assert!(pcm.iter().any(|sample| sample.abs() > 200), "guard carries the otherwise-lost FIR tail");
        source.prepare_rtp(320);
        let dtmf = rustrtc::rtp::RtpPacket::new(rustrtc::rtp::RtpHeader::new(101, 2, 320, 42), vec![1, 0, 0, 160]);
        transport.send_rtp(dtmf).await.unwrap();
        assert!(receipts.try_recv().is_err(), "same timestamp/SSRC DTMF cannot release an audio mark");
        let guard = rustrtc::rtp::RtpPacket::new(rustrtc::rtp::RtpHeader::new(0, 3, 320, 42), encoder.encode(&pcm).to_vec());
        transport.send_rtp(guard).await.unwrap();
        assert!(matches!(receipts.try_recv().unwrap(), MediaEvent::MarkPlayed { mark: m, .. } if m.mark_id == "late"));
        let mut bytes = [0; 1500];
        for _ in 0..3 {
            let n = tokio::time::timeout(std::time::Duration::from_secs(1), peer.recv(&mut bytes)).await.unwrap().unwrap();
            if bytes[1] & 0x7f == 0 && u32::from_be_bytes(bytes[4..8].try_into().unwrap()) == 320 {
                let decoded = decoder.decode(&bytes[12..n]);
                assert!(decoded.iter().any(|sample| sample.abs() > 200), "tail must reach actual remote RTP payload");
                return;
            }
        }
        panic!("guard RTP packet was not received");
    }

    #[test]
    fn failed_missing_or_out_of_order_transmission_never_confirms() {
        for mode in 0..3 {
            let mut p = Playout::new(1000);
            let mut out = Vec::new();
            p.push_pcm(1, tone(4, 320), &mut out);
            p.push_mark(mark("confirm", 1), &mut out);
            p.read(&mut [0; 320], &mut out);
            p.read(&mut [0; 320], &mut out);
            p.prepare_rtp(100);
            match mode {
                0 => p.transmitted(100, false, &mut out),
                1 => { p.in_flight.front_mut().unwrap().1 -= std::time::Duration::from_secs(2); p.read(&mut [0; 320], &mut out); }
                _ => { p.prepare_rtp(200); p.transmitted(200, true, &mut out); }
            }
            p.transmitted(100, true, &mut out);
            assert!(p.is_closed());
            assert!(!out.iter().any(|e| matches!(e, MediaEvent::MarkPlayed { .. })));
        }
    }

    #[test]
    fn clear_invalidates_mark_waiting_for_transport() {
        let mut p = Playout::new(1000);
        let mut out = Vec::new();
        p.push_pcm(1, tone(4, 320), &mut out);
        p.push_mark(mark("confirm", 1), &mut out);
        p.read(&mut [0; 320], &mut out);
        p.read(&mut [0; 320], &mut out);
        p.prepare_rtp(100);
        p.clear(2, &mut out);
        assert!(out.iter().any(|e| matches!(e, MediaEvent::MarkCleared { .. })));
        out.clear();
        p.transmitted(100, true, &mut out);
        assert!(out.is_empty());
    }

    // ── WebSocket session over a real loopback socket ─────────────────────

    struct Harness {
        vbox: tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        playout: SharedPlayout,
        playout_events_tx: mpsc::Sender<MediaEvent>,
        uplink_tx: mpsc::Sender<Vec<i16>>,
        dtmf_tx: mpsc::Sender<String>,
        close_tx: mpsc::Sender<CloseRequest>,
        outputs: mpsc::Receiver<MediaOutput>,
        cancel: CancellationToken,
        task: tokio::task::JoinHandle<()>,
    }

    fn attachment() -> MediaAttachment {
        MediaAttachment {
            call_id: "call-1".into(),
            binding_id: "bind-1".into(),
            media_session_id: "media-1".into(),
            pbx_call_id: "pbx-1".into(),
            pbx_instance_id: "pbx-a".into(),
        }
    }

    async fn harness() -> Harness {
        harness_with_slow_reader(false).await
    }

    async fn harness_with_slow_reader(slow: bool) -> Harness {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            if slow {
                socket2::SockRef::from(&stream).set_recv_buffer_size(1024).unwrap();
            }
            tokio_tungstenite::accept_async(stream).await.unwrap()
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        if slow {
            socket2::SockRef::from(&stream).set_send_buffer_size(1024).unwrap();
        }
        let (client, _) = tokio_tungstenite::client_async(format!("ws://{addr}/ws/rustpbx/call-1"), stream).await.unwrap();
        let vbox = accept.await.unwrap();
        let playout: SharedPlayout = Arc::new(parking_lot::Mutex::new(Playout::new(10_000)));
        let (playout_events_tx, playout_events) = mpsc::channel(MEDIA_CONTROL_CAPACITY);
        let (uplink_tx, uplink) = mpsc::channel(50);
        let (dtmf_tx, caller_dtmf) = mpsc::channel(MEDIA_CONTROL_CAPACITY);
        let (close_tx, close_requests) = mpsc::channel(4);
        let (outputs_tx, outputs) = mpsc::channel(MEDIA_CONTROL_CAPACITY + 1);
        let cancel = CancellationToken::new();
        let io = MediaIo {
            playout: playout.clone(),
            playout_events,
            uplink,
            caller_dtmf,
            close_requests,
            outputs: outputs_tx,
            cancel: cancel.clone(),
        };
        let task = tokio::spawn(run_media_session(client, attachment(), io, MediaLimits::default()));
        Harness {
            vbox,
            playout,
            playout_events_tx,
            uplink_tx,
            dtmf_tx,
            close_tx,
            outputs,
            cancel,
            task,
        }
    }

    async fn next_text(vbox: &mut tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>) -> Value {
        loop {
            let msg = tokio::time::timeout(std::time::Duration::from_secs(2), vbox.next())
                .await
                .expect("message in time")
                .expect("socket open")
                .expect("valid frame");
            if let Message::Text(t) = msg {
                return serde_json::from_str(&t).unwrap();
            }
        }
    }

    #[tokio::test]
    async fn delayed_high_water_cannot_repause_drained_playout_over_real_websocket() {
        let mut h = harness().await;
        let _ = next_text(&mut h.vbox).await;
        let mut delayed_high = Vec::new();
        let mut low = Vec::new();
        {
            let mut p = h.playout.lock();
            // Hold the producer's notification after releasing the shared lock,
            // exactly the scheduling window between push_pcm/read and flush.
            p.push_pcm(1, vec![1; SAMPLE_RATE as usize * 8], &mut delayed_high);
            assert!(p.high_water);
            p.read(&mut vec![0; SAMPLE_RATE as usize * 4], &mut low);
            assert!(!p.high_water);
        }
        assert!(matches!(delayed_high.as_slice(), [MediaEvent::FlowControl { level: "high_water", .. }]));
        assert!(matches!(low.as_slice(), [MediaEvent::FlowControl { level: "low_water", .. }]));
        h.playout_events_tx.send(low.pop().unwrap()).await.unwrap();
        assert_eq!(next_text(&mut h.vbox).await["level"], "low_water");
        h.playout_events_tx.send(delayed_high.pop().unwrap()).await.unwrap();
        let latest = next_text(&mut h.vbox).await;
        h.cancel.cancel();
        h.task.await.unwrap();
        assert_eq!(latest["level"], "low_water", "late producer notification must not re-pause an already drained queue");
        assert_eq!(latest["buffered_ms"], 4000);
    }

    #[tokio::test]
    async fn delayed_watermarks_follow_clear_and_rehigh_hysteresis_over_real_websocket() {
        let mut h = harness().await;
        let _ = next_text(&mut h.vbox).await;
        let mut high = Vec::new();
        let mut cleared = Vec::new();
        {
            let mut p = h.playout.lock();
            p.push_pcm(1, vec![1; SAMPLE_RATE as usize * 8], &mut high);
            p.clear(2, &mut cleared);
            assert!(!p.high_water);
        }
        h.playout_events_tx.send(high.pop().unwrap()).await.unwrap();
        let low_wire = next_text(&mut h.vbox).await;
        assert_eq!(low_wire["level"], "low_water");
        assert_eq!(low_wire["buffered_ms"], 0);
        let low = cleared.pop().unwrap();
        assert!(matches!(low, MediaEvent::FlowControl { level: "low_water", .. }));
        {
            let mut p = h.playout.lock();
            p.push_pcm(2, vec![2; SAMPLE_RATE as usize * 8], &mut Vec::new());
            p.read(&mut vec![0; SAMPLE_RATE as usize * 2], &mut Vec::new());
            // Between low and high thresholds, retain the high latch.
            assert!(p.high_water);
        }
        h.playout_events_tx.send(low).await.unwrap();
        let high_wire = next_text(&mut h.vbox).await;
        assert_eq!(high_wire["level"], "high_water");
        assert_eq!(high_wire["buffered_ms"], 6000);
        h.playout_events_tx.send(MediaEvent::FlowControl { level: "overflow", buffered_ms: 123 }).await.unwrap();
        let overflow = next_text(&mut h.vbox).await;
        assert_eq!(overflow["level"], "overflow");
        assert_eq!(overflow["buffered_ms"], 123);
        h.cancel.cancel();
        h.task.await.unwrap();
    }

    #[tokio::test]
    async fn session_sends_start_then_caller_audio_and_dtmf() {
        let mut h = harness().await;
        let start = next_text(&mut h.vbox).await;
        assert_eq!(start["type"], "start");
        assert_eq!(start["call_id"], "call-1");
        assert_eq!(start["media_session_id"], "media-1");
        assert_eq!(start["outbound_framing"], OUTBOUND_FRAMING);
        h.uplink_tx.send(vec![0x0102i16; SAMPLES_PER_20MS]).await.unwrap();
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), h.vbox.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Binary(bytes) = msg else { panic!("expected caller PCM") };
        assert_eq!(bytes.len(), 640);
        assert_eq!(&bytes[..2], &[0x02, 0x01], "little endian");
        h.dtmf_tx.try_send("5".into()).unwrap();
        let dtmf = next_text(&mut h.vbox).await;
        assert_eq!(dtmf["type"], "dtmf");
        assert_eq!(dtmf["digit"], "5");
        assert!(dtmf["event_id"].as_str().is_some());
        h.cancel.cancel();
        let _ = h.task.await;
    }

    #[tokio::test]
    async fn clear_is_applied_immediately_under_saturated_playout() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        // Queue 4 s of generation 1 while nobody consumes playout.
        for _ in 0..200 {
            h.vbox
                .send(Message::Binary(encode_generation_frame(1, &tone(1000, 320)).into()))
                .await
                .unwrap();
        }
        h.vbox
            .send(Message::Text(json!({"type":"mark","mark_id":"m1","generation":1,"turn_id":"t1"}).to_string().into()))
            .await
            .unwrap();
        // Wait until the reader has queued everything.
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.playout.lock().buffered_ms() < 4000 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("frames queued");
        let sent = std::time::Instant::now();
        h.vbox
            .send(Message::Text(json!({"type":"clear","generation":2,"sent_at_ms":1}).to_string().into()))
            .await
            .unwrap();
        let first = next_text(&mut h.vbox).await;
        let second = next_text(&mut h.vbox).await;
        let elapsed = sent.elapsed();
        assert_eq!(first["type"], "mark_cleared");
        assert_eq!(first["mark_id"], "m1");
        assert_eq!(second["type"], "cleared");
        assert_eq!(second["generation"], 2);
        assert_eq!(second["discarded_ms"], 4020);
        assert!(elapsed < std::time::Duration::from_millis(200), "clear took {elapsed:?}");
        assert_eq!(h.playout.lock().buffered_ms(), 0);
        h.cancel.cancel();
        let _ = h.task.await;
    }

    #[tokio::test]
    async fn consumption_alone_never_emits_mark_played() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        h.vbox
            .send(Message::Binary(encode_generation_frame(1, &tone(10, 320)).into()))
            .await
            .unwrap();
        h.vbox
            .send(Message::Text(json!({"type":"mark","mark_id":"confirm-1","generation":1,"turn_id":"t9","response_id":"r9"}).to_string().into()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.playout.lock().buffered_ms() < 20 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .unwrap();
        // Nothing is acknowledged until the egress consumes the audio.
        assert!(
            tokio::time::timeout(std::time::Duration::from_millis(100), h.vbox.next()).await.is_err(),
            "no mark_played before playout"
        );
        let mut source = VboxPlayoutSource::new(h.playout.clone(), h.playout_events_tx.clone(), h.cancel.clone());
        let mut buf = vec![0i16; 320];
        assert_eq!(source.read_samples(&mut buf), 320);
        assert!(tokio::time::timeout(std::time::Duration::from_millis(100), h.vbox.next()).await.is_err());
        source.read_samples(&mut buf);
        source.prepare_rtp(123);
        let mut events = Vec::new();
        h.playout.lock().transmitted(123, true, &mut events);
        for event in events { h.playout_events_tx.try_send(event).unwrap(); }
        let played = next_text(&mut h.vbox).await;
        assert_eq!(played["type"], "mark_played");
        assert_eq!(played["mark_id"], "confirm-1");
        assert_eq!(played["generation"], 1);
        assert_eq!(played["turn_id"], "t9");
        assert_eq!(played["response_id"], "r9");
        h.cancel.cancel();
        let _ = h.task.await;
    }

    #[tokio::test]
    async fn dropping_source_revokes_marks_and_gracefully_closes_real_socket() {
        let mut h = harness().await;
        let _ = next_text(&mut h.vbox).await;
        let mut out = Vec::new();
        h.playout.lock().push_mark(mark("never-confirm", 1), &mut out);
        let mut source = VboxPlayoutSource::new(h.playout.clone(), h.playout_events_tx.clone(), h.cancel.clone());
        source.read_samples(&mut [0; 320]);
        source.prepare_rtp(100);
        drop(source);
        h.playout.lock().transmitted(100, true, &mut out);
        assert!(!out.iter().any(|event| matches!(event, MediaEvent::MarkPlayed { .. })));
        let frame = tokio::time::timeout(std::time::Duration::from_secs(2), h.vbox.next()).await.unwrap().unwrap().unwrap();
        assert!(matches!(frame, Message::Close(Some(frame)) if u16::from(frame.code) == CLOSE_NORMAL));
        let _ = h.task.await;
    }

    #[tokio::test]
    async fn close_4000_from_vbox_is_reported_as_agent_hangup() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        h.vbox.send(close_message(4000, "agent_hangup")).await.unwrap();
        let out = tokio::time::timeout(std::time::Duration::from_secs(2), h.outputs.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out, MediaOutput::AgentHangup { reason: "agent_hangup".into() });
        let _ = h.task.await;
        assert!(h.playout.lock().is_closed());
    }

    #[tokio::test]
    async fn pbx_close_request_sends_the_requested_code() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        h.close_tx
            .send(CloseRequest { code: CLOSE_RWI_ORPHAN_TIMEOUT, reason: "rwi_orphan_timeout".into() })
            .await
            .unwrap();
        let msg = tokio::time::timeout(std::time::Duration::from_secs(2), h.vbox.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        let Message::Close(Some(frame)) = msg else { panic!("expected close, got {msg:?}") };
        assert_eq!(u16::from(frame.code), 4001);
        assert_eq!(frame.reason.as_str(), "rwi_orphan_timeout");
        let out = tokio::time::timeout(std::time::Duration::from_secs(2), h.outputs.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out, MediaOutput::Closed { code: Some(4001), reason: "rwi_orphan_timeout".into() });
        let _ = h.task.await;
    }

    #[tokio::test]
    async fn malformed_frames_are_dropped_until_the_limit() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        // Odd-length and short frames are ignored; a valid frame still plays.
        h.vbox.send(Message::Binary(vec![1u8; 11].into())).await.unwrap();
        h.vbox.send(Message::Binary(vec![1u8; 3].into())).await.unwrap();
        h.vbox
            .send(Message::Binary(encode_generation_frame(0, &tone(3, 320)).into()))
            .await
            .unwrap();
        tokio::time::timeout(std::time::Duration::from_secs(2), async {
            while h.playout.lock().buffered_ms() < 20 {
                tokio::time::sleep(std::time::Duration::from_millis(5)).await;
            }
        })
        .await
        .expect("valid frame queued after malformed ones");
        for _ in 0..30 {
            if h.vbox.send(Message::Binary(vec![9u8; 5].into())).await.is_err() {
                break;
            }
        }
        let out = tokio::time::timeout(std::time::Duration::from_secs(2), h.outputs.recv())
            .await
            .unwrap()
            .unwrap();
        assert_eq!(out, MediaOutput::Closed { code: Some(1007), reason: "too_many_malformed_frames".into() });
        let _ = h.task.await;
    }

    #[tokio::test]
    async fn agent_dtmf_and_ready_are_reported() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        h.vbox.send(Message::Text(json!({"type":"ready"}).to_string().into())).await.unwrap();
        h.vbox.send(Message::Text(json!({"type":"dtmf","digit":"9"}).to_string().into())).await.unwrap();
        let first = tokio::time::timeout(std::time::Duration::from_secs(2), h.outputs.recv()).await.unwrap().unwrap();
        let second = tokio::time::timeout(std::time::Duration::from_secs(2), h.outputs.recv()).await.unwrap().unwrap();
        assert_eq!(first, MediaOutput::Ready);
        assert_eq!(second, MediaOutput::SendDtmf { digits: "9".into() });
        h.cancel.cancel();
        let _ = h.task.await;
    }
    // Exercise the production write helper against an actual TCP/WebSocket
    // peer that completes its handshake and then stops reading entirely.
    async fn stalled_socket() -> (
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
        tokio_tungstenite::WebSocketStream<tokio::net::TcpStream>,
    ) {
        let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
        let addr = listener.local_addr().unwrap();
        let accept = tokio::spawn(async move {
            let (stream, _) = listener.accept().await.unwrap();
            socket2::SockRef::from(&stream).set_recv_buffer_size(1024).unwrap();
            tokio_tungstenite::accept_async(stream).await.unwrap()
        });
        let stream = tokio::net::TcpStream::connect(addr).await.unwrap();
        socket2::SockRef::from(&stream).set_send_buffer_size(1024).unwrap();
        let (sender, _) = tokio_tungstenite::client_async(format!("ws://{addr}/stalled"), stream).await.unwrap();
        (sender, accept.await.unwrap())
    }

    #[tokio::test]
    async fn stalled_real_websocket_write_obeys_cancellation() {
        let (mut sender, _nonreading_peer) = stalled_socket().await;
        let cancel = CancellationToken::new();
        let writer_cancel = cancel.clone();
        let task = tokio::spawn(async move {
            send_media(&mut sender, Message::Binary(vec![0; 16 * 1024 * 1024].into()), &writer_cancel).await
        });
        tokio::time::sleep(std::time::Duration::from_millis(50)).await;
        assert!(!task.is_finished(), "real socket must be blocked before cancellation");
        cancel.cancel();
        assert!(!tokio::time::timeout(std::time::Duration::from_millis(250), task).await.unwrap().unwrap());
    }

    #[tokio::test]
    async fn stalled_real_websocket_write_obeys_deadline() {
        let (mut sender, _nonreading_peer) = stalled_socket().await;
        let cancel = CancellationToken::new();
        let began = std::time::Instant::now();
        let sent = tokio::time::timeout(std::time::Duration::from_secs(3),
            send_media(&mut sender, Message::Binary(vec![0; 16 * 1024 * 1024].into()), &cancel)
        ).await.unwrap();
        assert!(!sent, "a stalled write must not report success");
        assert!(began.elapsed() >= std::time::Duration::from_millis(MEDIA_WRITE_TIMEOUT_MS));
    }

    #[tokio::test]
    async fn pending_mark_flood_closes_real_media_session_without_played_ack() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        h.vbox.send(Message::Binary(encode_generation_frame(1, &tone(9, 320)).into())).await.unwrap();
        // No egress consumes this audio; every mark remains pending.
        for n in 0..=MAX_PENDING_MARKS {
            h.vbox.send(Message::Text(json!({"type":"mark", "mark_id": format!("m{n}"), "generation":1})
                .to_string().into())).await.unwrap();
        }
        let output = tokio::time::timeout(std::time::Duration::from_secs(2), h.outputs.recv()).await.unwrap().unwrap();
        assert_eq!(output, MediaOutput::Closed { code: Some(1011), reason: "playout_queue_overflow".into() });
        tokio::time::timeout(std::time::Duration::from_secs(2), h.task).await.unwrap().unwrap();
        assert!(h.playout.lock().is_closed());
        while let Ok(Some(Ok(message))) = tokio::time::timeout(std::time::Duration::from_millis(100), h.vbox.next()).await {
            if let Message::Text(text) = message {
                let value: Value = serde_json::from_str(&text).unwrap();
                assert_ne!(value["type"], "mark_played");
            }
        }
    }

    #[tokio::test]
    async fn control_reply_flood_ends_session_when_real_peer_stops_reading() {
        let mut h = harness_with_slow_reader(true).await;
        let _start = next_text(&mut h.vbox).await;
        h.playout.lock().clear(2, &mut Vec::new());
        // Each stale mark creates a large mark_cleared reply. The real socket
        // backpressures the writer; the independent reader fills its bounded
        // reply channel and must terminate instead of waiting for the writer.
        let message = Message::Text(json!({"type":"mark", "mark_id":"x".repeat(60_000), "generation":1}).to_string().into());
        let flood = tokio::spawn(async move {
            for _ in 0..1000 {
                if h.vbox.send(message.clone()).await.is_err() { break; }
            }
            h.vbox
        });
        let output = tokio::time::timeout(std::time::Duration::from_secs(3), h.outputs.recv()).await.unwrap().unwrap();
        assert_eq!(output, MediaOutput::Closed { code: Some(1011), reason: "control_queue_overflow".into() });
        tokio::time::timeout(std::time::Duration::from_secs(2), h.task).await.unwrap().unwrap();
        assert!(h.playout.lock().is_closed());
        flood.abort();
        let _ = flood.await;
    }

    #[tokio::test]
    async fn output_flood_reserves_terminal_outcome_slot() {
        let mut h = harness().await;
        let _start = next_text(&mut h.vbox).await;
        for _ in 0..=MEDIA_CONTROL_CAPACITY {
            h.vbox.send(Message::Text(json!({"type":"ready"}).to_string().into())).await.unwrap();
        }
        tokio::time::timeout(std::time::Duration::from_secs(2), h.task).await.unwrap().unwrap();
        assert!(h.playout.lock().is_closed());
        for _ in 0..MEDIA_CONTROL_CAPACITY {
            assert_eq!(h.outputs.recv().await, Some(MediaOutput::Ready));
        }
        assert_eq!(h.outputs.recv().await, Some(MediaOutput::Closed { code: Some(1011), reason: "output_queue_overflow".into() }));
        assert!(h.outputs.recv().await.is_none());
    }

    #[test]
    fn rtp_control_queue_overflow_closes_playout_and_cancels_attachment() {
        let playout = Arc::new(parking_lot::Mutex::new(Playout::new(20)));
        let (tx, mut rx) = mpsc::channel(1);
        let cancel = CancellationToken::new();
        tx.try_send(MediaEvent::FlowControl { level: "high_water", buffered_ms:20 }).unwrap();
        playout.lock().push_pcm(1, tone(9, 320), &mut Vec::new());
        let mut source = VboxPlayoutSource::new(playout.clone(), tx, cancel.clone());
        let mut pcm = [0;320];
        source.read_samples(&mut pcm); // low_water cannot fit in the full channel
        assert!(playout.lock().is_closed());
        assert!(cancel.is_cancelled());
        assert!(matches!(rx.try_recv(), Ok(MediaEvent::FlowControl { level: "high_water", .. })));
        assert!(rx.try_recv().is_err());
        assert_eq!(source.read_samples(&mut pcm), 0);
    }

}
