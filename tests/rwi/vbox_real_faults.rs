//! Parameterized real PBX transport load. The controller/provider boundary is
//! deterministic; this is not vbox admission, carrier or AI-provider acceptance.
use super::*;

#[derive(Default, serde::Serialize)]
struct AudioStats {
    uplink_tone_frames: usize,
    downlink_tone_frames: usize,
    cross_call_frames: usize,
    max_uplink_gap_ms: u128,
    max_downlink_gap_ms: u128,
    #[serde(skip)] last_up: Option<Instant>,
    #[serde(skip)] last_down: Option<Instant>,
    closed: bool,
}
struct LiveCall {
    id: String,
    direction: &'static str,
    setup_ms: u128,
    ua: Arc<TestUa>,
    rtp: CallerRtp,
    stats: Arc<parking_lot::Mutex<AudioStats>>,
    close: mpsc::Sender<u16>,
    pump: tokio::task::JoinHandle<()>,
}
fn setting(name: &str, default: u64) -> u64 {
    std::env::var(name).map(|v| v.parse().expect("numeric load parameter")).unwrap_or(default)
}
fn caller_hz(index: usize) -> f64 { 600.0 + 50.0 * index as f64 }
fn agent_hz(index: usize) -> f64 { 1800.0 + 50.0 * index as f64 }
fn tone_index(pcm: &[i16], rate: u32, count: usize, caller: bool) -> Option<usize> {
    if rms(pcm)<800.0 { return None; }
    let mut powers:Vec<_>=(0..count).map(|i|(i,goertzel_power(pcm,rate,if caller {caller_hz(i)} else {agent_hz(i)}))).collect();
    powers.sort_by(|a,b| b.1.partial_cmp(&a.1).unwrap());
    if powers[0].1 > powers.get(1).map(|p|p.1*8.0).unwrap_or(0.0) {Some(powers[0].0)} else {None}
}

async fn outbound(h: &mut Harness, ctl: &mut Controller, ua: &TestUa, rtp: &CallerRtp, id: &str, user: &str)
    -> (SocketAddr, WebSocketStream<TcpStream>) {
    let ack = ctl.command("originate", 1, json!({"call_id":id,"binding_id":format!("bind-{id}"),
        "request_id":id,"tenant_id":"load-test", "payload":{
            "call_id":id,"binding_id":format!("bind-{id}"),"media_session_id":format!("m-{id}"),
            "ws_url":format!("/ws/rustpbx/{id}?token=load"),"token":"load",
            "destination":format!("sip:{user}@127.0.0.1:{}",ua.local_port()),
            "from":"load-originator","to":user,"outbound_framing":"generation_u64_pcm16le"}})).await;
    assert_eq!(ack["type"],"command_ack","{ack}");
    let incoming = wait_for_ua_event(ua, Duration::from_secs(5), |e| matches!(e,TestUaEvent::IncomingCall(..))).await;
    let Some(TestUaEvent::IncomingCall(dialog,offer)) = incoming else { panic!("outbound callee not reached") };
    let target = extract_media_endpoint(offer.as_deref().unwrap()).unwrap();
    ua.answer_call(&dialog,Some(make_sdp(rtp.port()))).await.unwrap();
    assert_eq!(ctl.event("answered",Duration::from_secs(5)).await["call_id"],id);
    let (_,mut media) = h.next_media().await;
    assert_eq!(media_text(&mut media,"start",Duration::from_secs(3)).await["call_id"],id);
    media.send(Message::Text(json!({"type":"ready"}).to_string().into())).await.unwrap();
    (target,media)
}

async fn start_call(h: &mut Harness, ctl: &mut Controller, index: usize, cycle: u64, seconds: u64, mixed: &str, count: usize) -> LiveCall {
    let began = Instant::now();
    let user = if index % 2 == 0 {"alice"} else {"bob"}.to_string();
    let id = format!("load-{cycle}-{index}");
    let ua = Arc::new(h.server.create_ua(&user).await.unwrap());
    let rtp = CallerRtp::bind().await;
    let is_outbound = mixed == "outbound" || (mixed == "mixed" && index % 2 == 1);
    let (target,media) = if is_outbound {
        outbound(h,ctl,&ua,&rtp,&id,&user).await
    } else {
        let (_,_,target,media) = establish_inbound(h,ctl,&ua,&rtp,1,&id,&format!("m-{id}")).await;
        (target,media)
    };
    rtp.send_tone(target,caller_hz(index),seconds + 300);
    let stats = Arc::new(parking_lot::Mutex::new(AudioStats::default()));
    let shared = stats.clone();
    let (close,mut close_rx) = mpsc::channel::<u16>(1);
    let pump = tokio::spawn(async move {
        let (mut writer,mut reader) = media.split();
        let mut tick = tokio::time::interval(Duration::from_millis(20));
        tick.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
        loop {
            tokio::select! {
                biased;
                Some(code) = close_rx.recv() => {
                    let _ = tokio::time::timeout(Duration::from_secs(1),writer.send(Message::Close(Some(CloseFrame {
                        code:code.into(),reason:"load_fault".into()})))).await;
                    break;
                }
                message = reader.next() => match message {
                    Some(Ok(Message::Binary(bytes))) => {
                        let pcm = pcm16le_to_samples(&bytes);
                        if tone_index(&pcm,16000,count,true)==Some(index) {
                            let now = Instant::now(); let mut s = shared.lock();
                            s.uplink_tone_frames += 1;
                            if let Some(last) = s.last_up { s.max_uplink_gap_ms = s.max_uplink_gap_ms.max(now.duration_since(last).as_millis()); }
                            s.last_up = Some(now);
                        } else if tone_index(&pcm,16000,count,true).is_some() { shared.lock().cross_call_frames += 1; }
                    }
                    Some(Ok(Message::Close(_))) | Some(Err(_)) | None => break,
                    _ => {}
                },
                _ = tick.tick() => {
                    let pcm = tone(agent_hz(index),16000,320,0,9000.0);
                    if !matches!(tokio::time::timeout(Duration::from_secs(1),writer.send(Message::Binary(encode_generation_frame(1,&pcm).into()))).await,Ok(Ok(()))) { break; }
                }
            }
        }
        shared.lock().closed = true;
    });
    LiveCall {id,direction:if is_outbound {"outbound"} else {"inbound"},setup_ms:began.elapsed().as_millis(),ua,rtp,stats,close,pump}
}

async fn exercise(isolate: bool) {
    let count = setting("VBOX_PBX_LOAD_CONCURRENCY",3) as usize;
    let seconds = setting("VBOX_PBX_LOAD_SECONDS",5);
    let cycles = setting("VBOX_PBX_LOAD_CYCLES",1);
    let gap_budget = setting("VBOX_PBX_LOAD_MAX_GAP_MS",500) as u128;
    let mix = std::env::var("VBOX_PBX_LOAD_MIX").unwrap_or_else(|_| "mixed".into());
    assert!((1..=15).contains(&count) && seconds>=3 && seconds<=3600 && cycles>0 && cycles<=100);
    assert!(matches!(mix.as_str(),"mixed"|"inbound"|"outbound"));
    assert!(!isolate || count>=2);
    let mut h = harness(30).await;
    let mut ctl = Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls",1,json!({"payload":{"include_terminated_since":0}})).await;
    let mut results = Vec::new();
    for cycle in 0..cycles {
        let mut calls = Vec::new();
        for index in 0..count { calls.push(start_call(&mut h,&mut ctl,index,cycle,seconds,&mix,count).await); }
        let snap = ctl.command("list_owned_calls",1,json!({"payload":{}})).await;
        assert_eq!(snap["active_calls"].as_array().unwrap().len(),count);
        let mut faulted = false;
        let began = Instant::now();
        while began.elapsed() < Duration::from_secs(seconds) {
            tokio::time::sleep(Duration::from_millis(100)).await;
            for (index,call) in calls.iter().enumerate() {
                // Drain every100ms: recording memory does not grow with a soak.
                let capture = std::mem::take(&mut *call.rtp.captured.lock());
                let mut s = call.stats.lock();
                for (at,pcm) in capture {
                    if tone_index(&pcm,8000,count,false)==Some(index) {
                        s.downlink_tone_frames += 1;
                        if let Some(last)=s.last_down { s.max_downlink_gap_ms=s.max_downlink_gap_ms.max(at.duration_since(last).as_millis()); }
                        s.last_down=Some(at);
                    } else if tone_index(&pcm,8000,count,false).is_some() {
                        s.cross_call_frames += 1;
                    }
                }
                if !(faulted && index==0) {
                    assert!(!s.closed,"unexpected media disconnect on {}",call.id);
                    if began.elapsed() > Duration::from_secs(1) {
                        assert!(s.last_up.is_some_and(|last|last.elapsed().as_millis()<=gap_budget),"uplink stalled {}",call.id);
                        assert!(s.last_down.is_some_and(|last|last.elapsed().as_millis()<=gap_budget),"downlink stalled {}",call.id);
                    }
                }
            }
            if isolate && !faulted && began.elapsed() >= Duration::from_secs(seconds/2) {
                calls[0].close.send(4000).await.unwrap();
                let ended = ctl.event("call_ended",Duration::from_secs(5)).await;
                assert_eq!(ended["call_id"],calls[0].id);
                assert!(wait_for_ua_event(&calls[0].ua,Duration::from_secs(3),|e| matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
                faulted=true;
            }
        }
        for (index,call) in calls.into_iter().enumerate() {
            let stats = call.stats.lock();
            let min_frames = if isolate && index==0 { seconds*10 } else {seconds*25};
            assert!(stats.uplink_tone_frames >= min_frames as usize,"uplink {}",call.id);
            assert!(stats.downlink_tone_frames >= min_frames as usize,"downlink {}",call.id);
            assert_eq!(stats.cross_call_frames,0,"cross-call audio {}",call.id);
            assert!(stats.max_uplink_gap_ms<=gap_budget && stats.max_downlink_gap_ms<=gap_budget,"audio gap {}: up{} down{}",call.id,stats.max_uplink_gap_ms,stats.max_downlink_gap_ms);
            results.push(json!({"call_id":call.id,"direction":call.direction,"setup_ms":call.setup_ms,"audio":*stats}));
            drop(stats);
            if !(isolate && index==0) {
                assert_eq!(ctl.command("hangup",1,json!({"call_id":call.id,"payload":{"reason":"load_complete"}})).await["type"],"command_ack");
                assert_eq!(ctl.event("call_ended",Duration::from_secs(5)).await["call_id"],call.id);
                assert!(wait_for_ua_event(&call.ua,Duration::from_secs(3),|e| matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
            }
            call.rtp.cancel.cancel();
            let _ = call.close.try_send(1000);
            tokio::time::timeout(Duration::from_secs(3),call.pump).await.unwrap().unwrap();
        }
        let final_snap = ctl.command("list_owned_calls",1,json!({"payload":{"include_terminated_since":0}})).await;
        assert_eq!(final_snap["active_calls"],json!([]));
        assert_eq!(final_snap["terminal_tombstones"].as_array().unwrap().len(),count*(cycle as usize+1));
        ctl.events.clear();
    }
    let result=json!({"status":"passed","scope":"actual PBX/SIP/RTP/WebSocket transport; deterministic controller; no vbox admission/PG/carrier/AI",
        "concurrency":count,"seconds_per_cycle":seconds,"cycles":cycles,"mix":mix,"isolated_media_4000":isolate,
        "max_audio_gap_budget_ms":gap_budget,"remaining_owned_calls":0,"calls":results});
    if let Ok(path)=std::env::var("VBOX_PBX_LOAD_REPORT") {std::fs::write(path,serde_json::to_vec_pretty(&result).unwrap()).unwrap();}
    eprintln!("VBOX_REAL_TRANSPORT_MEASUREMENTS={result}");
    h.server.stop();
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn vbox_contract_multi_call_media_failure_is_isolated() { exercise(true).await; }

#[tokio::test(flavor="multi_thread",worker_threads=4)]
#[ignore = "parameterized real load; run through rustpbx_upstream_faults.py"]
async fn vbox_real_transport_load() { exercise(false).await; }

static PANIC_SEEN: std::sync::atomic::AtomicBool = std::sync::atomic::AtomicBool::new(false);
struct PanicTransaction { server: rustpbx::proxy::server::SipServerRef }
#[async_trait::async_trait]
impl rustpbx::proxy::ProxyModule for PanicTransaction {
    fn name(&self)->&str {"vbox-test-panic"}
    async fn on_start(&mut self)->anyhow::Result<()> {Ok(())}
    async fn on_stop(&self)->anyhow::Result<()> {Ok(())}
    async fn on_transaction_begin(&self, _:tokio_util::sync::CancellationToken,
        tx:&mut rsipstack::transaction::transaction::Transaction, _:rustpbx::call::TransactionCookie)
        ->anyhow::Result<rustpbx::proxy::ProxyAction> {
        if tx.original.method==rsipstack::sip::Method::Invite && tx.original.uri.to_string().contains("panic-test") {
            assert!(self.server.runnings_tx.load(std::sync::atomic::Ordering::SeqCst)>0,"panic must happen after accounting increments");
            PANIC_SEEN.store(true,std::sync::atomic::Ordering::SeqCst);
            panic!("explicit test-injected transaction panic");
        }
        Ok(rustpbx::proxy::ProxyAction::Continue)
    }
}
#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn vbox_contract_real_transaction_panic_restores_accounting_and_new_calls() {
    PANIC_SEEN.store(false,std::sync::atomic::Ordering::SeqCst);
    let mut h=harness_with_modules(30,vec![("vbox-test-panic".into(),|server,_|Ok(Box::new(PanicTransaction{server})))]).await;
    let mut ctl=Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls",1,json!({"payload":{}})).await;
    let alice=Arc::new(h.server.create_ua("alice").await.unwrap());
    let bad=tokio::spawn({let alice=alice.clone();async move {alice.make_call("panic-test",Some(make_sdp(19002))).await}});
    tokio::time::timeout(Duration::from_secs(3),async {
        while !PANIC_SEEN.load(std::sync::atomic::Ordering::SeqCst) || h.server.server_ref.runnings_tx.load(std::sync::atomic::Ordering::SeqCst)!=0 {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    }).await.expect("panic must unwind and restore accounting");
    bad.abort(); let _=bad.await;
    let rtp=CallerRtp::bind().await;
    let (_,_,target,mut media)=establish_inbound(&mut h,&mut ctl,&alice,&rtp,1,"after-panic","m-after-panic").await;
    rtp.send_tone(target,CALLER_TONE_HZ,5);
    send_agent_audio(&mut media,1,AGENT_TONE_HZ,50).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(rtp.tone_frames(AGENT_TONE_HZ).len()>=40,"actual RTP works after transaction unwind");
    assert_eq!(ctl.command("hangup",1,json!({"call_id":"after-panic","payload":{"reason":"test_complete"}})).await["type"],"command_ack");
    ctl.event("call_ended",Duration::from_secs(5)).await;
    assert!(wait_for_ua_event(&alice,Duration::from_secs(3),|e|matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
    assert_eq!(ctl.command("list_owned_calls",1,json!({"payload":{}})).await["active_calls"],json!([]));
    rtp.cancel.cancel();
    h.server.stop();
}

#[tokio::test(flavor="multi_thread",worker_threads=4)]
async fn vbox_contract_slow_real_rwi_consumer_is_fenced_and_reconciles() {
    let mut h=harness(30).await;
    let mut ctl=Controller::connect(&h.rwi_url).await;
    ctl.command("list_owned_calls",1,json!({"payload":{}})).await;
    let alice=Arc::new(h.server.create_ua("alice").await.unwrap());
    let rtp=CallerRtp::bind().await;
    let (_,_,target,mut media)=establish_inbound(&mut h,&mut ctl,&alice,&rtp,1,"slow-rwi","m-slow-rwi").await;
    rtp.send_tone(target,CALLER_TONE_HZ,20);
    send_agent_audio(&mut media,1,AGENT_TONE_HZ,200).await;
    let (stalled_socket,normal_receive_capacity)=match ctl.ws.get_mut() {
        MaybeTlsStream::Plain(tcp)=>{
            let socket=socket2::SockRef::from(&*tcp).try_clone().unwrap();
            let normal=socket.recv_buffer_size().unwrap();
            socket.set_recv_buffer_size(1024).unwrap();
            (socket,normal)
        }
        _=>panic!("real local RWI must use the expected TCP socket"),
    };
    let fault_start=Instant::now();
    let (mut writer,mut reader)=ctl.ws.split();
    let flood=tokio::spawn(async move {
        for index in 0..20000 {
            let command=json!({"type":"command","schema_version":"pbx_command_v1","operation":"capabilities",
                "command_id":format!("slow-{index}"),"owner_epoch":1,"deadline_at":chrono::Utc::now().timestamp_millis()+10000});
            if writer.send(Message::Text(command.to_string().into())).await.is_err(){break;}
        }
    });
    // Keep the real receive half unpolled until native backpressure has
    // actually fenced the controller. A fixed sleep followed by reading can
    // drain the queue before overflow on slower hosts, removing the fault.
    let fault_deadline=Instant::now()+Duration::from_secs(15);
    loop {
        if h.gateway.read().vbox.controller().is_none()
            && fault_start.elapsed()>=Duration::from_secs(2) {break;}
        assert!(Instant::now()<fault_deadline,"real reply backpressure never fenced the controller");
        // Continue real media while waiting for the observed native fault.
        send_agent_audio(&mut media,1,AGENT_TONE_HZ,10).await;
        tokio::time::sleep(Duration::from_millis(200)).await;
    }
    let native_fenced_at=Instant::now();
    // The fault has happened. Restore capacity only now so queued replies and
    // the real peer Close/EOF can drain without Linux's tiny-window delay.
    // This recovery is needed for Linux's tiny receive window. Darwin can
    // reject changing the option after peer termination; it drains without it.
    if cfg!(target_os="linux") {
        stalled_socket.set_recv_buffer_size(normal_receive_capacity).unwrap();
    }
    let mut closed=false;
    let until=Instant::now()+Duration::from_secs(5);
    while Instant::now()<until {
        match tokio::time::timeout(Duration::from_millis(500),reader.next()).await {
            Ok(None)|Ok(Some(Err(_)))|Ok(Some(Ok(Message::Close(_))))=>{closed=true;break;}
            _=>{}
        }
    }
    flood.abort();let _=flood.await;
    assert!(closed,"slow real RWI consumer must be disconnected on bounded queue overflow");
    eprintln!("actual native controller fence after {:?}",native_fenced_at.duration_since(fault_start));
    let during=rtp.tone_frames(AGENT_TONE_HZ).into_iter().filter(|at|*at>=fault_start+Duration::from_secs(1)).collect::<Vec<_>>();
    assert!(during.len()>=40,"independent RTP continues during RWI backpressure inside orphan hold");
    assert!(during[0].duration_since(fault_start+Duration::from_secs(1))<Duration::from_millis(300),"RTP must cover the beginning of the measured fault window");
    assert!(native_fenced_at.saturating_duration_since(*during.last().unwrap())<Duration::from_millis(300),"RTP must continue through actual native fencing");
    assert!(during.windows(2).all(|p|p[1].duration_since(p[0])<Duration::from_millis(300)));
    let mut recovered=Controller::connect(&h.rwi_url).await;
    let snapshot=recovered.command("list_owned_calls",2,json!({"payload":{"include_terminated_since":0}})).await;
    assert_eq!(snapshot["active_calls"].as_array().unwrap().len(),1);
    assert_eq!(snapshot["active_calls"][0]["call_id"],"slow-rwi");
    let recovered_at=Instant::now();
    send_agent_audio(&mut media,1,AGENT_TONE_HZ,50).await;
    tokio::time::sleep(Duration::from_millis(1200)).await;
    assert!(rtp.tone_frames(AGENT_TONE_HZ).iter().filter(|at|**at>=recovered_at).count()>=40,"RTP continues after controller reconciliation");
    assert_eq!(recovered.command("hangup",2,json!({"call_id":"slow-rwi","payload":{"reason":"test_complete"}})).await["type"],"command_ack");
    recovered.event("call_ended",Duration::from_secs(5)).await;
    let snapshot=recovered.command("list_owned_calls",2,json!({"payload":{"include_terminated_since":0}})).await;
    assert_eq!(snapshot["active_calls"],json!([]));
    assert_eq!(snapshot["terminal_tombstones"].as_array().unwrap().len(),1);
    assert!(wait_for_ua_event(&alice,Duration::from_secs(3),|e|matches!(e,TestUaEvent::CallTerminated(_))).await.is_some());
    rtp.cancel.cancel();h.server.stop();
}


/// Actual SIP/RTP peer pool for the separately running Erlang admission and
/// supervised-worker acceptance fixture. No Rust controller participates.
pub(super) async fn erlang_bulk(h: &Harness, dir: &std::path::Path) {
    let seconds = setting("VBOX_INTEROP_BULK_SECONDS", 15);
    assert!((5..=3600).contains(&seconds));
    let mut peers = Vec::new();
    let mut descriptions = Vec::new();
    for index in 0..15 {
        let ua = Arc::new(h.server.create_ua("bob").await.unwrap());
        descriptions.push(json!({"index":index,"uri":format!("sip:bob@127.0.0.1:{}",ua.local_port()),
            "caller_hz":caller_hz(index),"agent_hz":agent_hz(index)}));
        peers.push(ua);
    }
    let until = Instant::now() + Duration::from_secs(seconds + 180);
    let mut tasks = Vec::new();
    for (index, ua) in peers.into_iter().enumerate() {
        let directory = dir.to_owned();
        tasks.push(tokio::spawn(async move {
            let rtp = CallerRtp::bind().await;
            let incoming = wait_for_ua_event(&ua, Duration::from_secs(90), |e| matches!(e,TestUaEvent::IncomingCall(..))).await;
            let Some(TestUaEvent::IncomingCall(dialog,offer)) = incoming else { panic!("bulk peer {index}: INVITE absent") };
            let target = extract_media_endpoint(offer.as_deref().unwrap()).unwrap();
            ua.answer_call(&dialog,Some(make_sdp(rtp.port()))).await.unwrap();
            rtp.send_tone(target,caller_hz(index),seconds+180);
            let termination_ua = ua.clone();
            let termination_directory = directory.clone();
            let termination = tokio::spawn(async move {
                let ended = wait_for_ua_event(&termination_ua, until.saturating_duration_since(Instant::now()),
                    |e|matches!(e,TestUaEvent::CallTerminated(_))).await.is_some();
                if ended && index == 6 {
                    std::fs::write(termination_directory.join("bulk-victim-ended.json"),
                        json!({"index":index,"actual_sip_terminated":true}).to_string()).unwrap();
                }
                ended
            });
            let mut stats = AudioStats::default();
            let mut first_down = None;
            loop {
                // Drain instead of retaining audio for the duration of a soak.
                for (at, pcm) in std::mem::take(&mut *rtp.captured.lock()) {
                    match tone_index(&pcm,8000,15,false) {
                        Some(found) if found == index => {
                            first_down.get_or_insert(at);
                            stats.downlink_tone_frames += 1;
                            if let Some(last) = stats.last_down { stats.max_downlink_gap_ms = stats.max_downlink_gap_ms.max(at.duration_since(last).as_millis()); }
                            stats.last_down = Some(at);
                        }
                        Some(_) => stats.cross_call_frames += 1,
                        None => {}
                    }
                }
                if directory.join("bulk-end.json").exists() { break; }
                assert!(Instant::now()<until,"bulk peer {index}: fixture deadline");
                tokio::time::sleep(Duration::from_millis(100)).await;
            }
            assert!(tokio::time::timeout(Duration::from_secs(15),termination).await
                .expect("SIP termination observer deadline").expect("SIP termination observer task"),
                "bulk peer {index}: actual SIP BYE missing");
            rtp.cancel.cancel();
            assert!(stats.downlink_tone_frames>=100,"bulk peer {index}: insufficient actual audio {}",stats.downlink_tone_frames);
            assert_eq!(stats.cross_call_frames,0,"bulk peer {index}: cross-call audio");
            // Fixture deliberately crashes peer 7's model worker; its recovery
            // gap is measured but the other fourteen must remain continuous.
            if index != 6 {
                assert!(stats.max_downlink_gap_ms<=500,"bulk peer {index}: gap {}ms",stats.max_downlink_gap_ms);
                assert!(stats.downlink_tone_frames >= seconds as usize * 40,"bulk peer {index}: sustained audio missing");
                assert!(stats.last_down.unwrap().duration_since(first_down.unwrap()) >= Duration::from_secs(seconds.saturating_sub(2)),"bulk peer {index}: audio stopped before soak ended");
            } else {
                assert!(stats.downlink_tone_frames >= seconds as usize * 30,"fault victim failed to resume audio");
                assert!(stats.max_downlink_gap_ms<=1500,"fault victim recovery exceeded1500ms");
                assert!(stats.last_down.unwrap().duration_since(first_down.unwrap()) >= Duration::from_secs(seconds.saturating_sub(2)),"fault victim audio stopped before soak ended");
            }
            json!({"index":index,"audio":stats,"sip_terminated":true})
        }));
    }
    std::fs::write(dir.join("bulk-pbx.json"),json!({"callees":descriptions,"seconds":seconds,"fault_index":6}).to_string()).unwrap();
    let mut results = Vec::new();
    for task in tasks { results.push(task.await.unwrap()); }
    std::fs::write(dir.join("bulk-results.json"),json!({"status":"passed","calls":results,"remaining_sip_calls":0}).to_string()).unwrap();
}
