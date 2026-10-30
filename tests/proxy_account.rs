//! Real Proxy accounts isolate reloads and deliver transport loss to serial hosts.
use rptadv_echolink::{
    ffi::*,
    wire::{ControlPacket, write_control},
};
use std::{
    io::{Read, Write},
    mem::size_of,
    net::{Shutdown, TcpListener, TcpStream},
    ptr,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::{Duration, Instant},
};
#[path = "support/settings.rs"]
mod settings_fixture;
use settings_fixture::{secrets, settings, span};
#[path = "support/codec_fault.rs"]
mod codec_fault;
#[path = "support/context.rs"]
mod context;
#[path = "support/peer_contract.rs"]
mod peer_contract;

fn fault_context(api: &Descriptor, fault: &codec_fault::FaultCodec) -> *mut Context {
    let directory = std::env::var("RPTADV_CODEC_DIR").unwrap();
    let speex = format!("{directory}/librptadv_codec_speex.so.1");
    let jitter = format!("{directory}/rpt_advanced/librptadv_jitter_adapter.so.1");
    let config = RuntimeConfig {
        worker_threads: 1,
        gsm_library: span(fault.path().to_str().unwrap()),
        speex_library: span(&speex),
        jitter_library: span(&jitter),
        ..Default::default()
    };
    let mut context = ptr::null_mut();
    assert_eq!(
        unsafe { api.context_create.unwrap()(&config, &mut context) },
        Status::Ok
    );
    context
}

fn receive(socket: &mut TcpStream) -> (u8, Vec<u8>) {
    let mut header = [0; 9];
    socket.read_exact(&mut header).unwrap();
    let mut payload = vec![0; u32::from_le_bytes(header[5..].try_into().unwrap()) as usize];
    socket.read_exact(&mut payload).unwrap();
    (header[0], payload)
}
fn send(socket: &mut TcpStream, kind: u8, address: [u8; 4], payload: &[u8]) {
    let mut packet = vec![kind];
    packet.extend(address);
    packet.extend((payload.len() as u32).to_le_bytes());
    packet.extend(payload);
    socket.write_all(&packet).unwrap();
}
fn wait(mut ready: impl FnMut() -> bool) -> bool {
    let end = Instant::now() + Duration::from_secs(3);
    while !ready() {
        if Instant::now() >= end {
            return false;
        }
        std::thread::sleep(Duration::from_millis(2));
    }
    true
}
fn authenticated(listener: &TcpListener) -> TcpStream {
    let (mut socket, _) = listener.accept().unwrap();
    socket
        .set_read_timeout(Some(Duration::from_secs(3)))
        .unwrap();
    socket.write_all(b"12345678").unwrap();
    socket.read_exact(&mut [0; 24]).unwrap(); // Seven-character callsign, newline, digest.
    socket
}
fn begin_directory(socket: &mut TcpStream) -> Vec<u8> {
    receive_directory(socket, 1);
    send(socket, 4, [0; 4], &[0; 4]);
    receive_directory(socket, 2)
}
fn receive_directory(socket: &mut TcpStream, expected: u8) -> Vec<u8> {
    loop {
        let (kind, payload) = receive(socket);
        // The one Proxy stream multiplexes directory TCP and QSO UDP records.
        if !matches!(kind, 5 | 6) {
            assert_eq!(kind, expected);
            return payload;
        }
    }
}
fn end_directory(socket: &mut TcpStream, response: &[u8]) {
    send(socket, 2, [0; 4], response);
    receive_directory(socket, 3);
    send(socket, 3, [0; 4], &[]);
}
fn registered(listener: &TcpListener) -> TcpStream {
    let mut socket = authenticated(listener);
    for response in [
        b"OK".as_slice(),
        b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n",
    ] {
        begin_directory(&mut socket);
        end_directory(&mut socket, response);
    }
    socket
}
struct ContextOwner(&'static Descriptor, *mut Context);
impl Drop for ContextOwner {
    fn drop(&mut self) {
        unsafe { self.0.context_destroy.unwrap()(self.1) };
    }
}
struct PreparedAccount(&'static Descriptor, *mut Account);
impl Drop for PreparedAccount {
    fn drop(&mut self) {
        unsafe { self.0.account_destroy.unwrap()(self.1) };
    }
}
fn prepare(
    api: &'static Descriptor,
    context: *mut Context,
    config: &AccountConfig,
) -> *mut Account {
    let mut account = ptr::null_mut();
    assert_eq!(
        unsafe {
            api.account_prepare.unwrap()(
                context,
                config,
                &secrets("fixture-password"),
                &mut account,
            )
        },
        Status::Ok
    );
    account
}
struct Trace {
    api: &'static Descriptor,
    qso: *mut Qso,
    state: AtomicUsize,
    cause: AtomicUsize,
    sent: AtomicBool,
    checked: AtomicBool,
    closing_checked: AtomicBool,
    pcm: AtomicUsize,
    texts: AtomicUsize,
    text_enabled: AtomicBool,
    queued: AtomicUsize,
    concealed: AtomicUsize,
    gaps: AtomicUsize,
    codec_plc: AtomicUsize,
    fallback: AtomicUsize,
    errors: AtomicUsize,
    keyed: AtomicBool,
    unkeyed: AtomicBool,
    finish: AtomicBool,
    drained: AtomicBool,
    limit: AtomicUsize,
    codec: Option<Arc<codec_fault::FaultCodec>>,
}
// Unwind in test code must still join foreign callbacks before releasing Trace.
struct Attached {
    account: *mut Account,
    qso: *mut Qso,
    registration: *mut Registration,
    trace: Box<Trace>,
}
impl Drop for Attached {
    fn drop(&mut self) {
        let api = self.trace.api;
        unsafe {
            api.peer_detach.unwrap()(self.registration);
            api.peer_destroy.unwrap()(self.qso);
            api.account_destroy.unwrap()(self.account);
        }
    }
}
fn attach(
    api: &'static Descriptor,
    context: *mut Context,
    account: *mut Account,
    blocked: bool,
    codec: Option<Arc<codec_fault::FaultCodec>>,
) -> Attached {
    fresh(api, account);
    let mut qso = ptr::null_mut();
    assert_eq!(
        unsafe { api.dial.unwrap()(account, 123456, &mut qso) },
        Status::Ok
    );
    assert_eq!(
        unsafe { peer_contract::before_attach(api, context, qso) },
        0
    );
    let trace = Box::new(Trace {
        api,
        qso,
        state: AtomicUsize::new(usize::MAX),
        cause: AtomicUsize::new(0),
        sent: AtomicBool::new(false),
        checked: AtomicBool::new(false),
        closing_checked: AtomicBool::new(false),
        pcm: AtomicUsize::new(0),
        texts: AtomicUsize::new(0),
        text_enabled: AtomicBool::new(true),
        queued: AtomicUsize::new(0),
        concealed: AtomicUsize::new(0),
        gaps: AtomicUsize::new(0),
        codec_plc: AtomicUsize::new(0),
        fallback: AtomicUsize::new(0),
        errors: AtomicUsize::new(0),
        keyed: AtomicBool::new(false),
        unkeyed: AtomicBool::new(false),
        finish: AtomicBool::new(false),
        drained: AtomicBool::new(false),
        limit: AtomicUsize::new(if blocked { 0 } else { 73 }),
        codec,
    });
    let mut fixture = Attached {
        account,
        qso,
        registration: ptr::null_mut(),
        trace,
    };
    assert_eq!(
        unsafe {
            api.peer_attach.unwrap()(
                context,
                qso,
                Some(step),
                ptr::from_ref(fixture.trace.as_ref()).cast_mut().cast(),
                &mut fixture.registration,
            )
        },
        Status::Ok
    );
    fixture
}
fn fresh(api: &Descriptor, account: *mut Account) {
    let mut health = AccountObservation {
        struct_size: size_of::<AccountObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    assert!(wait(|| unsafe {
        api.account_observe.unwrap()(account, &mut health) == Status::Ok && health.fresh != 0
    }));
}
fn identity(api: &Descriptor, qso: *mut Qso) -> (u64, u64, String) {
    let mut info = PeerInfo {
        struct_size: size_of::<PeerInfo>() as u32,
        abi_version: 1,
        ..unsafe { std::mem::zeroed() }
    };
    assert_eq!(
        unsafe { api.peer_info.unwrap()(qso, &mut info) },
        Status::Ok
    );
    (
        info.account_generation,
        info.qso_generation,
        String::from_utf8(
            unsafe { std::slice::from_raw_parts(info.local_node.data, info.local_node.length) }
                .to_vec(),
        )
        .unwrap(),
    )
}
fn start_media(socket: &mut TcpStream, speex: bool) -> (Vec<u8>, Vec<u8>) {
    let mut sdes = [0; 512];
    let count = write_control(
        &ControlPacket::Sdes {
            source: 7,
            callsign: "W1XYZ-R".into(),
            station_name: "Peer".into(),
            speex,
        },
        &mut sdes,
    )
    .unwrap();
    send(socket, 6, [127, 0, 0, 2], &sdes[..count]);
    let mut media = Vec::new();
    let mut chat = Vec::new();
    while media.is_empty() || chat.is_empty() {
        let (kind, payload) = receive(socket);
        if kind == 5 {
            if payload.starts_with(b"oNDATA") {
                chat = payload;
            } else {
                media = payload;
            }
        }
    }
    send(socket, 5, [127, 0, 0, 2], &media);
    send(
        socket,
        5,
        [127, 0, 0, 2],
        b"oNDATAW1XYZ-R>Incoming message\r\n\0",
    );
    (media, chat)
}
unsafe extern "C" fn audio(
    opaque: *mut std::ffi::c_void,
    _: u64,
    _: *const f32,
    count: usize,
    concealed: u32,
) -> usize {
    let trace = unsafe { &*opaque.cast::<Trace>() };
    let accepted = count.min(trace.limit.load(Ordering::Acquire));
    trace.pcm.fetch_add(accepted, Ordering::Release);
    if concealed != 0 {
        trace.concealed.fetch_add(accepted, Ordering::Release);
    }
    accepted
}
unsafe extern "C" fn gap(opaque: *mut std::ffi::c_void, _: u64, count: u32) -> u32 {
    let trace = unsafe { &*opaque.cast::<Trace>() };
    let accepted = (count as usize).min(trace.limit.load(Ordering::Acquire));
    trace.gaps.fetch_add(accepted, Ordering::Release);
    accepted as u32
}
unsafe extern "C" fn text(opaque: *mut std::ffi::c_void, sender: ByteSpan, body: ByteSpan) {
    let trace = unsafe { &*opaque.cast::<Trace>() };
    let index = trace.texts.load(Ordering::Acquire);
    let expected = [
        b"Incoming message".as_slice(),
        b"Second message",
        b"Third message",
    ];
    let bytes = |span: ByteSpan| unsafe { std::slice::from_raw_parts(span.data, span.length) };
    if bytes(sender) != b"W1XYZ-R" || expected.get(index).copied() != Some(bytes(body)) {
        trace.errors.fetch_add(1, Ordering::Release);
    }
    trace.texts.fetch_add(1, Ordering::Release);
}
// Use the released encoder for state/expiry probes, not invented RTP bytes.
fn prepared(trace: &Trace) -> (Status, rptadv_codec_client::EncodeResultRaw, [u8; 264]) {
    let mut result: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
    result.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
    let mut packet = [0; 264];
    let pcm = [0.1; 640];
    let status = unsafe {
        trace.api.peer_prepare_audio.unwrap()(
            trace.qso,
            pcm.as_ptr(),
            pcm.len(),
            packet.as_mut_ptr(),
            packet.len(),
            0,
            ptr::from_mut(&mut result).cast(),
        )
    };
    (status, result, packet)
}
unsafe extern "C" fn step(opaque: *mut std::ffi::c_void) -> i32 {
    let trace = unsafe { &*opaque.cast::<Trace>() };
    let mut read = ReadResult::default();
    let sink = PcmSink {
        struct_size: size_of::<PcmSink>() as u32,
        abi_version: 1,
        context: opaque,
        audio: Some(audio),
        gap: Some(gap),
    };
    if !trace.checked.swap(true, Ordering::AcqRel) {
        trace.errors.fetch_add(
            unsafe { peer_contract::serial(trace.api, trace.qso, &sink) },
            Ordering::Release,
        );
        let (status, result, packet) = prepared(trace);
        let voice = ByteSpan {
            data: packet.as_ptr(),
            length: result.encoded_bytes,
        };
        if status != Status::Ok
            || unsafe { trace.api.peer_send_prepared.unwrap()(trace.qso, voice, 640, 0) }
                != Status::Pending
            || unsafe {
                trace.api.peer_send_text.unwrap()(trace.qso, span("K1ABC-R"), span("Pending"))
            } != Status::Pending
            || unsafe { trace.api.peer_reset_encoder.unwrap()(trace.qso) } != Status::Ok
        {
            trace.errors.fetch_add(1, Ordering::Release);
        }
        if let Some(codec) = &trace.codec {
            codec.select(9);
            let reset = unsafe { trace.api.peer_reset_encoder.unwrap()(trace.qso) };
            codec.select(10);
            let mut result: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
            result.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
            let pcm = [0.1; 640];
            let mut packet = [0; 264];
            let prepare = unsafe {
                trace.api.peer_prepare_audio.unwrap()(
                    trace.qso,
                    pcm.as_ptr(),
                    pcm.len(),
                    packet.as_mut_ptr(),
                    packet.len(),
                    0,
                    ptr::from_mut(&mut result).cast(),
                )
            };
            codec.select(0);
            let restored = unsafe { trace.api.peer_reset_encoder.unwrap()(trace.qso) };
            if reset != Status::Failed || prepare != Status::Failed || restored != Status::Ok {
                trace.errors.fetch_add(1, Ordering::Release);
            }
        }
    }
    let status = unsafe {
        trace.api.peer_read.unwrap()(
            trace.qso,
            u32::from(trace.drained.load(Ordering::Acquire)),
            &sink,
            trace.text_enabled.load(Ordering::Acquire).then_some(text),
            opaque,
            &mut read,
        )
    };
    if status != Status::Ok {
        trace.errors.fetch_add(1, Ordering::Release);
    }
    if read.activity == 1 {
        trace.keyed.store(true, Ordering::Release);
    }
    if read.activity == 2 {
        trace.unkeyed.store(true, Ordering::Release);
    }
    if read.finish_pending != 0 {
        trace.finish.store(true, Ordering::Release);
    }
    if read.state == 1 && !trace.sent.swap(true, Ordering::AcqRel) {
        let (status, result, packet) = prepared(trace);
        let mut before = PeerObservation {
            struct_size: size_of::<PeerObservation>() as u32,
            abi_version: 1,
            ..Default::default()
        };
        let mut after = before;
        let mut elapsed = 0;
        let voice = ByteSpan {
            data: packet.as_ptr(),
            length: result.encoded_bytes,
        };
        // A stale packet is accepted as expired, without transport admission.
        if status != Status::Ok
            || unsafe { trace.api.peer_elapsed_ms.unwrap()(trace.qso, &mut elapsed) } != Status::Ok
            || unsafe { trace.api.peer_observe.unwrap()(trace.qso, &mut before) } != Status::Ok
            || unsafe {
                trace.api.peer_send_prepared.unwrap()(
                    trace.qso,
                    voice,
                    640,
                    (elapsed as u32).wrapping_sub(200),
                )
            } != Status::Ok
            || unsafe { trace.api.peer_observe.unwrap()(trace.qso, &mut after) } != Status::Ok
            || before.proxy_admitted != after.proxy_admitted
            || unsafe {
                trace.api.peer_send_prepared.unwrap()(trace.qso, voice, 640, elapsed as u32)
            } != Status::Ok
            || unsafe {
                trace.api.peer_send_text.unwrap()(trace.qso, span("K1ABC-R"), span("Test message"))
            } != Status::Ok
        {
            trace.errors.fetch_add(1, Ordering::Release);
        }
    }
    if read.state == 2 && !trace.closing_checked.swap(true, Ordering::AcqRel) {
        let (status, result, packet) = prepared(trace);
        let voice = ByteSpan {
            data: packet.as_ptr(),
            length: result.encoded_bytes,
        };
        // Graceful closing still owns its finite media, but admits no new send.
        let sent = unsafe { trace.api.peer_send_prepared.unwrap()(trace.qso, voice, 640, 0) };
        let text = unsafe {
            trace.api.peer_send_text.unwrap()(trace.qso, span("K1ABC-R"), span("Closing"))
        };
        // Transport EOF also closes preparation itself, unlike graceful BYE.
        if !matches!(status, Status::Ok | Status::Closed)
            || sent != Status::Closed
            || text != Status::Closed
        {
            trace.errors.fetch_add(1, Ordering::Release);
        }
    }
    let mut observed = PeerObservation {
        struct_size: size_of::<PeerObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    if unsafe { trace.api.peer_observe.unwrap()(trace.qso, &mut observed) } == Status::Ok {
        trace
            .queued
            .store(observed.ingress_queued_packets as usize, Ordering::Release);
        trace
            .codec_plc
            .store(observed.codec_plc_samples as usize, Ordering::Release);
        trace
            .fallback
            .store(observed.fallback_gap_samples as usize, Ordering::Release);
        trace
            .cause
            .store(observed.first_cause as usize, Ordering::Release);
        trace
            .state
            .store(observed.state as usize, Ordering::Release);
    }
    if trace.limit.load(Ordering::Acquire) != 0 {
        unsafe { trace.api.peer_set_deadline.unwrap()(trace.qso, 20_000_000) };
    }
    if read.state == 3 { -1 } else { 0 }
}

#[test]
fn proxy_closure_delivers_a_final_serial_turn_with_the_first_cause() {
    for (speex, blocked, faults, cause) in [
        (false, false, false, 5),
        (true, false, false, 5),
        (false, true, false, 5),
        (false, false, true, 5),
        (false, false, false, 1),
        (false, false, false, 2),
    ] {
        proxy_case(speex, blocked, faults, cause);
    }
}

#[test]
fn failed_codec_open_or_unserializable_sdes_never_publishes_a_registration() {
    unsafe extern "C" fn unexpected(context: *mut std::ffi::c_void) -> i32 {
        unsafe { &*context.cast::<AtomicUsize>() }.fetch_add(1, Ordering::Release);
        -1
    }
    for mode in [7, 8, 0] {
        let fault = codec_fault::FaultCodec::new(rptadv_codec_client::ProfileId::GsmFr);
        let _catalog = fault.catalog();
        let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
        let context = fault_context(api, &fault);
        let _context = ContextOwner(api, context);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || registered(&listener));
        let servers = [span("127.0.0.1")];
        let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
        config.transport.kind = 1;
        config.transport.proxy_host = span("127.0.0.1");
        config.transport.proxy_port = port;
        let long_location = "L".repeat(241);
        if mode == 0 {
            config.station.name = ByteSpan::default();
            config.station.location = span(&long_location);
        }
        let account = prepare(api, context, &config);
        let _account = PreparedAccount(api, account);
        assert_eq!(
            unsafe { api.account_activate.unwrap()(account, None, ptr::null_mut()) },
            Status::Ok
        );
        let socket = server.join().unwrap();
        fresh(api, account);
        let mut qso = ptr::null_mut();
        assert_eq!(
            unsafe { api.dial.unwrap()(account, 123456, &mut qso) },
            Status::Ok
        );
        fault.select(mode);
        let calls = AtomicUsize::new(0);
        let mut registration = ptr::dangling_mut();
        assert_eq!(
            unsafe {
                api.peer_attach.unwrap()(
                    context,
                    qso,
                    Some(unexpected),
                    ptr::from_ref(&calls).cast_mut().cast(),
                    &mut registration,
                )
            },
            if mode == 0 {
                Status::Invalid
            } else {
                Status::Failed
            }
        );
        assert!(registration.is_null());
        assert_eq!(calls.load(Ordering::Acquire), 0);
        assert_eq!(fault.calls(false), 0);
        fault.select(0);
        assert_eq!(
            unsafe {
                api.peer_attach.unwrap()(
                    context,
                    qso,
                    Some(unexpected),
                    ptr::from_ref(&calls).cast_mut().cast(),
                    &mut registration,
                )
            },
            Status::Invalid,
            "failed initialization consumes the one pending attachment"
        );
        assert!(registration.is_null());
        unsafe { api.peer_destroy.unwrap()(qso) };
        socket.shutdown(Shutdown::Both).unwrap();
    }
}

#[test]
fn pending_peer_revalidation_observes_timeout_staleness_and_changed_directory_identity() {
    for mode in 0..3 {
        let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
        let context = context::resource_context(api);
        let _context = ContextOwner(api, context);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || registered(&listener));
        let servers = [span("127.0.0.1")];
        let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
        config.transport.kind = 1;
        config.transport.proxy_host = span("127.0.0.1");
        config.transport.proxy_port = port;
        match mode {
            0 => config.policy.connect_timeout_ms = 100,
            1 => config.policy.directory_freshness_ms = 100,
            _ => config.policy.directory_refresh_ms = 100,
        }
        let account = prepare(api, context, &config);
        let _account = PreparedAccount(api, account);
        assert_eq!(
            unsafe { api.account_activate.unwrap()(account, None, ptr::null_mut()) },
            Status::Ok
        );
        let mut socket = server.join().unwrap();
        fresh(api, account);
        let mut qso = ptr::null_mut();
        assert_eq!(
            unsafe { api.dial.unwrap()(account, 123456, &mut qso) },
            Status::Ok
        );
        if mode == 2 {
            assert_eq!(begin_directory(&mut socket), b"F0\r");
            end_directory(
                &mut socket,
                b"@@@\n1\nW1XYZ-R\nPeer\n654321\n127.0.0.2\n+++\n",
            );
        }
        let mut info = PeerInfo {
            struct_size: size_of::<PeerInfo>() as u32,
            abi_version: 1,
            ..unsafe { std::mem::zeroed() }
        };
        let expected = [Status::Closed, Status::Pending, Status::Invalid][mode];
        assert!(wait(|| unsafe {
            api.validate_pending.unwrap()(qso, &mut info)
        } == expected));
        unsafe extern "C" fn no_work(_: *mut std::ffi::c_void) -> i32 {
            -1
        }
        let mut registration = ptr::dangling_mut();
        assert_eq!(
            unsafe {
                api.peer_attach.unwrap()(
                    context,
                    qso,
                    Some(no_work),
                    ptr::null_mut(),
                    &mut registration,
                )
            },
            expected
        );
        assert!(registration.is_null());
        unsafe { api.peer_destroy.unwrap()(qso) };
        socket.shutdown(Shutdown::Both).unwrap();
    }
}
fn proxy_case(speex: bool, blocked: bool, faults: bool, cause: usize) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready, socket) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        ready.send(registered(&listener)).unwrap();
    });
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let codec = faults.then(|| {
        Arc::new(codec_fault::FaultCodec::new(
            rptadv_codec_client::ProfileId::GsmFr,
        ))
    });
    let context = codec.as_ref().map_or_else(
        || context::resource_context(api),
        |codec| fault_context(api, codec),
    );
    let _context = ContextOwner(api, context);
    let servers = [span("127.0.0.1")];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.transport.kind = 1;
    config.transport.proxy_host = span("127.0.0.1");
    config.transport.proxy_port = port;
    let account = prepare(api, context, &config);
    assert_eq!(
        unsafe { api.account_activate.unwrap()(account, None, ptr::null_mut()) },
        Status::Ok
    );
    let mut socket = socket.recv_timeout(Duration::from_secs(3)).unwrap();
    server.join().unwrap();
    let mut fixture = attach(api, context, account, blocked, codec);
    let trace = fixture.trace.as_ref();
    let (media, chat) = start_media(&mut socket, speex);
    let loss_trial = !blocked && !faults && cause == 5;
    if loss_trial {
        // Admit the later packet before the first packet's playout deadline,
        // keeping the missing span inside one known finite jitter timeline.
        let mut later = media.clone();
        let sequence = u16::from_be_bytes(later[2..4].try_into().unwrap()).wrapping_add(2);
        let timestamp = u32::from_be_bytes(later[4..8].try_into().unwrap()).wrapping_add(1280);
        later[2..4].copy_from_slice(&sequence.to_be_bytes());
        later[4..8].copy_from_slice(&timestamp.to_be_bytes());
        send(&mut socket, 5, [127, 0, 0, 2], &later);
    }
    let expected_pcm = if loss_trial {
        if speex { 1920 } else { 1280 }
    } else {
        640
    };
    let connected = wait(|| trace.state.load(Ordering::Acquire) == 1);
    let received = wait(|| {
        trace.keyed.load(Ordering::Acquire)
            && trace.pcm.load(Ordering::Acquire) == if blocked { 0 } else { expected_pcm }
            && trace.texts.load(Ordering::Acquire) == 1
    });
    if received && !blocked && !faults && cause == 5 {
        // No callback means retained text, not permission to discard it or
        // consume the following message out of order.
        trace.text_enabled.store(false, Ordering::Release);
        for body in [b"Second message".as_slice(), b"Third message"] {
            let mut packet = b"oNDATAW1XYZ-R>".to_vec();
            packet.extend(body);
            packet.extend(b"\r\n\0");
            send(&mut socket, 5, [127, 0, 0, 2], &packet);
        }
        assert!(wait(|| trace.queued.load(Ordering::Acquire) != 0));
        assert_eq!(trace.texts.load(Ordering::Acquire), 1);
        trace.text_enabled.store(true, Ordering::Release);
        unsafe { api.peer_wake.unwrap()(fixture.registration) };
        assert!(wait(|| trace.texts.load(Ordering::Acquire) == 3));
        unsafe { api.peer_wake.unwrap()(fixture.registration) };
        // GSM reports a ring gap; Speex supplies its own codec-native PLC.
        assert_eq!(
            trace.concealed.load(Ordering::Acquire),
            if speex { 640 } else { 0 }
        );
        assert_eq!(
            trace.gaps.load(Ordering::Acquire),
            if speex { 0 } else { 640 }
        );
        assert!(wait(|| trace.codec_plc.load(Ordering::Acquire)
            == if speex { 640 } else { 0 }
            && trace.fallback.load(Ordering::Acquire)
                == if speex { 0 } else { 640 }));
    }
    match cause {
        1 => unsafe { api.peer_stop.unwrap()(fixture.registration) },
        2 => {
            let mut bye = [0; 64];
            let count = write_control(
                &ControlPacket::Bye {
                    source: 7,
                    reason: Vec::new(),
                },
                &mut bye,
            )
            .unwrap();
            send(&mut socket, 6, [127, 0, 0, 2], &bye[..count]);
        }
        _ => socket.shutdown(Shutdown::Both).unwrap(),
    }
    let final_turn = wait(|| trace.cause.load(Ordering::Acquire) == cause);
    trace.limit.store(73, Ordering::Release);
    unsafe { api.peer_wake.unwrap()(fixture.registration) };
    let finite = wait(|| trace.finish.load(Ordering::Acquire));
    let premature_unkey = trace.unkeyed.load(Ordering::Acquire);
    trace.drained.store(true, Ordering::Release);
    unsafe { api.peer_wake.unwrap()(fixture.registration) };
    let released =
        wait(|| trace.unkeyed.load(Ordering::Acquire) && trace.state.load(Ordering::Acquire) == 3);
    let mut health = AccountObservation {
        struct_size: size_of::<AccountObservation>() as u32,
        abi_version: 1,
        ..Default::default()
    };
    let account_observed = wait(|| unsafe {
        api.account_observe.unwrap()(account, &mut health) == Status::Ok
            && if cause == 5 {
                health.first_cause == 2
            } else {
                health.available != 0
            }
    });
    unsafe {
        api.peer_detach.unwrap()(fixture.registration);
    }
    fixture.registration = ptr::null_mut();
    assert!(connected, "fixture did not establish the actual QSO");
    assert_eq!(chat, b"oNDATAK1ABC-R>Test message\r\n\0");
    assert_eq!(&media[..2], &[0xc0, if speex { 0x96 } else { 3 }]);
    assert!(
        received,
        "Proxy GSM/Speex and chat did not reach the real source bridge"
    );
    assert!(
        final_turn,
        "account failure retired the owner without notifying its host"
    );
    assert!(
        finite && !premature_unkey && released,
        "closure cause={cause}, speex={speex}, blocked={blocked}, faults={faults}: finite={finite}, premature_unkey={premature_unkey}, released={released}, state={}, read_errors={}",
        trace.state.load(Ordering::Acquire),
        trace.errors.load(Ordering::Acquire)
    );
    assert_eq!(trace.errors.load(Ordering::Acquire), 0);
    assert_eq!(trace.pcm.load(Ordering::Acquire), expected_pcm);
    assert!(trace.closing_checked.load(Ordering::Acquire));
    assert!(account_observed);
    if cause == 5 {
        assert!(health.state == 7 && health.available == 0);
    } else {
        assert_eq!(health.first_cause, 0);
        socket.shutdown(Shutdown::Both).unwrap();
    }
}

#[test]
fn slow_directory_and_exclusive_reload_do_not_interrupt_another_real_account() {
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let context = context::resource_context(api);
    let _context = ContextOwner(api, context);
    let listeners = [
        TcpListener::bind("127.0.0.1:0").unwrap(),
        TcpListener::bind("127.0.0.1:0").unwrap(),
    ];
    let servers = [span("127.0.0.1")];
    let mut fixtures = Vec::new();
    let mut sockets = Vec::new();
    for (index, listener) in listeners.into_iter().enumerate() {
        let mut config = settings(
            if index == 0 { "K1ABC-R" } else { "K2ABC-R" },
            "Loopback",
            &servers,
            &[],
        );
        config.local_node = span(if index == 0 { "524950" } else { "524951" });
        config.transport.kind = 1;
        config.transport.proxy_host = span("127.0.0.1");
        config.transport.proxy_port = listener.local_addr().unwrap().port();
        let server = std::thread::spawn(move || registered(&listener));
        let account = prepare(api, context, &config);
        assert_eq!(
            unsafe { api.account_activate.unwrap()(account, None, ptr::null_mut()) },
            Status::Ok
        );
        sockets.push(server.join().unwrap());
        fixtures.push(attach(api, context, account, false, None));
    }
    let before_a = identity(api, fixtures[0].qso);
    let before_b = identity(api, fixtures[1].qso);
    assert_eq!(before_a.2, "524950");
    assert_eq!(before_b.2, "524951");

    let mut updated = settings("K1ABC-R", "Loopback", &servers, &[]);
    updated.station.name = span("Updated");
    updated.transport.kind = 1;
    updated.transport.proxy_host = span("127.0.0.1");
    updated.transport.proxy_port = sockets[0].local_addr().unwrap().port();
    assert_eq!(
        unsafe {
            api.account_reconfigure.unwrap()(
                fixtures[0].account,
                &updated,
                &secrets("fixture-password"),
            )
        },
        Status::Ok
    );
    let request = begin_directory(&mut sockets[0]);
    assert!(request.windows(7).any(|part| part == b"Updated"));
    // Account A has not received its directory response. Account B must still
    // decode real audio and chat on the same one-worker runtime.
    let (mut media, _) = start_media(&mut sockets[1], false);
    let trace = fixtures[1].trace.as_ref();
    assert!(wait(
        || trace.pcm.load(Ordering::Acquire) == 640 && trace.texts.load(Ordering::Acquire) == 1
    ));
    end_directory(&mut sockets[0], b"OK");

    let replacement = TcpListener::bind("127.0.0.1:0").unwrap();
    updated.transport.proxy_port = replacement.local_addr().unwrap().port();
    let prepared = PreparedAccount(api, prepare(api, context, &updated));
    let account = fixtures[0].account as usize;
    let candidate = prepared.1 as usize;
    std::thread::scope(|scope| {
        let changing = scope.spawn(move || unsafe {
            api.account_apply_prepared.unwrap()(account as *mut Account, candidate as *mut Account)
        });
        let logout = begin_directory(&mut sockets[0]);
        assert!(logout.windows(3).any(|part| part == b"OFF"));
        end_directory(&mut sockets[0], b"OK");
        let (mut replacement_socket, _) = replacement.accept().unwrap();
        // Hold authentication open while B receives the next distinct RTP
        // packet; a global lifecycle lock or blocked executor would stop it.
        trace.drained.store(true, Ordering::Release);
        unsafe { api.peer_wake.unwrap()(fixtures[1].registration) };
        let sequence = u16::from_be_bytes(media[2..4].try_into().unwrap()).wrapping_add(1);
        media[2..4].copy_from_slice(&sequence.to_be_bytes());
        send(&mut sockets[1], 5, [127, 0, 0, 2], &media);
        assert!(wait(|| trace.pcm.load(Ordering::Acquire) >= 1280));
        replacement_socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        replacement_socket.write_all(b"12345678").unwrap();
        replacement_socket.read_exact(&mut [0; 24]).unwrap();
        for response in [
            b"OK".as_slice(),
            b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n",
        ] {
            begin_directory(&mut replacement_socket);
            end_directory(&mut replacement_socket, response);
        }
        assert_eq!(changing.join().unwrap(), Status::Ok);
        fresh(api, fixtures[0].account);
        let mut next = ptr::null_mut();
        assert_eq!(
            unsafe { api.dial.unwrap()(fixtures[0].account, 123456, &mut next) },
            Status::Ok
        );
        let after_a = identity(api, next);
        unsafe { api.peer_destroy.unwrap()(next) };
        assert!(after_a.0 > before_a.0);
        assert_eq!(after_a.2, before_a.2);
        assert_eq!(identity(api, fixtures[1].qso), before_b);
        assert_eq!(trace.errors.load(Ordering::Acquire), 0);
        assert_eq!(
            unsafe { api.peer_set_deadline.unwrap()(fixtures[0].qso, 0) },
            Status::Closed
        );
        replacement_socket.shutdown(Shutdown::Both).unwrap();
    });
    for socket in sockets {
        socket.shutdown(Shutdown::Both).unwrap();
    }
}
