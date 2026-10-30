//! Actual account transport failure must reach the registered serial host.
use rptadv_echolink::{
    ffi::*,
    wire::{ControlPacket, write_control},
};
use std::{
    io::{Read, Write},
    mem::size_of,
    net::{Shutdown, TcpListener, TcpStream},
    ptr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};
#[path = "support/settings.rs"]
mod settings_fixture;
use settings_fixture::{secrets, settings, span};
#[path = "support/context.rs"]
mod context;

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
struct Trace {
    api: &'static Descriptor,
    qso: *mut Qso,
    state: AtomicUsize,
    cause: AtomicUsize,
    sent: AtomicBool,
    pcm: AtomicUsize,
    texts: AtomicUsize,
    errors: AtomicUsize,
    keyed: AtomicBool,
    unkeyed: AtomicBool,
    finish: AtomicBool,
    drained: AtomicBool,
    limit: AtomicUsize,
}
// Unwind in test code must still join foreign callbacks before releasing Trace.
struct Attached {
    context: *mut Context,
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
            api.context_destroy.unwrap()(self.context);
        }
    }
}
unsafe extern "C" fn audio(
    opaque: *mut std::ffi::c_void,
    _: u64,
    _: *const f32,
    count: usize,
    _: u32,
) -> usize {
    let trace = unsafe { &*opaque.cast::<Trace>() };
    let accepted = count.min(trace.limit.load(Ordering::Acquire));
    trace.pcm.fetch_add(accepted, Ordering::Release);
    accepted
}
unsafe extern "C" fn gap(_: *mut std::ffi::c_void, _: u64, count: u32) -> u32 {
    count
}
unsafe extern "C" fn text(opaque: *mut std::ffi::c_void, _: ByteSpan, _: ByteSpan) {
    unsafe { &*opaque.cast::<Trace>() }
        .texts
        .fetch_add(1, Ordering::Release);
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
    let status = unsafe {
        trace.api.peer_read.unwrap()(
            trace.qso,
            u32::from(trace.drained.load(Ordering::Acquire)),
            &sink,
            Some(text),
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
        let mut result: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
        result.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
        let mut packet = [0; 264];
        let pcm = [0.1; 640];
        let prepared = unsafe {
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
        if prepared != Status::Ok
            || unsafe {
                trace.api.peer_send_prepared.unwrap()(
                    trace.qso,
                    ByteSpan {
                        data: packet.as_ptr(),
                        length: result.encoded_bytes,
                    },
                    640,
                    0,
                )
            } != Status::Ok
            || unsafe {
                trace.api.peer_send_text.unwrap()(trace.qso, span("K1ABC-R"), span("Test message"))
            } != Status::Ok
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
fn proxy_eof_delivers_a_final_serial_turn_with_the_transport_cause() {
    for (speex, blocked) in [(false, false), (true, false), (false, true)] {
        proxy_case(speex, blocked);
    }
}
fn proxy_case(speex: bool, blocked: bool) {
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    let port = listener.local_addr().unwrap().port();
    let (ready, socket) = std::sync::mpsc::channel();
    let server = std::thread::spawn(move || {
        let (mut socket, _) = listener.accept().unwrap();
        socket
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        socket.write_all(b"12345678").unwrap();
        socket.read_exact(&mut [0; 24]).unwrap(); // K1ABC-R newline and raw digest.
        for response in [
            b"OK".as_slice(),
            b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n",
        ] {
            assert_eq!(receive(&mut socket).0, 1);
            send(&mut socket, 4, [0; 4], &[0; 4]);
            assert_eq!(receive(&mut socket).0, 2);
            send(&mut socket, 2, [0; 4], response);
            assert_eq!(receive(&mut socket).0, 3);
            send(&mut socket, 3, [0; 4], &[]);
        }
        ready.send(socket).unwrap();
    });
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let context = context::resource_context(api);
    let servers = [span("127.0.0.1")];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.transport.kind = 1;
    config.transport.proxy_host = span("127.0.0.1");
    config.transport.proxy_port = port;
    let mut account = ptr::null_mut();
    assert_eq!(
        unsafe {
            api.account_prepare.unwrap()(
                context,
                &config,
                &secrets("fixture-password"),
                &mut account,
            )
        },
        Status::Ok
    );
    assert_eq!(
        unsafe { api.account_activate.unwrap()(account, None, ptr::null_mut()) },
        Status::Ok
    );
    let mut socket = socket.recv_timeout(Duration::from_secs(3)).unwrap();
    server.join().unwrap();
    let mut qso = ptr::null_mut();
    assert_eq!(
        unsafe { api.dial.unwrap()(account, 123456, &mut qso) },
        Status::Ok
    );
    let trace = Box::new(Trace {
        api,
        qso,
        state: AtomicUsize::new(usize::MAX),
        cause: AtomicUsize::new(0),
        sent: AtomicBool::new(false),
        pcm: AtomicUsize::new(0),
        texts: AtomicUsize::new(0),
        errors: AtomicUsize::new(0),
        keyed: AtomicBool::new(false),
        unkeyed: AtomicBool::new(false),
        finish: AtomicBool::new(false),
        drained: AtomicBool::new(false),
        limit: AtomicUsize::new(if blocked { 0 } else { 73 }),
    });
    let mut fixture = Attached {
        context,
        account,
        qso,
        registration: ptr::null_mut(),
        trace,
    };
    let trace = fixture.trace.as_ref();
    assert_eq!(
        unsafe {
            api.peer_attach.unwrap()(
                context,
                qso,
                Some(step),
                ptr::from_ref(trace).cast_mut().cast(),
                &mut fixture.registration,
            )
        },
        Status::Ok
    );
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
    send(&mut socket, 6, [127, 0, 0, 2], &sdes[..count]);
    let connected = wait(|| trace.state.load(Ordering::Acquire) == 1);
    let mut media = Vec::new();
    let mut chat = Vec::new();
    while media.is_empty() || chat.is_empty() {
        let (kind, payload) = receive(&mut socket);
        if kind == 5 {
            if payload.starts_with(b"oNDATA") {
                chat = payload;
            } else {
                media = payload;
            }
        }
    }
    send(&mut socket, 5, [127, 0, 0, 2], &media);
    send(
        &mut socket,
        5,
        [127, 0, 0, 2],
        b"oNDATAW1XYZ-R>Incoming message\r\n\0",
    );
    let received = wait(|| {
        trace.keyed.load(Ordering::Acquire)
            && trace.pcm.load(Ordering::Acquire) == if blocked { 0 } else { 640 }
            && trace.texts.load(Ordering::Acquire) == 1
    });
    socket.shutdown(Shutdown::Both).unwrap();
    let final_turn = wait(|| trace.cause.load(Ordering::Acquire) == 5);
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
    let account_failed = wait(|| unsafe {
        api.account_observe.unwrap()(account, &mut health) == Status::Ok && health.first_cause == 2
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
        "transport loss finite={finite}, premature_unkey={premature_unkey}, released={released}, state={}, read_errors={}",
        trace.state.load(Ordering::Acquire),
        trace.errors.load(Ordering::Acquire)
    );
    assert_eq!(trace.errors.load(Ordering::Acquire), 0);
    assert_eq!(trace.pcm.load(Ordering::Acquire), 640);
    assert!(account_failed && health.state == 7 && health.available == 0);
}
