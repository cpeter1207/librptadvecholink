//! Private ownership failures use real retained QSOs and the released providers.
use super::*;
use crate::settings_fixture;
use std::{
    io::{BufRead, Write},
    net::TcpListener,
    panic::{AssertUnwindSafe, catch_unwind},
    sync::atomic::{AtomicUsize, Ordering},
};
#[path = "../support/context.rs"]
mod context;
#[path = "../support/proxy_login.rs"]
mod proxy_login;
use settings_fixture::{secrets, settings, span};

/// The same three directory transactions as account_settings' direct fixture.
struct DirectPeer {
    api: &'static ffi::Descriptor,
    context: *mut ffi::Context,
    account: *mut ffi::Account,
    qso: *mut ffi::Qso,
    registration: *mut ffi::Registration,
    directory: Option<std::thread::JoinHandle<()>>,
}
impl DirectPeer {
    fn open() -> Self {
        Self::with_proxy(false)
    }
    fn with_proxy(proxy: bool) -> Self {
        let api = unsafe { &*crate::rptadv_echolink_descriptor_v1() };
        let context = context::resource_context(api);
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(!proxy).unwrap();
        let proxy_port = listener.local_addr().unwrap().port();
        let address = listener.local_addr().unwrap().to_string();
        let directory = std::thread::spawn(move || {
            if proxy {
                let mut socket = proxy_login::registered(&listener);
                let request = proxy_login::begin_directory(&mut socket);
                assert!(
                    request
                        .windows(b"OFF-V3.40".len())
                        .any(|part| part == b"OFF-V3.40")
                );
                proxy_login::end_directory(&mut socket, b"OK");
                return;
            }
            for (lines, response) in [
                (4, b"OK".as_slice()),
                (1, b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n"),
                (3, b"OK"),
            ] {
                let deadline = Instant::now() + Duration::from_secs(3);
                let mut socket = loop {
                    match listener.accept() {
                        Ok((socket, _)) => break socket,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            assert!(Instant::now() < deadline);
                            std::thread::sleep(Duration::from_millis(2));
                        }
                        Err(error) => panic!("directory accept: {error}"),
                    }
                };
                socket
                    .set_read_timeout(Some(Duration::from_secs(3)))
                    .unwrap();
                let mut reader = std::io::BufReader::new(&mut socket);
                let mut request = Vec::new();
                for _ in 0..lines {
                    assert_ne!(reader.read_until(b'\r', &mut request).unwrap(), 0);
                }
                socket.write_all(response).unwrap();
            }
        });
        let audio = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = audio.local_addr().unwrap().port();
        let control = std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port + 1)).unwrap();
        let servers = [span(&address)];
        let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
        config.transport.audio_port = port;
        config.transport.kind = u32::from(proxy);
        config.transport.proxy_host = span("127.0.0.1");
        config.transport.proxy_port = proxy_port;
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
        drop((audio, control));
        let mut result = Self {
            api,
            context,
            account,
            qso: ptr::null_mut(),
            registration: ptr::null_mut(),
            directory: Some(directory),
        };
        assert_eq!(
            unsafe { api.account_activate.unwrap()(account, None, ptr::null_mut()) },
            Status::Ok
        );
        let mut observation = ffi::AccountObservation {
            struct_size: size_of::<ffi::AccountObservation>() as u32,
            abi_version: 1,
            ..Default::default()
        };
        assert!(proxy_login::wait(|| {
            (unsafe { api.account_observe.unwrap()(account, &mut observation) }) == Status::Ok
                && observation.fresh != 0
        }));
        assert_eq!(
            unsafe { api.dial.unwrap()(account, 123456, &mut result.qso) },
            Status::Ok
        );
        result
    }
}
impl Drop for DirectPeer {
    fn drop(&mut self) {
        unsafe {
            if !self.registration.is_null() {
                self.api.peer_detach.unwrap()(self.registration);
            }
            self.api.peer_destroy.unwrap()(self.qso);
            self.api.account_destroy.unwrap()(self.account);
            self.api.context_destroy.unwrap()(self.context);
        }
        let joined = self.directory.take().unwrap().join();
        if !std::thread::panicking() {
            joined.unwrap();
        }
    }
}

#[test]
fn internal_lock_poison_cannot_publish_or_consume_an_attachment() {
    unsafe extern "C" fn step(opaque: *mut c_void) -> i32 {
        unsafe { &*opaque.cast::<AtomicUsize>() }.fetch_add(1, Ordering::Release);
        -1
    }
    let mut fixture = DirectPeer::open();
    let peer = unsafe { &*fixture.qso.cast::<Peer>() };
    let calls = AtomicUsize::new(0);
    for drivers in [true, false] {
        assert!(
            catch_unwind(AssertUnwindSafe(|| {
                if drivers {
                    let _guard = peer.binding.drivers.lock().unwrap();
                    panic!("private control lock failure");
                } else {
                    let _guard = peer.pending.lock().unwrap();
                    panic!("private pending lock failure");
                }
            }))
            .is_err()
        );
        let result = unsafe {
            fixture.api.peer_attach.unwrap()(
                fixture.context,
                fixture.qso,
                Some(step),
                ptr::from_ref(&calls).cast_mut().cast(),
                &mut fixture.registration,
            )
        };
        peer.binding.drivers.clear_poison();
        peer.pending.clear_poison();
        assert_eq!(result, Status::Failed);
        assert!(fixture.registration.is_null());
        assert!(peer.pending.lock().unwrap().is_some());
        assert_eq!(calls.load(Ordering::Acquire), 0);
    }
    assert_eq!(
        unsafe {
            fixture.api.peer_attach.unwrap()(
                fixture.context,
                fixture.qso,
                Some(step),
                ptr::from_ref(&calls).cast_mut().cast(),
                &mut fixture.registration,
            )
        },
        Status::Ok
    );
    let deadline = Instant::now() + Duration::from_secs(3);
    while calls.load(Ordering::Acquire) == 0 {
        assert!(Instant::now() < deadline);
        std::thread::sleep(Duration::from_millis(2));
    }
    assert!(!fixture.registration.is_null());
}

#[test]
fn serial_direct_sends_preserve_readiness_credit_and_the_original_os_failure() {
    use crate::transport::direct::DirectSockets;
    struct Probe {
        qso: *mut ffi::Qso,
        pending: bool,
        result: std::sync::mpsc::Sender<Result<(), String>>,
    }
    unsafe extern "C" fn step(opaque: *mut c_void) -> i32 {
        let probe = unsafe { &*opaque.cast::<Probe>() };
        let result = catch_unwind(AssertUnwindSafe(|| {
            let peer = unsafe { &*probe.qso.cast::<Peer>() };
            let serial = unsafe { (*peer.serial.get()).as_mut() }.unwrap();
            if probe.pending {
                use std::{
                    future::Future,
                    task::{Context, Poll, Waker},
                };
                // No await/yield on this sole worker: these genuine new sockets
                // have not received their first reactor readiness notification.
                let mut bind = std::pin::pin!(DirectSockets::bind(
                    "127.0.0.1:0".parse().unwrap(),
                    "127.0.0.1:0".parse().unwrap()
                ));
                let Poll::Ready(Ok(sockets)) =
                    bind.as_mut().poll(&mut Context::from_waker(Waker::noop()))
                else {
                    panic!("literal loopback binding was not immediate");
                };
                let original = std::mem::replace(
                    &mut serial.transport,
                    PeerTransport::Direct(Arc::new(sockets)),
                );
                serial.queue_control(peer, false).unwrap();
                let packet = serial.control.as_ref().unwrap().clone();
                let blocked = serial.blocked_sends;
                let status = serial.send(peer, Channel::Control, &packet, None);
                serial.transport = original;
                assert_eq!(status, Status::Pending);
                assert_eq!(serial.blocked_sends, blocked + 1);
                assert!(matches!(
                    serial.writable.take(),
                    Some(crate::transport::driver::PeerWritable::Direct(
                        _,
                        Channel::Control
                    ))
                ));
            } else {
                // The initial control flush ran on this owner before the host
                // callback; a real ICMP error must retain its exact OS category.
                let observation = serial.receiver.observe(Instant::now()).unwrap();
                assert_eq!(
                    observation.session.first_cause,
                    Some(crate::session::Cause::TransportLost)
                );
                assert!(serial.cause_detail.starts_with(b"ConnectionRefused"));
            }
        }))
        .map_err(|_| "serial direct-send contract failed".to_owned());
        let _ = probe.result.send(result);
        -1
    }
    for pending in [true, false] {
        let mut fixture = DirectPeer::open();
        if !pending {
            let account = unsafe { &*fixture.account.cast::<crate::owner::Account>() };
            let sockets = account
                .state
                .lock()
                .unwrap()
                .active
                .as_ref()
                .unwrap()
                .network
                .direct_sockets()
                .unwrap();
            let target = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
            let closed = target.local_addr().unwrap();
            drop(target);
            let runtime = unsafe { &*fixture.context.cast::<crate::Context>() };
            runtime.runtime.block_on(async {
                let socket = sockets.test_socket(Channel::Control);
                socket.connect(closed).await.unwrap();
                socket.writable().await.unwrap();
                socket.send(b"probe").await.unwrap();
            });
        }
        let (result, observed) = std::sync::mpsc::channel();
        let probe = Probe {
            qso: fixture.qso,
            pending,
            result,
        };
        assert_eq!(
            unsafe {
                fixture.api.peer_attach.unwrap()(
                    fixture.context,
                    fixture.qso,
                    Some(step),
                    ptr::from_ref(&probe).cast_mut().cast(),
                    &mut fixture.registration,
                )
            },
            Status::Ok
        );
        let result = observed.recv_timeout(Duration::from_secs(3));
        // Join the callback before releasing its stack-owned Probe, including
        // on a failed assertion reported by the serial test body.
        unsafe { fixture.api.peer_detach.unwrap()(fixture.registration) };
        fixture.registration = ptr::null_mut();
        result.unwrap().unwrap();
    }
}

#[test]
fn proxy_credit_keeps_the_exact_control_packet_until_the_writer_resumes() {
    struct Probe {
        qso: *mut ffi::Qso,
        phase: AtomicUsize,
        result: std::sync::mpsc::Sender<Result<(), String>>,
    }
    unsafe extern "C" fn step(opaque: *mut c_void) -> i32 {
        let probe = unsafe { &*opaque.cast::<Probe>() };
        let phase = probe.phase.fetch_add(1, Ordering::AcqRel);
        let result = catch_unwind(AssertUnwindSafe(|| {
            let peer = unsafe { &*probe.qso.cast::<Peer>() };
            let serial = unsafe { (*peer.serial.get()).as_mut() }.unwrap();
            if phase == 0 {
                let PeerTransport::Proxy(output) = &serial.transport else {
                    panic!("selected Proxy transport missing")
                };
                let output = output.clone();
                serial.queue_control(peer, false).unwrap();
                let packet = serial.control.as_ref().unwrap().clone();
                let mut queued = 0;
                loop {
                    match output.try_queue(
                        ProxyRecord {
                            kind: ProxyKind::UdpControl,
                            address: peer.identity.address,
                            payload: &packet,
                        },
                        None,
                    ) {
                        Ok(()) => {
                            queued += 1;
                            assert!(queued < 65_536);
                        }
                        Err(crate::transport::proxy::QueueError::Full) => break,
                        Err(error) => panic!("unexpected queue failure: {error:?}"),
                    }
                }
                assert!(queued != 0);
                let blocked = serial.blocked_sends;
                serial.flush_control(peer);
                assert_eq!(serial.control.as_ref(), Some(&packet));
                assert_eq!(serial.blocked_sends, blocked + 1);
                assert!(matches!(
                    serial.writable,
                    Some(crate::transport::driver::PeerWritable::Proxy { .. })
                ));
            } else {
                assert!(
                    serial.control.is_none(),
                    "ready owner did not flush its retained control packet"
                );
            }
        }))
        .map_err(|_| "Proxy control-credit contract failed".to_owned());
        if phase != 0 || result.is_err() {
            let _ = probe.result.send(result);
            -1
        } else {
            0
        }
    }
    let mut fixture = DirectPeer::with_proxy(true);
    let (result, observed) = std::sync::mpsc::channel();
    let probe = Probe {
        qso: fixture.qso,
        phase: AtomicUsize::new(0),
        result,
    };
    assert_eq!(
        unsafe {
            fixture.api.peer_attach.unwrap()(
                fixture.context,
                fixture.qso,
                Some(step),
                ptr::from_ref(&probe).cast_mut().cast(),
                &mut fixture.registration,
            )
        },
        Status::Ok
    );
    let result = observed.recv_timeout(Duration::from_secs(3));
    unsafe { fixture.api.peer_detach.unwrap()(fixture.registration) };
    fixture.registration = ptr::null_mut();
    result.unwrap().unwrap();
    assert!(probe.phase.load(Ordering::Acquire) >= 2);
}

#[test]
fn heartbeat_serialization_failure_closes_with_owner_failure_not_transport_loss() {
    struct Probe {
        qso: *mut ffi::Qso,
        result: std::sync::mpsc::Sender<Result<(), String>>,
    }
    unsafe extern "C" fn step(opaque: *mut c_void) -> i32 {
        let probe = unsafe { &*opaque.cast::<Probe>() };
        let result = catch_unwind(AssertUnwindSafe(|| {
            let peer = unsafe { &*probe.qso.cast::<Peer>() };
            let serial = unsafe { (*peer.serial.get()).as_mut() }.unwrap();
            let long_location = "L".repeat(241);
            // The C record contains borrowed spans/scalars, never Rust owners;
            // keep its settings anchor alive while copying the replacement.
            let mut config = unsafe { ptr::read(serial.settings.as_abi()) };
            config.station.name = ffi::ByteSpan::default();
            config.station.location = span(&long_location);
            serial.settings = Arc::new(
                unsafe {
                    crate::account::settings::PreparedSettings::copy(
                        &config,
                        &secrets("fixture-password"),
                    )
                }
                .unwrap(),
            );
            serial.control = None;
            serial.action(peer, Some(QsoAction::Heartbeat));
            let observation = serial.receiver.observe(Instant::now()).unwrap();
            assert_eq!(
                observation.session.first_cause,
                Some(crate::session::Cause::OwnerFailed)
            );
            assert!(serial.control.is_none());
        }))
        .map_err(|_| "heartbeat serialization failure lost its owner cause".to_owned());
        let _ = probe.result.send(result);
        -1
    }
    let mut fixture = DirectPeer::open();
    let (result, observed) = std::sync::mpsc::channel();
    let probe = Probe {
        qso: fixture.qso,
        result,
    };
    assert_eq!(
        unsafe {
            fixture.api.peer_attach.unwrap()(
                fixture.context,
                fixture.qso,
                Some(step),
                ptr::from_ref(&probe).cast_mut().cast(),
                &mut fixture.registration,
            )
        },
        Status::Ok
    );
    let result = observed.recv_timeout(Duration::from_secs(3));
    unsafe { fixture.api.peer_detach.unwrap()(fixture.registration) };
    fixture.registration = ptr::null_mut();
    result.unwrap().unwrap();
}
