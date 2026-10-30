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

fn binding_for_test(
    original: &Binding,
    control: crate::account::network::NetworkControl,
    settings: Arc<crate::account::settings::PreparedSettings>,
) -> Arc<Binding> {
    Arc::new(Binding {
        runtime: Arc::clone(&original.runtime),
        _resources: Arc::clone(&original._resources),
        current: arc_swap::ArcSwap::from(Arc::clone(&settings)),
        settings,
        control,
        generation: original.generation,
        live: std::sync::atomic::AtomicBool::new(true),
        drivers: std::sync::Mutex::new(Vec::new()),
        first_failure: std::sync::Mutex::new(None),
    })
}

#[test]
fn account_bridge_revalidates_accepted_work_before_publishing_a_qso() {
    use crate::account::{
        directory::DirectoryState,
        network::{NetworkEvent, tests::Harness},
    };
    type Events = std::sync::Mutex<Vec<(u32, Status)>>;
    unsafe extern "C" fn observed(opaque: *mut c_void, event: *const ffi::Event) {
        let events = unsafe { &*opaque.cast::<Events>() };
        let event = unsafe { &*event };
        events.lock().unwrap().push((event.kind, event.status));
    }
    let fixture = DirectPeer::open();
    let original = unsafe { &*fixture.qso.cast::<Peer>() };
    for case in 0..5 {
        let events = Events::default();
        let callback = (case != 0).then_some(crate::owner::Callback {
            function: observed,
            context: ptr::from_ref(&events).cast_mut().cast(),
        });
        original.binding.runtime.block_on(async {
            let mut harness = Harness::new(Arc::clone(&original.binding.settings)).await;
            let binding = binding_for_test(
                &original.binding,
                harness.control.clone(),
                Arc::clone(&original.binding.settings),
            );
            let (stopping, stop) = tokio::sync::watch::channel(false);
            if case == 4 {
                let (input, output) = tokio::sync::mpsc::channel(1);
                input
                    .send(NetworkEvent::Directory {
                        state: DirectoryState::Registering,
                        fresh: true,
                        failures: 0,
                    })
                    .await
                    .unwrap();
                drop(input);
                crate::owner::bridge(binding, output, stop, callback).await;
            } else {
                harness.pending();
                if case == 1 {
                    harness.stale_directory();
                }
                let (_, empty) = tokio::sync::mpsc::channel(1);
                let output = std::mem::replace(&mut harness.events, empty);
                if case == 0 {
                    drop(harness);
                    crate::owner::bridge(binding, output, stop, callback).await;
                } else {
                    let reader = crate::owner::bridge(Arc::clone(&binding), output, stop, callback);
                    let network = async {
                        harness.service().await;
                        match case {
                            2 => binding.live.store(false, Ordering::Release),
                            3 => {
                                stopping.send_replace(true);
                            }
                            _ => {}
                        }
                        drop(harness);
                    };
                    tokio::join!(reader, network);
                }
            }
        });
        assert_eq!(
            *events.lock().unwrap(),
            match case {
                1 => vec![(0, Status::Invalid)],
                4 => vec![(0, Status::Pending)],
                _ => Vec::new(),
            },
            "case {case} must not publish a stale or stopping QSO"
        );
    }
}

#[test]
fn dial_preserves_admission_timeout_and_post_reply_revalidation_failures() {
    use crate::account::network::tests::Harness;
    let fixture = DirectPeer::open();
    let account = unsafe { &*fixture.account.cast::<crate::owner::Account>() };
    let original = Arc::clone(
        &account
            .state
            .lock()
            .unwrap()
            .active
            .as_ref()
            .unwrap()
            .binding,
    );
    let mut configuration = unsafe { ptr::read(original.settings.as_abi()) };
    configuration.policy.connect_timeout_ms = 20;
    let settings = Arc::new(
        unsafe {
            crate::account::settings::PreparedSettings::copy(
                &configuration,
                &secrets("fixture-password"),
            )
        }
        .unwrap(),
    );
    for case in 0..3 {
        let mut harness = account
            .runtime
            .block_on(Harness::new(Arc::clone(&settings)));
        let binding = binding_for_test(&original, harness.control.clone(), Arc::clone(&settings));
        account
            .state
            .lock()
            .unwrap()
            .active
            .as_mut()
            .unwrap()
            .binding = Arc::clone(&binding);
        if case == 0 {
            harness.stale_directory();
        }
        let mut retained = Some(harness);
        let network = (case != 1).then(|| {
            let mut harness = retained.take().unwrap();
            let binding = Arc::clone(&binding);
            account.runtime.spawn(async move {
                harness.service().await;
                if case == 2 {
                    binding.live.store(false, Ordering::Release);
                }
                harness
            })
        });
        let mut output = ptr::null_mut();
        let status = unsafe { fixture.api.dial.unwrap()(fixture.account, 123456, &mut output) };
        account
            .state
            .lock()
            .unwrap()
            .active
            .as_mut()
            .unwrap()
            .binding = Arc::clone(&original);
        if let Some(network) = network {
            account.runtime.block_on(network).unwrap();
        }
        assert!(output.is_null());
        assert_eq!(
            status,
            [Status::Invalid, Status::Failed, Status::Closed][case]
        );
    }
}

#[test]
fn real_account_io_failure_preserves_reload_errors_and_retirement_ownership() {
    for abort_bridge in [false, true] {
        let fixture = DirectPeer::open();
        let account = unsafe { &*fixture.account.cast::<crate::owner::Account>() };
        let (binding, sockets) = {
            let state = account.state.lock().unwrap();
            let active = state.active.as_ref().unwrap();
            if abort_bridge {
                active.bridge.abort();
            }
            (
                Arc::clone(&active.binding),
                active.network.direct_sockets().unwrap(),
            )
        };
        if abort_bridge {
            assert!(proxy_login::wait(|| account
                .state
                .lock()
                .unwrap()
                .active
                .as_ref()
                .unwrap()
                .bridge
                .is_finished()));
        }
        let target = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let endpoint = target.local_addr().unwrap();
        drop(target);
        account.runtime.block_on(async {
            let socket = sockets.test_socket(Channel::Audio);
            socket.connect(endpoint).await.unwrap();
            socket.writable().await.unwrap();
            socket.send(b"probe").await.unwrap();
        });
        // Preserve the pending ICMP error but make its socket read-ready.
        let target = std::net::UdpSocket::bind(endpoint).unwrap();
        target
            .send_to(b"wake", sockets.local_addr(Channel::Audio).unwrap())
            .unwrap();
        assert!(proxy_login::wait(|| matches!(
            account.runtime.block_on(binding.control.observe()),
            Err(crate::account::network::NetworkError::Stopped)
        )));
        if !abort_bridge {
            assert!(proxy_login::wait(|| !binding.live.load(Ordering::Acquire)));
            assert!(matches!(
                *binding.first_failure.lock().unwrap(),
                Some(crate::owner::TransportFailure::Io(
                    std::io::ErrorKind::ConnectionRefused
                ))
            ));
        }
        let previous = Arc::clone(&binding.settings);
        let mut next = unsafe { ptr::read(previous.as_abi()) };
        next.policy.receive_gain_db = -6.0;
        assert_eq!(
            unsafe {
                fixture.api.account_reconfigure.unwrap()(
                    fixture.account,
                    &next,
                    &secrets("fixture-password"),
                )
            },
            if abort_bridge {
                Status::Closed
            } else {
                Status::Ok
            }
        );
        // A stopped network owner is distinct from a failed bridge join. The
        // former may be replaced; the latter must reject the handoff.
        next.transport.kind = 1;
        next.transport.proxy_host = span("127.0.0.1");
        next.transport.proxy_port = previous
            .directory_servers()
            .next()
            .unwrap()
            .parse::<std::net::SocketAddr>()
            .unwrap()
            .port();
        assert_eq!(
            unsafe {
                fixture.api.account_reconfigure.unwrap()(
                    fixture.account,
                    &next,
                    &secrets("fixture-password"),
                )
            },
            Status::Failed
        );
        if abort_bridge {
            assert!(account.state.lock().unwrap().active.is_none());
        } else {
            assert!(
                account
                    .state
                    .lock()
                    .unwrap()
                    .active
                    .as_ref()
                    .unwrap()
                    .binding
                    .generation
                    > binding.generation
            );
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
    assert_eq!(
        unsafe {
            fixture.api.account_reconfigure.unwrap()(fixture.account, ptr::null(), ptr::null())
        },
        Status::Invalid
    );
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
    let context = unsafe { &*fixture.context.cast::<crate::Context>() };
    {
        let _executor = context.runtime.enter();
        // An invalid teardown context must not consume any of these real
        // retained owners. Their ordinary off-executor Drop still joins them.
        unsafe {
            fixture.api.peer_detach.unwrap()(fixture.registration);
            fixture.api.peer_destroy.unwrap()(fixture.qso);
            fixture.api.account_destroy.unwrap()(fixture.account);
        }
    }
    let mut info = ffi::PeerInfo {
        struct_size: size_of::<ffi::PeerInfo>() as u32,
        abi_version: 1,
        ..unsafe { std::mem::zeroed() }
    };
    assert_eq!(
        unsafe { fixture.api.validate_pending.unwrap()(fixture.qso, &mut info) },
        Status::Ok
    );
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
                serial.cause_detail.fill(0);
                serial.remember(&"later failure");
                assert_eq!(serial.cause_detail, [0; 32]);
            }
            peer.binding.live.store(false, Ordering::Release);
            assert_eq!(
                serial.send(peer, Channel::Control, b"", None),
                Status::Closed
            );
            let mut encoded = rptadv_codec_client::EncodeResultRaw {
                struct_size: size_of::<rptadv_codec_client::EncodeResultRaw>() as u32,
                ..Default::default()
            };
            assert_eq!(
                unsafe {
                    peer_prepare_audio(
                        probe.qso,
                        ptr::null(),
                        0,
                        ptr::null_mut(),
                        0,
                        0,
                        ptr::from_mut(&mut encoded).cast(),
                    )
                },
                Status::Closed
            );
            assert_eq!(
                unsafe { peer_send_prepared(probe.qso, ffi::ByteSpan::default(), 0, 0) },
                Status::Closed
            );
            peer.binding.live.store(true, Ordering::Release);
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
                serial.action(peer, Some(QsoAction::Heartbeat));
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

#[test]
fn retired_serial_turn_and_failed_capabilities_cannot_enter_the_host() {
    struct Probe {
        qso: *mut ffi::Qso,
        result: std::sync::mpsc::Sender<Result<(), String>>,
    }
    unsafe extern "C" fn failed_observation(
        _: *const c_void,
        _: *mut rptadv_jitter_client::abi::Observation,
    ) -> i32 {
        -1
    }
    unsafe extern "C" fn count(opaque: *mut c_void) -> i32 {
        unsafe { &*opaque.cast::<AtomicUsize>() }.fetch_add(1, Ordering::Release);
        0
    }
    unsafe extern "C" fn step(opaque: *mut c_void) -> i32 {
        let probe = unsafe { &*opaque.cast::<Probe>() };
        let result = catch_unwind(AssertUnwindSafe(|| {
            let peer = unsafe { &*probe.qso.cast::<Peer>() };
            let resources = &peer.binding._resources;
            let mut descriptor = unsafe {
                ptr::read(
                    resources
                        .jitter_descriptor()
                        .cast::<rptadv_jitter_client::abi::Descriptor>(),
                )
            };
            descriptor.observe = Some(failed_observation);
            let origin = Instant::now();
            let playout = unsafe {
                Playout::new(
                    resources.catalog(),
                    Codec::GsmFr,
                    0.0,
                    ptr::from_ref(&descriptor).cast(),
                    origin,
                )
            }
            .unwrap();
            let coordinator = crate::account::coordination::Coordinator::new(
                crate::transport::routing::Router::new(
                    crate::account::Account::new(1, Vec::new(), Vec::new()).unwrap(),
                    1,
                    Duration::from_secs(30),
                )
                .unwrap(),
            )
            .unwrap();
            let replacement = Receiver::new(
                Qso::new(
                    peer.identity.clone(),
                    peer.generation,
                    timers(&peer.binding.settings.as_abi().policy),
                    origin,
                )
                .unwrap(),
                playout,
                CodecChoice::new(resources.catalog()).unwrap(),
                coordinator.port(
                    peer.identity.node_number,
                    peer.generation,
                    Arc::clone(&peer.control),
                ),
            );
            let original = {
                let serial = unsafe { (*peer.serial.get()).as_mut() }.unwrap();
                std::mem::replace(&mut serial.receiver, replacement)
            };
            let mut observation = ffi::PeerObservation {
                struct_size: size_of::<ffi::PeerObservation>() as u32,
                abi_version: 1,
                ..Default::default()
            };
            let observed = unsafe { peer_observe(probe.qso, &mut observation) };
            {
                let serial = unsafe { (*peer.serial.get()).as_mut() }.unwrap();
                serial.receiver = original; // Drop before the stack descriptor.
                let writer = crate::transport::proxy::ProxyWriter::new(tokio::io::sink());
                let output = writer.output();
                drop(writer); // A genuinely closed queue, not a fabricated IO result.
                let transport =
                    std::mem::replace(&mut serial.transport, PeerTransport::Proxy(output));
                let sent =
                    serial.send(peer, Channel::Control, &[0xc0, 201, 0, 1, 0, 0, 0, 0], None);
                serial.transport = transport;
                assert_eq!(sent, Status::Closed);
            }
            assert_eq!(observed, Status::Failed);
            peer.control.stop();
            let calls = AtomicUsize::new(0);
            assert!(
                serial_turn(
                    peer,
                    Callback {
                        function: count,
                        context: ptr::from_ref(&calls).cast_mut().cast(),
                    },
                    PeerEvent::Wake
                )
                .is_none()
            );
            assert_eq!(calls.load(Ordering::Acquire), 0);
        }))
        .map_err(|_| "retired serial/capability contract failed".to_owned());
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
