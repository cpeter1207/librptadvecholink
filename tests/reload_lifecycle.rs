//! Real ABI reloads use only bounded loopback directory and UDP fixtures.
use rptadv_echolink::ffi::*;
use std::{
    io::{BufRead, BufReader, Write},
    net::{TcpListener, UdpSocket},
    ptr,
    sync::atomic::{AtomicBool, AtomicUsize, Ordering},
    time::{Duration, Instant},
};
#[path = "support/settings.rs"]
mod settings_fixture;
use settings_fixture::{secrets, settings, span};
#[path = "support/context.rs"]
mod context;

fn wait(mut ready: impl FnMut() -> bool) {
    let end = Instant::now() + Duration::from_secs(5);
    while !ready() {
        assert!(Instant::now() < end, "account reload did not complete");
        std::thread::sleep(Duration::from_millis(2));
    }
}
fn port_pair() -> u16 {
    for _ in 0..64 {
        let audio = UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = audio.local_addr().unwrap().port();
        if port < u16::MAX && UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port + 1)).is_ok() {
            return port;
        }
    }
    panic!("no loopback port pair available");
}

#[test]
fn prepared_snapshot_exchange_is_context_bound_and_requires_an_inactive_candidate() {
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let apply = api
        .account_apply_prepared
        .expect("prepared apply operation missing");
    let mut first = ptr::null_mut();
    let mut second = ptr::null_mut();
    let config = RuntimeConfig {
        worker_threads: 1,
        ..Default::default()
    };
    assert_eq!(
        unsafe { api.context_create.unwrap()(&config, &mut first) },
        Status::Ok
    );
    assert_eq!(
        unsafe { api.context_create.unwrap()(&config, &mut second) },
        Status::Ok
    );
    let prepare = |context| {
        let mut account = ptr::null_mut();
        assert_eq!(
            unsafe {
                api.account_prepare.unwrap()(
                    context,
                    &AccountConfig::default(),
                    ptr::null(),
                    &mut account,
                )
            },
            Status::Ok
        );
        account
    };
    let target = prepare(first);
    let candidate = prepare(first);
    let spare = prepare(first);
    let other = prepare(second);
    for (left, right, status) in [
        (ptr::null_mut(), candidate, Status::Invalid),
        (target, ptr::null_mut(), Status::Invalid),
        (target, target, Status::Invalid),
        (target, other, Status::Invalid),
        (target, candidate, Status::Ok),
        (candidate, target, Status::Ok),
    ] {
        assert_eq!(unsafe { apply(left, right) }, status);
    }
    let executor = tokio::runtime::Builder::new_current_thread()
        .build()
        .unwrap();
    assert_eq!(
        executor.block_on(async { unsafe { apply(target, candidate) } }),
        Status::Invalid
    );
    assert_eq!(
        unsafe { api.account_activate.unwrap()(candidate, None, ptr::null_mut()) },
        Status::Ok
    );
    assert_eq!(unsafe { apply(target, candidate) }, Status::Invalid);
    assert_eq!(unsafe { api.account_stop.unwrap()(candidate) }, Status::Ok);
    assert_eq!(unsafe { apply(target, candidate) }, Status::Closed);
    assert_eq!(unsafe { api.account_stop.unwrap()(target) }, Status::Ok);
    assert_eq!(unsafe { apply(target, spare) }, Status::Closed);
    assert_eq!(unsafe { apply(target, other) }, Status::Invalid);
    assert_eq!(unsafe { apply(candidate, target) }, Status::Closed);
    unsafe {
        api.account_destroy.unwrap()(target);
        api.account_destroy.unwrap()(candidate);
        api.account_destroy.unwrap()(spare);
        api.account_destroy.unwrap()(other);
        api.context_destroy.unwrap()(first);
        api.context_destroy.unwrap()(second);
    }
}
unsafe extern "C" fn event(context: *mut std::ffi::c_void, event: *const Event) {
    let event = unsafe { &*event };
    if event.kind == 0 && event.status == Status::Ok {
        unsafe { &*context.cast::<AtomicUsize>() }.fetch_add(1, Ordering::Release);
    }
}

#[test]
fn activated_disabled_account_retains_callbacks_through_enable_disable_enable() {
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let update = api
        .account_reconfigure
        .expect("account reload operation missing");
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = std::thread::spawn(move || {
        for (online, name) in [
            (Some(true), "Site"),
            (None, ""),
            (Some(true), "Updated site"),
            (Some(false), "Updated site"),
            (Some(true), "Updated site"),
            (None, ""),
            (Some(false), "Updated site"),
        ] {
            let mut socket = None;
            wait(|| match listener.accept() {
                Ok((accepted, _)) => {
                    socket = Some(accepted);
                    true
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => panic!("fixture accept failed: {error}"),
            });
            let mut socket = socket.unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(&socket);
            let mut field = Vec::new();
            reader.read_until(b'\r', &mut field).unwrap();
            if let Some(online) = online {
                assert_eq!(field, b"lK1ABC-R\xac\xacfixture-password\r");
                field.clear();
                reader.read_until(b'\r', &mut field).unwrap();
                assert!(field.starts_with(if online { b"ONLINE" } else { b"OFF-V3.40" }));
                field.clear();
                reader.read_until(b'\r', &mut field).unwrap();
                assert_eq!(field, format!("{name}\r").as_bytes());
                if online {
                    field.clear();
                    reader.read_until(b'\r', &mut field).unwrap();
                }
                socket.write_all(b"OK").unwrap();
            } else {
                assert_eq!(field, b"F0\r");
                socket.write_all(b"@@@\n0\n+++\n").unwrap();
            }
        }
    });
    let context = context::resource_context(api);
    let servers = [span(&address)];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.transport.audio_port = port_pair();
    config.enabled = 0;
    let keys = secrets("fixture-password");
    let mut account = ptr::null_mut();
    assert_eq!(
        unsafe { api.account_prepare.unwrap()(context, &config, &keys, &mut account) },
        Status::Ok
    );
    let online = AtomicUsize::new(0);
    assert_eq!(
        unsafe {
            api.account_activate.unwrap()(
                account,
                Some(event),
                ptr::from_ref(&online).cast_mut().cast(),
            )
        },
        Status::Ok
    );
    config.enabled = 1;
    assert_eq!(unsafe { update(account, &config, &keys) }, Status::Ok);
    wait(|| online.load(Ordering::Acquire) >= 1);
    config.station.name = span("Updated site");
    assert_eq!(unsafe { update(account, &config, &keys) }, Status::Ok);
    wait(|| online.load(Ordering::Acquire) >= 2);
    config.enabled = 0;
    assert_eq!(unsafe { update(account, &config, &keys) }, Status::Ok);
    config.enabled = 1;
    assert_eq!(unsafe { update(account, &config, &keys) }, Status::Ok);
    wait(|| online.load(Ordering::Acquire) >= 3);
    assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
    assert_eq!(unsafe { update(account, &config, &keys) }, Status::Closed);
    unsafe {
        api.account_destroy.unwrap()(account);
        api.context_destroy.unwrap()(context);
    }
    server.join().unwrap();
}

#[test]
fn exclusive_reload_quiesces_retained_peers_and_restores_configuration_on_bind_failure() {
    struct HeldStep {
        entered: AtomicBool,
        release: AtomicBool,
    }
    unsafe extern "C" fn held(context: *mut std::ffi::c_void) -> i32 {
        let step = unsafe { &*context.cast::<HeldStep>() };
        step.entered.store(true, Ordering::Release);
        wait(|| step.release.load(Ordering::Acquire));
        0
    }
    for (failed_replacement, prepared_update) in
        [(false, false), (true, false), (false, true), (true, true)]
    {
        let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            for (password, online) in [
                ("fixture-password", Some(true)),
                ("", None),
                ("fixture-password", Some(false)),
                (
                    if failed_replacement {
                        "fixture-password"
                    } else {
                        "replacement-password"
                    },
                    Some(true),
                ),
                ("", None),
                (
                    if failed_replacement {
                        "fixture-password"
                    } else {
                        "replacement-password"
                    },
                    Some(false),
                ),
            ] {
                let mut socket = None;
                wait(|| match listener.accept() {
                    Ok((accepted, _)) => {
                        socket = Some(accepted);
                        true
                    }
                    Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                    Err(error) => panic!("fixture accept failed: {error}"),
                });
                let mut socket = socket.unwrap();
                socket
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut reader = BufReader::new(&socket);
                let mut request = Vec::new();
                for _ in 0..match online {
                    Some(true) => 4,
                    Some(false) => 3,
                    None => 1,
                } {
                    assert_ne!(reader.read_until(b'\r', &mut request).unwrap(), 0);
                }
                if let Some(online) = online {
                    // The legacy wire separator is two raw AC bytes, not UTF-8.
                    let mut expected = b"lK1ABC-R\xac\xac".to_vec();
                    expected.extend_from_slice(password.as_bytes());
                    expected.push(b'\r');
                    assert!(request.starts_with(&expected));
                    assert!(
                        request
                            .windows(if online { 6 } else { 9 })
                            .any(|bytes| bytes
                                == if online {
                                    b"ONLINE".as_slice()
                                } else {
                                    b"OFF-V3.40".as_slice()
                                })
                    );
                    socket.write_all(b"OK").unwrap();
                } else {
                    assert_eq!(request, b"F0\r");
                    socket
                        .write_all(b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n")
                        .unwrap();
                }
            }
        });
        // The directory owner must be able to finish while this peer callback is held.
        let context = context::resource_context_with_threads(api, 2);
        let servers = [span(&address)];
        let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
        config.transport.audio_port = port_pair();
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
        let online = AtomicUsize::new(0);
        assert_eq!(
            unsafe {
                api.account_activate.unwrap()(
                    account,
                    Some(event),
                    ptr::from_ref(&online).cast_mut().cast(),
                )
            },
            Status::Ok
        );
        wait(|| online.load(Ordering::Acquire) >= 1);
        let mut old = ptr::null_mut();
        assert_eq!(
            unsafe { api.dial.unwrap()(account, 123456, &mut old) },
            Status::Ok
        );
        let mut old_info = PeerInfo {
            struct_size: std::mem::size_of::<PeerInfo>() as u32,
            abi_version: 1,
            ..unsafe { std::mem::zeroed() }
        };
        assert_eq!(
            unsafe { api.peer_info.unwrap()(old, &mut old_info) },
            Status::Ok
        );
        let step = HeldStep {
            entered: AtomicBool::new(false),
            release: AtomicBool::new(false),
        };
        let mut registration = ptr::null_mut();
        assert_eq!(
            unsafe {
                api.peer_attach.unwrap()(
                    context,
                    old,
                    Some(held),
                    ptr::from_ref(&step).cast_mut().cast(),
                    &mut registration,
                )
            },
            Status::Ok
        );
        wait(|| step.entered.load(Ordering::Acquire));
        let occupied = UdpSocket::bind("127.0.0.1:0").unwrap();
        if failed_replacement {
            config.transport.audio_port = occupied.local_addr().unwrap().port();
        }
        let address = account as usize;
        let config_address = ptr::from_ref(&config) as usize;
        let mut prepared = ptr::null_mut();
        if prepared_update {
            let mut password = String::from("replacement-password");
            assert_eq!(
                unsafe {
                    api.account_prepare.unwrap()(
                        context,
                        &config,
                        &secrets(&password),
                        &mut prepared,
                    )
                },
                Status::Ok
            );
            password.clear();
        }
        let prepared_address = prepared as usize;
        let (done, result) = std::sync::mpsc::channel();
        let updating = std::thread::spawn(move || {
            let status = unsafe {
                if prepared_update {
                    api.account_apply_prepared.unwrap()(
                        address as *mut Account,
                        prepared_address as *mut Account,
                    )
                } else {
                    api.account_reconfigure.unwrap()(
                        address as *mut Account,
                        config_address as *const AccountConfig,
                        &secrets("replacement-password"),
                    )
                }
            };
            done.send(status).unwrap();
        });
        let premature = result.recv_timeout(Duration::from_millis(50)).ok();
        step.release.store(true, Ordering::Release);
        let status =
            premature.unwrap_or_else(|| result.recv_timeout(Duration::from_secs(5)).unwrap());
        updating.join().unwrap();
        assert!(
            premature.is_none(),
            "reload returned before its old callback finished"
        );
        assert_eq!(
            status,
            if failed_replacement {
                Status::Failed
            } else {
                Status::Ok
            }
        );
        wait(|| online.load(Ordering::Acquire) >= 2);
        assert_eq!(
            unsafe { api.validate_pending.unwrap()(old, &mut old_info) },
            Status::Closed
        );
        let mut read = ReadResult::default();
        assert_eq!(
            unsafe {
                api.peer_read.unwrap()(old, 0, ptr::null(), None, ptr::null_mut(), &mut read)
            },
            Status::Closed
        );
        assert_eq!(
            unsafe { api.peer_send_text.unwrap()(old, span("K1ABC-R"), span("retired")) },
            Status::Closed
        );
        assert_eq!(
            unsafe { api.peer_reset_encoder.unwrap()(old) },
            Status::Closed
        );
        assert_eq!(
            unsafe { api.peer_set_deadline.unwrap()(old, 0) },
            Status::Closed
        );
        let mut next = ptr::null_mut();
        assert_eq!(
            unsafe { api.dial.unwrap()(account, 123456, &mut next) },
            Status::Ok
        );
        let mut next_info = PeerInfo {
            struct_size: std::mem::size_of::<PeerInfo>() as u32,
            abi_version: 1,
            ..unsafe { std::mem::zeroed() }
        };
        assert_eq!(
            unsafe { api.peer_info.unwrap()(next, &mut next_info) },
            Status::Ok
        );
        assert!(next_info.account_generation > old_info.account_generation);
        unsafe {
            api.peer_detach.unwrap()(registration);
            api.peer_destroy.unwrap()(old);
            api.peer_destroy.unwrap()(next);
        }
        assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
        unsafe {
            api.account_destroy.unwrap()(account);
            api.account_destroy.unwrap()(prepared);
            api.context_destroy.unwrap()(context);
        }
        server.join().unwrap();
    }
}

#[test]
fn live_gain_reload_wakes_existing_owner_without_replacing_its_generation() {
    for prepared_update in [false, true] {
        live_gain_reload(prepared_update);
    }
}

fn live_gain_reload(prepared_update: bool) {
    struct Trace {
        api: &'static Descriptor,
        qso: usize,
        calls: AtomicUsize,
        packet: std::sync::Mutex<Vec<u8>>,
    }
    unsafe extern "C" fn encode(context: *mut std::ffi::c_void) -> i32 {
        let trace = unsafe { &*context.cast::<Trace>() };
        let qso = trace.qso as *mut Qso;
        let mut result: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
        result.struct_size = std::mem::size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
        let mut packet = [0; 264];
        if unsafe { trace.api.peer_reset_encoder.unwrap()(qso) } != Status::Ok
            || unsafe {
                trace.api.peer_prepare_audio.unwrap()(
                    qso,
                    [0.1; 640].as_ptr(),
                    640,
                    packet.as_mut_ptr(),
                    packet.len(),
                    0,
                    ptr::from_mut(&mut result).cast(),
                )
            } != Status::Ok
            || result.encoded_bytes != 144
        {
            return -1;
        }
        *trace.packet.lock().unwrap() = packet[..result.encoded_bytes].to_vec();
        trace.calls.fetch_add(1, Ordering::Release);
        0
    }
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let listener = TcpListener::bind("127.0.0.1:0").unwrap();
    listener.set_nonblocking(true).unwrap();
    let address = listener.local_addr().unwrap().to_string();
    let server = std::thread::spawn(move || {
        for (fields, response) in [
            (4, b"OK".as_slice()),
            (
                1,
                b"@@@\n1\nW1XYZ-R\nPeer\n123456\n127.0.0.2\n+++\n".as_slice(),
            ),
            (3, b"OK".as_slice()),
        ] {
            let mut socket = None;
            wait(|| match listener.accept() {
                Ok((accepted, _)) => {
                    socket = Some(accepted);
                    true
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => panic!("fixture accept failed: {error}"),
            });
            let mut socket = socket.unwrap();
            socket
                .set_read_timeout(Some(Duration::from_secs(2)))
                .unwrap();
            let mut reader = BufReader::new(&socket);
            for _ in 0..fields {
                assert_ne!(reader.read_until(b'\r', &mut Vec::new()).unwrap(), 0);
            }
            socket.write_all(response).unwrap();
        }
    });
    let context = context::resource_context(api);
    let servers = [span(&address)];
    let mut config = settings("K1ABC-R", "Loopback", &servers, &[]);
    config.transport.audio_port = port_pair();
    let keys = secrets("fixture-password");
    let mut account = ptr::null_mut();
    assert_eq!(
        unsafe { api.account_prepare.unwrap()(context, &config, &keys, &mut account) },
        Status::Ok
    );
    let online = AtomicUsize::new(0);
    assert_eq!(
        unsafe {
            api.account_activate.unwrap()(
                account,
                Some(event),
                ptr::from_ref(&online).cast_mut().cast(),
            )
        },
        Status::Ok
    );
    wait(|| online.load(Ordering::Acquire) >= 1);
    let mut qso = ptr::null_mut();
    assert_eq!(
        unsafe { api.dial.unwrap()(account, 123456, &mut qso) },
        Status::Ok
    );
    let trace = Trace {
        api,
        qso: qso as usize,
        calls: AtomicUsize::new(0),
        packet: std::sync::Mutex::new(Vec::new()),
    };
    let mut registration = ptr::null_mut();
    assert_eq!(
        unsafe {
            api.peer_attach.unwrap()(
                context,
                qso,
                Some(encode),
                ptr::from_ref(&trace).cast_mut().cast(),
                &mut registration,
            )
        },
        Status::Ok
    );
    wait(|| trace.calls.load(Ordering::Acquire) == 1);
    let original = trace.packet.lock().unwrap().clone();
    config.policy.transmit_gain_db = 0.0;
    let mut prepared = ptr::null_mut();
    if prepared_update {
        assert_eq!(
            unsafe { api.account_prepare.unwrap()(context, &config, &keys, &mut prepared) },
            Status::Ok
        );
    }
    assert_eq!(
        unsafe {
            if prepared_update {
                config.policy.transmit_gain_db = -20.0;
                api.account_apply_prepared.unwrap()(account, prepared)
            } else {
                api.account_reconfigure.unwrap()(account, &config, &keys)
            }
        },
        Status::Ok
    );
    wait(|| trace.calls.load(Ordering::Acquire) >= 2);
    let changed = trace.packet.lock().unwrap().clone();
    let directory = std::env::var("RPTADV_CODEC_DIR").unwrap();
    let catalog =
        rptadv_codec_client::CodecCatalog::load(&[rptadv_codec_client::ProviderSelection {
            path: std::path::PathBuf::from(format!("{directory}/librptadv_codec_gsm.so.1")),
            profiles: vec![rptadv_codec_client::ProfileId::GsmFr],
        }])
        .unwrap();
    let mut expected = rptadv_echolink::media::Packetizer::new(
        &catalog,
        rptadv_echolink::wire::Codec::GsmFr,
        0.0,
        0,
        1,
    )
    .unwrap();
    expected.push(&[0.1; 640]).unwrap();
    assert_eq!(&changed[12..], &expected.packet().unwrap().bytes[12..]);
    assert_ne!(&original[12..], &changed[12..]);
    assert_eq!(
        &changed[2..4],
        &[0, 1],
        "live update reset the RTP sequence"
    );
    assert_eq!(
        online.load(Ordering::Acquire),
        1,
        "gain-only update re-registered the account"
    );
    if prepared_update {
        assert_eq!(
            unsafe { api.account_apply_prepared.unwrap()(account, prepared) },
            Status::Ok
        );
        wait(|| trace.calls.load(Ordering::Acquire) >= 3);
        let restored = trace.packet.lock().unwrap().clone();
        assert_eq!(&restored[12..], &original[12..]);
        assert_eq!(&restored[2..4], &[0, 2], "rollback reset RTP sequence");
        assert_eq!(
            online.load(Ordering::Acquire),
            1,
            "live rollback reconnected"
        );
    }
    unsafe {
        api.peer_detach.unwrap()(registration);
        api.peer_destroy.unwrap()(qso);
    }
    assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
    unsafe {
        api.account_destroy.unwrap()(account);
        api.account_destroy.unwrap()(prepared);
        api.context_destroy.unwrap()(context);
    }
    server.join().unwrap();
}
