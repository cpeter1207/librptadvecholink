//! Prepared configuration owns every foreign byte before asynchronous account work starts.
use rptadv_echolink::{account::settings::PreparedSettings, ffi::*};
use std::{mem::size_of, ptr};

fn span(text: &str) -> ByteSpan {
    ByteSpan {
        data: text.as_ptr(),
        length: text.len(),
    }
}

fn settings(
    call: &str,
    location: &str,
    servers: &[ByteSpan],
    patterns: &[ByteSpan],
) -> AccountConfig {
    AccountConfig {
        enabled: 1,
        node_number: u32::MAX,
        local_node: span("524950"),
        callsign: span(call),
        station: StationMetadata {
            name: span("Site"),
            location: span(location),
            email: span("op@example.invalid"),
            status_message: span("Available"),
            latitude: 45.0,
            longitude: -93.0,
            frequency_mhz: 146.52,
            tone_hz: 114.8,
            power_w: 10.0,
            antenna_height_m: 12.0,
            antenna_gain_db: -2.0,
            antenna_direction_deg: 180.0,
        },
        transport: TransportConfig {
            kind: 0,
            bind_address: span("127.0.0.1"),
            audio_port: 5198,
            proxy_port: 8100,
            proxy_host: span("proxy.example.invalid"),
            directory_servers: StringList {
                items: servers.as_ptr(),
                count: servers.len(),
            },
        },
        policy: AccountPolicy {
            max_stations: 20,
            max_pending_handshakes: 20,
            heartbeat_interval_ms: 10_000,
            heartbeat_timeout_ms: 100_000,
            connect_timeout_ms: 30_000,
            directory_timeout_ms: 10_000,
            registration_refresh_ms: 300_000,
            directory_refresh_ms: 240_000,
            directory_retry_ms: 20_000,
            directory_freshness_ms: 900_000,
            receive_gain_db: -3.0,
            transmit_gain_db: 3.0,
            announcement: 3,
            telemetry: 3,
            telemetry_dynamic: 1,
            allow: StringList {
                items: patterns.as_ptr(),
                count: patterns.len(),
            },
            deny: StringList::default(),
        },
        ..AccountConfig::default()
    }
}
fn secrets(password: &str) -> AccountSecrets {
    AccountSecrets {
        struct_size: size_of::<AccountSecrets>() as u32,
        abi_version: 1,
        directory_password: span(password),
        proxy_password: span("fixture-proxy-password"),
    }
}

#[test]
fn prepared_config_outlives_all_caller_buffers_and_moves_to_an_account_worker() {
    let prepared = {
        let mut call = String::from("K1ABC-R");
        let mut location = String::from("North site");
        let mut directory = String::from("directory.example.invalid");
        let mut password = String::from("fixture-directory-password");
        let servers = [span(&directory)];
        let patterns = [span("K*"), span("W?")];
        let prepared = unsafe {
            PreparedSettings::copy(
                &settings(&call, &location, &servers, &patterns),
                &secrets(&password),
            )
        }
        .unwrap();
        for original in [&mut call, &mut location, &mut directory, &mut password] {
            let replacement = "X".repeat(original.len());
            original.replace_range(.., &replacement);
        }
        prepared
    };
    let prepared = std::sync::Arc::new(prepared);
    let worker = std::sync::Arc::clone(&prepared);
    let result = std::thread::spawn(move || {
        let prepared = worker;
        assert_eq!(prepared.local_node(), "524950");
        assert_eq!(prepared.callsign(), "K1ABC-R");
        assert_eq!(
            prepared.directory_servers().collect::<Vec<_>>(),
            ["directory.example.invalid"]
        );
        assert_eq!(prepared.allow().collect::<Vec<_>>(), ["K*", "W?"]);
        assert_eq!(prepared.deny().count(), 0);
        assert_eq!(prepared.proxy_host(), "proxy.example.invalid");
        assert_eq!(prepared.proxy_password(), "fixture-proxy-password");
        assert_eq!(prepared.directory_login().request(&rptadv_echolink::account::directory::Operation::Register, 9, 3).unwrap(), b"lK1ABC-R\xac\xacfixture-directory-password\rONLINE1.00R(9: 3)\rSite\rop@example.invalid\r");
        let config = prepared.as_abi();
        assert_eq!(config.node_number, u32::MAX);
        assert_eq!(config.station.longitude, -93.0);
        assert_eq!(config.station.antenna_gain_db, -2.0);
        assert_eq!(config.policy.receive_gain_db, -3.0);
        assert_eq!(config.policy.transmit_gain_db, 3.0);
    });
    assert_eq!(prepared.callsign(), "K1ABC-R");
    result.join().unwrap();
}

#[test]
fn malformed_records_or_borrowed_spans_fail_without_publishing_settings() {
    let servers = [span("directory.example.invalid")];
    let valid_secrets = secrets("fixture-password");
    for invalid in 0..9 {
        let mut config = settings("K1ABC-R", "North", &servers, &[]);
        match invalid {
            0 => config.struct_size = 0,
            1 => config.abi_version = 2,
            2 => config.enabled = 2,
            3 => {
                config.callsign = ByteSpan {
                    data: ptr::null(),
                    length: 1,
                }
            }
            4 => config.station.location = span("bad\rfield"),
            5 => {
                config.transport.directory_servers = StringList {
                    items: ptr::null(),
                    count: 1,
                }
            }
            6 => {
                config.policy.allow = StringList {
                    items: ptr::null(),
                    count: usize::MAX,
                }
            }
            7 => config.policy.receive_gain_db = f64::NAN,
            _ => {
                config.callsign = ByteSpan {
                    data: b"\xff".as_ptr(),
                    length: 1,
                }
            }
        }
        assert!(unsafe { PreparedSettings::copy(&config, &valid_secrets) }.is_err());
    }
    let config = settings("K1ABC-R", "North", &servers, &[]);
    for invalid in 0..3 {
        let mut secret = secrets("fixture-password");
        match invalid {
            0 => secret.struct_size = 0,
            1 => secret.abi_version = 2,
            _ => secret.directory_password = ByteSpan::default(),
        }
        assert!(unsafe { PreparedSettings::copy(&config, &secret) }.is_err());
    }
}

#[test]
fn aprs_location_quantizes_physical_metadata_and_carries_minutes() {
    let servers = [span("directory.example.invalid")];
    let secret = secrets("fixture-password");
    let mut config = settings("K1ABC-R", "North", &servers, &[]);
    let prepared = unsafe { PreparedSettings::copy(&config, &secret) }.unwrap();
    assert_eq!(
        prepared.aprs_location(" On @ 0903 [2/20]").unwrap(),
        ")EL-K1ABC-!4500.00NE09300.00W0PHG3204/146520/114 On @ 0903 [2/20]"
    );
    config.station.latitude = -89.9999999;
    config.station.longitude = 179.9999999;
    config.station.power_w = 1e30;
    config.station.antenna_height_m = 0.0;
    config.station.antenna_gain_db = 100.0;
    config.station.antenna_direction_deg = 0.0;
    let prepared = unsafe { PreparedSettings::copy(&config, &secret) }.unwrap();
    assert_eq!(
        prepared.aprs_location(" ready").unwrap(),
        ")EL-K1ABC-!9000.00SE18000.00E0PHG9098/146520/114 ready"
    );
    config.station.antenna_direction_deg = -1.0;
    let prepared = unsafe { PreparedSettings::copy(&config, &secret) }.unwrap();
    assert!(prepared.aprs_location("").unwrap().contains("PHG9090"));
    assert!(prepared.aprs_location("bad\nstatus").is_err());
    assert!(prepared.aprs_location(&"x".repeat(256)).is_err());
    config.station.antenna_direction_deg = -0.5;
    assert!(unsafe { PreparedSettings::copy(&config, &secret) }.is_err());
}

fn resource_context(api: &Descriptor) -> *mut Context {
    let directory = std::env::var("RPTADV_CODEC_DIR").unwrap();
    let gsm = format!("{directory}/librptadv_codec_gsm.so.1");
    let speex = format!("{directory}/librptadv_codec_speex.so.1");
    let jitter = format!("{directory}/rpt_advanced/librptadv_jitter_adapter.so.1");
    let runtime = RuntimeConfig {
        worker_threads: 1,
        gsm_library: span(&gsm),
        speex_library: span(&speex),
        jitter_library: span(&jitter),
        ..RuntimeConfig::default()
    };
    let mut context = ptr::null_mut();
    assert_eq!(
        unsafe { api.context_create.unwrap()(&runtime, &mut context) },
        Status::Ok
    );
    context
}

#[test]
fn enabled_abi_preparation_owns_settings_without_binding_or_logging_in() {
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
    let context = resource_context(api);
    let tcp = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
    tcp.set_nonblocking(true).unwrap();
    let address = tcp.local_addr().unwrap().to_string();
    let servers = [span(&address)];
    let udp = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
    let mut config = settings("K1ABC-R", "North", &servers, &[]);
    config.transport.audio_port = udp.local_addr().unwrap().port();
    let mut account = ptr::null_mut();
    let mut password = String::from("fixture-password");
    assert_eq!(
        unsafe {
            api.account_prepare.unwrap()(context, &config, &secrets(&password), &mut account)
        },
        Status::Ok
    );
    password.replace_range(.., "removed");
    assert_eq!(
        tcp.accept().unwrap_err().kind(),
        std::io::ErrorKind::WouldBlock
    );
    unsafe { api.context_destroy.unwrap()(context) };
    assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
    unsafe { api.account_destroy.unwrap()(account) };
}

#[test]
fn activated_abi_account_dials_revalidates_incoming_ownership_and_quiesces() {
    use std::{
        io::{BufRead, Write},
        sync::atomic::{AtomicBool, AtomicPtr, AtomicUsize, Ordering},
        time::{Duration, Instant},
    };
    fn info() -> PeerInfo {
        PeerInfo {
            struct_size: size_of::<PeerInfo>() as u32,
            abi_version: 1,
            node_number: 0,
            capabilities: 0,
            account_generation: 0,
            qso_generation: 0,
            callsign: ByteSpan::default(),
            local_node: ByteSpan::default(),
        }
    }
    struct Trace {
        api: &'static Descriptor,
        fresh: AtomicBool,
        incoming: AtomicPtr<Qso>,
        rejected: AtomicUsize,
        accept: AtomicBool,
    }
    struct StepTrace {
        api: &'static Descriptor,
        qso: *mut Qso,
        speex: bool,
        calls: AtomicUsize,
        errors: AtomicUsize,
        packet: std::sync::Mutex<Vec<u8>>,
        packet_time: AtomicUsize,
        texts: AtomicUsize,
        pcm: AtomicUsize,
        gaps: AtomicUsize,
        keyed: AtomicBool,
        unkeyed: AtomicBool,
        finish: AtomicBool,
        drained: AtomicBool,
        closed: AtomicBool,
    }
    unsafe extern "C" fn audio(
        context: *mut std::ffi::c_void,
        start: u64,
        pcm: *const f32,
        count: usize,
        concealed: u32,
    ) -> usize {
        let trace = unsafe { &*context.cast::<StepTrace>() };
        if start != trace.pcm.load(Ordering::Acquire) as u64
            || concealed != 0
            || unsafe { std::slice::from_raw_parts(pcm, count) }
                .iter()
                .any(|value| !value.is_finite())
        {
            trace.errors.fetch_add(1, Ordering::Release);
        }
        let accepted = count.min(73);
        trace.pcm.fetch_add(accepted, Ordering::Release);
        accepted
    }
    unsafe extern "C" fn gap(context: *mut std::ffi::c_void, _: u64, count: u32) -> u32 {
        let trace = unsafe { &*context.cast::<StepTrace>() };
        trace.gaps.fetch_add(count as usize, Ordering::Release);
        count
    }
    unsafe extern "C" fn quiet(_: *mut std::ffi::c_void) -> i32 {
        -1
    }
    unsafe extern "C" fn text(context: *mut std::ffi::c_void, sender: ByteSpan, body: ByteSpan) {
        let trace = unsafe { &*context.cast::<StepTrace>() };
        if unsafe { std::slice::from_raw_parts(sender.data, sender.length) } != b"W1XYZ-R"
            || unsafe { std::slice::from_raw_parts(body.data, body.length) } != b"Incoming message"
        {
            trace.errors.fetch_add(1, Ordering::Release);
        }
        trace.texts.fetch_add(1, Ordering::Release);
    }
    unsafe extern "C" fn step(context: *mut std::ffi::c_void) -> i32 {
        let trace = unsafe { &*context.cast::<StepTrace>() };
        let mut identity = info();
        let mut result = rptadv_echolink::ffi::ReadResult::default();
        if trace.calls.load(Ordering::Acquire) == 0 {
            if unsafe {
                trace.api.peer_send_text.unwrap()(trace.qso, span("K1ABC-R"), span("Test message"))
            } != Status::Ok
            {
                trace.errors.fetch_add(1, Ordering::Release);
            }
            let mut encoded: rptadv_codec_client::EncodeResultRaw = unsafe { std::mem::zeroed() };
            encoded.struct_size = size_of::<rptadv_codec_client::EncodeResultRaw>() as u32;
            let pcm = [0.1; 640];
            let prepare = trace.api.peer_prepare_audio.unwrap();
            if unsafe {
                prepare(
                    trace.qso,
                    pcm.as_ptr(),
                    pcm.len(),
                    ptr::null_mut(),
                    0,
                    0,
                    ptr::from_mut(&mut encoded).cast(),
                )
            } != Status::Ok
                || encoded.status != 2
                || encoded.required_capacity != 264
                || encoded.consumed_frames != 0
            {
                trace.errors.fetch_add(1, Ordering::Release);
            }
            let mut packet = [0; 264];
            if unsafe {
                prepare(
                    trace.qso,
                    pcm.as_ptr(),
                    pcm.len(),
                    packet.as_mut_ptr(),
                    packet.len(),
                    0,
                    ptr::from_mut(&mut encoded).cast(),
                )
            } != Status::Ok
                || encoded.status != 0
                || encoded.consumed_frames != 640
                || if trace.speex {
                    !(13..=264).contains(&encoded.encoded_bytes)
                } else {
                    encoded.encoded_bytes != 144
                }
                || encoded.packet_frames != 640
                || encoded.source_start_frames != 0
                || encoded.source_frames != if trace.speex { 600 } else { 640 }
                || packet[..4] != [0xc0, if trace.speex { 0x96 } else { 3 }, 0, 0]
            {
                trace.errors.fetch_add(1, Ordering::Release);
            }
            *trace.packet.lock().unwrap() = packet[..encoded.encoded_bytes].to_vec();
            let mut elapsed = 0;
            if unsafe { trace.api.peer_elapsed_ms.unwrap()(trace.qso, &mut elapsed) } != Status::Ok
            {
                trace.errors.fetch_add(1, Ordering::Release);
            }
            trace.packet_time.store(elapsed as usize, Ordering::Release);
            if unsafe { trace.api.peer_reset_encoder.unwrap()(trace.qso) } != Status::Ok
                || unsafe {
                    prepare(
                        trace.qso,
                        pcm.as_ptr(),
                        pcm.len(),
                        packet.as_mut_ptr(),
                        packet.len(),
                        0,
                        ptr::from_mut(&mut encoded).cast(),
                    )
                } != Status::Ok
                || packet[..4] != [0xc0, if trace.speex { 0x96 } else { 3 }, 0, 1]
                || encoded.source_start_frames != 0
            {
                trace.errors.fetch_add(1, Ordering::Release);
            }
        }
        {
            let mut pending = trace.packet.lock().unwrap();
            if !pending.is_empty() {
                match unsafe {
                    trace.api.peer_send_prepared.unwrap()(
                        trace.qso,
                        ByteSpan {
                            data: pending.as_ptr(),
                            length: pending.len(),
                        },
                        640,
                        trace.packet_time.load(Ordering::Acquire) as u32,
                    )
                } {
                    Status::Ok => pending.clear(),
                    Status::Pending | Status::Full => {}
                    _ => {
                        trace.errors.fetch_add(1, Ordering::Release);
                    }
                }
            }
        }
        let sink = PcmSink {
            struct_size: size_of::<PcmSink>() as u32,
            abi_version: 1,
            context,
            audio: Some(audio),
            gap: Some(gap),
        };
        let status = unsafe {
            trace.api.peer_read.unwrap()(
                trace.qso,
                // A retired Speex product generation discards its accepted tail;
                // GSM proves successful playout. Neither may release before the token.
                if trace.drained.load(Ordering::Acquire) {
                    if trace.speex { 2 } else { 1 }
                } else {
                    0
                },
                &sink,
                Some(text),
                context,
                &mut result,
            )
        };
        if status != Status::Ok {
            trace.errors.fetch_add(1, Ordering::Release);
        }
        if result.activity == 1 {
            trace.keyed.store(true, Ordering::Release);
        }
        if result.activity == 2 {
            trace.unkeyed.store(true, Ordering::Release);
        }
        if result.finish_pending == 1 {
            trace.finish.store(true, Ordering::Release);
        }
        let closed = result.state == 3;
        trace.closed.store(closed, Ordering::Release);
        if unsafe { trace.api.peer_info.unwrap()(trace.qso, &mut identity) } != Status::Ok
            || identity.node_number != 123456
            || unsafe {
                trace.api.peer_read.unwrap()(
                    trace.qso,
                    0,
                    ptr::null(),
                    None,
                    ptr::null_mut(),
                    &mut result,
                )
            } != Status::Ok
            || (result.state != 1 && result.state != 2 && result.state != 3)
            || unsafe { trace.api.peer_set_deadline.unwrap()(trace.qso, 20_000_000) } != Status::Ok
        {
            trace.errors.fetch_add(1, Ordering::Release);
        }
        trace.calls.fetch_add(1, Ordering::Release);
        if closed { -1 } else { 0 }
    }
    unsafe extern "C" fn notify(context: *mut std::ffi::c_void, event: *const Event) {
        let trace = unsafe { &*context.cast::<Trace>() };
        let event = unsafe { &*event };
        if event.kind == 0 && event.status == Status::Ok {
            trace.fresh.store(true, Ordering::Release);
        } else if event.kind == 1 {
            let mut identity = info();
            assert_eq!(
                unsafe { trace.api.validate_pending.unwrap()(event.qso, &mut identity) },
                Status::Ok
            );
            assert_eq!(identity.node_number, 123456);
            if trace.accept.load(Ordering::Acquire) {
                unsafe { event.taken.write(1) };
                trace.incoming.store(event.qso, Ordering::Release);
            } else {
                trace.rejected.fetch_add(1, Ordering::Release);
            }
        }
    }
    #[track_caller]
    fn wait(mut ready: impl FnMut() -> bool) {
        let deadline = Instant::now() + Duration::from_secs(3);
        while !ready() {
            assert!(
                Instant::now() < deadline,
                "account operation did not progress"
            );
            std::thread::sleep(Duration::from_millis(1));
        }
    }
    for speex in [false, true] {
        let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
        let context = resource_context(api);
        let listener = std::net::TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        let address = listener.local_addr().unwrap().to_string();
        let server = std::thread::spawn(move || {
            for (index, response) in [b"OK".as_slice(), b"@@@\n2\nW1XYZ-R\nIncoming\n123456\n127.0.0.2\nN2DEF-R\nOutgoing\n654321\n127.0.0.1\n+++\n", b"OK"].into_iter().enumerate() {
            let mut accepted = None;
            wait(|| match listener.accept() {
                Ok(socket) => { accepted = Some(socket.0); true },
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => false,
                Err(error) => panic!("listener failed: {error:?}"),
            });
            let mut socket = accepted.unwrap();
            socket.set_read_timeout(Some(Duration::from_secs(3))).unwrap();
            let mut reader = std::io::BufReader::new(&mut socket);
            let mut request = Vec::new();
            for _ in 0..[4, 1, 3][index] { assert_ne!(reader.read_until(b'\r', &mut request).unwrap(), 0); }
            match index {
                0 => assert!(request.starts_with(b"lK1ABC-R\xac\xacfixture-password\rONLINE1.00R(")),
                1 => assert_eq!(request, b"F0\r"),
                _ => assert!(request.windows(b"OFF-V3.40".len()).any(|bytes| bytes == b"OFF-V3.40")),
            }
            socket.write_all(response).unwrap();
        }
        });
        let audio = std::net::UdpSocket::bind("127.0.0.1:0").unwrap();
        let port = audio.local_addr().unwrap().port();
        let control = std::net::UdpSocket::bind((std::net::Ipv4Addr::LOCALHOST, port + 1)).unwrap();
        let servers = [span(&address)];
        let mut config = settings("K1ABC-R", "North", &servers, &[]);
        config.transport.audio_port = port;
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
        let trace = Trace {
            api,
            fresh: AtomicBool::new(false),
            incoming: AtomicPtr::default(),
            rejected: AtomicUsize::new(0),
            accept: AtomicBool::new(true),
        };
        let activate = api
            .account_activate
            .expect("real account activation missing");
        assert_eq!(
            unsafe {
                activate(
                    account,
                    Some(notify),
                    ptr::from_ref(&trace).cast_mut().cast(),
                )
            },
            Status::Ok
        );
        wait(|| trace.fresh.load(Ordering::Acquire));
        let mut outgoing = ptr::null_mut();
        assert_eq!(
            unsafe { api.dial.unwrap()(account, 654321, &mut outgoing) },
            Status::Ok
        );
        let peer = std::net::UdpSocket::bind("127.0.0.2:0").unwrap();
        let media = std::net::UdpSocket::bind("127.0.0.2:5198").unwrap();
        media
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let feedback = std::net::UdpSocket::bind("127.0.0.2:5199").unwrap();
        feedback
            .set_read_timeout(Some(Duration::from_secs(3)))
            .unwrap();
        let mut sdes = [0; 512];
        let length = rptadv_echolink::wire::write_control(
            &rptadv_echolink::wire::ControlPacket::Sdes {
                source: 7,
                callsign: "W1XYZ-R".into(),
                station_name: "Peer".into(),
                speex,
            },
            &mut sdes,
        )
        .unwrap();
        peer.send_to(&sdes[..length], (std::net::Ipv4Addr::LOCALHOST, port + 1))
            .unwrap();
        wait(|| !trace.incoming.load(Ordering::Acquire).is_null());
        let incoming = trace.incoming.swap(ptr::null_mut(), Ordering::AcqRel);
        let mut identity = info();
        assert_eq!(
            unsafe { api.peer_info.unwrap()(incoming, &mut identity) },
            Status::Ok
        );
        assert_ne!(identity.account_generation, 0);
        assert_ne!(identity.qso_generation, 0);
        assert_eq!(
            unsafe { std::slice::from_raw_parts(identity.callsign.data, identity.callsign.length) },
            b"W1XYZ-R"
        );
        let serial = StepTrace {
            api,
            qso: incoming,
            speex,
            calls: AtomicUsize::new(0),
            errors: AtomicUsize::new(0),
            packet: std::sync::Mutex::new(Vec::new()),
            packet_time: AtomicUsize::new(0),
            texts: AtomicUsize::new(0),
            pcm: AtomicUsize::new(0),
            gaps: AtomicUsize::new(0),
            keyed: AtomicBool::new(false),
            unkeyed: AtomicBool::new(false),
            finish: AtomicBool::new(false),
            drained: AtomicBool::new(false),
            closed: AtomicBool::new(false),
        };
        let mut registration = ptr::null_mut();
        api.peer_read.expect("source-rate receive bridge missing");
        api.peer_prepare_audio
            .expect("prepared-media ABI bridge missing");
        api.peer_send_prepared
            .expect("prepared send bridge missing");
        api.peer_reset_encoder
            .expect("encoder reset bridge missing");
        api.peer_elapsed_ms.expect("source clock bridge missing");
        api.peer_send_text.expect("human text bridge missing");
        assert_eq!(
            unsafe {
                api.peer_attach.expect("serial owner missing")(
                    context,
                    incoming,
                    Some(step),
                    ptr::from_ref(&serial).cast_mut().cast(),
                    &mut registration,
                )
            },
            Status::Ok
        );
        assert!(!registration.is_null());
        let mut retiring_registration = ptr::null_mut();
        assert_eq!(
            unsafe {
                api.peer_attach.unwrap()(
                    context,
                    outgoing,
                    Some(quiet),
                    ptr::null_mut(),
                    &mut retiring_registration,
                )
            },
            Status::Ok
        );
        unsafe { api.context_destroy.unwrap()(context) };
        wait(|| serial.calls.load(Ordering::Acquire) >= 3);
        let mut chat = [0; 128];
        let (count, _) = media.recv_from(&mut chat).unwrap();
        assert_eq!(&chat[..count], b"oNDATAK1ABC-R>Test message\r\n\0");
        media
            .send_to(
                b"oNDATAW1XYZ-R>Incoming message\r\n\0",
                (std::net::Ipv4Addr::LOCALHOST, port),
            )
            .unwrap();
        wait(|| serial.texts.load(Ordering::Acquire) == 1);
        let mut encoded = [0; 264];
        let (media_length, _) = media.recv_from(&mut encoded).unwrap();
        assert_eq!(&encoded[..4], &[0xc0, if speex { 0x96 } else { 3 }, 0, 0]);
        media
            .send_to(
                &encoded[..media_length],
                (std::net::Ipv4Addr::LOCALHOST, port),
            )
            .unwrap();
        wait(|| serial.pcm.load(Ordering::Acquire) == 640 && serial.finish.load(Ordering::Acquire));
        assert!(serial.keyed.load(Ordering::Acquire));
        assert!(
            !serial.unkeyed.load(Ordering::Acquire),
            "must wait for the exact product finite token"
        );
        serial.drained.store(true, Ordering::Release);
        unsafe { api.peer_wake.unwrap()(registration) };
        wait(|| {
            serial.unkeyed.load(Ordering::Acquire) || serial.errors.load(Ordering::Acquire) != 0
        });
        assert_eq!(
            serial.errors.load(Ordering::Acquire),
            0,
            "played and explicitly cancelled finite prefixes must both release the talker"
        );
        unsafe { api.peer_stop.unwrap()(registration) };
        wait(|| serial.closed.load(Ordering::Acquire));
        let mut control = [0; 512];
        loop {
            let (count, _) = feedback.recv_from(&mut control).unwrap();
            if matches!(
                rptadv_echolink::wire::parse_datagram(
                    rptadv_echolink::wire::Channel::Control,
                    &control[..count]
                )
                .unwrap(),
                rptadv_echolink::wire::WirePacket::Control(
                    rptadv_echolink::wire::ControlPacket::Bye { .. }
                )
            ) {
                break;
            }
        }
        unsafe { api.peer_detach.unwrap()(registration) };
        let calls = serial.calls.load(Ordering::Acquire);
        assert_eq!(serial.errors.load(Ordering::Acquire), 0);
        peer.send_to(&sdes[..length], (std::net::Ipv4Addr::LOCALHOST, port + 1))
            .unwrap();
        std::thread::sleep(Duration::from_millis(30));
        assert_eq!(serial.calls.load(Ordering::Acquire), calls);
        unsafe { api.peer_destroy.unwrap()(incoming) };
        trace.accept.store(false, Ordering::Release);
        wait(|| {
            peer.send_to(&sdes[..length], (std::net::Ipv4Addr::LOCALHOST, port + 1))
                .unwrap();
            trace.rejected.load(Ordering::Acquire) != 0
        });
        assert_eq!(unsafe { api.account_stop.unwrap()(account) }, Status::Ok);
        assert_eq!(
            unsafe { api.validate_pending.unwrap()(outgoing, &mut info()) },
            Status::Closed
        );
        unsafe {
            api.account_destroy.unwrap()(account);
            api.peer_detach.unwrap()(retiring_registration);
            api.peer_destroy.unwrap()(outgoing);
        }
        server.join().unwrap();
    }
}
