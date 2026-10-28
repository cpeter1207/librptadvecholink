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

#[test]
fn enabled_abi_preparation_owns_settings_without_binding_or_logging_in() {
    let api = unsafe { &*rptadv_echolink::rptadv_echolink_descriptor_v1() };
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
