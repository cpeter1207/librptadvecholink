//! Borrowed resolved account fixtures shared by account and reload tests.
use rptadv_echolink::ffi::*;
use std::mem::size_of;
pub fn span(text: &str) -> ByteSpan {
    ByteSpan {
        data: text.as_ptr(),
        length: text.len(),
    }
}

pub fn settings(
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
pub fn secrets(password: &str) -> AccountSecrets {
    AccountSecrets {
        struct_size: size_of::<AccountSecrets>() as u32,
        abi_version: 1,
        directory_password: span(password),
        proxy_password: span("fixture-proxy-password"),
    }
}
