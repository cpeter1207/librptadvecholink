//! Immutable owned backing for borrowed C account records; no socket or login side effects.

use super::directory::DirectoryLogin;
use crate::ffi::{AccountConfig, AccountSecrets, ByteSpan, Status, StringList};
use std::{
    mem::{needs_drop, size_of},
    net::Ipv4Addr,
    ptr, slice, str,
};

const MAX_OWNED_BYTES: usize = 1024 * 1024;
const _: () = assert!(!needs_drop::<AccountConfig>() && !needs_drop::<AccountSecrets>());

/// Account preparation copies foreign bytes once, retaining stable allocations until shutdown.
/// There is deliberately no Debug/Display implementation for credential-bearing state.
pub struct PreparedSettings {
    config: AccountConfig,
    secrets: AccountSecrets,
    buffers: Vec<Box<[u8]>>,
    lists: Vec<Box<[ByteSpan]>>,
    remaining: usize,
}

// Every exposed pointer is replaced with immutable, uniquely retained boxed storage
// before publication. No method mutates those buffers after preparation.
unsafe impl Send for PreparedSettings {}
unsafe impl Sync for PreparedSettings {}

impl PreparedSettings {
    /// Validate resolved enabled-account values and own all borrowed fields, without I/O.
    /// The one-MiB account-copy budget includes list storage and string bytes.
    ///
    /// # Safety
    /// Each nonnull span/list truthfully describes aligned initialized readable memory, retained
    /// without mutation until this function returns. Null is valid only at zero length.
    pub unsafe fn copy(config: &AccountConfig, secrets: &AccountSecrets) -> Result<Self, Status> {
        if config.struct_size < size_of::<AccountConfig>() as u32
            || config.abi_version != 1
            || config.enabled != 1
            || config.node_number == 0
            || secrets.struct_size < size_of::<AccountSecrets>() as u32
            || secrets.abi_version != 1
            || !valid_numbers(config)
        {
            return Err(Status::Invalid);
        }
        // SAFETY: ABI records are borrowed POD, enforced by the compile-time Drop check.
        // On any failure their raw fields own nothing; only the backing vectors release data.
        let mut owned = Self {
            config: unsafe { ptr::read(config) },
            secrets: unsafe { ptr::read(secrets) },
            buffers: Vec::new(),
            lists: Vec::new(),
            remaining: MAX_OWNED_BYTES,
        };
        owned.config.struct_size = size_of::<AccountConfig>() as u32;
        owned.secrets.struct_size = size_of::<AccountSecrets>() as u32;
        owned.config.local_node = unsafe { owned.bytes(config.local_node, 32)? };
        owned.config.callsign = unsafe { owned.bytes(config.callsign, 15)? };
        owned.config.station.name = unsafe { owned.bytes(config.station.name, 240)? };
        owned.config.station.location = unsafe { owned.bytes(config.station.location, 4096)? };
        owned.config.station.email = unsafe { owned.bytes(config.station.email, 4096)? };
        owned.config.station.status_message =
            unsafe { owned.bytes(config.station.status_message, 4096)? };
        owned.config.transport.bind_address =
            unsafe { owned.bytes(config.transport.bind_address, 253)? };
        owned.config.transport.proxy_host =
            unsafe { owned.bytes(config.transport.proxy_host, 253)? };
        owned.config.transport.directory_servers =
            unsafe { owned.list(config.transport.directory_servers, 253)? };
        owned.config.policy.allow = unsafe { owned.list(config.policy.allow, 64)? };
        owned.config.policy.deny = unsafe { owned.list(config.policy.deny, 64)? };
        owned.secrets.directory_password =
            unsafe { owned.bytes(secrets.directory_password, 4096)? };
        owned.secrets.proxy_password = unsafe { owned.bytes(secrets.proxy_password, 4096)? };
        if owned.local_node().is_empty()
            || !owned.local_node().bytes().all(|byte| byte.is_ascii_digit())
            || owned.callsign().is_empty()
            || !owned
                .callsign()
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
            || owned.known(owned.secrets.directory_password).is_empty()
            || owned.config.transport.directory_servers.count == 0
            || owned
                .directory_servers()
                .any(|host| host.is_empty() || host.chars().any(char::is_whitespace))
        {
            return Err(Status::Invalid);
        }
        match owned.config.transport.kind {
            0 if owned.config.transport.audio_port != 0
                && owned.config.transport.audio_port < u16::MAX
                && owned
                    .known(owned.config.transport.bind_address)
                    .parse::<Ipv4Addr>()
                    .is_ok() => {}
            1 if owned.config.transport.proxy_port != 0
                && !owned.proxy_host().is_empty()
                && !owned.proxy_host().chars().any(char::is_whitespace) => {}
            _ => return Err(Status::Invalid),
        }
        Ok(owned)
    }

    fn charge(&mut self, bytes: usize) -> Result<(), Status> {
        self.remaining = self.remaining.checked_sub(bytes).ok_or(Status::Invalid)?;
        Ok(())
    }

    unsafe fn bytes(&mut self, field: ByteSpan, maximum: usize) -> Result<ByteSpan, Status> {
        if field.length > maximum || (field.data.is_null() && field.length != 0) {
            return Err(Status::Invalid);
        }
        self.charge(field.length)?;
        let bytes = if field.length == 0 {
            &[]
        } else {
            // SAFETY: the caller's span contract and checked length cover this borrow.
            unsafe { slice::from_raw_parts(field.data, field.length) }
        };
        if str::from_utf8(bytes)
            .map_err(|_| Status::Invalid)?
            .chars()
            .any(char::is_control)
        {
            return Err(Status::Invalid);
        }
        let copy = bytes.to_vec().into_boxed_slice();
        let field = ByteSpan {
            data: copy.as_ptr(),
            length: copy.len(),
        };
        self.buffers.push(copy);
        Ok(field)
    }

    unsafe fn list(&mut self, field: StringList, maximum: usize) -> Result<StringList, Status> {
        let storage = field
            .count
            .checked_mul(size_of::<ByteSpan>())
            .ok_or(Status::Invalid)?;
        if field.items.is_null() && field.count != 0 {
            return Err(Status::Invalid);
        }
        self.charge(storage)?;
        let input = if field.count == 0 {
            &[]
        } else {
            // SAFETY: checked byte budget also bounds slice length; the caller owns this list.
            unsafe { slice::from_raw_parts(field.items, field.count) }
        };
        let mut copy = Vec::with_capacity(input.len());
        for span in input {
            copy.push(unsafe { self.bytes(*span, maximum)? });
        }
        let copy = copy.into_boxed_slice();
        let field = StringList {
            items: copy.as_ptr(),
            count: copy.len(),
        };
        self.lists.push(copy);
        Ok(field)
    }

    fn known(&self, span: ByteSpan) -> &str {
        // SAFETY: only private records are passed here; every span has been replaced
        // with retained immutable storage and checked as UTF-8 during preparation.
        let bytes = if span.length == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(span.data, span.length) }
        };
        unsafe { str::from_utf8_unchecked(bytes) }
    }

    fn entries(&self, list: StringList) -> impl Iterator<Item = &str> {
        // SAFETY: these are only the private, copied lists retained by self.lists.
        let entries = if list.count == 0 {
            &[]
        } else {
            unsafe { slice::from_raw_parts(list.items, list.count) }
        };
        entries.iter().map(|span| self.known(*span))
    }

    /// Borrow the owned record for a synchronous foreign call; retain this owner throughout it.
    pub fn as_abi(&self) -> &AccountConfig {
        &self.config
    }

    /// Identity, credentials or the active transport require an account handoff.
    /// Inactive Direct/Proxy fields are irrelevant; policy and metadata remain live.
    pub fn requires_handoff(&self, next: &Self) -> bool {
        self.local_node() != next.local_node()
            || self.callsign() != next.callsign()
            || self.config.node_number != next.config.node_number
            || self.directory_login().password != next.directory_login().password
            || self.config.transport.kind != next.config.transport.kind
            || match self.config.transport.kind {
                0 => {
                    self.bind_address() != next.bind_address()
                        || self.config.transport.audio_port != next.config.transport.audio_port
                }
                _ => {
                    self.proxy_host() != next.proxy_host()
                        || self.config.transport.proxy_port != next.config.transport.proxy_port
                        || self.proxy_password() != next.proxy_password()
                }
            }
    }
    /// Owning controller identity.
    pub fn local_node(&self) -> &str {
        self.known(self.config.local_node)
    }
    /// This account's validated station label.
    pub fn callsign(&self) -> &str {
        self.known(self.config.callsign)
    }
    pub(crate) fn bind_address(&self) -> &str {
        self.known(self.config.transport.bind_address)
    }
    /// Configured standard Proxy endpoint, not an HTTP proxy.
    pub fn proxy_host(&self) -> &str {
        self.known(self.config.transport.proxy_host)
    }
    /// Borrow only for standard Proxy authentication; never log or format it.
    pub fn proxy_password(&self) -> &str {
        self.known(self.secrets.proxy_password)
    }
    /// Ordered directory hosts, independent of QSO endpoints.
    pub fn directory_servers(&self) -> impl Iterator<Item = &str> {
        self.entries(self.config.transport.directory_servers)
    }
    /// Normalized allow patterns resolved by the controller.
    pub fn allow(&self) -> impl Iterator<Item = &str> {
        self.entries(self.config.policy.allow)
    }
    /// Deny patterns retain precedence over allow.
    pub fn deny(&self) -> impl Iterator<Item = &str> {
        self.entries(self.config.policy.deny)
    }
    /// Format RF/location metadata for the EchoLink APRS LOCATION item.
    /// Coordinates round to hundredths of a minute with degree carry. PHG
    /// values use nearest representable digits clamped to 0..9; height H0 is
    /// the smallest representable height (10 feet), including unknown/zero.
    /// Direction -1 is omnidirectional; zero/360 is north (PHG8).
    pub fn aprs_location(&self, status: &str) -> Result<String, crate::wire::WireError> {
        use crate::wire::WireError;
        if status.chars().any(char::is_control) {
            return Err(WireError::Malformed);
        }
        if status.len() > 255 {
            return Err(WireError::TooLarge);
        }
        let station = &self.config.station;
        let latitude = (station.latitude.abs() * 6000.0).round() as u32;
        let longitude = (station.longitude.abs() * 6000.0).round() as u32;
        let digit = |value: f64| value.round().clamp(0.0, 9.0) as u8;
        let power = digit(station.power_w.sqrt());
        let height = digit((station.antenna_height_m * 3.28084 / 10.0).log2());
        let gain = digit(station.antenna_gain_db);
        let direction = if station.antenna_direction_deg < 0.0 {
            0
        } else {
            let sector = (station.antenna_direction_deg / 45.0).round() as u8;
            if sector == 0 { 8 } else { sector }
        };
        let location = format!(
            ")EL-{:<6.6}!{:02}{:02}.{:02}{}E{:03}{:02}.{:02}{}0PHG{power}{height}{gain}{direction}/{:06}/{:03}{status}",
            self.callsign(),
            latitude / 6000,
            latitude / 100 % 60,
            latitude % 100,
            if station.latitude < 0.0 { 'S' } else { 'N' },
            longitude / 6000,
            longitude / 100 % 60,
            longitude % 100,
            if station.longitude < 0.0 { 'W' } else { 'E' },
            (station.frequency_mhz * 1000.0).round().min(999_999.0) as u32,
            (station.tone_hz + 0.05).floor().min(999.0) as u32,
        );
        if location.len() > 255 {
            return Err(WireError::TooLarge);
        }
        Ok(location)
    }
    /// Borrow credentials only while creating one directory request.
    pub fn directory_login(&self) -> DirectoryLogin<'_> {
        let name = self.known(self.config.station.name);
        DirectoryLogin {
            callsign: self.callsign(),
            password: self.known(self.secrets.directory_password),
            description: if name.is_empty() {
                self.known(self.config.station.location)
            } else {
                name
            },
            email: self.known(self.config.station.email),
        }
    }
}

fn valid_numbers(config: &AccountConfig) -> bool {
    let policy = &config.policy;
    let station = &config.station;
    [
        policy.max_stations,
        policy.max_pending_handshakes,
        policy.heartbeat_interval_ms,
        policy.heartbeat_timeout_ms,
        policy.connect_timeout_ms,
        policy.directory_timeout_ms,
        policy.registration_refresh_ms,
        policy.directory_refresh_ms,
        policy.directory_retry_ms,
        policy.directory_freshness_ms,
    ]
    .iter()
    .all(|value| *value != 0)
        && policy
            .max_stations
            .checked_add(policy.max_pending_handshakes)
            .is_some()
        && policy.announcement <= 3
        && policy.telemetry <= 3
        && policy.telemetry_dynamic <= 1
        && [policy.receive_gain_db, policy.transmit_gain_db]
            .iter()
            .all(|value| value.is_finite() && (-60.0..=60.0).contains(value))
        && station.latitude.is_finite()
        && (-90.0..=90.0).contains(&station.latitude)
        && station.longitude.is_finite()
        && (-180.0..=180.0).contains(&station.longitude)
        && station.antenna_direction_deg.is_finite()
        && (station.antenna_direction_deg == -1.0
            || (0.0..=360.0).contains(&station.antenna_direction_deg))
        && station.antenna_gain_db.is_finite()
        && [
            station.frequency_mhz,
            station.tone_hz,
            station.power_w,
            station.antenna_height_m,
        ]
        .iter()
        .all(|value| value.is_finite() && *value >= 0.0)
}
