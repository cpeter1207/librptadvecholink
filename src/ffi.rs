//! C-compatible records. Packet-library and Tokio objects never cross this boundary.
use std::{ffi::c_void, mem::size_of, ptr, sync::Arc};

/// Bounded operation outcome; Full does not consume input.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum Status {
    /// Completed successfully.
    Ok = 0,
    /// Waiting for readiness, not a terminal failure.
    Pending = 1,
    /// Capacity exhausted without consuming the offered input.
    Full = 2,
    /// Capability is unavailable.
    Unsupported = 3,
    /// Invalid sized record, argument or byte span.
    Invalid = 4,
    /// Handle's session is closed.
    Closed = 5,
    /// Unrecoverable owner/setup failure.
    Failed = 6,
}

/// Borrowed bytes, valid for the duration of the enclosing operation.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct ByteSpan {
    /// Readable bytes; null permitted only for zero length.
    pub data: *const u8,
    /// Number of readable bytes.
    pub length: usize,
}
impl Default for ByteSpan {
    fn default() -> Self {
        Self {
            data: ptr::null(),
            length: 0,
        }
    }
}

/// Borrowed contiguous list of byte spans.
#[derive(Clone, Copy)]
#[repr(C)]
pub struct StringList {
    /// List pointer; null permitted only when count is zero.
    pub items: *const ByteSpan,
    /// Number of list entries.
    pub count: usize,
}
impl Default for StringList {
    fn default() -> Self {
        Self {
            items: ptr::null(),
            count: 0,
        }
    }
}

/// One provider-owned worker pool; created off audio and retained by all accounts.
pub struct Context {
    pub(crate) runtime: Arc<tokio::runtime::Runtime>,
}
/// Opaque account, released only after its owners and callbacks have stopped.
pub struct Account {
    pub(crate) _runtime: Arc<tokio::runtime::Runtime>,
}
/// Opaque provider-created QSO; its definition is private to this DSO.
#[repr(C)]
pub struct Qso {
    _private: [u8; 0],
}
/// Opaque serial-step registration, retained until detach returns.
#[repr(C)]
pub struct Registration {
    _private: [u8; 0],
}

/// Context setup: zero workers selects available CPUs clamped to one through four.
#[repr(C)]
pub struct RuntimeConfig {
    /// Complete readable record size.
    pub struct_size: u32,
    /// Must be one.
    pub abi_version: u32,
    /// Explicit worker count 1..256 or zero for the default.
    pub worker_threads: u32,
}
impl Default for RuntimeConfig {
    fn default() -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            abi_version: 1,
            worker_threads: 0,
        }
    }
}

/// Station metadata only; none of these values change radio hardware.
#[derive(Default)]
#[repr(C)]
pub struct StationMetadata {
    /// UTF-8 station name.
    pub name: ByteSpan,
    /// UTF-8 location.
    pub location: ByteSpan,
    /// Contact email.
    pub email: ByteSpan,
    /// Station information, never a controller command.
    pub status_message: ByteSpan,
    /// Decimal degrees north.
    pub latitude: f64,
    /// Decimal degrees east.
    pub longitude: f64,
    /// Advertised RF frequency in MHz.
    pub frequency_mhz: f64,
    /// Advertised CTCSS tone frequency in Hz.
    pub tone_hz: f64,
    /// Advertised power in watts.
    pub power_w: f64,
    /// Antenna height in metres.
    pub antenna_height_m: f64,
    /// Advertised antenna gain in dB.
    pub antenna_gain_db: f64,
    /// Antenna direction in degrees.
    pub antenna_direction_deg: f64,
}

/// Selected account transport; secrets are deliberately separate.
#[derive(Default)]
#[repr(C)]
pub struct TransportConfig {
    /// Direct=0, standard TCP Proxy=1.
    pub kind: u32,
    /// Local IPv4 bind address.
    pub bind_address: ByteSpan,
    /// Direct UDP audio port; control is audio_port+1.
    pub audio_port: u16,
    /// Proxy TCP port, normally 8100.
    pub proxy_port: u16,
    /// Proxy hostname; not a generic HTTP proxy.
    pub proxy_host: ByteSpan,
    /// Ordered directory hostnames.
    pub directory_servers: StringList,
}

/// Resolved account limits, deadlines, gains and announcement policy.
#[derive(Default)]
#[repr(C)]
pub struct AccountPolicy {
    /// Maximum admitted QSOs.
    pub max_stations: u32,
    /// Separate pending unauthenticated-handshake budget.
    pub max_pending_handshakes: u32,
    /// Time between control heartbeats.
    pub heartbeat_interval_ms: u32,
    /// Valid-packet liveness timeout.
    pub heartbeat_timeout_ms: u32,
    /// Initial QSO setup timeout.
    pub connect_timeout_ms: u32,
    /// Directory connect/read operation deadline.
    pub directory_timeout_ms: u32,
    /// Directory login refresh interval.
    pub registration_refresh_ms: u32,
    /// Normal directory snapshot refresh interval.
    pub directory_refresh_ms: u32,
    /// Failed-directory retry interval.
    pub directory_retry_ms: u32,
    /// Maximum age for new-call directory authentication.
    pub directory_freshness_ms: u32,
    /// Receive gain in dB applied once at the protocol edge.
    pub receive_gain_db: f64,
    /// Transmit gain in dB applied once at the protocol edge.
    pub transmit_gain_db: f64,
    /// Off=0, node=1, callsign=2, both=3.
    pub announcement: u32,
    /// Off=0, on=1, follow=2, demand=3.
    pub telemetry: u32,
    /// Nonzero permits authorized runtime telemetry selection.
    pub telemetry_dynamic: u32,
    /// Normalized wildcard allow patterns.
    pub allow: StringList,
    /// Deny patterns take precedence.
    pub deny: StringList,
}

/// Fully resolved per-node configuration, borrowed during prepare/reconfigure.
#[repr(C)]
pub struct AccountConfig {
    /// Complete readable record size.
    pub struct_size: u32,
    /// Must be one.
    pub abi_version: u32,
    /// Disabled=0, enabled=1.
    pub enabled: u32,
    /// Complete real EchoLink ID, not an ASL alias.
    pub node_number: u32,
    /// Owning controller node.
    pub local_node: ByteSpan,
    /// Validated station callsign including -L/-R suffix if applicable.
    pub callsign: ByteSpan,
    /// Advertised station metadata.
    pub station: StationMetadata,
    /// Selected transport.
    pub transport: TransportConfig,
    /// Controller-resolved policy and timings.
    pub policy: AccountPolicy,
}
impl Default for AccountConfig {
    fn default() -> Self {
        Self {
            struct_size: size_of::<Self>() as u32,
            abi_version: 1,
            enabled: 0,
            node_number: 0,
            local_node: ByteSpan::default(),
            callsign: ByteSpan::default(),
            station: StationMetadata::default(),
            transport: TransportConfig::default(),
            policy: AccountPolicy::default(),
        }
    }
}

/// Account-specific secrets; never printable or part of status/log records.
#[repr(C)]
pub struct AccountSecrets {
    /// Complete readable record size.
    pub struct_size: u32,
    /// Must be one.
    pub abi_version: u32,
    /// Directory password.
    pub directory_password: ByteSpan,
    /// Distinct Proxy password.
    pub proxy_password: ByteSpan,
}

/// Immutable connection identity, copied before another operation can retire it.
#[repr(C)]
pub struct PeerInfo {
    /// Caller-provided output record size.
    pub struct_size: u32,
    /// Must be one.
    pub abi_version: u32,
    /// Complete real EchoLink station ID.
    pub node_number: u32,
    /// Available operations for this connection.
    pub capabilities: u32,
    /// Owning account generation.
    pub account_generation: u64,
    /// Exact QSO generation.
    pub qso_generation: u64,
    /// Validated callsign, stable until QSO destruction.
    pub callsign: ByteSpan,
    /// Owning controller node, stable until QSO destruction.
    pub local_node: ByteSpan,
}

/// Borrowed account event; an incoming QSO's ownership is explicitly accepted or retained.
#[repr(C)]
pub struct Event {
    /// Complete record size.
    pub struct_size: u32,
    /// Status=0, pending QSO=1, terminal QSO=2.
    pub kind: u32,
    /// Provider-created QSO or null for account status.
    pub qso: *mut Qso,
    /// Account/QSO generation appropriate to kind.
    pub generation: u64,
    /// Value-free operation/termination status.
    pub status: Status,
}
/// Off-audio event callback. Must not unwind; copy borrowed data before returning.
pub type EventFn = unsafe extern "C" fn(*mut c_void, *const Event);
/// Serial peer turn; negative stops notification, detach waits for quiescence.
pub type StepFn = unsafe extern "C" fn(*mut c_void) -> i32;

/// Versioned provider table. Null operation slots are not advertised as capabilities.
#[repr(C)]
pub struct Descriptor {
    /// Complete table size.
    pub struct_size: u32,
    /// Exact ABI one.
    pub abi_version: u32,
    /// NUL-padded rptadv.echolink.v1.
    pub capability: [u8; 24],
    /// Lifecycle=1, account network=2, prepared media=4, text=8.
    pub capabilities: u64,
    /// Create one shared worker pool; failure clears output.
    pub context_create:
        Option<unsafe extern "C" fn(*const RuntimeConfig, *mut *mut Context) -> Status>,
    /// Release caller ownership; accounts retain their own runtime ownership.
    pub context_destroy: Option<unsafe extern "C" fn(*mut Context)>,
    /// Validate/copy configuration without login; failure clears output.
    pub account_prepare: Option<
        unsafe extern "C" fn(
            *mut Context,
            *const AccountConfig,
            *const AccountSecrets,
            *mut *mut Account,
        ) -> Status,
    >,
    /// Start account I/O off audio; callback data is borrowed only during invocation.
    pub account_activate:
        Option<unsafe extern "C" fn(*mut Account, Option<EventFn>, *mut c_void) -> Status>,
    /// Prepare/commit an account generation without touching other accounts.
    pub account_reconfigure: Option<
        unsafe extern "C" fn(*mut Account, *const AccountConfig, *const AccountSecrets) -> Status,
    >,
    /// Gate new work and stop this account; repeated stop is harmless.
    pub account_stop: Option<unsafe extern "C" fn(*mut Account) -> Status>,
    /// Release after stop and all borrowed callbacks finish; null is accepted.
    pub account_destroy: Option<unsafe extern "C" fn(*mut Account)>,
    /// Dial a real numeric EchoLink ID; failure clears QSO output.
    pub dial: Option<unsafe extern "C" fn(*mut Account, u32, *mut *mut Qso) -> Status>,
    /// Recheck provider-bound pending admission, not caller-supplied proof.
    pub validate_pending: Option<unsafe extern "C" fn(*const Qso, *mut PeerInfo) -> Status>,
    /// Copy the immutable peer identity/capability record.
    pub peer_info: Option<unsafe extern "C" fn(*const Qso, *mut PeerInfo) -> Status>,
    /// Borrow a QSO for one serial callback owner until detach.
    pub peer_attach: Option<
        unsafe extern "C" fn(
            *mut Context,
            *mut Qso,
            Option<StepFn>,
            *mut c_void,
            *mut *mut Registration,
        ) -> Status,
    >,
    /// Callback-safe nonblocking stop request.
    pub peer_stop: Option<unsafe extern "C" fn(*mut Registration)>,
    /// Stop and wait off executor; never invoke from the registered callback.
    pub peer_detach: Option<unsafe extern "C" fn(*mut Registration)>,
    /// Signal the existing peer owner without spawning per-packet tasks.
    pub peer_wake: Option<unsafe extern "C" fn(*mut Registration)>,
    /// Next product turn in relative nanoseconds; u64::MAX clears it.
    pub peer_set_deadline: Option<unsafe extern "C" fn(*mut Qso, u64) -> Status>,
}
