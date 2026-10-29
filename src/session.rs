//! Serialized QSO state; packet errors and silence are not terminal events.

use crate::directory::DirectoryEntry;
use std::{
    net::Ipv4Addr,
    time::{Duration, Instant},
};

/// Independent monotonic control deadlines, resolved before creating the QSO.
#[derive(Clone, Copy)]
pub struct Timers {
    /// Interval between sent SDES heartbeats.
    pub heartbeat: Duration,
    /// Maximum time since valid, session-bound voice/control.
    pub liveness: Duration,
    /// Initial SDES establishment deadline.
    pub connect: Duration,
}

/// QSO state retains closing until already accepted media has drained.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QsoState {
    /// Directory verification succeeded; fresh SDES is still required.
    Connecting,
    /// Fresh session-bound SDES established this generation.
    Connected,
    /// No new media admitted; previously accepted media is draining.
    Closing,
    /// Draining completed; the owner may detach and destroy the handle.
    Closed,
}

/// First terminal cause, retained even if cleanup discovers another failure.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Cause {
    /// Authorized local/controller operation; no provider reconnect.
    LocalRequest,
    /// Valid session-bound remote BYE.
    RemoteBye,
    /// Fresh SDES did not arrive before setup expired.
    ConnectExpired,
    /// Valid voice/control stopped for the configured liveness interval.
    LivenessExpired,
    /// Selected direct/Proxy transport failed.
    TransportLost,
    /// Unrecoverable owner/media failure, not an individual packet error.
    OwnerFailed,
}

/// Parsed packet classification; only the serialized provider can bind its endpoint.
pub enum Packet {
    /// An independently validated SDES station identity.
    Sdes {
        /// SDES station label, matched to the verified directory copy.
        callsign: String,
        /// Control SSRC, zero when the legacy client omits it.
        source: u32,
    },
    /// Valid source-rate media; sequence/jitter handling follows this gate.
    Voice {
        /// RTP SSRC, retained independently from control identifiers.
        source: u32,
    },
    /// Valid RTCP receiver report.
    Report {
        /// Report sender's control SSRC.
        source: u32,
    },
    /// Valid RTCP BYE.
    Bye {
        /// Departing control SSRC.
        source: u32,
    },
    /// A malformed packet is counted by the wire owner, never a hangup.
    Malformed,
    /// Human chat does not prove voice/control liveness.
    Chat,
    /// Well-formed but unsupported packet type.
    Unsupported,
}

/// One serialized owner event; generation belongs to its internal envelope.
pub enum QsoEvent {
    /// Received packet with the actual direct/Proxy remote address.
    Packet {
        /// Observed remote address, never a text-message label.
        address: Ipv4Addr,
        /// Parser result delivered by the serialized transport owner.
        packet: Packet,
    },
    /// Service the next monotonic deadline.
    Poll,
    /// Authorized local disconnect.
    Stop,
    /// Direct socket or selected Proxy failed.
    TransportLost,
    /// Fatal codec/owner error, not malformed remote input.
    OwnerFailed,
    /// All previously accepted media reached its final playout endpoint.
    Drained,
}

/// At most one bounded action is produced per event; no callback allocation is needed.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum QsoAction {
    /// Send current SDES; ordinary silence still requires these heartbeats.
    Heartbeat,
    /// Publish this verified connection exactly once.
    Connected,
    /// Offer valid media to first-talker selection, not directly to RF.
    Voice,
    /// Stop new admission, send BYE if usable, and log the first cause once.
    Closing {
        /// Preserved first terminal cause.
        cause: Cause,
        /// False for a remote BYE or unusable transport.
        send_bye: bool,
    },
}

/// Metadata copied by the sole QSO owner. Ages and remaining deadlines are
/// monotonic milliseconds; u64::MAX means an inactive deadline.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    /// Exact lifecycle state, including finite tail drainage.
    pub state: QsoState,
    /// First close cause; cleanup cannot replace it.
    pub first_cause: Option<Cause>,
    /// Valid packet burst guard, not selected RF activity.
    pub raw_receiving: bool,
    /// Time since the last valid session-bound voice/control packet.
    pub last_valid_age_ms: u64,
    /// Time until the next sent heartbeat, or inactive.
    pub heartbeat_due_ms: u64,
    /// Time until setup/liveness expires, or inactive.
    pub liveness_due_ms: u64,
}

/// Provider-owned session identity and liveness, never a host-supplied verification flag.
pub struct Qso {
    peer: DirectoryEntry,
    generation: u64,
    timers: Timers,
    state: QsoState,
    created: Instant,
    last_valid: Instant,
    last_heartbeat: Instant,
    last_voice: Option<Instant>,
    control_source: Option<u32>,
    voice_source: Option<u32>,
    first_cause: Option<Cause>,
}

impl Qso {
    /// Copy diagnostics without renewing a deadline or advancing state.
    pub fn observe(&self, now: Instant) -> Observation {
        let active = matches!(self.state, QsoState::Connecting | QsoState::Connected);
        let remaining = |anchor: Instant, period: Duration| {
            if active {
                ms(period.saturating_sub(now.saturating_duration_since(anchor)))
            } else {
                u64::MAX
            }
        };
        let (anchor, period) = if self.state == QsoState::Connecting {
            (self.created, self.timers.connect)
        } else {
            (self.last_valid, self.timers.liveness)
        };
        Observation {
            state: self.state,
            first_cause: self.first_cause,
            raw_receiving: self.raw_receiving(now),
            last_valid_age_ms: ms(now.saturating_duration_since(self.last_valid)),
            heartbeat_due_ms: remaining(self.last_heartbeat, self.timers.heartbeat),
            liveness_due_ms: remaining(anchor, period),
        }
    }
    /// Retain a fresh directory copy; establishing SDES remains mandatory.
    pub fn new(
        peer: DirectoryEntry,
        generation: u64,
        timers: Timers,
        now: Instant,
    ) -> Option<Self> {
        if generation == 0
            || [timers.heartbeat, timers.liveness, timers.connect].contains(&Duration::ZERO)
        {
            return None;
        }
        Some(Self {
            peer,
            generation,
            timers,
            state: QsoState::Connecting,
            created: now,
            last_valid: now,
            last_heartbeat: now,
            last_voice: None,
            control_source: None,
            voice_source: None,
            first_cause: None,
        })
    }

    /// Current state, copied without granting admission authority.
    pub fn state(&self) -> QsoState {
        self.state
    }

    /// Immutable routing generation; queued packets from retired routes cannot enter this QSO.
    pub fn generation(&self) -> u64 {
        self.generation
    }

    /// First close cause; later cleanup cannot overwrite it.
    pub fn first_cause(&self) -> Option<Cause> {
        self.first_cause
    }

    /// Replace intervals without renewing their elapsed timestamp anchors or
    /// reviving a closing connection. Invalid intervals leave the policy intact.
    pub fn set_timers(&mut self, timers: Timers) -> bool {
        if [timers.heartbeat, timers.liveness, timers.connect].contains(&Duration::ZERO) {
            return false;
        }
        self.timers = timers;
        true
    }

    /// Next control heartbeat/setup/liveness deadline for the existing serial owner.
    /// Closing generations wait only for media drain, never a heartbeat polling loop.
    pub fn deadline(&self) -> Option<Instant> {
        let expiry = match self.state {
            QsoState::Connecting => self.created.checked_add(self.timers.connect),
            QsoState::Connected => self.last_valid.checked_add(self.timers.liveness),
            QsoState::Closing | QsoState::Closed => return None,
        };
        [
            expiry,
            self.last_heartbeat.checked_add(self.timers.heartbeat),
        ]
        .into_iter()
        .flatten()
        .min()
    }

    /// Raw packet activity only; the account separately reports selected/drained activity.
    pub fn raw_receiving(&self, now: Instant) -> bool {
        self.state == QsoState::Connected
            && self.last_voice.is_some_and(|last| {
                now.saturating_duration_since(last) < Duration::from_millis(320)
            })
    }

    /// Schedule the raw receive-release guard independently from control heartbeats.
    /// The account still waits for accepted media and the product ring to drain.
    pub fn receive_deadline(&self, now: Instant) -> Option<Instant> {
        self.last_voice
            .filter(|_| self.raw_receiving(now))?
            .checked_add(Duration::from_millis(320))
    }

    /// Consume one generation-fenced event outside real-time callbacks.
    pub fn handle(&mut self, generation: u64, event: QsoEvent, now: Instant) -> Option<QsoAction> {
        if generation != self.generation || self.state == QsoState::Closed {
            return None;
        }
        if self.state == QsoState::Closing {
            if matches!(event, QsoEvent::Drained) {
                self.state = QsoState::Closed;
            }
            return None;
        }
        match event {
            QsoEvent::Stop => self.close(Cause::LocalRequest, true),
            QsoEvent::TransportLost => self.close(Cause::TransportLost, false),
            QsoEvent::OwnerFailed => self.close(Cause::OwnerFailed, true),
            QsoEvent::Drained => None,
            QsoEvent::Poll => {
                let expired = if self.state == QsoState::Connecting {
                    (now.saturating_duration_since(self.created) >= self.timers.connect)
                        .then_some(Cause::ConnectExpired)
                } else {
                    (now.saturating_duration_since(self.last_valid) >= self.timers.liveness)
                        .then_some(Cause::LivenessExpired)
                };
                if let Some(cause) = expired {
                    return self.close(cause, true);
                }
                if now.saturating_duration_since(self.last_heartbeat) >= self.timers.heartbeat {
                    self.last_heartbeat = now;
                    Some(QsoAction::Heartbeat)
                } else {
                    None
                }
            }
            QsoEvent::Packet { address, packet } => {
                if address != self.peer.address {
                    return None;
                }
                match packet {
                    Packet::Sdes { callsign, source } => {
                        if !callsign.eq_ignore_ascii_case(&self.peer.callsign)
                            || !matches_source(self.control_source, source)
                        {
                            return None;
                        }
                        if source != 0 {
                            self.control_source = Some(source);
                        }
                        self.last_valid = self.last_valid.max(now);
                        if self.state == QsoState::Connecting {
                            self.state = QsoState::Connected;
                            Some(QsoAction::Connected)
                        } else {
                            None
                        }
                    }
                    Packet::Voice { source }
                        if self.state == QsoState::Connected
                            && matches_source(self.voice_source, source) =>
                    {
                        if source != 0 {
                            self.voice_source = Some(source);
                        }
                        self.last_valid = self.last_valid.max(now);
                        self.last_voice = Some(self.last_voice.unwrap_or(now).max(now));
                        Some(QsoAction::Voice)
                    }
                    Packet::Report { source }
                        if self.state == QsoState::Connected
                            && matches_source(self.control_source, source) =>
                    {
                        self.last_valid = self.last_valid.max(now);
                        None
                    }
                    Packet::Bye { source }
                        if self.state == QsoState::Connected
                            && matches_source(self.control_source, source) =>
                    {
                        self.close(Cause::RemoteBye, false)
                    }
                    _ => None,
                }
            }
        }
    }

    fn close(&mut self, cause: Cause, send_bye: bool) -> Option<QsoAction> {
        self.first_cause = Some(cause);
        self.state = QsoState::Closing;
        Some(QsoAction::Closing { cause, send_bye })
    }
}

pub(crate) fn ms(duration: Duration) -> u64 {
    duration.as_millis().min(u128::from(u64::MAX)) as u64
}

fn matches_source(known: Option<u32>, source: u32) -> bool {
    source == 0 || known.is_none_or(|known| known == source)
}
