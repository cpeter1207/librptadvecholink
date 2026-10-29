//! One QSO's serial receive composition; no executor, PCM ring or codec is duplicated.
use crate::{
    account::{
        Selection,
        coordination::{OfferError, TalkerPort},
    },
    media::{CodecChoice, MediaError},
    playout::{PcmSink, Playout, Progress, Reception},
    session::{Packet, Qso, QsoAction, QsoEvent, QsoState},
    transport::{mailbox::Datagram, routing::PendingHandshake},
    wire::{Codec, ControlPacket, WirePacket, parse_datagram},
};
use std::{net::IpAddr, time::Instant};

/// One admitted datagram's result, copied before invoking any host step callback.
pub enum Incoming {
    /// Invalid, stale, suppressed or unsupported input; never a request to hang up.
    Ignored,
    /// Same immutable packet retained until talker selection and jitter admission complete.
    Retained,
    /// Connection/control action from the existing QSO state machine.
    Action(QsoAction),
    /// Human text only; station information and protocol controls never reach this variant.
    Text {
        /// Represented chat sender, not the authenticated connection identity.
        sender: String,
        /// Literal bounded text; synthesis/forwarding policy belongs to the product.
        body: String,
    },
}

/// Copied results of one bounded receive turn, not a callback into a borrowed owner.
pub struct Turn {
    /// Actual jitter/output progress; Blocked must wait for downstream capacity.
    pub progress: Progress,
    /// Selected activity edge, never raw contender/heartbeat activity.
    pub activity: Option<bool>,
    /// One timer action, including a preserved first terminal cause.
    pub action: Option<QsoAction>,
}

/// Exactly one serial worker owns this object. Host notification occurs only after
/// ending its mutable borrow. The PCM sink only publishes to the existing product
/// ring; it must not reenter this owner. Outgoing encoding remains independently owned.
pub struct Receiver {
    qso: Qso,
    playout: Playout,
    codecs: CodecChoice,
    talker: TalkerPort,
    pending: Option<Datagram>,
    granted: bool,
    selected: bool,
    ending: bool,
    highest_sequence: Option<u16>,
    playout_deadline: Option<Instant>,
}

impl Receiver {
    /// Compose already prepared dynamic capabilities and the account-bound talker port.
    pub fn new(qso: Qso, playout: Playout, codecs: CodecChoice, talker: TalkerPort) -> Self {
        Self {
            qso,
            playout,
            codecs,
            talker,
            pending: None,
            granted: false,
            selected: false,
            ending: false,
            highest_sequence: None,
            playout_deadline: None,
        }
    }

    /// No new packet may replace the retained one or enter a closing generation.
    pub fn can_receive(&self) -> bool {
        self.pending.is_none()
            && matches!(self.qso.state(), QsoState::Connecting | QsoState::Connected)
    }

    /// Current QSO state; closed is reached only after accepted downstream media drains.
    pub fn state(&self) -> QsoState {
        self.qso.state()
    }

    /// All accepted source PCM has been published, but the product still owns
    /// its finite tail. Publish one end marker per burst before waiting for
    /// product-ring drainage; raw guard expiry is not permission to discard PCM.
    pub fn product_finish_pending(&self) -> bool {
        self.selected && self.ending && self.pending.is_none() && self.playout.drained()
    }

    /// Negotiated egress format; receive still selects each packet's own codec.
    pub fn codec(&self) -> Codec {
        self.codecs.codec()
    }

    /// Consume the router-verified incoming establishment on its original arrival
    /// time. The owner calls this once when moving the admitted connection into
    /// its receiver; it must not wait for a second remote heartbeat. QSO identity
    /// checks still precede codec negotiation and connection publication.
    pub fn initial(&mut self, initial: &PendingHandshake) -> Option<QsoAction> {
        let IpAddr::V4(address) = initial.source.ip() else {
            return None;
        };
        let ControlPacket::Sdes {
            source, callsign, ..
        } = &initial.control
        else {
            return None;
        };
        let action = self.control(
            QsoEvent::Packet {
                address,
                packet: Packet::Sdes {
                    source: *source,
                    callsign: callsign.clone(),
                },
            },
            initial.arrived,
        );
        if action == Some(QsoAction::Connected) {
            self.codecs.observe_sdes(&initial.control);
        }
        action
    }

    /// Apply local/transport events without inventing packet-error hangups.
    pub fn control(&mut self, event: QsoEvent, now: Instant) -> Option<QsoAction> {
        let action = self.qso.handle(self.qso.generation(), event, now);
        if matches!(action, Some(QsoAction::Closing { .. })) {
            self.playout.end();
            self.ending = true;
        }
        action
    }

    /// Process only account-routed, endpoint-verified packets from this generation.
    /// The packet's actual arrival drives liveness and first-talker selection.
    /// A retained input is a caller contract error, never silently overwritten.
    pub fn packet(&mut self, packet: Datagram) -> Result<Incoming, MediaError> {
        if packet.generation() != self.qso.generation() {
            return Ok(Incoming::Ignored);
        }
        if !self.can_receive() {
            return if self.pending.is_some() {
                Err(MediaError::Contract)
            } else {
                Ok(Incoming::Ignored)
            };
        }
        let IpAddr::V4(address) = packet.source().ip() else {
            return Ok(Incoming::Ignored);
        };
        let Ok(wire) = parse_datagram(packet.class(), packet.payload()) else {
            return Ok(Incoming::Ignored);
        };
        if let WirePacket::Voice(voice) = &wire {
            // Resetting jitter between bursts must not give retired RTP packets
            // a new playout epoch. Reordering within an active burst remains intact.
            if self.ending
                && self.playout.drained()
                && self
                    .highest_sequence
                    .is_some_and(|last| voice.sequence.wrapping_sub(last) as i16 <= 0)
            {
                return Ok(Incoming::Ignored);
            }
        }
        let input = match &wire {
            WirePacket::Control(ControlPacket::Sdes {
                source, callsign, ..
            }) => Packet::Sdes {
                source: *source,
                callsign: callsign.clone(),
            },
            WirePacket::Control(ControlPacket::Report { source }) => {
                Packet::Report { source: *source }
            }
            WirePacket::Control(ControlPacket::Bye { source, .. }) => {
                Packet::Bye { source: *source }
            }
            WirePacket::Voice(voice) if self.playout.validate(voice)? => {
                Packet::Voice { source: voice.ssrc }
            }
            WirePacket::Chat { .. } => Packet::Chat,
            _ => return Ok(Incoming::Ignored),
        };
        let action = self.qso.handle(
            packet.generation(),
            QsoEvent::Packet {
                address,
                packet: input,
            },
            packet.arrived(),
        );
        if self.qso.state() == QsoState::Connected {
            match &wire {
                WirePacket::Control(control @ ControlPacket::Sdes { .. }) => {
                    self.codecs.observe_sdes(control)
                }
                WirePacket::Chat { sender, body } => {
                    return Ok(Incoming::Text {
                        sender: (*sender).into(),
                        body: (*body).into(),
                    });
                }
                _ => {}
            }
        }
        if action == Some(QsoAction::Voice) {
            match self.talker.offer(packet.arrived()) {
                Ok(()) => {
                    self.pending = Some(packet);
                    return Ok(Incoming::Retained);
                }
                Err(OfferError::Full) => return Ok(Incoming::Ignored),
                Err(_) => return Err(MediaError::Contract),
            }
        }
        if matches!(action, Some(QsoAction::Closing { .. })) {
            self.playout.end();
            self.ending = true;
        }
        Ok(action.map_or(Incoming::Ignored, Incoming::Action))
    }

    /// Advance one retained grant and one jitter prefix. Product drain includes
    /// its pending input and inbound ring, not merely successful PCM publication.
    pub fn poll(
        &mut self,
        now: Instant,
        product_drained: bool,
        sink: &mut dyn PcmSink,
    ) -> Result<Turn, MediaError> {
        let action = self.control(QsoEvent::Poll, now);
        let mut activity = None;
        if self.pending.is_some() && !self.granted {
            match self.talker.selection().map_err(|_| MediaError::Contract)? {
                None => {
                    return Ok(Turn {
                        progress: Progress::Idle(None),
                        activity,
                        action,
                    });
                }
                Some(Selection::Selected) => self.granted = true,
                Some(_) => {
                    self.pending = None;
                }
            }
        }
        if self.granted {
            if self.ending && self.playout.drained() {
                self.playout.reset()?;
                self.ending = false;
            }
            let packet = self.pending.as_ref().ok_or(MediaError::Contract)?;
            let WirePacket::Voice(voice) =
                parse_datagram(packet.class(), packet.payload()).map_err(MediaError::Wire)?
            else {
                return Err(MediaError::Contract);
            };
            match self.playout.receive(voice, packet.arrived())? {
                Reception::Busy => {}
                Reception::Invalid => return Err(MediaError::Contract),
                Reception::Admitted(_) => {
                    if self
                        .highest_sequence
                        .is_none_or(|last| voice.sequence.wrapping_sub(last) as i16 > 0)
                    {
                        self.highest_sequence = Some(voice.sequence);
                    }
                    self.pending = None;
                    self.granted = false;
                    if !self.selected {
                        self.selected = true;
                        activity = Some(true);
                    }
                }
            }
        }
        if !self.qso.raw_receiving(now) && self.selected && !self.ending {
            self.playout.end();
            self.ending = true;
        }
        let progress = self.playout.poll(now, sink)?;
        self.playout_deadline = if let Progress::Idle(deadline) = progress {
            deadline
        } else {
            None
        };
        let drained = self.pending.is_none() && self.playout.drained() && product_drained;
        if self.selected {
            match self.talker.report_drained(drained) {
                Ok(()) | Err(OfferError::Full) => {}
                Err(_) => return Err(MediaError::Contract),
            }
            if self.talker.take_unkey() {
                self.selected = false;
                activity = Some(false);
            }
        }
        if drained && self.qso.state() == QsoState::Closing {
            self.control(QsoEvent::Drained, now);
        }
        Ok(Turn {
            progress,
            activity,
            action,
        })
    }

    /// Real protocol, raw-burst and jitter deadlines; no idle polling interval.
    pub fn deadline(&self, now: Instant) -> Option<Instant> {
        [
            self.qso.deadline(),
            self.qso.receive_deadline(now),
            self.playout_deadline,
        ]
        .into_iter()
        .flatten()
        .min()
    }
}
