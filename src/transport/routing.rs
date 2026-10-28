//! Account-owned endpoint routing. Unknown datagrams never allocate a task or media queue.

use super::mailbox::{self, Consumer, Producer, PushError};
use crate::{
    account::{Account, Admission},
    directory::{DirectoryEntry, DirectorySnapshot},
    wire::{Channel, ControlPacket, WirePacket, parse_datagram},
};
use std::{
    collections::BTreeMap,
    net::{Ipv4Addr, SocketAddr},
    time::{Duration, Instant},
};

/// Bounded, directory-verified setup record, not yet a published connection.
#[derive(Clone, Debug)]
pub struct PendingHandshake {
    token: u64,
    /// Verified full station identity, not a represented chat sender.
    pub identity: DirectoryEntry,
    /// Observed control endpoint; Proxy framing uses port zero.
    pub source: SocketAddr,
    /// Original monotonic SDES arrival, retained across host validation.
    pub arrived: Instant,
    /// Validated station description needed to establish the new QSO.
    pub control: ControlPacket,
}

impl PendingHandshake {
    /// Nonrepeating account-local token; stale completions cannot accept a replacement.
    pub fn token(&self) -> u64 {
        self.token
    }
}

/// A single routing decision; full queues shed this packet, never hang up a peer.
pub enum RouteResult {
    /// Packet transferred into exactly one generation's bounded mailbox.
    Routed,
    /// New verified candidate; the control owner must explicitly accept or reject it.
    Pending(PendingHandshake),
    /// Malformed, unverified, ambiguous, repeated-pending or setup-budget-limited input.
    Dropped,
    /// Exact mailbox admission result, retained for observability.
    Backpressure(PushError),
}

/// Connection ownership returned only after directory and account-policy validation.
pub struct Connection {
    /// Verified identity copied for this connection's lifetime.
    pub identity: DirectoryEntry,
    /// Nonzero generation shared with every queued packet and cleanup operation.
    pub generation: u64,
    /// Incoming establishment; outbound calls still wait for fresh remote SDES.
    pub initial: Option<PendingHandshake>,
    /// Sole receive owner, moved into the QSO task after host preparation.
    pub ingress: Consumer,
}

struct Route {
    identity: DirectoryEntry,
    generation: u64,
    producer: Producer,
    control_endpoint: Option<SocketAddr>,
    audio_endpoint: Option<SocketAddr>,
    control_source: Option<u32>,
    audio_source: Option<u32>,
}

impl Route {
    fn accepts(
        &self,
        channel: Channel,
        source: SocketAddr,
        packet: &WirePacket<'_>,
        only: bool,
    ) -> bool {
        match packet {
            WirePacket::Control(ControlPacket::Sdes {
                callsign,
                source: id,
                ..
            }) => {
                callsign.eq_ignore_ascii_case(&self.identity.callsign)
                    && self.control_endpoint.is_none_or(|known| known == source)
                    && matches_source(self.control_source, *id)
            }
            WirePacket::Control(
                ControlPacket::Report { source: id } | ControlPacket::Bye { source: id, .. },
            ) => self.control_endpoint == Some(source) && matches_source(self.control_source, *id),
            WirePacket::Voice(voice) => {
                channel == Channel::Audio
                    && self.control_endpoint.is_some()
                    && self.audio_endpoint.map_or(only, |known| known == source)
                    && matches_source(self.audio_source, voice.ssrc)
            }
            WirePacket::Chat { .. } | WirePacket::StationInfo(_) => {
                channel == Channel::Audio
                    && self.control_endpoint.is_some()
                    && self.audio_endpoint.map_or(only, |known| known == source)
            }
            WirePacket::Ignored => false,
        }
    }

    fn bind(&mut self, channel: Channel, source: SocketAddr, packet: &WirePacket<'_>) {
        let id = match packet {
            WirePacket::Control(ControlPacket::Sdes { source: id, .. }) => Some(*id),
            WirePacket::Voice(voice) => Some(voice.ssrc),
            _ => None,
        };
        let (endpoint, identifier) = match channel {
            Channel::Audio => (&mut self.audio_endpoint, &mut self.audio_source),
            Channel::Control => (&mut self.control_endpoint, &mut self.control_source),
        };
        *endpoint = Some(source);
        if let Some(id) = id.filter(|id| *id != 0) {
            *identifier = Some(id);
        }
    }
}

/// Mutated only by the account's serialized control/network owner, never by audio callbacks.
/// Different source addresses have independent queues; shared-IP ambiguity is dropped,
/// not resolved by trusting a represented sender or assuming a fixed remote UDP port.
pub struct Router {
    pub(crate) account: Account,
    maximum_pending: usize,
    setup_timeout: Duration,
    next: u64,
    pending: BTreeMap<u64, PendingHandshake>,
    routes: BTreeMap<Ipv4Addr, Vec<Route>>,
}

impl Router {
    /// Configure a separate positive pending-handshake budget and setup timeout.
    pub fn new(account: Account, maximum_pending: u32, setup_timeout: Duration) -> Option<Self> {
        if maximum_pending == 0 || setup_timeout.is_zero() {
            return None;
        }
        Some(Self {
            account,
            maximum_pending: maximum_pending as usize,
            setup_timeout,
            next: 0,
            pending: BTreeMap::new(),
            routes: BTreeMap::new(),
        })
    }

    /// Current retained setup records; there is no task or payload ring per unknown packet.
    pub fn pending_count(&self) -> usize {
        self.pending.len()
    }

    fn generation(&mut self) -> Result<u64, Admission> {
        self.next = self.next.checked_add(1).ok_or(Admission::Full)?;
        Ok(self.next)
    }

    fn expire(&mut self, now: Instant) {
        self.pending.retain(|_, pending| {
            now.saturating_duration_since(pending.arrived) < self.setup_timeout
        });
    }

    /// Route one original datagram. A fresh SDES may reserve only bounded identity metadata.
    pub fn receive(
        &mut self,
        directory: &DirectorySnapshot,
        channel: Channel,
        source: SocketAddr,
        arrived: Instant,
        bytes: &[u8],
    ) -> RouteResult {
        self.expire(arrived);
        let SocketAddr::V4(endpoint) = source else {
            return RouteResult::Dropped;
        };
        let Ok(packet) = parse_datagram(channel, bytes) else {
            return RouteResult::Dropped;
        };
        if let Some(routes) = self.routes.get_mut(endpoint.ip()) {
            let mut matches = routes
                .iter()
                .enumerate()
                .filter(|(_, route)| route.accepts(channel, source, &packet, routes.len() == 1));
            if let Some((index, _)) = matches.next() {
                if matches.next().is_some() {
                    return RouteResult::Dropped;
                }
                let route = &mut routes[index];
                route.bind(channel, source, &packet);
                return match route.producer.try_push(channel, source, arrived, bytes) {
                    Ok(()) => RouteResult::Routed,
                    Err(error) => RouteResult::Backpressure(error),
                };
            }
        }
        let WirePacket::Control(ControlPacket::Sdes {
            source: id,
            callsign,
            station_name,
            speex,
        }) = packet
        else {
            return RouteResult::Dropped;
        };
        let Ok(identity) = directory.lookup(&callsign, *endpoint.ip(), arrived) else {
            return RouteResult::Dropped;
        };
        if self.pending.len() >= self.maximum_pending
            || self
                .pending
                .values()
                .any(|pending| pending.identity.node_number == identity.node_number)
            || self
                .routes
                .values()
                .flatten()
                .any(|route| route.identity.node_number == identity.node_number)
        {
            return RouteResult::Dropped;
        }
        let Ok(token) = self.generation() else {
            return RouteResult::Dropped;
        };
        let pending = PendingHandshake {
            token,
            identity,
            source,
            arrived,
            control: ControlPacket::Sdes {
                source: id,
                callsign,
                station_name,
                speex,
            },
        };
        self.pending.insert(token, pending.clone());
        RouteResult::Pending(pending)
    }

    /// Accept exactly one pending generation after rechecking directory freshness and policy.
    pub fn accept(
        &mut self,
        directory: &DirectorySnapshot,
        token: u64,
        now: Instant,
    ) -> Result<Connection, Admission> {
        self.expire(now);
        let initial = self.pending.remove(&token).ok_or(Admission::Unverified)?;
        let peer = directory
            .lookup(&initial.identity.callsign, initial.identity.address, now)
            .map_err(|_| Admission::Unverified)?;
        if peer.node_number != initial.identity.node_number {
            return Err(Admission::Unverified);
        }
        self.publish(directory, peer, token, Some(initial), now)
    }

    /// Start an outbound, directory-verified connection; voice still requires fresh SDES.
    pub fn dial(
        &mut self,
        directory: &DirectorySnapshot,
        node: u32,
        now: Instant,
    ) -> Result<Connection, Admission> {
        let peer = directory
            .by_node(node, now)
            .map_err(|_| Admission::Unverified)?;
        let generation = self.generation()?;
        self.publish(directory, peer, generation, None, now)
    }

    fn publish(
        &mut self,
        directory: &DirectorySnapshot,
        peer: DirectoryEntry,
        generation: u64,
        initial: Option<PendingHandshake>,
        now: Instant,
    ) -> Result<Connection, Admission> {
        let identity =
            self.account
                .admit(directory, &peer.callsign, peer.address, generation, now)?;
        let (producer, ingress) = mailbox::channel(generation).unwrap();
        let mut route = Route {
            identity: identity.clone(),
            generation,
            producer,
            control_endpoint: None,
            audio_endpoint: None,
            control_source: None,
            audio_source: None,
        };
        if let Some(initial) = initial.as_ref() {
            route.bind(
                Channel::Control,
                initial.source,
                &WirePacket::Control(initial.control.clone()),
            );
        }
        self.routes.entry(identity.address).or_default().push(route);
        Ok(Connection {
            identity,
            generation,
            initial,
            ingress,
        })
    }

    /// Unregister before stopping/joining a QSO; stale cleanup cannot remove its replacement.
    pub fn remove(&mut self, node: u32, generation: u64) {
        self.routes.retain(|_, routes| {
            routes.retain(|route| {
                (route.identity.node_number, route.generation) != (node, generation)
            });
            !routes.is_empty()
        });
        self.account.remove(node, generation);
    }
}

fn matches_source(known: Option<u32>, source: u32) -> bool {
    source == 0 || known.is_none_or(|known| known == source)
}
