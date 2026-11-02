//! Serialized account admission and EchoLink-only first-talker ownership.

mod aprs;
pub mod coordination;
pub mod directory;
pub mod network;
pub mod settings;

use crate::directory::{DirectoryEntry, DirectorySnapshot};
use std::{
    collections::BTreeMap,
    net::Ipv4Addr,
    time::{Duration, Instant},
};

const UNKEY_AFTER: Duration = Duration::from_millis(320);
const RELEASE_AFTER: Duration = Duration::from_millis(800);

/// Safe admission rejection, independent of controller policy after attachment.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Admission {
    /// Missing/stale directory, identity/address mismatch or invalid generation.
    Unverified,
    /// Callsign policy rejected the verified station; deny always wins.
    Denied,
    /// The same real station is already admitted in this account.
    Duplicate,
    /// Configured station budget is exhausted.
    Full,
}

/// Valid media is selected before any controller receive/DTMF activity is published.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum Selection {
    /// Media belongs to the current selected talker and may enter its media timeline.
    Selected,
    /// Another station owns the account; discard this contender's audio/DTMF.
    Suppressed,
    /// No current admission for this node/generation.
    Ignored,
}

struct Talker {
    identity: (u32, u64),
    last_packet: Instant,
    keyed: bool,
}

/// Owned only by the account's serialized control task, never by an audio callback.
pub struct Account {
    maximum: u32,
    allow: Vec<String>,
    deny: Vec<String>,
    peers: BTreeMap<u32, u64>,
    talker: Option<Talker>,
    suppressed: u64,
}

impl Account {
    /// Apply validated admission policy without changing established generations
    /// or the selected talker's timers. New calls use the new deny-first policy.
    pub(crate) fn update_policy(&mut self, settings: &settings::PreparedSettings) {
        self.maximum = settings.as_abi().policy.max_stations;
        self.allow = settings.allow().map(str::to_ascii_uppercase).collect();
        self.deny = settings.deny().map(str::to_ascii_uppercase).collect();
    }

    /// Configure a positive station budget and normalized callsign policy.
    pub fn new(maximum: u32, allow: Vec<String>, deny: Vec<String>) -> Option<Self> {
        if maximum == 0 {
            return None;
        }
        Some(Self {
            maximum,
            allow: allow
                .into_iter()
                .map(|value| value.to_ascii_uppercase())
                .collect(),
            deny: deny
                .into_iter()
                .map(|value| value.to_ascii_uppercase())
                .collect(),
            peers: BTreeMap::new(),
            talker: None,
            suppressed: 0,
        })
    }

    /// Recheck fresh identity/address and enforce limits before publishing a QSO.
    pub fn admit(
        &mut self,
        directory: &DirectorySnapshot,
        callsign: &str,
        address: Ipv4Addr,
        generation: u64,
        now: Instant,
    ) -> Result<DirectoryEntry, Admission> {
        if generation == 0 {
            return Err(Admission::Unverified);
        }
        let peer = directory
            .lookup(callsign, address, now)
            .map_err(|_| Admission::Unverified)?;
        if self
            .deny
            .iter()
            .any(|pattern| matches(pattern, &peer.callsign))
            || (!self.allow.is_empty()
                && !self
                    .allow
                    .iter()
                    .any(|pattern| matches(pattern, &peer.callsign)))
        {
            return Err(Admission::Denied);
        }
        if self.peers.contains_key(&peer.node_number) {
            return Err(Admission::Duplicate);
        }
        if self.peers.len() >= self.maximum as usize {
            return Err(Admission::Full);
        }
        self.peers.insert(peer.node_number, generation);
        Ok(peer)
    }

    /// Remove only the exact generation; an old cleanup cannot detach a replacement.
    pub fn remove(&mut self, node: u32, generation: u64) {
        if self.peers.get(&node) == Some(&generation) {
            self.peers.remove(&node);
        }
    }

    /// Number of established/reserved stations, bounded by configuration.
    pub fn admitted(&self) -> usize {
        self.peers.len()
    }

    /// Valid media that arrived from non-selected, admitted stations.
    pub fn suppressed_packets(&self) -> u64 {
        self.suppressed
    }

    /// Selected identity may remain reserved after RF unkey while ownership expires.
    pub fn selected(&self) -> Option<(u32, u64)> {
        self.talker.as_ref().map(|talker| talker.identity)
    }

    /// Selected/drained activity, not raw contender or heartbeat activity.
    pub fn receiving(&self) -> bool {
        self.talker.as_ref().is_some_and(|talker| talker.keyed)
    }

    /// Admit media only after the corresponding QSO has validated its packet.
    pub fn voice(&mut self, node: u32, generation: u64, now: Instant) -> Selection {
        if self.peers.get(&node) != Some(&generation) {
            return Selection::Ignored;
        }
        match self.talker.as_mut() {
            Some(talker) if talker.identity != (node, generation) => {
                self.suppressed = self.suppressed.saturating_add(1);
                Selection::Suppressed
            }
            Some(talker) => {
                // Concurrent routing may deliver an older arrival after a newer one.
                talker.last_packet = talker.last_packet.max(now);
                talker.keyed = true;
                Selection::Selected
            }
            None => {
                self.talker = Some(Talker {
                    identity: (node, generation),
                    last_packet: now,
                    keyed: true,
                });
                Selection::Selected
            }
        }
    }

    /// Publish one final unkey only after both the 320 ms guard and accepted drain.
    /// Ownership releases after 800 ms from the last selected packet and drain,
    /// never 800 ms after unkey and never because a contender remains active.
    pub fn poll(&mut self, now: Instant, drained: bool) -> Option<(u32, u64)> {
        let talker = self.talker.as_mut()?;
        let quiet = now.saturating_duration_since(talker.last_packet);
        let unkey = if drained && quiet >= UNKEY_AFTER && talker.keyed {
            talker.keyed = false;
            Some(talker.identity)
        } else {
            None
        };
        if drained && quiet >= RELEASE_AFTER {
            self.talker = None;
        }
        unkey
    }
}

// Callsigns and patterns are short ASCII fields; '*' and '?' have shell semantics.
fn matches(pattern: &str, text: &str) -> bool {
    let (pattern, text) = (pattern.as_bytes(), text.as_bytes());
    let (mut p, mut t, mut retry) = (0, 0, 0);
    let mut star = None;
    while t < text.len() {
        if pattern.get(p) == Some(&b'*') {
            star = Some(p);
            p += 1;
            retry = t;
        } else if pattern
            .get(p)
            .is_some_and(|byte| *byte == b'?' || *byte == text[t])
        {
            p += 1;
            t += 1;
        } else if let Some(position) = star {
            retry += 1;
            t = retry;
            p = position + 1;
        } else {
            return false;
        }
    }
    pattern[p..].iter().all(|byte| *byte == b'*')
}
