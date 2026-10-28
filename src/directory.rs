//! Bounded directory transactions with atomic publication and freshness-gated admission.
#[cfg(test)]
mod tests;

use flate2::{Decompress, FlushDecompress, Status};
use std::{
    collections::{BTreeMap, BTreeSet},
    net::Ipv4Addr,
    sync::Arc,
    time::{Duration, Instant},
};

pub(crate) const MAX_COMPRESSED: usize = 2 * 1024 * 1024;
pub(crate) const MAX_EXPANDED: usize = 16 * 1024 * 1024;
const MAX_RECORDS: usize = 100_000;
pub(crate) const MAX_LINE: usize = 1024;

/// An authenticated directory identity, copied into each established QSO.
#[derive(Clone, Debug, PartialEq, Eq)]
pub struct DirectoryEntry {
    /// Positive full EchoLink node number, never an ASL dial alias.
    pub node_number: u32,
    /// Uppercase validated station identity.
    pub callsign: String,
    /// Display metadata; not an authentication or clock source.
    pub description: String,
    /// Directory-observed public IPv4 address.
    pub address: Ipv4Addr,
}

/// Content-free refresh failures; a failure never erases the current snapshot.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectoryError {
    /// Truncated, ambiguous or structurally invalid response.
    Malformed,
    /// A bounded input, output, record or line budget was exceeded.
    Limit,
    /// A delta cannot be applied before the initial full snapshot.
    NoSnapshot,
    /// Server returned an error record instead of a usable directory.
    Rejected,
}

/// Fresh directory authentication must match both identity and address.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum AdmissionError {
    /// No usable snapshot, or its monotonic freshness deadline elapsed.
    Stale,
    /// No matching callsign or numeric identity.
    Unknown,
    /// Callsign exists but its observed source address does not match.
    Address,
}

/// One account's last good directory, replaced only after a complete refresh.
#[derive(Clone)]
pub struct DirectorySnapshot {
    freshness: Duration,
    published: Option<Instant>,
    snapshot_id: String,
    entries: Arc<BTreeMap<String, DirectoryEntry>>,
    failures: u64,
}

impl DirectorySnapshot {
    /// Start without trusted records; freshness is checked only for new admission.
    pub fn new(freshness: Duration) -> Self {
        Self {
            freshness,
            published: None,
            snapshot_id: "0".into(),
            entries: Arc::new(BTreeMap::new()),
            failures: 0,
        }
    }

    /// The last committed server token; failed updates cannot advance it.
    pub fn snapshot_id(&self) -> &str {
        &self.snapshot_id
    }

    /// Total rejected refreshes, including malformed and oversized responses.
    pub fn failures(&self) -> u64 {
        self.failures
    }

    /// Validate a bounded plain/zlib response and atomically publish its candidate.
    pub fn update(&mut self, input: &[u8], now: Instant) -> Result<(), DirectoryError> {
        let candidate = self.candidate(input);
        match candidate {
            Ok((entries, snapshot_id)) => {
                self.entries = Arc::new(entries);
                self.snapshot_id = snapshot_id;
                self.published = Some(now);
                Ok(())
            }
            Err(error) => {
                self.failures = self.failures.saturating_add(1);
                Err(error)
            }
        }
    }

    /// Authenticate an SDES identity without trusting an IP-only match.
    pub fn lookup(
        &self,
        callsign: &str,
        address: Ipv4Addr,
        now: Instant,
    ) -> Result<DirectoryEntry, AdmissionError> {
        self.require_fresh(now)?;
        let entry = self
            .entries
            .get(&callsign.to_ascii_uppercase())
            .ok_or(AdmissionError::Unknown)?;
        if entry.address != address {
            return Err(AdmissionError::Address);
        }
        Ok(entry.clone())
    }

    /// Resolve an outbound full numeric identity through the same fresh snapshot.
    pub fn by_node(
        &self,
        node_number: u32,
        now: Instant,
    ) -> Result<DirectoryEntry, AdmissionError> {
        self.require_fresh(now)?;
        self.entries
            .values()
            .find(|entry| entry.node_number == node_number)
            .cloned()
            .ok_or(AdmissionError::Unknown)
    }

    pub(crate) fn require_fresh(&self, now: Instant) -> Result<(), AdmissionError> {
        self.published
            .and_then(|published| now.checked_duration_since(published))
            .filter(|age| *age < self.freshness)
            .map(|_| ())
            .ok_or(AdmissionError::Stale)
    }

    fn candidate(
        &self,
        input: &[u8],
    ) -> Result<(BTreeMap<String, DirectoryEntry>, String), DirectoryError> {
        let expanded;
        let bytes = if input.starts_with(b"@@@") || input.starts_with(b"DDD") {
            if input.len() > MAX_EXPANDED {
                return Err(DirectoryError::Limit);
            }
            input
        } else {
            if input.len() > MAX_COMPRESSED {
                return Err(DirectoryError::Limit);
            }
            // ASL discards the first four framing bytes; their meaning is not assumed.
            expanded = inflate(input.get(4..).ok_or(DirectoryError::Malformed)?)?;
            &expanded
        };
        if bytes.contains(&0) {
            return Err(DirectoryError::Malformed);
        }
        if bytes
            .split(|byte| *byte == b'\n')
            .any(|line| line.len() > MAX_LINE)
        {
            return Err(DirectoryError::Limit);
        }
        // Station descriptions may contain legacy bytes. Identities/IPs still validate ASCII.
        let text = String::from_utf8_lossy(bytes);
        let mut lines = text.lines();
        let partial = match lines.next() {
            Some("@@@") => false,
            Some("DDD") => true,
            _ => return Err(DirectoryError::Malformed),
        };
        if partial && self.published.is_none() {
            return Err(DirectoryError::NoSnapshot);
        }
        let header = lines.next().ok_or(DirectoryError::Malformed)?;
        let (count, token) = header.split_once(':').unwrap_or((header, ""));
        let expected = decimal(count)? as usize;
        if expected > MAX_RECORDS {
            return Err(DirectoryError::Limit);
        }
        // ASL's SNAPSHOT_SZ field holds 49 bytes plus its string terminator.
        if token.len() > 49 || token.chars().any(|ch| !ch.is_ascii_graphic()) {
            return Err(DirectoryError::Malformed);
        }
        let mut entries = if partial {
            self.entries.as_ref().clone()
        } else {
            BTreeMap::new()
        };
        let mut additions = BTreeSet::new();
        let mut records = 0usize;
        let mut counted = 0usize;
        loop {
            let callsign = lines.next().ok_or(DirectoryError::Malformed)?;
            if callsign == "+++" {
                break;
            }
            records += 1;
            if records > MAX_RECORDS {
                return Err(DirectoryError::Limit);
            }
            let description = lines.next().ok_or(DirectoryError::Malformed)?;
            let node = lines.next().ok_or(DirectoryError::Malformed)?;
            let address = lines.next().ok_or(DirectoryError::Malformed)?;
            if callsign == "." {
                continue;
            }
            counted += 1;
            if callsign == " " {
                if description.starts_with("INCORRECT PASSWORD") {
                    return Err(DirectoryError::Rejected);
                }
                continue;
            }
            let callsign = callsign.to_ascii_uppercase();
            if callsign.is_empty()
                || callsign.len() > 15
                || !callsign
                    .bytes()
                    .all(|byte| byte.is_ascii_alphanumeric() || b"-/*".contains(&byte))
                || !additions.insert(callsign.clone())
            {
                return Err(DirectoryError::Malformed);
            }
            let node_number = decimal(node)?;
            if node_number == 0 {
                return Err(DirectoryError::Malformed);
            }
            let address = address.parse().map_err(|_| DirectoryError::Malformed)?;
            entries.insert(
                callsign.clone(),
                DirectoryEntry {
                    node_number,
                    callsign,
                    description: description.into(),
                    address,
                },
            );
        }
        if !partial && counted != expected {
            return Err(DirectoryError::Malformed);
        }
        if partial {
            let mut removals = 0;
            loop {
                let callsign = lines.next().ok_or(DirectoryError::Malformed)?;
                if callsign == "+++" {
                    break;
                }
                removals += 1;
                if removals > MAX_RECORDS {
                    return Err(DirectoryError::Limit);
                }
                entries.remove(&callsign.to_ascii_uppercase());
            }
        }
        if lines.any(|line| !line.is_empty()) {
            return Err(DirectoryError::Malformed);
        }
        if entries.len() > MAX_RECORDS {
            return Err(DirectoryError::Limit);
        }
        let mut nodes = BTreeSet::new();
        if !entries
            .values()
            .all(|entry| nodes.insert(entry.node_number))
        {
            return Err(DirectoryError::Malformed);
        }
        Ok((
            entries,
            if token.is_empty() {
                "0".into()
            } else {
                token.into()
            },
        ))
    }
}

/// Immutable, atomically published directory view for final host admission.
/// It grants no account/QSO ownership; callers must also validate the exact live
/// account and connection generation. No owner task, mutex or wait is entered.
#[derive(Clone)]
pub struct DirectoryView(Arc<arc_swap::ArcSwap<DirectorySnapshot>>);

impl DirectoryView {
    pub(crate) fn new(snapshot: &DirectorySnapshot) -> Self {
        Self(Arc::new(arc_swap::ArcSwap::from_pointee(snapshot.clone())))
    }
    pub(crate) fn publish(&self, snapshot: &DirectorySnapshot) {
        self.0.store(Arc::new(snapshot.clone()));
    }
    /// Copy the current fresh numeric identity, without reserving a QSO.
    pub fn by_node(&self, node: u32, now: Instant) -> Result<DirectoryEntry, AdmissionError> {
        self.0.load().by_node(node, now)
    }
    /// Recheck current callsign/address/freshness after pending host work.
    pub fn lookup(
        &self,
        callsign: &str,
        address: Ipv4Addr,
        now: Instant,
    ) -> Result<DirectoryEntry, AdmissionError> {
        self.0.load().lookup(callsign, address, now)
    }
}

fn decimal(text: &str) -> Result<u32, DirectoryError> {
    if text.is_empty() || !text.bytes().all(|byte| byte.is_ascii_digit()) {
        return Err(DirectoryError::Malformed);
    }
    text.parse().map_err(|_| DirectoryError::Malformed)
}

fn inflate(input: &[u8]) -> Result<Vec<u8>, DirectoryError> {
    let mut decoder = Decompress::new(true);
    let mut output = Vec::new();
    loop {
        let mut chunk = [0; 8192];
        let previous_in = decoder.total_in();
        let previous_out = decoder.total_out();
        let status = decoder
            .decompress(
                &input[previous_in as usize..],
                &mut chunk,
                FlushDecompress::None,
            )
            .map_err(|_| DirectoryError::Malformed)?;
        let produced = (decoder.total_out() - previous_out) as usize;
        if output.len() + produced > MAX_EXPANDED {
            return Err(DirectoryError::Limit);
        }
        output.extend_from_slice(&chunk[..produced]);
        if status == Status::StreamEnd {
            return if decoder.total_in() == input.len() as u64 {
                Ok(output)
            } else {
                Err(DirectoryError::Malformed)
            };
        }
        if decoder.total_in() == previous_in && produced == 0 {
            return Err(DirectoryError::Malformed);
        }
    }
}
