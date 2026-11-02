//! Per-QSO bounded MPSC ingress; no blocking send or payload copy on failed admission.

use crate::wire::{Channel, MAX_DATAGRAM};
use std::{
    net::SocketAddr,
    sync::{
        Arc, Weak,
        atomic::{AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc};

const TOTAL_BYTES: usize = 256 * 1024;
// Preserve room for one maximum legal UDP control packet even at media's byte limit.
const MEDIA_BYTES: usize = TOTAL_BYTES - 64 * 1024;

/// Local queue admission result; none of these errors requests a remote hangup.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum PushError {
    /// Slot or retained-payload-byte budget exhausted; nothing was copied.
    Full,
    /// The receiving owner has closed admission.
    Closed,
    /// Payload cannot be a legal IPv4 UDP datagram.
    Oversize,
}

#[derive(Default)]
pub(crate) struct Budget(AtomicU64);

impl Budget {
    /// Test the same combined byte budget without reserving any credit.
    pub(crate) fn available_for(&self, length: usize, media: bool) -> bool {
        Self::fits(self.0.load(Ordering::Acquire), length, media)
    }

    fn fits(old: u64, length: usize, media: bool) -> bool {
        let total = (old >> 32) as usize + length;
        let media_bytes = (old as u32) as usize + if media { length } else { 0 };
        total <= TOTAL_BYTES && media_bytes <= MEDIA_BYTES
    }
    pub(crate) fn reserve(
        self: &Arc<Self>,
        length: usize,
        media: bool,
    ) -> Result<Charge, PushError> {
        // Both byte counts change in one CAS; a control/media race cannot overbook them.
        let amount = ((length as u64) << 32) | if media { length as u64 } else { 0 };
        self.0
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |old| {
                Self::fits(old, length, media).then_some(old + amount)
            })
            .map_err(|_| PushError::Full)?;
        Ok(Charge {
            budget: Arc::clone(self),
            amount,
        })
    }

    pub(crate) fn retained_bytes(&self) -> usize {
        (self.0.load(Ordering::Acquire) >> 32) as usize
    }
}

pub(crate) struct Charge {
    budget: Arc<Budget>,
    amount: u64,
}
impl Drop for Charge {
    fn drop(&mut self) {
        self.budget.0.fetch_sub(self.amount, Ordering::AcqRel);
    }
}

/// Unique immutable packet owner, retaining its byte/slot charge until actual release.
pub struct Datagram {
    payload: Vec<u8>,
    class: Channel,
    source: SocketAddr,
    arrived: Instant,
    generation: u64,
    _charge: Charge,
    _media_slot: Option<OwnedSemaphorePermit>,
}

impl Datagram {
    /// Audio/text versus control socket provenance, independent of the remote source port.
    pub fn class(&self) -> Channel {
        self.class
    }
    /// Borrow bytes only while this packet owner is retained.
    pub fn payload(&self) -> &[u8] {
        &self.payload
    }
    /// Actual observed endpoint, copied before routing.
    pub fn source(&self) -> SocketAddr {
        self.source
    }
    /// Original monotonic arrival, not the later worker execution time.
    pub fn arrived(&self) -> Instant {
        self.arrived
    }
    /// Fixed generation of the route that admitted the packet.
    pub fn generation(&self) -> u64 {
        self.generation
    }
}

/// Cloneable routing handle; one stalled peer never uses another handle's budgets.
#[derive(Clone)]
pub struct Producer {
    sender: mpsc::Sender<Datagram>,
    media_slots: Arc<Semaphore>,
    budget: Arc<Budget>,
    generation: u64,
    full_drops: Arc<AtomicU64>,
}

impl Producer {
    /// Reserve a FIFO slot, media allowance and byte budget before copying the payload.
    pub fn try_push(
        &self,
        class: Channel,
        source: SocketAddr,
        arrived: Instant,
        payload: &[u8],
    ) -> Result<(), PushError> {
        let result = self.push(class, source, arrived, payload);
        if result == Err(PushError::Full) {
            self.full_drops.fetch_add(1, Ordering::Relaxed);
        }
        result
    }

    fn push(
        &self,
        class: Channel,
        source: SocketAddr,
        arrived: Instant,
        payload: &[u8],
    ) -> Result<(), PushError> {
        if payload.len() > MAX_DATAGRAM {
            return Err(PushError::Oversize);
        }
        let permit = self.sender.try_reserve().map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => PushError::Full,
            mpsc::error::TrySendError::Closed(_) => PushError::Closed,
        })?;
        let media = class == Channel::Audio;
        let slot = if media {
            Some(
                Arc::clone(&self.media_slots)
                    .try_acquire_owned()
                    .map_err(|_| PushError::Full)?,
            )
        } else {
            None
        };
        let charge = self.budget.reserve(payload.len(), media)?;
        permit.send(Datagram {
            payload: payload.to_vec(),
            class,
            source,
            arrived,
            generation: self.generation,
            _charge: charge,
            _media_slot: slot,
        });
        Ok(())
    }

    /// Payload bytes still queued or borrowed by the serialized consumer.
    pub fn retained_bytes(&self) -> usize {
        self.budget.retained_bytes()
    }
}

/// Unique serial receive owner; close then drain before dropping callback state.
pub struct Consumer {
    receiver: mpsc::Receiver<Datagram>,
    observer: Observer,
}

impl Consumer {
    /// Copy a metadata-only weak route observer before transferring this owner.
    pub fn observer(&self) -> Observer {
        self.observer.clone()
    }
    /// Poll one packet without waiting; callers process at most 16 per owner turn.
    pub fn try_recv(&mut self) -> Option<Datagram> {
        self.receiver.try_recv().ok()
    }
    /// Sleep until data or closed admission, without a task per datagram.
    pub async fn recv(&mut self) -> Option<Datagram> {
        self.receiver.recv().await
    }
    /// Reject new reservations; preexisting reservations must still be drained.
    pub fn close(&mut self) {
        self.receiver.close();
    }
}

/// Copied ingress accounting; reservations count as queued until released.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub struct Observation {
    /// Reserved/queued slots, excluding a packet already lent to the consumer.
    pub queued_packets: u32,
    /// Total route slots; zero means the producer is no longer available.
    pub capacity_packets: u32,
    /// Failed slot, media or byte-credit admissions; never a hangup.
    pub full_drops: u64,
    /// Bytes charged to queued and currently borrowed packets.
    pub retained_bytes: u64,
}

/// Observes current credit without retaining a routing producer or consumer.
#[derive(Clone)]
pub struct Observer {
    sender: mpsc::WeakSender<Datagram>,
    budget: Weak<Budget>,
    full_drops: Arc<AtomicU64>,
}
impl Observer {
    /// Copy independently sampled counters without borrowing the receive owner.
    pub fn observe(&self) -> Observation {
        let (queued_packets, capacity_packets) = self.sender.upgrade().map_or((0, 0), |sender| {
            (
                (sender.max_capacity() - sender.capacity()) as u32,
                sender.max_capacity() as u32,
            )
        });
        Observation {
            queued_packets,
            capacity_packets,
            full_drops: self.full_drops.load(Ordering::Relaxed),
            retained_bytes: self
                .budget
                .upgrade()
                .map_or(0, |budget| budget.retained_bytes() as u64),
        }
    }
}

/// Allocate one nonzero generation's bounded route outside real-time callbacks.
pub fn channel(generation: u64) -> Option<(Producer, Consumer)> {
    if generation == 0 {
        return None;
    }
    let (sender, receiver) = mpsc::channel(64);
    let budget = Arc::new(Budget::default());
    let full_drops = Arc::new(AtomicU64::new(0));
    let observer = Observer {
        sender: sender.downgrade(),
        budget: Arc::downgrade(&budget),
        full_drops: Arc::clone(&full_drops),
    };
    Some((
        Producer {
            sender,
            media_slots: Arc::new(Semaphore::new(56)),
            budget,
            generation,
            full_drops,
        },
        Consumer { receiver, observer },
    ))
}
