//! Bounded cross-peer talker requests; protocol and PCM owners never wait for a grant.

use super::{RELEASE_AFTER, Selection, UNKEY_AFTER};
use crate::transport::{driver::Control, routing::Router};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicU64, Ordering},
    },
    time::Instant,
};
use tokio::sync::{mpsc, oneshot};

struct Endpoint {
    identity: (u32, u64),
    wake: Arc<Control>,
    unkey: AtomicBool,
}

enum Command {
    Offer {
        endpoint: Arc<Endpoint>,
        arrived: Instant,
        reply: oneshot::Sender<Selection>,
    },
    Drain {
        endpoint: Arc<Endpoint>,
        drained: bool,
    },
}

/// Explicit queue outcomes; none is a reason to close a network session.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum OfferError {
    /// This peer still owns an unresolved request.
    Busy,
    /// Account queue credit is exhausted; nothing was admitted.
    Full,
    /// Account control has stopped; no further work can be admitted.
    Closed,
}

/// One serialized QSO's request endpoint. Move it with that owner, never into radio callbacks.
/// A pending offer retains its original datagram outside this control-only channel.
pub struct TalkerPort {
    endpoint: Arc<Endpoint>,
    sender: mpsc::Sender<Command>,
    pending: Option<oneshot::Receiver<Selection>>,
    last_drain: Option<bool>,
    overflows: Arc<AtomicU64>,
}

impl TalkerPort {
    fn submit(&self, command: Command) -> Result<(), OfferError> {
        self.sender.try_send(command).map_err(|error| match error {
            mpsc::error::TrySendError::Full(_) => {
                self.overflows.fetch_add(1, Ordering::Relaxed);
                OfferError::Full
            }
            mpsc::error::TrySendError::Closed(_) => OfferError::Closed,
        })
    }

    /// Offer only wire/codec-validated, session-bound media; return without waiting.
    /// There is one outstanding request per peer. A oneshot's unique receiver fences
    /// its reply without a wrapping sequence counter or synthetic protocol packet.
    pub fn offer(&mut self, arrived: Instant) -> Result<(), OfferError> {
        if self.pending.is_some() {
            return Err(OfferError::Busy);
        }
        let (reply, receiver) = oneshot::channel();
        self.submit(Command::Offer {
            endpoint: Arc::clone(&self.endpoint),
            arrived,
            reply,
        })?;
        self.pending = Some(receiver);
        Ok(())
    }

    /// Nonblocking grant collection on the existing serial peer turn.
    pub fn selection(&mut self) -> Result<Option<Selection>, OfferError> {
        let Some(receiver) = self.pending.as_mut() else {
            return Ok(None);
        };
        match receiver.try_recv() {
            Ok(selection) => {
                self.pending = None;
                self.last_drain = None;
                Ok(Some(selection))
            }
            Err(oneshot::error::TryRecvError::Empty) => Ok(None),
            Err(oneshot::error::TryRecvError::Closed) => {
                self.pending = None;
                Err(OfferError::Closed)
            }
        }
    }

    /// Report combined pending-packet, jitter-output AND product-ring drainage.
    /// Retry Full; duplicate successfully submitted state needs no new queue credit.
    pub fn report_drained(&mut self, drained: bool) -> Result<(), OfferError> {
        if self.pending.is_some() {
            return Err(OfferError::Busy);
        }
        if self.last_drain != Some(drained) {
            self.submit(Command::Drain {
                endpoint: Arc::clone(&self.endpoint),
                drained,
            })?;
            self.last_drain = Some(drained);
        }
        Ok(())
    }

    /// Consume one selected-source unkey notification after the account wakes this owner.
    pub fn take_unkey(&self) -> bool {
        self.endpoint.unkey.swap(false, Ordering::AcqRel)
    }
}

/// One account's routing/talker owner. Network tasks service this alongside I/O/deadlines;
/// no extra runtime, OS thread, per-packet task or audio-side lock is introduced.
pub struct Coordinator {
    router: Router,
    sender: mpsc::Sender<Command>,
    receiver: mpsc::Receiver<Command>,
    selected: Option<Arc<Endpoint>>,
    drained: bool,
    overflows: Arc<AtomicU64>,
}

impl Coordinator {
    /// Derive two control slots per configured station, without a separate arbitrary limit.
    /// Return None only when the target address space cannot represent that capacity.
    pub fn new(router: Router) -> Option<Self> {
        let capacity = usize::try_from(u64::from(router.account.maximum) * 2).ok()?;
        let (sender, receiver) = mpsc::channel(capacity);
        Some(Self {
            router,
            sender,
            receiver,
            selected: None,
            drained: true,
            overflows: Arc::new(AtomicU64::new(0)),
        })
    }

    /// Borrow routing on its same serialized owner for admission/dial/datagram operations.
    pub fn router(&mut self) -> &mut Router {
        &mut self.router
    }

    /// Prepare one endpoint for a provider-bound node/generation, before peer attachment.
    pub fn port(&self, node: u32, generation: u64, wake: Arc<Control>) -> TalkerPort {
        TalkerPort {
            endpoint: Arc::new(Endpoint {
                identity: (node, generation),
                wake,
                unkey: AtomicBool::new(false),
            }),
            sender: self.sender.clone(),
            pending: None,
            last_drain: None,
            overflows: Arc::clone(&self.overflows),
        }
    }

    /// Handle exactly one queued request, then return to the account's select loop.
    /// False means closed and fully drained; callbacks are only nonblocking owner wakes.
    pub async fn service(&mut self) -> bool {
        let Some(command) = self.receiver.recv().await else {
            return false;
        };
        match command {
            Command::Offer {
                endpoint,
                arrived,
                reply,
            } => {
                if reply.is_closed() {
                    return true;
                }
                let (node, generation) = endpoint.identity;
                let selection = self.router.account.voice(node, generation, arrived);
                if selection == Selection::Selected {
                    // Fence accepted-but-not-yet-admitted PCM before the grant is observable.
                    self.drained = false;
                    endpoint.unkey.store(false, Ordering::Release);
                    self.selected = Some(Arc::clone(&endpoint));
                }
                let _ = reply.send(selection);
                endpoint.wake.wake();
            }
            Command::Drain { endpoint, drained } => {
                if self
                    .selected
                    .as_ref()
                    .is_some_and(|selected| Arc::ptr_eq(selected, &endpoint))
                {
                    self.drained = drained;
                }
            }
        }
        // Queue progress restores control credit. Wake the selected owner so a
        // full drain report can retry without an idle polling/retry timer.
        if let Some(selected) = &self.selected {
            selected.wake.wake();
        }
        true
    }

    /// Apply the existing quiet guard only after accepted downstream PCM drains.
    pub fn poll(&mut self, now: Instant) -> Option<(u32, u64)> {
        let unkey = self.router.account.poll(now, self.drained);
        if let Some(endpoint) = self
            .selected
            .as_ref()
            .filter(|endpoint| Some(endpoint.identity) == unkey)
        {
            endpoint.unkey.store(true, Ordering::Release);
            endpoint.wake.wake();
        }
        if self.router.account.selected().is_none() {
            self.selected = None;
        }
        unkey
    }

    /// Next guard/release deadline. Past deadlines park while waiting for real drain feedback.
    /// Call poll before choosing the next wait; drain messages also wake the account select loop.
    pub fn deadline(&self, now: Instant) -> Option<Instant> {
        let talker = self.router.account.talker.as_ref()?;
        let deadline = talker.last_packet.checked_add(if talker.keyed {
            UNKEY_AFTER
        } else {
            RELEASE_AFTER
        })?;
        (deadline > now || self.drained).then_some(deadline)
    }

    /// Retire exact routing/admission after the host has detached that QSO.
    /// Detach cancels its pending PCM, so no report from its dead port is required.
    /// Keep the ordinary silence guard; an old generation cannot cancel a replacement.
    pub fn remove(&mut self, node: u32, generation: u64) {
        self.router.remove(node, generation);
        if self
            .selected
            .as_ref()
            .is_some_and(|selected| selected.identity == (node, generation))
        {
            self.drained = true;
        }
    }

    /// Stop new control admission, then service remaining commands until service returns false.
    pub fn close(&mut self) {
        self.receiver.close();
    }

    /// Control-queue credit failures, not wire loss or remote delivery failures.
    pub fn overflows(&self) -> u64 {
        self.overflows.load(Ordering::Relaxed)
    }
}
