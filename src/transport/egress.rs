//! Per-QSO queue eligibility metadata. No socket, runtime, PCM or timer lives here.
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::Poll,
};

/// Retained independently of the QSO. Queue tickets own metadata, never the QSO.
#[derive(Default)]
pub(crate) struct Lifetime {
    closed: AtomicBool,
    pending: AtomicUsize,
}
impl Lifetime {
    pub(crate) fn close(&self) {
        self.closed.store(true, Ordering::Release);
    }
    pub(crate) fn disposed(&self) -> bool {
        self.closed.load(Ordering::Acquire) && self.pending.load(Ordering::Acquire) == 0
    }
}
pub(crate) struct Epoch {
    open: Mutex<bool>,
    lifetime: Arc<Lifetime>,
}
impl Epoch {
    fn new(lifetime: Arc<Lifetime>) -> Self {
        Self {
            open: Mutex::new(true),
            lifetime,
        }
    }
    fn revoke(&self) {
        *self.open.lock().unwrap_or_else(|error| error.into_inner()) = false;
    }
    pub(crate) fn ticket(self: &Arc<Self>) -> Ticket {
        // Admission already owns one of the Proxy's56 media slots. A ticket is
        // created only after all bounded queue/byte reservations succeeded.
        self.lifetime.pending.fetch_add(1, Ordering::AcqRel);
        Ticket(self.clone())
    }
}
/// A single actual queued wire frame, released only with its last retained byte.
pub(crate) struct Ticket(Arc<Epoch>);
impl Ticket {
    pub(crate) fn write<T>(
        &self,
        started: bool,
        write: impl FnOnce() -> Poll<io::Result<T>>,
    ) -> Poll<io::Result<Option<T>>> {
        // Hold only across one nonblocking poll_write. Revocation linearizes
        // before the first byte; a partial frame must finish to preserve TCP.
        let open = self
            .0
            .open
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !started && (!*open || self.0.lifetime.closed.load(Ordering::Acquire)) {
            return Poll::Ready(Ok(None));
        }
        write().map(|result| result.map(Some))
    }
}
impl Drop for Ticket {
    fn drop(&mut self) {
        self.0.lifetime.pending.fetch_sub(1, Ordering::AcqRel);
    }
}

/// The existing serial QSO owner controls encoder resets and epoch transitions.
pub(crate) struct Egress {
    epoch: Arc<Epoch>,
    token: u64,
    retiring: bool,
    reset_needed: bool,
}
#[derive(Debug, PartialEq, Eq)]
pub(crate) struct Exhausted;
impl Egress {
    pub(crate) fn new(lifetime: Arc<Lifetime>) -> Self {
        Self {
            epoch: Arc::new(Epoch::new(lifetime)),
            token: 1,
            retiring: false,
            reset_needed: false,
        }
    }
    pub(crate) fn epoch(&self) -> Option<Arc<Epoch>> {
        (!self.retiring).then(|| self.epoch.clone())
    }
    pub(crate) fn retiring(&self) -> bool {
        self.retiring
    }
    /// Returns true until the caller's actual encoder reset has succeeded.
    pub(crate) fn begin(&mut self) -> bool {
        if !self.retiring {
            self.epoch.revoke();
            self.retiring = true;
            self.reset_needed = true;
        }
        self.reset_needed
    }
    pub(crate) fn reset_completed(&mut self) {
        self.reset_needed = false;
    }
    #[cfg(test)]
    pub(crate) fn exhaust(&mut self) {
        self.token = u64::MAX;
    }
    /// Call only after actual encoder reset. Pending includes partial TCP frames.
    pub(crate) fn complete(&mut self) -> Result<Option<u64>, Exhausted> {
        if self.epoch.lifetime.pending.load(Ordering::Acquire) != 0 {
            return Ok(None);
        }
        let token = self.token.checked_add(1).ok_or(Exhausted)?;
        self.epoch = Arc::new(Epoch::new(self.epoch.lifetime.clone()));
        self.token = token;
        self.retiring = false;
        Ok(Some(token))
    }
}

#[cfg(test)]
#[path = "../../tests/unit/egress.rs"]
mod tests;
