//! One bounded, readiness-driven protocol owner per QSO on the shared Tokio pool.

use super::{
    direct::DirectSockets,
    mailbox::{Consumer, Datagram},
    proxy::ProxyOutput,
};
use crate::wire::{Channel, ProxyKind};
use std::{
    io,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Instant,
};
use tokio::{
    runtime::Runtime,
    sync::{Notify, watch},
    task::JoinHandle,
};

/// Callback-safe signals, prepared before a task can execute or reenter its host.
pub struct Control {
    stop: watch::Sender<bool>,
    notify: Notify,
    pending: AtomicBool,
    close: AtomicBool,
}

impl Default for Control {
    fn default() -> Self {
        Self {
            stop: watch::channel(false).0,
            notify: Notify::new(),
            pending: AtomicBool::new(false),
            close: AtomicBool::new(false),
        }
    }
}

impl Control {
    /// Nonblocking stop request, not a barrier; detach performs quiescence off the executor.
    pub fn stop(&self) {
        self.stop.send_replace(true);
    }

    /// Inspect teardown on a non-audio owner; shutdown never implies packet loss.
    pub fn is_stopped(&self) -> bool {
        *self.stop.borrow()
    }

    /// Ask the serial QSO owner to send BYE and drain accepted media. This is
    /// not a quiescence barrier; final detach still uses the independent stop.
    pub fn request_close(&self) {
        self.close.store(true, Ordering::Release);
        // A product wake may already be consumed while its pending bit is
        // still set. Closing must supply its own permit in that window.
        self.notify.notify_one();
    }

    /// Coalesce product/egress work without creating a new task or entering an audio callback.
    pub fn wake(&self) {
        if !self.pending.swap(true, Ordering::AcqRel) {
            self.notify.notify_one();
        }
    }
}

/// One immutable input handed to the connection's sole protocol/codec owner.
pub enum PeerEvent {
    /// Initial or coalesced product wake.
    Wake,
    /// Graceful local close; the owner remains runnable through BYE/media drain.
    Close,
    /// Admitted packet with its original arrival, endpoint and generation.
    Packet(Datagram),
    /// The owner's actual advertised deadline is due.
    Deadline,
    /// A previously unready socket can be retried with the same prepared bytes.
    Writable,
    /// Readiness failed; preserve the category rather than inventing a generic hangup.
    Io(io::ErrorKind),
}

/// Exact transport credit needed by an already-prepared outgoing packet.
pub enum PeerWritable {
    /// UDP socket readiness after a nonblocking send returned Pending.
    Direct(Arc<DirectSockets>, Channel),
    /// Shared Proxy credit for this kind and length, without reserving it.
    Proxy {
        /// Selected account transport; no direct fallback is attempted.
        output: ProxyOutput,
        /// Wire kind controls the reserved media/control allowances.
        kind: ProxyKind,
        /// Retained packet's payload length, excluding Proxy framing.
        payload_bytes: usize,
    },
}

impl PeerWritable {
    async fn ready(&self) -> io::Result<()> {
        match self {
            Self::Direct(sockets, channel) => sockets.writable(*channel).await,
            Self::Proxy {
                output,
                kind,
                payload_bytes,
            } => output
                .writable(*kind, *payload_bytes)
                .await
                .map_err(io::Error::from),
        }
    }
}

/// Copied wait state; no mutable connection borrow survives an await or external callback.
pub struct PeerWait {
    /// False when retained input cannot yet advance; queued packets must not drive a spin loop.
    pub receive: bool,
    /// Next real protocol/media/playout deadline; None parks without periodic polling.
    pub deadline: Option<Instant>,
    /// Set only after an actual Pending send; idle writable sockets never drive the owner.
    pub writable: Option<PeerWritable>,
}

impl Default for PeerWait {
    fn default() -> Self {
        Self {
            receive: true,
            deadline: None,
            writable: None,
        }
    }
}

/// Retains task, callback and runtime through final detach. Never destroy on an executor thread.
pub struct PeerDriver {
    control: Arc<Control>,
    task: Mutex<Option<JoinHandle<()>>>,
    runtime: Arc<Runtime>,
}

impl PeerDriver {
    /// Start a bounded, nonblocking owner step. No peer borrow may span an external callback.
    /// A full/blocked input is retried only by a real deadline or product wake, not queue presence.
    pub fn attach(
        runtime: Arc<Runtime>,
        control: Arc<Control>,
        mut consumer: Consumer,
        mut step: impl FnMut(PeerEvent) -> Option<PeerWait> + Send + 'static,
    ) -> Self {
        let active = Arc::clone(&control);
        let mut stopping = control.stop.subscribe();
        let task = runtime.spawn(async move {
            let mut event = PeerEvent::Wake;
            let mut turns = 0;
            loop {
                // End the watch borrow before a callback can reenter Control::stop.
                let stopped = *stopping.borrow();
                if stopped {
                    break;
                }
                if active.close.swap(false, Ordering::AcqRel) {
                    event = PeerEvent::Close;
                }
                active.pending.store(false, Ordering::Release);
                let Some(wait) = step(event) else {
                    break;
                };
                turns += 1;
                if turns == 16 {
                    turns = 0;
                    tokio::task::yield_now().await;
                }
                event = tokio::select! {
                    _ = stopping.changed() => break,
                    packet = consumer.recv(), if wait.receive => match packet {
                        Some(packet) => PeerEvent::Packet(packet),
                        None => break,
                    },
                    _ = active.notify.notified() => PeerEvent::Wake,
                    _ = async {
                        match wait.deadline {
                            Some(deadline) => tokio::time::sleep_until(deadline.into()).await,
                            None => std::future::pending().await,
                        }
                    } => PeerEvent::Deadline,
                    ready = async {
                        match wait.writable.as_ref() {
                            Some(pending) => pending.ready().await,
                            None => std::future::pending().await,
                        }
                    } => match ready {
                        Ok(()) => PeerEvent::Writable,
                        Err(error) => PeerEvent::Io(error.kind()),
                    },
                };
            }
        });
        Self {
            control,
            task: Mutex::new(Some(task)),
            runtime,
        }
    }

    /// Shared-access barrier permits a running callback to stop itself while detach waits.
    /// The tiny join mutex is control-only and is never acquired by packet or audio processing.
    pub(crate) fn wait(&self) {
        self.control.stop();
        let mut task = self.task.lock().unwrap();
        if let Some(task) = task.take() {
            let _ = self.runtime.block_on(task);
        }
    }

    /// Revisit copied account policy on the existing serial owner, without a task.
    pub(crate) fn wake(&self) {
        self.control.wake();
    }

    /// Stop and join off-executor before releasing captured contexts or a provider DSO.
    pub fn detach(self) {}
}

impl Drop for PeerDriver {
    fn drop(&mut self) {
        self.wait();
    }
}

#[cfg(test)]
#[path = "../../tests/unit/driver_control.rs"]
mod tests;
