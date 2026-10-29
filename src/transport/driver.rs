//! One bounded, readiness-driven protocol owner per QSO on the shared Tokio pool.

use super::{
    direct::DirectSockets,
    mailbox::{Consumer, Datagram},
};
use crate::wire::Channel;
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
}

impl Default for Control {
    fn default() -> Self {
        Self {
            stop: watch::channel(false).0,
            notify: Notify::new(),
            pending: AtomicBool::new(false),
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
    /// Admitted packet with its original arrival, endpoint and generation.
    Packet(Datagram),
    /// The owner's actual advertised deadline is due.
    Deadline,
    /// A previously unready socket can be retried with the same prepared bytes.
    Writable,
    /// Readiness failed; preserve the category rather than inventing a generic hangup.
    Io(io::ErrorKind),
}

/// Copied wait state; no mutable connection borrow survives an await or external callback.
pub struct PeerWait {
    /// False when retained input cannot yet advance; queued packets must not drive a spin loop.
    pub receive: bool,
    /// Next real protocol/media/playout deadline; None parks without periodic polling.
    pub deadline: Option<Instant>,
    /// Set only after an actual Pending send; idle writable sockets never drive the owner.
    pub writable: Option<(Arc<DirectSockets>, Channel)>,
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
                            Some((sockets, channel)) => sockets.writable(*channel).await,
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

    /// Stop and join off-executor before releasing captured contexts or a provider DSO.
    pub fn detach(self) {}
}

impl Drop for PeerDriver {
    fn drop(&mut self) {
        self.wait();
    }
}
