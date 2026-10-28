//! One account's serialized registration/refresh schedule; directory failure never closes QSOs.

use crate::{
    directory::{DirectoryError, DirectorySnapshot},
    transport::TransactionError,
};
use std::time::{Duration, Instant};

/// Independent per-account directory timings, resolved before activation.
#[derive(Clone, Copy)]
pub struct DirectoryPolicy {
    /// One connect plus request/reply deadline.
    pub operation_timeout: Duration,
    /// Interval after successful registration.
    pub registration_refresh: Duration,
    /// Interval after successful snapshot publication.
    pub directory_refresh: Duration,
    /// Delay before retrying against the next configured server.
    pub retry: Duration,
}

/// Directory health is separate from QSO liveness and public inbound reachability.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectoryState {
    /// Initial or renewed directory login is due/in flight.
    Registering,
    /// Latest operation succeeded; further deadlines remain scheduled.
    Online,
    /// An operation failed; old snapshots and calls remain intact.
    Retrying,
    /// Logout/cancellation is in progress; no new operation may start.
    Stopping,
    /// Shutdown completed, including a failed finite logout attempt.
    Stopped,
}

/// A typed operation contains no credentials; construct its wire request just before I/O.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum Operation {
    /// Register or renew this account's current identity and metadata.
    Register,
    /// Fetch the full/delta response for the last committed server token.
    Snapshot {
        /// Server token, never an admission credential.
        snapshot_id: String,
    },
    /// Finite best-effort offline notification, without retrying shutdown.
    Logout,
}

/// Borrow account credentials only while constructing a directory transaction.
/// Neither this record nor its serialized request belongs in diagnostics.
pub struct DirectoryLogin<'a> {
    /// Normalized station callsign, including a link/repeater suffix when configured.
    pub callsign: &'a str,
    /// Account-specific directory password, not its Proxy password.
    pub password: &'a str,
    /// Configured directory display/QTH field.
    pub description: &'a str,
    /// Public contact email included in online registration.
    pub email: &'a str,
}

impl DirectoryLogin<'_> {
    /// Serialize the ASL online fields or EchoLib offline request, with no truncation.
    /// `hour` and `day` are local civil hour and day-of-month; monotonic retry/liveness
    /// deadlines never depend on them. Snapshot requests reuse only the committed token.
    pub fn request(
        &self,
        operation: &Operation,
        hour: u8,
        day: u8,
    ) -> Result<Vec<u8>, TransactionError> {
        if let Operation::Snapshot { snapshot_id } = operation {
            if snapshot_id.is_empty()
                || snapshot_id.len() > 49
                || !snapshot_id.bytes().all(|byte| byte.is_ascii_graphic())
            {
                return Err(TransactionError::Malformed);
            }
            return Ok(format!("F{snapshot_id}\r").into_bytes());
        }
        if self.callsign.is_empty()
            || self.callsign.len() > 15
            || !self
                .callsign
                .bytes()
                .all(|byte| byte.is_ascii_alphanumeric() || b"-/*_".contains(&byte))
            || self.password.is_empty()
            || [self.password, self.description, self.email]
                .iter()
                .any(|field| {
                    field.len() > 1023 || field.bytes().any(|byte| byte.is_ascii_control())
                })
        {
            return Err(TransactionError::Malformed);
        }
        let status = if *operation == Operation::Logout {
            "OFF-V3.40".to_owned()
        } else {
            if hour > 23 || !(1..=31).contains(&day) {
                return Err(TransactionError::Malformed);
            }
            let class = if self.callsign.starts_with('*') {
                'B'
            } else {
                'R'
            };
            format!("ONLINE1.00{class}({hour}:{day:2})")
        };
        let mut request = format!("l{}", self.callsign).into_bytes();
        request.extend_from_slice(b"\xac\xac");
        request.extend_from_slice(self.password.as_bytes());
        request.push(b'\r');
        request.extend_from_slice(status.as_bytes());
        request.push(b'\r');
        request.extend_from_slice(self.description.as_bytes());
        request.push(b'\r');
        if *operation == Operation::Register {
            request.extend_from_slice(self.email.as_bytes());
            request.push(b'\r');
        }
        if request.len() > 1023 {
            return Err(TransactionError::Limit);
        }
        Ok(request)
    }
}

/// One owned network transaction; at most one exists per account.
#[derive(Clone, Debug)]
pub struct DirectoryJob {
    /// Local non-repeating completion identity; not sent on the wire.
    pub id: u64,
    /// Selected configured hostname.
    pub server: String,
    /// Typed request, without a password or serialized body.
    pub operation: Operation,
    /// Whole-operation deadline used by the transport.
    pub timeout: Duration,
}

/// Content-free failure returned exactly once for a matching completion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum DirectoryFailure {
    /// Socket, timeout or negative registration result.
    Transport(TransactionError),
    /// Snapshot validation failed without publishing a partial candidate.
    Snapshot(DirectoryError),
}

/// Completion tokens never wrap and accidentally identify an old operation.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ScheduleError {
    /// This owner exhausted its finite token space and must be replaced.
    Exhausted,
}

/// Pure monotonic scheduling state, driven by one bounded asynchronous directory worker.
pub struct DirectoryControl {
    servers: Vec<String>,
    server: usize,
    policy: DirectoryPolicy,
    state: DirectoryState,
    pending: Option<DirectoryJob>,
    next_id: u64,
    registered: bool,
    must_register: bool,
    last_registration: Option<Instant>,
    last_snapshot: Option<Instant>,
    retry_since: Option<Instant>,
    failures: u64,
}

impl DirectoryControl {
    /// Reject missing servers and timings outside the configuration's positive-u32-ms range.
    pub fn new(servers: Vec<String>, policy: DirectoryPolicy) -> Option<Self> {
        if servers.is_empty()
            || [
                policy.operation_timeout,
                policy.registration_refresh,
                policy.directory_refresh,
                policy.retry,
            ]
            .iter()
            .any(|value| value.is_zero() || *value > Duration::from_millis(u32::MAX.into()))
        {
            return None;
        }
        Some(Self {
            servers,
            server: 0,
            policy,
            state: DirectoryState::Registering,
            pending: None,
            next_id: 0,
            registered: false,
            must_register: true,
            last_registration: None,
            last_snapshot: None,
            retry_since: None,
            failures: 0,
        })
    }

    /// Metadata-only health; it does not claim that remote clients can reach this account.
    pub fn state(&self) -> DirectoryState {
        self.state
    }

    /// Failed matching transactions, including parse failures but excluding stale completions.
    pub fn failures(&self) -> u64 {
        self.failures
    }

    /// Start only a due operation. The worker applies the returned transport deadline.
    pub fn poll(
        &mut self,
        snapshot: &DirectorySnapshot,
        now: Instant,
    ) -> Result<Option<DirectoryJob>, ScheduleError> {
        if self.pending.is_some()
            || matches!(
                self.state,
                DirectoryState::Stopping | DirectoryState::Stopped
            )
        {
            return Ok(None);
        }
        if self
            .retry_since
            .is_some_and(|failed| now.saturating_duration_since(failed) < self.policy.retry)
        {
            return Ok(None);
        }
        self.retry_since = None;
        if self.must_register
            || self.last_registration.is_none_or(|last| {
                now.saturating_duration_since(last) >= self.policy.registration_refresh
            })
        {
            self.begin(Operation::Register).map(Some)
        } else if self
            .last_snapshot
            .is_none_or(|last| now.saturating_duration_since(last) >= self.policy.directory_refresh)
        {
            self.begin(Operation::Snapshot {
                snapshot_id: snapshot.snapshot_id().into(),
            })
            .map(Some)
        } else {
            Ok(None)
        }
    }

    /// Publish only the matching generation's complete result; failures preserve all live calls.
    pub fn finish(
        &mut self,
        id: u64,
        result: Result<&[u8], TransactionError>,
        snapshot: &mut DirectorySnapshot,
        now: Instant,
    ) -> Option<DirectoryFailure> {
        let pending = self.pending.take_if(|pending| pending.id == id)?;
        let outcome = match result {
            Err(error) => Err(DirectoryFailure::Transport(error)),
            Ok(reply) => match &pending.operation {
                Operation::Register | Operation::Logout => {
                    if reply == b"OK" {
                        Ok(())
                    } else {
                        Err(DirectoryFailure::Transport(TransactionError::Rejected))
                    }
                }
                Operation::Snapshot { .. } => snapshot
                    .update(reply, now)
                    .map_err(DirectoryFailure::Snapshot),
            },
        };
        if pending.operation == Operation::Logout {
            self.state = DirectoryState::Stopped;
            self.registered = false;
        } else if outcome.is_ok() {
            self.state = DirectoryState::Online;
            if pending.operation == Operation::Register {
                self.registered = true;
                self.must_register = false;
                self.last_registration = Some(now);
            } else {
                self.last_snapshot = Some(now);
            }
        } else {
            self.server = (self.server + 1) % self.servers.len();
            self.retry_since = Some(now);
            self.must_register = true;
            self.state = DirectoryState::Retrying;
        }
        if let Err(error) = outcome {
            self.failures = self.failures.saturating_add(1);
            Some(error)
        } else {
            None
        }
    }

    /// Cancel the exact pending operation before a finite logout; repeated stop is inert.
    pub fn stop(&mut self) -> Result<Option<DirectoryJob>, ScheduleError> {
        if matches!(
            self.state,
            DirectoryState::Stopping | DirectoryState::Stopped
        ) {
            return Ok(None);
        }
        self.pending = None;
        self.state = DirectoryState::Stopping;
        if self.registered {
            self.begin(Operation::Logout).map(Some)
        } else {
            self.state = DirectoryState::Stopped;
            Ok(None)
        }
    }

    fn begin(&mut self, operation: Operation) -> Result<DirectoryJob, ScheduleError> {
        let Some(id) = self.next_id.checked_add(1) else {
            self.state = DirectoryState::Stopped;
            return Err(ScheduleError::Exhausted);
        };
        self.next_id = id;
        if operation == Operation::Register {
            self.state = DirectoryState::Registering;
        }
        let job = DirectoryJob {
            id,
            server: self.servers[self.server].clone(),
            operation,
            timeout: self.policy.operation_timeout,
        };
        self.pending = Some(job.clone());
        Ok(job)
    }
}
