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
/// Discriminants are the copied account-observation state codes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
#[repr(u32)]
pub enum DirectoryState {
    /// Initial or renewed directory login is due/in flight.
    Registering = 1,
    /// Latest operation succeeded; further deadlines remain scheduled.
    Online = 2,
    /// An operation failed; old snapshots and calls remain intact.
    Retrying = 3,
    /// Logout/cancellation is in progress; no new operation may start.
    Stopping = 4,
    /// Shutdown completed, including a failed finite logout attempt.
    Stopped = 5,
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
                    field.len() > 4096 || field.bytes().any(|byte| byte.is_ascii_control())
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
        // Field budgets bound the request below 16 KiB, within the Proxy's 64 KiB budget.
        // ASL's local printf buffer is not a protocol limit on resolved metadata.
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

/// Credential-free registration health copied on the account owner.
#[derive(Clone, Copy, Debug)]
pub struct Observation {
    /// Current registration/refresh state, not public reachability.
    pub state: DirectoryState,
    /// A successful registration has not yet been logged out.
    pub registered: bool,
    /// Failed matching transactions; stale completion tokens do not count.
    pub failures: u64,
    /// Started operations, including finite logout attempts.
    pub operations: u64,
    /// Remaining retry delay, absent outside retry backoff.
    pub retry_in_ms: Option<u64>,
    /// Age of the last successful login, absent before success.
    pub registration_age_ms: Option<u64>,
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
    register_again: bool,
    last_registration: Option<Instant>,
    last_snapshot: Option<Instant>,
    retry_since: Option<Instant>,
    failures: u64,
}

impl DirectoryControl {
    /// Inspect the original monotonic anchors without renewing or polling them.
    pub fn observe(&self, now: Instant) -> Observation {
        Observation {
            state: self.state,
            registered: self.registered,
            failures: self.failures,
            operations: self.next_id,
            retry_in_ms: self.retry_since.map(|since| {
                crate::session::ms(
                    self.policy
                        .retry
                        .saturating_sub(now.saturating_duration_since(since)),
                )
            }),
            registration_age_ms: self
                .last_registration
                .map(|last| crate::session::ms(now.saturating_duration_since(last))),
        }
    }
    /// Reject missing servers and timings outside the configuration's positive-u32-ms range.
    pub fn new(servers: Vec<String>, policy: DirectoryPolicy) -> Option<Self> {
        if !valid_policy(&servers, policy) {
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
            register_again: false,
            last_registration: None,
            last_snapshot: None,
            retry_since: None,
            failures: 0,
        })
    }

    /// Update server/timing policy without cancelling a borrowed transaction,
    /// resetting its completion token, or discarding a committed snapshot. A
    /// metadata/server change requests renewal after any current registration.
    /// Invalid or stopped updates leave the old schedule untouched.
    pub fn reconfigure(
        &mut self,
        servers: Vec<String>,
        policy: DirectoryPolicy,
        refresh: bool,
    ) -> bool {
        if !valid_policy(&servers, policy)
            || matches!(
                self.state,
                DirectoryState::Stopping | DirectoryState::Stopped
            )
        {
            return false;
        }
        let changed_servers = servers != self.servers;
        if changed_servers {
            self.servers = servers;
            self.server = 0;
        }
        self.policy = policy;
        if refresh || changed_servers {
            self.must_register = true;
            self.register_again |= self
                .pending
                .as_ref()
                .is_some_and(|job| job.operation == Operation::Register);
        }
        true
    }

    /// Metadata-only health; it does not claim that remote clients can reach this account.
    pub fn state(&self) -> DirectoryState {
        self.state
    }

    /// Failed matching transactions, including parse failures but excluding stale completions.
    pub fn failures(&self) -> u64 {
        self.failures
    }

    /// Next due account operation; in-flight I/O has its own finite timeout.
    /// The network owner waits on this deadline alongside packets and commands,
    /// rather than periodically polling registration state during idle time.
    pub fn deadline(&self, now: Instant) -> Option<Instant> {
        if self.pending.is_some()
            || matches!(
                self.state,
                DirectoryState::Stopping | DirectoryState::Stopped
            )
        {
            return None;
        }
        if let Some(failed) = self.retry_since {
            return failed.checked_add(self.policy.retry);
        }
        // Only a successful registration clears must_register; it also sets
        // last_registration in that same owner turn.
        if self.must_register || self.last_snapshot.is_none() {
            return Some(now);
        }
        [
            self.last_registration
                .and_then(|last| last.checked_add(self.policy.registration_refresh)),
            self.last_snapshot
                .and_then(|last| last.checked_add(self.policy.directory_refresh)),
        ]
        .into_iter()
        .flatten()
        .min()
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
                self.must_register = std::mem::take(&mut self.register_again);
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
            self.register_again = false;
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

fn valid_policy(servers: &[String], policy: DirectoryPolicy) -> bool {
    !servers.is_empty()
        && [
            policy.operation_timeout,
            policy.registration_refresh,
            policy.directory_refresh,
            policy.retry,
        ]
        .iter()
        .all(|value| !value.is_zero() && *value <= Duration::from_millis(u32::MAX.into()))
}

#[cfg(test)]
pub(crate) mod tests {
    use super::*;

    /// Build the finite-token boundary without looping through prior jobs.
    pub(crate) fn at_token(next_id: u64) -> DirectoryControl {
        let policy = DirectoryPolicy {
            operation_timeout: Duration::from_secs(1),
            registration_refresh: Duration::from_secs(300),
            directory_refresh: Duration::from_secs(240),
            retry: Duration::from_secs(20),
        };
        let mut owner = DirectoryControl::new(vec!["server".into()], policy).unwrap();
        owner.next_id = next_id;
        owner
    }

    #[test]
    fn completion_tokens_never_wrap_or_reuse_an_old_job() {
        let mut owner = at_token(u64::MAX - 1);
        let now = Instant::now();
        let mut snapshot = DirectorySnapshot::new(Duration::from_secs(900));
        let last = owner.poll(&snapshot, now).unwrap().unwrap();
        assert_eq!(last.id, u64::MAX);
        owner.finish(last.id, Ok(b"OK"), &mut snapshot, now);
        assert!(matches!(
            owner.poll(&snapshot, now),
            Err(ScheduleError::Exhausted)
        ));
        assert_eq!(owner.state(), DirectoryState::Stopped);
        assert!(owner.poll(&snapshot, now).unwrap().is_none());
    }
}
