//! Backoff after a backend refuses to log a connection in.
//!
//! A provider that answers a login with `502 connection limit reached` (or any
//! other refusal) will answer the next login the same way until another
//! connection on the account is released. Without a gate, every request that
//! falls through to that backend opens a fresh TLS session, authenticates,
//! fails, and drops the socket, which hammers the provider and can trip its
//! failed-login blocking for the whole account. The gate makes pool creation
//! fail locally for a growing interval, so routing moves on to the next backend
//! without touching the network.

use std::sync::Mutex;
use std::time::{Duration, Instant};

/// First wait after a refused login.
pub(crate) const INITIAL_DELAY: Duration = Duration::from_secs(5);

/// Longest wait between login attempts while the backend keeps refusing.
pub(crate) const MAX_DELAY: Duration = Duration::from_secs(60);

#[derive(Debug, Default)]
struct State {
    /// Consecutive refused attempts, counted once per backoff window.
    failures: u32,
    /// No login may be attempted before this instant.
    retry_not_before: Option<Instant>,
}

/// Shared by every pool slot of one backend.
#[derive(Debug, Default)]
pub(crate) struct AuthBackoff {
    state: Mutex<State>,
}

impl AuthBackoff {
    /// Remaining wait if a login must not be attempted at `now`.
    pub(crate) fn remaining(&self, now: Instant) -> Option<Duration> {
        let state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        state
            .retry_not_before
            .and_then(|deadline| deadline.checked_duration_since(now))
            .filter(|left| !left.is_zero())
    }

    /// Record a refused login and return the wait that now applies.
    ///
    /// Attempts that were already in flight when the window opened fail
    /// together; only the first of them extends the delay, so a pool of N
    /// concurrent creators does not jump N steps up the schedule.
    pub(crate) fn record_failure(&self, now: Instant) -> Duration {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if let Some(deadline) = state.retry_not_before
            && now < deadline
        {
            return deadline - now;
        }
        let delay = delay_for(state.failures);
        state.failures = state.failures.saturating_add(1);
        state.retry_not_before = Some(now + delay);
        delay
    }

    /// A login succeeded: forget the refusals so the next one starts fresh.
    pub(crate) fn record_success(&self) {
        let mut state = self
            .state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        *state = State::default();
    }
}

fn delay_for(failures: u32) -> Duration {
    INITIAL_DELAY
        .saturating_mul(1u32.checked_shl(failures).unwrap_or(u32::MAX))
        .min(MAX_DELAY)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn open_until_first_failure() {
        let gate = AuthBackoff::default();
        assert_eq!(gate.remaining(Instant::now()), None);
    }

    #[test]
    fn failure_blocks_until_delay_elapses() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        assert_eq!(gate.record_failure(t0), INITIAL_DELAY);
        assert_eq!(gate.remaining(t0), Some(INITIAL_DELAY));
        assert_eq!(
            gate.remaining(t0 + Duration::from_secs(2)),
            Some(Duration::from_secs(3))
        );
        assert_eq!(gate.remaining(t0 + INITIAL_DELAY), None);
    }

    #[test]
    fn delay_doubles_per_window_and_caps() {
        let gate = AuthBackoff::default();
        let mut now = Instant::now();
        let mut seen = Vec::new();
        for _ in 0..8 {
            let delay = gate.record_failure(now);
            seen.push(delay.as_secs());
            now += delay;
        }
        assert_eq!(seen, [5, 10, 20, 40, 60, 60, 60, 60]);
    }

    #[test]
    fn concurrent_failures_in_one_window_count_once() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        gate.record_failure(t0);
        // Two more attempts that were already in flight report back later in the window.
        let left = gate.record_failure(t0 + Duration::from_secs(1));
        assert_eq!(left, Duration::from_secs(4));
        gate.record_failure(t0 + Duration::from_secs(2));
        // The next real attempt is the second step of the schedule, not the fourth.
        assert_eq!(
            gate.record_failure(t0 + INITIAL_DELAY),
            Duration::from_secs(10)
        );
    }

    #[test]
    fn success_resets_schedule() {
        let gate = AuthBackoff::default();
        let mut now = Instant::now();
        for _ in 0..3 {
            now += gate.record_failure(now);
        }
        gate.record_success();
        assert_eq!(gate.remaining(now), None);
        assert_eq!(gate.record_failure(now), INITIAL_DELAY);
    }

    #[test]
    fn huge_failure_counts_do_not_overflow() {
        assert_eq!(delay_for(u32::MAX), MAX_DELAY);
        assert_eq!(delay_for(40), MAX_DELAY);
    }
}
