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

/// How long callers that lose the race for the probe are told to wait.
pub(crate) const PROBE_RETRY: Duration = Duration::from_secs(1);

/// What a caller may do right now.
#[derive(Debug, PartialEq, Eq)]
pub(crate) enum Admission {
    /// No refusal is on record: log in normally.
    Open,
    /// The backoff window has closed: this caller alone probes with a real login.
    Probe,
    /// Do not touch the network; try again after this long.
    Wait(Duration),
}

#[derive(Debug, Default)]
struct State {
    /// Consecutive refused attempts, counted once per backoff window.
    failures: u32,
    /// No login may be attempted before this instant.
    retry_not_before: Option<Instant>,
    /// A probe login is in flight, so everyone else keeps waiting.
    probing: bool,
}

/// Shared by every pool slot of one backend.
#[derive(Debug, Default)]
pub(crate) struct AuthBackoff {
    state: Mutex<State>,
}

impl AuthBackoff {
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    /// Decide whether a login may be attempted at `now`.
    ///
    /// While refusals are on record, at most one caller at a time is admitted
    /// as the probe once the window closes. Without that, every pool slot waiting
    /// on the backend would log in together at each window boundary and bring
    /// the failed-login storm straight back.
    pub(crate) fn admit(&self, now: Instant) -> Admission {
        let mut state = self.lock();
        if let Some(left) = state
            .retry_not_before
            .and_then(|deadline| deadline.checked_duration_since(now))
            .filter(|left| !left.is_zero())
        {
            return Admission::Wait(left);
        }
        if state.failures == 0 {
            return Admission::Open;
        }
        if state.probing {
            return Admission::Wait(PROBE_RETRY);
        }
        state.probing = true;
        Admission::Probe
    }

    /// Record a refused login and return the wait that now applies.
    ///
    /// Attempts that were already in flight when the window opened fail
    /// together; only the first of them extends the delay, so a pool of N
    /// concurrent creators does not jump N steps up the schedule.
    pub(crate) fn record_failure(&self, now: Instant) -> Duration {
        let mut state = self.lock();
        state.probing = false;
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
        *self.lock() = State::default();
    }

    /// The probe ended without an answer (network error or cancelled), so let
    /// the next caller probe instead of leaving the gate shut.
    pub(crate) fn abandon_probe(&self) {
        self.lock().probing = false;
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

    fn remaining(gate: &AuthBackoff, now: Instant) -> Option<Duration> {
        match gate.admit(now) {
            Admission::Wait(left) => Some(left),
            Admission::Open => None,
            Admission::Probe => {
                gate.abandon_probe();
                None
            }
        }
    }

    #[test]
    fn open_until_first_failure() {
        let gate = AuthBackoff::default();
        assert_eq!(gate.admit(Instant::now()), Admission::Open);
    }

    #[test]
    fn failure_blocks_until_delay_elapses() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        assert_eq!(gate.record_failure(t0), INITIAL_DELAY);
        assert_eq!(remaining(&gate, t0), Some(INITIAL_DELAY));
        assert_eq!(
            remaining(&gate, t0 + Duration::from_secs(2)),
            Some(Duration::from_secs(3))
        );
        assert_eq!(remaining(&gate, t0 + INITIAL_DELAY), None);
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
        assert_eq!(remaining(&gate, now), None);
        assert_eq!(gate.record_failure(now), INITIAL_DELAY);
    }

    #[test]
    fn huge_failure_counts_do_not_overflow() {
        assert_eq!(delay_for(u32::MAX), MAX_DELAY);
        assert_eq!(delay_for(40), MAX_DELAY);
    }

    #[test]
    fn many_callers_are_admitted_before_any_refusal() {
        let gate = AuthBackoff::default();
        let now = Instant::now();
        for _ in 0..50 {
            assert_eq!(gate.admit(now), Admission::Open);
        }
    }

    #[test]
    fn only_one_probe_after_window_closes() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        gate.record_failure(t0);
        let later = t0 + INITIAL_DELAY;
        assert_eq!(gate.admit(later), Admission::Probe);
        for _ in 0..10 {
            assert_eq!(gate.admit(later), Admission::Wait(PROBE_RETRY));
        }
    }

    #[test]
    fn failed_probe_reopens_window_with_longer_delay() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        gate.record_failure(t0);
        let later = t0 + INITIAL_DELAY;
        assert_eq!(gate.admit(later), Admission::Probe);
        assert_eq!(gate.record_failure(later), Duration::from_secs(10));
        assert_eq!(gate.admit(later), Admission::Wait(Duration::from_secs(10)));
    }

    #[test]
    fn successful_probe_reopens_gate_for_everyone() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        gate.record_failure(t0);
        let later = t0 + INITIAL_DELAY;
        assert_eq!(gate.admit(later), Admission::Probe);
        gate.record_success();
        assert_eq!(gate.admit(later), Admission::Open);
        assert_eq!(gate.admit(later), Admission::Open);
    }

    #[test]
    fn abandoned_probe_lets_the_next_caller_probe() {
        let gate = AuthBackoff::default();
        let t0 = Instant::now();
        gate.record_failure(t0);
        let later = t0 + INITIAL_DELAY;
        assert_eq!(gate.admit(later), Admission::Probe);
        gate.abandon_probe();
        assert_eq!(gate.admit(later), Admission::Probe);
    }
}
