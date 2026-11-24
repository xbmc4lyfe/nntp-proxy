//! A small reserve of each backend's connections that article downloads may
//! not hold, so STAT probes never queue behind them.
//!
//! Under a full-speed download every connection is checked out for a body,
//! and a STAT probe waited for one: 7 s at p90 on the live proxy while
//! nzbget held every tier-0 connection. The reserve takes no extra logins:
//! it only lowers how many connections downloads may hold at once, so
//! `reserve` of the backend's connections are always left for light work.
//!
//! Sizing follows RustyGet's light pool (#703), scaled down because STATs
//! are short: none for a backend with three connections or fewer (a third of
//! them would be a lasting loss for downloads), one up to eight, two above.
//! Every [`RECOMPUTE`] the size moves by one: it shrinks (down to one on a
//! bigger backend, none on a small one) while downloads waited for a
//! connection and the reserve went unused, and it grows back when light
//! work filled it or downloads left connections free.

use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::time::{Duration, Instant};

/// How often the size is reconsidered.
pub(crate) const RECOMPUTE: Duration = Duration::from_secs(5);
/// Backends with this many connections or fewer keep no reserve.
const TOO_FEW_CONNECTIONS: usize = 3;
/// Backends with this many connections or fewer keep at most one.
const FEW_CONNECTIONS: usize = 8;
/// The most any backend keeps.
const MAX_RESERVE: usize = 2;
/// Longest a waiting download sleeps between re-checks, bounding the cost of
/// a missed wake.
const RECHECK: Duration = Duration::from_millis(50);

/// Whether a checkout is article download traffic or a latency-bound check.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum Work {
    /// Bodies, articles, heads and session traffic; limited to the
    /// backend's connections minus the reserve.
    #[default]
    Download,
    /// STAT probes; may use the reserve.
    Light,
}

/// The largest reserve for a backend allowed `max_connections`.
const fn ceiling(max_connections: usize) -> usize {
    if max_connections <= TOO_FEW_CONNECTIONS {
        0
    } else if max_connections <= FEW_CONNECTIONS {
        1
    } else {
        MAX_RESERVE
    }
}

/// The smallest reserve shrinking may leave.
const fn floor(max_connections: usize) -> usize {
    if max_connections <= FEW_CONNECTIONS {
        0
    } else {
        1
    }
}

/// What one window saw, for [`next_size`].
#[derive(Clone, Copy, Debug)]
struct Window {
    max_connections: usize,
    current: usize,
    /// Most light checkouts held at once.
    light_peak: usize,
    /// A download waited because downloads held their whole share.
    downloads_starved: bool,
}

fn next_size(w: Window) -> usize {
    let ceiling = ceiling(w.max_connections);
    let current = w.current.min(ceiling);
    if w.light_peak >= current.max(1) && current < ceiling {
        return current + 1;
    }
    if w.downloads_starved {
        if w.light_peak < current {
            return current.saturating_sub(1).max(floor(w.max_connections));
        }
        return current;
    }
    (current + 1).min(ceiling)
}

#[derive(Debug)]
struct State {
    window_start: Instant,
    light_peak: usize,
    downloads_starved: bool,
}

/// One backend's reserve, shared by every clone of its provider and every
/// connection checked out from it.
#[derive(Debug)]
pub(crate) struct StatReserve {
    enabled: AtomicBool,
    size: AtomicUsize,
    downloads: AtomicUsize,
    light: AtomicUsize,
    state: Mutex<State>,
    notify: tokio::sync::Notify,
}

impl Default for StatReserve {
    /// Off until the proxy enables it: downloads may then hold every connection.
    fn default() -> Self {
        Self {
            enabled: AtomicBool::new(false),
            size: AtomicUsize::new(0),
            downloads: AtomicUsize::new(0),
            light: AtomicUsize::new(0),
            state: Mutex::new(State {
                window_start: Instant::now(),
                light_peak: 0,
                downloads_starved: false,
            }),
            notify: tokio::sync::Notify::new(),
        }
    }
}

impl StatReserve {
    /// Start reserving, at the full size for `max_connections`.
    pub(crate) fn enable(&self, max_connections: usize) {
        self.size.store(ceiling(max_connections), Ordering::SeqCst);
        self.enabled.store(true, Ordering::SeqCst);
    }

    pub(crate) fn is_enabled(&self) -> bool {
        self.enabled.load(Ordering::Relaxed)
    }

    /// Current reserve size.
    pub(crate) fn size(&self) -> usize {
        self.size.load(Ordering::Relaxed)
    }

    /// Take a light permit; never waits (light work may always use the
    /// reserve and any connection downloads left free).
    pub(crate) fn light(self: &Arc<Self>, max_connections: usize) -> ReservePermit {
        let held = self.light.fetch_add(1, Ordering::SeqCst) + 1;
        if self.is_enabled() {
            let mut state = self.state();
            state.light_peak = state.light_peak.max(held);
            drop(state);
            self.maybe_recompute(max_connections);
        }
        ReservePermit {
            reserve: Arc::clone(self),
            work: Work::Light,
        }
    }

    /// Take a download permit if downloads hold less than their share.
    pub(crate) fn try_download(self: &Arc<Self>, max_connections: usize) -> Option<ReservePermit> {
        if !self.is_enabled() {
            self.downloads.fetch_add(1, Ordering::SeqCst);
            return Some(ReservePermit {
                reserve: Arc::clone(self),
                work: Work::Download,
            });
        }
        self.maybe_recompute(max_connections);
        let limit = max_connections.saturating_sub(self.size()).max(1);
        let taken = self
            .downloads
            .try_update(Ordering::SeqCst, Ordering::SeqCst, |held| {
                (held < limit).then_some(held + 1)
            })
            .is_ok();
        if taken {
            Some(ReservePermit {
                reserve: Arc::clone(self),
                work: Work::Download,
            })
        } else {
            self.state().downloads_starved = true;
            None
        }
    }

    /// Wait at most `wait` for a download permit.
    pub(crate) async fn download(
        self: &Arc<Self>,
        max_connections: impl Fn() -> usize,
        wait: Duration,
    ) -> Option<ReservePermit> {
        let deadline = tokio::time::Instant::now() + wait;
        loop {
            let notified = self.notify.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(permit) = self.try_download(max_connections()) {
                return Some(permit);
            }
            let now = tokio::time::Instant::now();
            if now >= deadline {
                return None;
            }
            let _ = tokio::time::timeout((deadline - now).min(RECHECK), notified).await;
        }
    }

    fn state(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }

    fn maybe_recompute(&self, max_connections: usize) {
        let Ok(mut state) = self.state.try_lock() else {
            return;
        };
        let now = Instant::now();
        if now.duration_since(state.window_start) < RECOMPUTE {
            return;
        }
        let current = self.size();
        let next = next_size(Window {
            max_connections,
            current,
            light_peak: state.light_peak,
            downloads_starved: state.downloads_starved,
        });
        if next != current {
            self.size.store(next, Ordering::SeqCst);
            tracing::debug!(from = current, to = next, "STAT reserve resized");
            if next < current {
                self.notify.notify_waiters();
            }
        }
        state.window_start = now;
        state.light_peak = self.light.load(Ordering::SeqCst);
        state.downloads_starved = false;
    }
}

/// A checked-out connection's claim on its backend's reserve accounting.
#[derive(Debug)]
pub(crate) struct ReservePermit {
    reserve: Arc<StatReserve>,
    work: Work,
}

impl Drop for ReservePermit {
    fn drop(&mut self) {
        match self.work {
            Work::Download => {
                self.reserve.downloads.fetch_sub(1, Ordering::SeqCst);
                self.reserve.notify.notify_waiters();
            }
            Work::Light => {
                self.reserve.light.fetch_sub(1, Ordering::SeqCst);
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn window(max: usize, current: usize, light_peak: usize, starved: bool) -> Window {
        Window {
            max_connections: max,
            current,
            light_peak,
            downloads_starved: starved,
        }
    }

    #[test]
    fn small_backends_keep_no_reserve_and_mid_ones_at_most_one() {
        assert_eq!(ceiling(2), 0);
        assert_eq!(ceiling(3), 0);
        assert_eq!(ceiling(4), 1);
        assert_eq!(ceiling(8), 1);
        assert_eq!(ceiling(9), 2);
        assert_eq!(ceiling(75), 2);
        assert_eq!(next_size(window(3, 0, 5, false)), 0);
    }

    #[test]
    fn starved_downloads_shrink_an_unused_reserve_to_the_floor() {
        assert_eq!(next_size(window(50, 2, 0, true)), 1);
        assert_eq!(
            next_size(window(50, 1, 0, true)),
            1,
            "a big backend keeps one"
        );
        assert_eq!(
            next_size(window(6, 1, 0, true)),
            0,
            "a small one keeps none"
        );
    }

    #[test]
    fn a_used_reserve_holds_or_grows_even_while_downloads_starve() {
        assert_eq!(next_size(window(50, 1, 1, true)), 2);
        assert_eq!(next_size(window(50, 2, 2, true)), 2);
        assert_eq!(next_size(window(50, 2, 1, true)), 1);
    }

    #[test]
    fn free_connections_let_the_reserve_grow_back() {
        assert_eq!(next_size(window(50, 1, 0, false)), 2);
        assert_eq!(next_size(window(50, 2, 0, false)), 2);
    }

    #[test]
    fn downloads_stop_at_their_share_and_light_work_still_gets_through() {
        let reserve = Arc::new(StatReserve::default());
        reserve.enable(10);
        let held: Vec<_> = (0..8).map(|_| reserve.try_download(10).unwrap()).collect();
        assert!(
            reserve.try_download(10).is_none(),
            "two connections stay reserved"
        );
        let _light = reserve.light(10);
        drop(held);
        assert!(reserve.try_download(10).is_some());
    }

    #[test]
    fn disabled_reserve_never_refuses_downloads() {
        let reserve = Arc::new(StatReserve::default());
        let held: Vec<_> = (0..20).map(|_| reserve.try_download(10)).collect();
        assert!(held.iter().all(Option::is_some));
    }

    #[tokio::test]
    async fn a_waiting_download_takes_a_returned_permit() {
        let reserve = Arc::new(StatReserve::default());
        reserve.enable(4);
        let held: Vec<_> = (0..3).map(|_| reserve.try_download(4).unwrap()).collect();
        let waiter = {
            let reserve = Arc::clone(&reserve);
            tokio::spawn(async move {
                reserve
                    .download(|| 4, Duration::from_secs(2))
                    .await
                    .is_some()
            })
        };
        tokio::time::sleep(Duration::from_millis(20)).await;
        drop(held);
        assert!(waiter.await.unwrap());
    }
}
