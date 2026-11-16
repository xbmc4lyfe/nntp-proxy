//! A small sample of recent `430`s from backbones shared by several backends.
//!
//! Servers grouped on one backbone skip each other after a `430`, so normal
//! traffic never shows whether a sibling would have had the article. Backbone
//! detection later asks every server of the group about these sampled
//! articles; a server that has one its sibling lacked proves they do not
//! serve the same articles (a different store, or different takedowns).

use super::availability_identity::{AvailabilitySlot, BackendSlots};
use std::collections::VecDeque;
use std::sync::Arc;
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};

/// Keep one in this many shared-backbone misses, so the lock stays cold.
const SAMPLE_EVERY: u64 = 32;
/// Most samples held; older ones are dropped.
const CAPACITY: usize = 256;

/// Recent sampled misses: message-ID (with brackets) and the slot that
/// recorded it.
#[derive(Debug)]
pub(crate) struct MissSampler {
    slots: Arc<BackendSlots>,
    seen: AtomicU64,
    samples: Mutex<VecDeque<(Box<str>, AvailabilitySlot)>>,
}

impl MissSampler {
    pub(crate) fn new(slots: Arc<BackendSlots>) -> Self {
        Self {
            slots,
            seen: AtomicU64::new(0),
            samples: Mutex::new(VecDeque::with_capacity(CAPACITY)),
        }
    }

    /// Consider a `430` recorded under `slot` for sampling.
    pub(crate) fn note(&self, message_id: &str, slot: AvailabilitySlot) {
        if !self.slots.is_shared(slot) {
            return;
        }
        if !self
            .seen
            .fetch_add(1, Ordering::Relaxed)
            .is_multiple_of(SAMPLE_EVERY)
        {
            return;
        }
        if let Ok(mut samples) = self.samples.lock() {
            if samples.len() == CAPACITY {
                samples.pop_front();
            }
            samples.push_back((message_id.into(), slot));
        }
    }

    /// Take up to `count` samples, newest first.
    pub(crate) fn take(&self, count: usize) -> Vec<(Box<str>, AvailabilitySlot)> {
        let Ok(mut samples) = self.samples.lock() else {
            return Vec::new();
        };
        let keep = samples.len().saturating_sub(count);
        samples.split_off(keep).into_iter().rev().collect()
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::types::BackendId;

    #[test]
    fn only_misses_on_a_shared_backbone_are_sampled() {
        let slot = |index| AvailabilitySlot::new(index).unwrap();
        let slots = Arc::new(BackendSlots::new(&[slot(0), slot(0), slot(1)]));
        let sampler = MissSampler::new(Arc::clone(&slots));
        for index in 0..(SAMPLE_EVERY * 4) {
            sampler.note(&format!("<lone-{index}@x>"), slot(1));
        }
        assert!(
            sampler.take(10).is_empty(),
            "a backbone of one is never audited"
        );
        for index in 0..(SAMPLE_EVERY * 4) {
            sampler.note(&format!("<shared-{index}@x>"), slot(0));
        }
        let taken = sampler.take(10);
        assert_eq!(taken.len(), 4);
        assert!(taken.iter().all(|(_, s)| *s == slot(0)));
        assert!(sampler.take(10).is_empty(), "taken samples are gone");

        slots.set(BackendId::from_index(1), slot(1));
        sampler.note("<after-split@x>", slot(0));
        assert!(sampler.take(10).is_empty(), "the slot is no longer shared");
    }
}
