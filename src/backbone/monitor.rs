//! The background task: probe, decide, regroup, log, annotate, audit.

use super::detect::{
    Assignment, Configured, Contradiction, Finding, Rescue, ServerInput, Verdict, decide,
};
use super::probe::{Fingerprint, fingerprint, has_article};
use super::registry::Registry;
use crate::cache::{AvailabilityIdentity, AvailabilityLayout, MissSampler, is_auto_backbone};
use crate::config::{BackboneDetection, Server};
use crate::pool::{BufferPool, DeadpoolConnectionProvider};
use crate::types::BackendId;
use std::collections::BTreeMap;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};
use tracing::{info, warn};

/// Let pools and the first clients settle before the first probe.
const FIRST_PROBE_DELAY: Duration = Duration::from_secs(15);
/// Longer than every probe command's own checkout and reply limits combined,
/// so it only catches a probe that hangs outside them.
const PROBE_LIMIT: Duration = Duration::from_secs(360);
/// How often sampled misses are checked on the rest of their backbone.
const AUDIT_EVERY: Duration = Duration::from_secs(600);
/// Sampled misses checked per audit.
const AUDIT_SAMPLES: usize = 24;
/// Rescues needed before a pair counts as serving different articles...
const MIN_RESCUES: u32 = 3;
/// ...and the share of checked misses they must be, in percent.
const MIN_RESCUE_PERCENT: u32 = 2;

/// Everything the detection task needs from the running proxy.
pub struct Detector {
    pub(crate) servers: Arc<[Server]>,
    pub(crate) providers: Vec<DeadpoolConnectionProvider>,
    pub(crate) buffer_pool: BufferPool,
    pub(crate) layout: AvailabilityLayout,
    pub(crate) mode: BackboneDetection,
    /// The config file to annotate; `None` when the config came from the
    /// environment.
    pub(crate) config_path: Option<PathBuf>,
    pub(crate) registry: &'static Registry,
    pub(crate) sampler: Arc<MissSampler>,
    pub(crate) state: tokio::sync::Mutex<Evidence>,
}

/// What the checks have learned so far.
#[derive(Default)]
pub(crate) struct Evidence {
    /// The latest probe answers, reused when an audit changes the picture.
    fingerprints: Vec<Option<Fingerprint>>,
    /// Per ordered pair (missing, checked): rescues and checks from audits.
    audits: BTreeMap<(usize, usize), (u32, u32)>,
}

impl Evidence {
    /// Pairs whose audits show one has articles the other lacks.
    fn rescues(&self) -> Vec<Rescue> {
        self.audits
            .iter()
            .filter(|(_, (rescues, checks))| {
                *rescues >= MIN_RESCUES && *rescues * 100 >= *checks * MIN_RESCUE_PERCENT
            })
            .map(|(&(missing, has), &(rescues, checks))| Rescue {
                missing,
                has,
                rescues,
                checks,
            })
            .collect()
    }
}

/// Identities of the known backbones, registered at startup so a server can
/// be moved onto one later without a restart.
pub(crate) fn candidate_identities(registry: &Registry) -> Vec<AvailabilityIdentity> {
    registry
        .backbone_names()
        .iter()
        .map(|name| AvailabilityIdentity::backbone(name))
        .collect()
}

/// Probe now and every registry interval, and audit sampled misses every
/// few minutes, until the runtime shuts down.
pub(crate) fn spawn(detector: Detector) {
    if detector.mode == BackboneDetection::Off || detector.servers.len() < 2 {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(FIRST_PROBE_DELAY).await;
        detector.check(true).await;
        let mut last_check = Instant::now();
        loop {
            tokio::time::sleep(AUDIT_EVERY).await;
            detector.audit().await;
            if last_check.elapsed() >= detector.registry.interval() {
                detector.check(false).await;
                last_check = Instant::now();
            }
        }
    });
}

impl Detector {
    /// Run one check now, logging every finding as on the first check.
    /// Does nothing when detection is off.
    pub async fn run(&self) {
        if self.mode != BackboneDetection::Off {
            self.check(true).await;
        }
    }

    /// Check sampled misses on the rest of their backbone now, regrouping if
    /// that shows a pair serving different articles. Does nothing when
    /// detection is off.
    pub async fn audit_now(&self) {
        if self.mode != BackboneDetection::Off {
            self.audit().await;
        }
    }

    /// Probe every server, then decide and apply.
    async fn check(&self, first: bool) {
        let groups = self.registry.probe_groups();
        let fingerprints: Vec<Option<Fingerprint>> =
            futures::future::join_all(self.providers.iter().enumerate().map(
                |(index, provider)| async move {
                    tokio::time::timeout(
                        PROBE_LIMIT,
                        fingerprint(
                            provider,
                            &self.buffer_pool,
                            BackendId::from_index(index),
                            groups,
                        ),
                    )
                    .await
                    .ok()
                },
            ))
            .await;
        let mut state = self.state.lock().await;
        state.fingerprints = fingerprints;
        // Audits below the bar fade, so stray rescues do not add up over
        // days. A pair already split keeps its evidence: apart, they no
        // longer share a slot, so no new audit could ever confirm it again,
        // and decaying it would rejoin them every interval. A restart clears
        // it, and audits then re-establish it if they still differ.
        let split: Vec<(usize, usize)> = state
            .rescues()
            .iter()
            .map(|rescue| (rescue.missing, rescue.has))
            .collect();
        for (pair, counts) in &mut state.audits {
            if !split.contains(pair) {
                *counts = (counts.0 / 2, counts.1 / 2);
            }
        }
        self.apply(&state, first);
    }

    /// Ask every server sharing a sampled miss's backbone whether it has the
    /// article.
    async fn audit(&self) {
        let samples = self.sampler.take(AUDIT_SAMPLES);
        if samples.is_empty() {
            return;
        }
        let slots = self.layout.shared_slots();
        let mut found: Vec<((usize, usize), bool)> = Vec::new();
        for (message_id, slot) in samples {
            let backends = slots.backends_on(slot);
            if backends.len() < 2 {
                continue;
            }
            let answers = futures::future::join_all(backends.iter().map(|backend| {
                has_article(
                    &self.providers[backend.as_index()],
                    &self.buffer_pool,
                    *backend,
                    &message_id,
                )
            }))
            .await;
            for (missing, answer) in backends.iter().zip(&answers) {
                if *answer != Some(false) {
                    continue;
                }
                for (checked, other) in backends.iter().zip(&answers) {
                    if let (true, Some(has)) = (checked != missing, other) {
                        found.push(((missing.as_index(), checked.as_index()), *has));
                    }
                }
            }
        }
        let mut state = self.state.lock().await;
        let before = state.rescues();
        for (pair, has) in found {
            let counts = state.audits.entry(pair).or_default();
            counts.1 += 1;
            if has {
                counts.0 += 1;
            }
        }
        let after = state.rescues();
        let newly: Vec<&Rescue> = after
            .iter()
            .filter(|rescue| {
                !before
                    .iter()
                    .any(|old| (old.missing, old.has) == (rescue.missing, rescue.has))
            })
            .collect();
        if !newly.is_empty() && !state.fingerprints.is_empty() {
            for rescue in newly {
                info!(
                    missing = %self.servers[rescue.missing].name,
                    has = %self.servers[rescue.has].name,
                    rescues = rescue.rescues,
                    checked = rescue.checks,
                    "Backbone audit: a server had articles its backbone sibling answered 430 for; they no longer share 430s"
                );
            }
            self.apply(&state, false);
        }
    }

    /// Decide from the evidence and regroup, log and annotate.
    fn apply(&self, state: &Evidence, first: bool) {
        let rescues = state.rescues();
        let inputs: Vec<ServerInput<'_>> = self
            .servers
            .iter()
            .enumerate()
            .map(|(index, server)| ServerInput {
                name: server.name.as_str(),
                host: server.host.as_str(),
                configured: configured(server),
                fingerprint: state.fingerprints.get(index).and_then(Option::as_ref),
            })
            .collect();
        let verdicts = decide(&inputs, self.registry, &rescues);

        let mut notes = Vec::with_capacity(verdicts.len());
        for (index, verdict) in verdicts.iter().enumerate() {
            let server = &self.servers[index];
            let backend = BackendId::from_index(index);
            let identity = self.identity(index, &verdict.assignment);
            let before = self.layout.identity_for_backend(backend).clone();
            let note = describe(&self.servers, index, verdict);
            if before != identity {
                if self.layout.reassign(backend, &identity) {
                    info!(
                        server = %server.name,
                        from = before.as_host(),
                        to = identity.as_host(),
                        "Backbone check regrouped a server: {note}"
                    );
                } else {
                    warn!(
                        server = %server.name,
                        to = identity.as_host(),
                        "Backbone check wants to regroup a server onto a backbone this process did not register; restart to apply: {note}"
                    );
                }
            } else if first {
                info!(server = %server.name, shares_as = identity.as_host(), "Backbone check: {note}");
            }
            notes.push((server.name.as_str().to_owned(), note));
        }

        if self.mode == BackboneDetection::Annotate
            && let Some(path) = &self.config_path
        {
            self.annotate(path, &notes);
        }
    }

    fn identity(&self, index: usize, assignment: &Assignment) -> AvailabilityIdentity {
        match assignment {
            Assignment::Backbone(name) => AvailabilityIdentity::backbone(name),
            Assignment::SameAs(first) => AvailabilityIdentity::from(&self.servers[*first].host),
            Assignment::Own => AvailabilityIdentity::from(&self.servers[index].host),
        }
    }

    fn annotate(&self, path: &std::path::Path, notes: &[(String, String)]) {
        static UNWRITABLE_REPORTED: AtomicBool = AtomicBool::new(false);
        let date = chrono::Utc::now().format("%Y-%m-%d").to_string();
        let text = match std::fs::read_to_string(path) {
            Ok(text) => text,
            Err(error) => {
                warn!(path = %path.display(), %error, "Backbone check could not read the config file to annotate it");
                return;
            }
        };
        let mut updated = text.clone();
        for (server, note) in notes {
            if let Some(next) = super::annotate::annotate(&updated, server, note, &date) {
                updated = next;
            }
        }
        if updated == text {
            return;
        }
        // Skip this round if the operator saved the file while we worked;
        // the next check annotates their version.
        if std::fs::read_to_string(path).ok().as_deref() != Some(text.as_str()) {
            return;
        }
        // A copy of the file as it was before the first note, kept once.
        // A copy of the file as it was before the first note, kept once,
        // named after the whole file name so `a.conf` and `a.toml` differ.
        let backup = {
            let mut name = path.as_os_str().to_owned();
            name.push(".before-backbone-check");
            PathBuf::from(name)
        };
        if !backup.exists() {
            let _ = std::fs::copy(path, &backup);
        }
        // Written in place, not renamed over: a config bind-mounted into a
        // container is a single file whose inode must stay.
        if let Err(error) = std::fs::write(path, &updated) {
            // A refused write (read-only file) leaves the original; a
            // partial one is put back.
            let intact = std::fs::read_to_string(path).ok().as_deref() == Some(text.as_str())
                || std::fs::write(path, &text).is_ok();
            if !intact {
                warn!(path = %path.display(), backup = %backup.display(), %error, "Backbone check could not write the config file or put the original back; restore it from the backup");
            } else if !UNWRITABLE_REPORTED.swap(true, Ordering::Relaxed) {
                info!(
                    path = %path.display(),
                    %error,
                    "Backbone check cannot write to the config file; the findings above are in the log instead. Set routing.backbone_detection = \"apply\" to stop trying"
                );
            }
            return;
        }
        // Read back: a short write is undone with the original, but text
        // that is neither ours nor the original is an operator's save that
        // landed meanwhile, and is left alone.
        match std::fs::read_to_string(path) {
            Ok(now) if now == updated => {
                info!(path = %path.display(), "Backbone check wrote its findings into the config file as comments");
            }
            Ok(now) if now != text && updated.strip_prefix(now.as_str()).is_none() => {}
            _ => {
                if std::fs::write(path, &text).is_err() {
                    warn!(path = %path.display(), backup = %backup.display(), "Backbone check could not verify its write to the config file or put the original back; restore it from the backup");
                }
            }
        }
    }
}

fn configured(server: &Server) -> Configured {
    match server.backbone.as_deref().map(str::trim) {
        Some(name) if !name.is_empty() && !is_auto_backbone(name) => {
            Configured::Named(name.to_ascii_lowercase())
        }
        _ => Configured::Auto,
    }
}

/// One sentence or two an operator can act on, for the log and the config.
pub(crate) fn describe(servers: &[Server], index: usize, verdict: &Verdict) -> String {
    let placed = match &verdict.assignment {
        Assignment::Backbone(name) => format!("backbone \"{name}\""),
        Assignment::SameAs(first) if *first == index => {
            "an unnamed backbone shared with the servers below".to_owned()
        }
        Assignment::SameAs(first) => format!("the same backbone as {}", servers[*first].name),
        Assignment::Own => "a backbone of its own here".to_owned(),
    };
    let checks = if verdict.checks_total > 0 {
        format!(
            " (met {} of {} checks)",
            verdict.checks_met, verdict.checks_total
        )
    } else {
        String::new()
    };
    let matches = if verdict.peers.is_empty() {
        String::new()
    } else {
        format!(
            ": its article numbers and retention match {}",
            verdict.peers.join(", ")
        )
    };
    let retention = if verdict.retention_differs.is_empty() {
        String::new()
    } else {
        format!(
            ". {} {} the same numbering but keep a different amount of history, so their 430s are kept apart",
            verdict.retention_differs.join(", "),
            if verdict.retention_differs.len() == 1 {
                "has"
            } else {
                "have"
            }
        )
    };
    let mut note = match &verdict.finding {
        Finding::Confirmed => format!("confirmed {placed}{matches}{checks}"),
        Finding::Unchecked => format!(
            "kept {placed} as configured: no other server on it gave article numbers to compare"
        ),
        Finding::Misconfigured {
            configured,
            against,
            why,
        } => {
            let differs = match why {
                Contradiction::Numbers => "their article numbers differ".to_owned(),
                Contradiction::Retention => "they keep different amounts of history".to_owned(),
                Contradiction::Articles { rescues, checks } => format!(
                    "audits found {rescues} of {checks} checked articles on one that the other answered 430 for"
                ),
            };
            format!(
                "config says \"{configured}\", the same as {against}, but {differs}; we think it is on {placed}{matches}{checks}. Its 430s follow the evidence until that changes"
            )
        }
        Finding::SameStoreOtherName { other } => format!(
            "kept {placed} as configured, though its article numbers and retention match {}, which are on \"{other}\"{checks}. Give them the same backbone to share their 430s",
            verdict.peers.join(", ")
        ),
        Finding::Detected => format!("detected {placed}{matches}{checks}"),
        Finding::Alone => {
            "no other server here shares its article numbers, so its 430s are not shared".to_owned()
        }
        Finding::NoNumbers { reason } => {
            let kept = match &verdict.assignment {
                Assignment::Backbone(name) => format!("; keeping \"{name}\" as configured"),
                _ => String::new(),
            };
            format!("could not check: {reason}{kept}")
        }
    };
    note.push_str(&retention);
    note
}
