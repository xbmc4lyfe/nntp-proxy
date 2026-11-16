//! The background task: probe, decide, regroup, log, annotate.

use super::detect::{Assignment, Configured, Finding, ServerInput, Verdict, decide};
use super::probe::{Fingerprint, fingerprint};
use super::registry::Registry;
use crate::cache::{AvailabilityIdentity, AvailabilityLayout, is_auto_backbone};
use crate::config::{BackboneDetection, Server};
use crate::pool::{BufferPool, DeadpoolConnectionProvider};
use crate::types::BackendId;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;
use tracing::{info, warn};

/// Let pools and the first clients settle before the first probe.
const FIRST_PROBE_DELAY: Duration = Duration::from_secs(15);
/// Longest one server's probe may take before it counts as unanswered.
const PROBE_LIMIT: Duration = Duration::from_secs(120);

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

/// Run detection now and then every registry interval, until the runtime
/// shuts down.
pub(crate) fn spawn(detector: Detector) {
    if detector.mode == BackboneDetection::Off || detector.servers.len() < 2 {
        return;
    }
    tokio::spawn(async move {
        tokio::time::sleep(FIRST_PROBE_DELAY).await;
        let mut first = true;
        loop {
            detector.run_once(first).await;
            first = false;
            tokio::time::sleep(detector.registry.interval()).await;
        }
    });
}

impl Detector {
    /// Run one check now, logging every finding as on the first check.
    /// Does nothing when detection is off.
    pub async fn run(&self) {
        if self.mode != BackboneDetection::Off {
            self.run_once(true).await;
        }
    }

    async fn run_once(&self, first: bool) {
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
        let inputs: Vec<ServerInput<'_>> = self
            .servers
            .iter()
            .zip(&fingerprints)
            .map(|(server, fingerprint)| ServerInput {
                name: server.name.as_str(),
                host: server.host.as_str(),
                configured: configured(server),
                fingerprint: fingerprint.as_ref(),
            })
            .collect();
        let verdicts = decide(&inputs, self.registry);

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
        // Written in place, not renamed over: a config bind-mounted into a
        // container is a single file whose inode must stay.
        match std::fs::write(path, &updated) {
            Ok(()) => {
                info!(path = %path.display(), "Backbone check wrote its findings into the config file as comments")
            }
            Err(error) => {
                if !UNWRITABLE_REPORTED.swap(true, Ordering::Relaxed) {
                    info!(
                        path = %path.display(),
                        %error,
                        "Backbone check cannot write to the config file; the findings above are in the log instead. Set routing.backbone_detection = \"apply\" to stop trying"
                    );
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
        } => format!(
            "config says \"{configured}\", but its article numbers differ from {against}'s, which is also set to \"{configured}\"; we think it is on {placed}{matches}{checks}. Its 430s follow the evidence until that changes"
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
