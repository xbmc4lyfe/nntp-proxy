//! Deciding, from probe evidence, which servers share one article store.
//!
//! Article numbers are the strong evidence. Each backbone numbers the
//! articles in a group itself, so servers reading one store report the same
//! highest number for a busy group (give or take the posts that arrived
//! between the two queries), while different backbones are millions or
//! billions apart. The lowest number shows how far back a server keeps
//! articles: two servers can share numbering but not retention, and then a
//! `430` from the one that keeps less says nothing about the other, so they
//! are kept apart.
//!
//! Brand hints and software wording only name or support a grouping; they
//! never form one.

use super::probe::{Fingerprint, Watermarks};
use super::registry::Registry;

/// Highest numbers this close (relative to their size) are one store.
const HIGH_TOLERANCE_PER_MILLION: u64 = 10;
/// Absolute slack for the posts that arrive between two queries.
const HIGH_SLACK: u64 = 100_000;
/// Lowest numbers this close (relative to the range held) keep the same
/// history.
const LOW_TOLERANCE_PERCENT: u64 = 1;

/// One configured server as detection sees it.
#[derive(Clone, Debug)]
pub(crate) struct ServerInput<'a> {
    pub(crate) name: &'a str,
    pub(crate) host: &'a str,
    pub(crate) configured: Configured,
    pub(crate) fingerprint: Option<&'a Fingerprint>,
}

/// What the operator wrote in `backbone`.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Configured {
    /// Left out, or `"auto"`.
    Auto,
    /// A backbone name, lowercased.
    Named(String),
}

/// Where a server's `430`s go.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Assignment {
    /// Shared with every server on this named backbone.
    Backbone(String),
    /// Shared with the unnamed group whose first server (in config order)
    /// is at this index; recorded under that server's host.
    SameAs(usize),
    /// Kept to this server's own host.
    Own,
}

/// Why a server got its assignment, for logs and config comments.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) enum Finding {
    /// The configured backbone agrees with the evidence.
    Confirmed,
    /// The configured backbone has nothing to compare against.
    Unchecked,
    /// The configured backbone contradicts the evidence.
    Misconfigured { configured: String, against: String },
    /// No backbone configured; the evidence placed it.
    Detected,
    /// No backbone configured and no other server shares its store.
    Alone,
    /// The server gave no article numbers, so it could not be checked.
    NoNumbers { reason: String },
}

/// The decision for one server.
#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct Verdict {
    pub(crate) assignment: Assignment,
    pub(crate) finding: Finding,
    /// Other servers found to share its store.
    pub(crate) peers: Vec<String>,
    /// Servers with the same numbering but different retention.
    pub(crate) retention_differs: Vec<String>,
    pub(crate) checks_met: u32,
    pub(crate) checks_total: u32,
}

#[derive(Clone, Copy, Debug, Eq, PartialEq)]
enum Relation {
    /// Same numbering and retention in every group both answered.
    Same,
    /// Same numbering, different retention.
    SameNumberingOtherRetention,
    /// Numbering differs in at least one group.
    Different,
    /// No group in common to compare.
    Unknown,
}

#[derive(Clone, Copy, Debug, Default)]
struct PairChecks {
    met: u32,
    total: u32,
}

fn compare(a: &Fingerprint, b: &Fingerprint) -> (Relation, PairChecks) {
    let mut checks = PairChecks::default();
    let mut compared = 0;
    let mut numbering_differs = false;
    let mut retention_differs = false;
    for (group, left) in &a.watermarks {
        let Some(right) = b.watermarks.get(group) else {
            continue;
        };
        compared += 1;
        checks.total += 2;
        if same_numbering(*left, *right) {
            checks.met += 1;
        } else {
            numbering_differs = true;
        }
        if same_retention(*left, *right) {
            checks.met += 1;
        } else {
            retention_differs = true;
        }
    }
    if let (Some(left), Some(right)) = (a.software, b.software) {
        checks.total += 1;
        if left == right {
            checks.met += 1;
        }
    }
    let relation = match (compared, numbering_differs, retention_differs) {
        (0, _, _) => Relation::Unknown,
        (_, true, _) => Relation::Different,
        (_, false, true) => Relation::SameNumberingOtherRetention,
        (_, false, false) => Relation::Same,
    };
    (relation, checks)
}

fn same_numbering(a: Watermarks, b: Watermarks) -> bool {
    let largest = a.high.max(b.high);
    let tolerance = (largest / 1_000_000 * HIGH_TOLERANCE_PER_MILLION).max(HIGH_SLACK);
    a.high.abs_diff(b.high) <= tolerance
}

fn same_retention(a: Watermarks, b: Watermarks) -> bool {
    let range = a
        .high
        .saturating_sub(a.low)
        .max(b.high.saturating_sub(b.low));
    let tolerance = (range / 100 * LOW_TOLERANCE_PERCENT).max(HIGH_SLACK);
    a.low.abs_diff(b.low) <= tolerance
}

fn has_numbers(server: &ServerInput<'_>) -> bool {
    server
        .fingerprint
        .is_some_and(|fingerprint| !fingerprint.watermarks.is_empty())
}

/// Decide every server's assignment from the evidence.
pub(crate) fn decide(servers: &[ServerInput<'_>], registry: &Registry) -> Vec<Verdict> {
    let count = servers.len();
    let mut relations = vec![vec![(Relation::Unknown, PairChecks::default()); count]; count];
    for a in 0..count {
        for b in 0..count {
            if a == b {
                continue;
            }
            if let (Some(left), Some(right)) = (servers[a].fingerprint, servers[b].fingerprint) {
                relations[a][b] = compare(left, right);
            }
        }
    }
    let relation = |a: usize, b: usize| relations[a][b].0;

    // Group servers whose numbers say they read one store.
    let mut group_of: Vec<usize> = (0..count).collect();
    fn root(group_of: &mut [usize], mut index: usize) -> usize {
        while group_of[index] != index {
            group_of[index] = group_of[group_of[index]];
            index = group_of[index];
        }
        index
    }
    for a in 0..count {
        for b in (a + 1)..count {
            if relation(a, b) == Relation::Same {
                let (ra, rb) = (root(&mut group_of, a), root(&mut group_of, b));
                group_of[ra.max(rb)] = ra.min(rb);
            }
        }
    }
    let groups: Vec<usize> = (0..count).map(|index| root(&mut group_of, index)).collect();
    let groups = &groups;
    let members = move |group: usize| (0..count).filter(move |&index| groups[index] == group);
    let configured_as = move |index: usize, name: &str| matches!(&servers[index].configured, Configured::Named(n) if n == name);

    // A configured name belongs to the evidence group holding most of the
    // servers configured with it (ties: the group where it is the larger
    // share of configured names, then config order). A server configured
    // with it in another group is contradicted. Servers without numbers
    // cannot be placed and are never contradicted.
    let mut owner_of_name: Vec<(String, usize)> = Vec::new();
    for index in 0..count {
        let Configured::Named(name) = &servers[index].configured else {
            continue;
        };
        if owner_of_name.iter().any(|(owned, _)| owned == name) {
            continue;
        }
        let score = move |group: usize| {
            let named = members(group)
                .filter(|&m| has_numbers(&servers[m]))
                .filter(|&m| matches!(servers[m].configured, Configured::Named(_)))
                .count();
            let with_name = members(group)
                .filter(|&m| has_numbers(&servers[m]) && configured_as(m, name))
                .count();
            // Compare shares as fractions without floats: with_name/named.
            (with_name, with_name * 1000 / named.max(1))
        };
        let best = (0..count)
            .filter(|&group| groups[group] == group && score(group).0 > 0)
            .min_by(|&a, &b| score(b).cmp(&score(a)).then(a.cmp(&b)));
        if let Some(group) = best {
            owner_of_name.push((name.clone(), group));
        }
    }
    let contradicted: Vec<Option<usize>> = (0..count)
        .map(|index| {
            let Configured::Named(name) = &servers[index].configured else {
                return None;
            };
            if !has_numbers(&servers[index]) {
                return None;
            }
            let (_, owner) = owner_of_name.iter().find(|(owned, _)| owned == name)?;
            (groups[index] != *owner).then(|| {
                members(*owner)
                    .find(|&m| configured_as(m, name))
                    .unwrap_or(*owner)
            })
        })
        .collect();

    // Name each group: the configured name it owns, else a brand hint no
    // other group already carries, else nothing.
    let mut names: Vec<Option<String>> = vec![None; count];
    for (name, group) in &owner_of_name {
        if names[*group].is_none() {
            names[*group] = Some(name.clone());
        }
    }
    for group in (0..count).filter(|&index| groups[index] == index) {
        if names[group].is_some() {
            continue;
        }
        let mut hints = members(group)
            .filter(|&member| has_numbers(&servers[member]))
            .map(|member| {
                let server = &servers[member];
                let greeting = server.fingerprint.and_then(|f| f.greeting.as_deref());
                registry.hint(std::iter::once(server.host).chain(greeting))
            })
            .collect::<Vec<_>>()
            .into_iter();
        let Some(Some(first)) = hints.next() else {
            continue;
        };
        let agreed = hints.all(|hint| hint.is_none_or(|hint| hint == first));
        let taken = names.iter().flatten().any(|name| name == first)
            || owner_of_name.iter().any(|(name, _)| name == first);
        if agreed && !taken {
            names[group] = Some(first.to_owned());
        }
    }

    (0..count)
        .map(|index| {
            let server = &servers[index];
            let group = groups[index];
            let peers: Vec<usize> = members(group).filter(|&m| m != index).collect();
            let retention_differs: Vec<String> = (0..count)
                .filter(|&other| relation(index, other) == Relation::SameNumberingOtherRetention)
                .map(|other| servers[other].name.to_owned())
                .collect();
            let mut checks = PairChecks::default();
            for &peer in &peers {
                checks.met += relations[index][peer].1.met;
                checks.total += relations[index][peer].1.total;
            }
            let group_assignment = match &names[group] {
                Some(name) => Assignment::Backbone(name.clone()),
                None if !peers.is_empty() => Assignment::SameAs(group),
                None => Assignment::Own,
            };
            let configured_name = match &server.configured {
                Configured::Named(name) => Some(name.clone()),
                Configured::Auto => None,
            };

            let (assignment, finding) = if !has_numbers(server) {
                let reason = server
                    .fingerprint
                    .and_then(|f| f.no_numbers_reason.clone())
                    .unwrap_or_else(|| "the probe did not finish".to_owned());
                let assignment = configured_name.map_or(Assignment::Own, Assignment::Backbone);
                (assignment, Finding::NoNumbers { reason })
            } else if let Some(name) = configured_name {
                if let Some(other) = contradicted[index] {
                    (
                        group_assignment,
                        Finding::Misconfigured {
                            configured: name,
                            against: servers[other].name.to_owned(),
                        },
                    )
                } else if peers.is_empty() || group_assignment == Assignment::Backbone(name.clone())
                {
                    // Nothing contradicts the configured name, and the
                    // evidence either agrees or found no one to compare with.
                    let checked = (0..count).any(|other| {
                        other != index
                            && servers[other].configured == Configured::Named(name.clone())
                            && relation(index, other) == Relation::Same
                    });
                    let finding = if checked {
                        Finding::Confirmed
                    } else {
                        Finding::Unchecked
                    };
                    (Assignment::Backbone(name), finding)
                } else {
                    (
                        group_assignment,
                        Finding::Misconfigured {
                            configured: name,
                            against: servers[peers[0]].name.to_owned(),
                        },
                    )
                }
            } else if peers.is_empty() && !matches!(group_assignment, Assignment::Backbone(_)) {
                (Assignment::Own, Finding::Alone)
            } else {
                (group_assignment, Finding::Detected)
            };

            Verdict {
                assignment,
                finding,
                peers: peers.iter().map(|&m| servers[m].name.to_owned()).collect(),
                retention_differs,
                checks_met: checks.met,
                checks_total: checks.total,
            }
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::collections::BTreeMap;

    const GROUPS: [&str; 3] = ["alt.one", "alt.two", "alt.three"];

    fn registry() -> Registry {
        Registry::parse(
            r#"
            [probe]
            groups = ["alt.one", "alt.two", "alt.three"]
            interval_hours = 6
            [[backbone]]
            name = "alpha"
            brands = ["AlphaNews", "Resold"]
            [[backbone]]
            name = "beta"
            brands = ["BetaNet", "Resold"]
            "#,
        )
        .unwrap()
    }

    /// A store numbered from `base`, holding everything from `low`.
    fn store(base: u64, low: u64, jitter: u64, software: u64) -> Fingerprint {
        let watermarks: BTreeMap<String, Watermarks> = GROUPS
            .iter()
            .enumerate()
            .map(|(index, group)| {
                let high = base * (index as u64 + 1) + jitter;
                ((*group).to_owned(), Watermarks { low, high })
            })
            .collect();
        Fingerprint {
            greeting: None,
            watermarks,
            software: Some(software),
            no_numbers_reason: None,
        }
    }

    fn silent(reason: &str) -> Fingerprint {
        Fingerprint {
            no_numbers_reason: Some(reason.to_owned()),
            ..Fingerprint::default()
        }
    }

    fn input<'a>(name: &'a str, configured: Option<&str>, fp: &'a Fingerprint) -> ServerInput<'a> {
        ServerInput {
            name,
            host: name,
            configured: configured.map_or(Configured::Auto, |n| Configured::Named(n.to_owned())),
            fingerprint: Some(fp),
        }
    }

    const ALPHA: u64 = 166_259_385_392;
    const GAMMA: u64 = 155_671_419_547;

    #[test]
    fn servers_reading_one_store_are_grouped_and_named_from_config() {
        let a = store(ALPHA, 2, 0, 1);
        let b = store(ALPHA, 2, 33, 1);
        let c = store(ALPHA, 2, 616, 2);
        let verdicts = decide(
            &[
                input("one", Some("alpha"), &a),
                input("two", Some("alpha"), &b),
                input("three", None, &c),
            ],
            &registry(),
        );
        assert_eq!(verdicts[0].finding, Finding::Confirmed);
        assert_eq!(verdicts[2].assignment, Assignment::Backbone("alpha".into()));
        assert_eq!(verdicts[2].finding, Finding::Detected);
        assert_eq!(verdicts[2].peers, ["one", "two"]);
        // 2 peers x (3 groups x 2 checks + software), software differs.
        assert_eq!((verdicts[2].checks_met, verdicts[2].checks_total), (12, 14));
    }

    #[test]
    fn different_numbering_is_never_grouped() {
        let a = store(ALPHA, 2, 0, 1);
        let b = store(ALPHA + 67_000_000, 2, 0, 1);
        let verdicts = decide(
            &[input("one", None, &a), input("two", None, &b)],
            &registry(),
        );
        assert_eq!(verdicts[0].finding, Finding::Alone);
        assert_eq!(verdicts[0].assignment, Assignment::Own);
        assert_eq!(verdicts[1].assignment, Assignment::Own);
    }

    #[test]
    fn a_configured_group_with_different_numbering_is_split() {
        let a = store(ALPHA, 2, 0, 1);
        let b = store(GAMMA, 2, 0, 1);
        let verdicts = decide(
            &[
                input("one", Some("alpha"), &a),
                input("two", Some("alpha"), &b),
            ],
            &registry(),
        );
        // A one-against-one split: the first configured server keeps the
        // name, the other is moved out, so neither shares the other's 430s.
        assert_eq!(verdicts[0].assignment, Assignment::Backbone("alpha".into()));
        assert_eq!(verdicts[1].assignment, Assignment::Own);
        assert_eq!(
            verdicts[1].finding,
            Finding::Misconfigured {
                configured: "alpha".into(),
                against: "one".into()
            }
        );
    }

    #[test]
    fn same_numbering_with_less_history_is_kept_apart() {
        let full = store(ALPHA, 2, 0, 1);
        let short = store(ALPHA, 120_000_000_000, 0, 1);
        let verdicts = decide(
            &[
                input("full", Some("alpha"), &full),
                input("short", Some("alpha"), &short),
            ],
            &registry(),
        );
        assert_ne!(verdicts[0].assignment, verdicts[1].assignment);
        assert_eq!(verdicts[1].retention_differs, ["full"]);
        assert!(matches!(verdicts[1].finding, Finding::Misconfigured { .. }));
    }

    #[test]
    fn a_server_without_numbers_keeps_its_configured_backbone() {
        let quiet = silent("LIST ACTIVE answered \"501 command error\"");
        let a = store(ALPHA, 2, 0, 1);
        let verdicts = decide(
            &[
                input("one", Some("alpha"), &a),
                input("quiet", Some("alpha"), &quiet),
            ],
            &registry(),
        );
        assert_eq!(verdicts[1].assignment, Assignment::Backbone("alpha".into()));
        assert!(matches!(verdicts[1].finding, Finding::NoNumbers { .. }));
        assert_eq!(verdicts[0].assignment, Assignment::Backbone("alpha".into()));
        assert_eq!(verdicts[0].finding, Finding::Unchecked);
    }

    #[test]
    fn a_server_without_numbers_and_no_config_stays_on_its_own() {
        let quiet = silent("timed out");
        let verdicts = decide(&[input("quiet", None, &quiet)], &registry());
        assert_eq!(verdicts[0].assignment, Assignment::Own);
    }

    #[test]
    fn an_unnamed_group_shares_under_its_first_member() {
        let a = store(GAMMA, 76_070_143_120, 0, 1);
        let b = store(GAMMA, 76_070_143_120, 11_064, 2);
        let verdicts = decide(
            &[input("one", None, &a), input("two", None, &b)],
            &registry(),
        );
        assert_eq!(verdicts[0].assignment, Assignment::SameAs(0));
        assert_eq!(verdicts[1].assignment, Assignment::SameAs(0));
        assert_eq!(verdicts[1].finding, Finding::Detected);
    }

    #[test]
    fn a_brand_hint_names_a_group_but_never_forms_one() {
        let mut a = store(GAMMA, 2, 0, 1);
        a.greeting = Some("200 news.betanet.example ready".into());
        let b = store(GAMMA, 2, 50, 2);
        let lone = store(ALPHA, 2, 0, 3);
        let mut inputs = vec![input("one", None, &a), input("two", None, &b)];
        inputs.push(ServerInput {
            host: "news.betanet.example",
            ..input("lone", None, &lone)
        });
        let verdicts = decide(&inputs, &registry());
        assert_eq!(verdicts[1].assignment, Assignment::Backbone("beta".into()));
        assert_eq!(
            verdicts[2].assignment,
            Assignment::Own,
            "a hint alone does not join another store's group"
        );
    }

    #[test]
    fn a_wrong_name_on_a_lone_matching_server_moves_it_to_its_real_group() {
        let a = store(ALPHA, 2, 0, 1);
        let b = store(ALPHA, 2, 10, 1);
        let c = store(GAMMA, 2, 0, 1);
        let verdicts = decide(
            &[
                input("one", Some("alpha"), &a),
                input("two", Some("gamma"), &b),
                input("three", Some("gamma"), &c),
            ],
            &registry(),
        );
        assert_eq!(verdicts[1].assignment, Assignment::Backbone("alpha".into()));
        assert_eq!(
            verdicts[1].finding,
            Finding::Misconfigured {
                configured: "gamma".into(),
                against: "three".into()
            }
        );
        assert_eq!(verdicts[2].assignment, Assignment::Backbone("gamma".into()));
    }

    #[test]
    fn two_groups_never_share_one_name() {
        let a = store(ALPHA, 2, 0, 1);
        let b = store(ALPHA, 2, 10, 1);
        let c = store(GAMMA, 2, 0, 1);
        let d = store(GAMMA, 2, 10, 1);
        let verdicts = decide(
            &[
                input("one", Some("alpha"), &a),
                input("two", Some("alpha"), &b),
                input("three", Some("alpha"), &c),
                input("four", None, &d),
            ],
            &registry(),
        );
        assert_eq!(verdicts[0].assignment, Assignment::Backbone("alpha".into()));
        assert_ne!(verdicts[2].assignment, verdicts[0].assignment);
        assert_eq!(verdicts[2].assignment, verdicts[3].assignment);
    }
}
