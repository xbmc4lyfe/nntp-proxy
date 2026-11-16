//! Backbone detection against hostile or broken servers.
//!
//! Each test stands up mock NNTP servers that misbehave in one way while
//! answering the backbone probe (`LIST ACTIVE <group>`, `HELP`,
//! `STAT <missing>`), runs one or more checks, and then checks three things:
//! nothing panics or hangs (every test is bounded by a timeout), routing still
//! asks a server that holds an article unless the evidence really grouped it
//! with the one that said `430`, and the operator's config file still parses
//! with every line the check does not own left byte for byte.
//!
//! The usual pair is a hostile server `H` (tier 0, holds nothing) and a
//! sibling `S` (tier 1, holds everything). Ungrouped, every client `ARTICLE`
//! is a `430` from `H` rescued by `S` (`220`); grouped, the `430` from `H`
//! skips `S` and the client sees `430`.

use std::future::Future;
use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU64, AtomicUsize, Ordering};
use std::time::Duration;

use anyhow::{Context, Result, anyhow};
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use nntp_proxy::config::{BackboneDetection, Routing, Server};
use nntp_proxy::types::{MaxConnections, Port};
use nntp_proxy::{BackboneCheck, Config, NntpProxy, RoutingMode};

use crate::test_helpers::{
    connect_and_read_greeting, independent_provider_host, spawn_test_proxy_on_random_port,
};

const STORE_A: u64 = 166_259_385_392;
const STORE_B: u64 = 155_671_419_547;
const PROBE_GROUPS: [&str; 3] = [
    "alt.binaries.boneless",
    "alt.binaries.multimedia",
    "alt.binaries.teevee",
];
const MARKER: &str = "# backbone-check";
/// Bound for tests whose servers answer promptly.
const QUICK: u64 = 60;
/// Bound for tests that wait out the probe's 30 s per-command limit.
const SLOW: u64 = 200;

// ---------------------------------------------------------------------------
// Mock servers
// ---------------------------------------------------------------------------

/// One step of a mock's answer.
#[derive(Clone)]
enum Act {
    Send(Vec<u8>),
    Pause(Duration),
    /// Keep the connection open and never answer again.
    Hang,
    Close,
}

fn send(bytes: impl AsRef<[u8]>) -> Act {
    Act::Send(bytes.as_ref().to_vec())
}

/// What one mock connection has seen.
#[derive(Clone, Default)]
struct Conn {
    user: Option<String>,
    pass: Option<String>,
}

/// Overrides a mock's default answer when it returns `Some`.
type Hook = Arc<dyn Fn(&str, &Conn) -> Option<Vec<Act>> + Send + Sync>;

#[derive(Clone)]
struct Spec {
    name: String,
    tier: u8,
    backbone: Option<String>,
    /// Base of the article numbers reported; shared so a test can move it.
    store: Arc<AtomicU64>,
    has_articles: bool,
    greeting: Vec<Act>,
    hook: Option<Hook>,
    credentials: Option<(String, String)>,
}

impl Spec {
    fn new(name: &str, tier: u8, store: u64, has_articles: bool) -> Self {
        Self {
            name: name.to_owned(),
            tier,
            backbone: None,
            store: Arc::new(AtomicU64::new(store)),
            has_articles,
            greeting: vec![send("200 mock ready\r\n")],
            hook: None,
            credentials: None,
        }
    }

    fn hook(
        mut self,
        hook: impl Fn(&str, &Conn) -> Option<Vec<Act>> + Send + Sync + 'static,
    ) -> Self {
        self.hook = Some(Arc::new(hook));
        self
    }

    fn greeting(mut self, greeting: Vec<Act>) -> Self {
        self.greeting = greeting;
        self
    }

    fn credentials(mut self, user: &str, pass: &str) -> Self {
        self.credentials = Some((user.to_owned(), pass.to_owned()));
        self
    }

    /// Answer `LIST ACTIVE <group>` with `reply(group, store)`.
    fn list(self, reply: impl Fn(&str, u64) -> Vec<Act> + Send + Sync + 'static) -> Self {
        let store = self.store.clone();
        self.hook(move |command, _| {
            command
                .strip_prefix("LIST ACTIVE ")
                .map(|group| reply(group, store.load(Ordering::Relaxed)))
        })
    }
}

fn high(store: u64, group: &str) -> u64 {
    let index = PROBE_GROUPS
        .iter()
        .position(|g| g.eq_ignore_ascii_case(group))
        .unwrap_or(0);
    store * (index as u64 + 1) + 17
}

/// The `LIST ACTIVE` line a well-behaved server on `store` gives.
fn active(store: u64, group: &str) -> String {
    format!("{group} {} 2 y", high(store, group))
}

fn default_reply(command: &str, store: u64, has_articles: bool, lookups: &AtomicUsize) -> Vec<u8> {
    if let Some(group) = command.strip_prefix("LIST ACTIVE ") {
        return format!("215 list follows\r\n{}\r\n.\r\n", active(store, group)).into_bytes();
    }
    if command.starts_with("AUTHINFO USER") {
        return b"381 password please\r\n".to_vec();
    }
    if command.starts_with("AUTHINFO PASS") {
        return b"281 welcome\r\n".to_vec();
    }
    if command == "HELP" {
        return b"100 help\r\n  ARTICLE\r\n  STAT\r\n.\r\n".to_vec();
    }
    if let Some((verb @ ("ARTICLE" | "STAT"), id)) = command.split_once(' ') {
        let probe = id.ends_with(".invalid>");
        if !probe {
            lookups.fetch_add(1, Ordering::Relaxed);
        }
        return match (has_articles && !probe, verb) {
            (true, "STAT") => format!("223 0 {id}\r\n"),
            (true, _) => format!("220 0 {id}\r\nSubject: x\r\n\r\nbody of {id}\r\n.\r\n"),
            (false, _) => "430 no such article\r\n".to_owned(),
        }
        .into_bytes();
    }
    match command {
        "MODE READER" => b"200 reader\r\n".to_vec(),
        "DATE" => b"111 20260101000000\r\n".to_vec(),
        "QUIT" => b"205 bye\r\n".to_vec(),
        _ => b"500 unknown command\r\n".to_vec(),
    }
}

/// Run `acts`; `false` once the connection should end.
async fn perform(stream: &mut tokio::net::TcpStream, acts: &[Act]) -> bool {
    for act in acts {
        match act {
            Act::Send(bytes) => {
                if stream.write_all(bytes).await.is_err() {
                    return false;
                }
            }
            Act::Pause(duration) => tokio::time::sleep(*duration).await,
            Act::Hang => {
                tokio::time::sleep(Duration::from_secs(3600)).await;
                return false;
            }
            Act::Close => return false,
        }
    }
    true
}

async fn spawn_mock(spec: &Spec, lookups: Arc<AtomicUsize>) -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let spec = spec.clone();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let lookups = lookups.clone();
            let spec = spec.clone();
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                if !perform(stream.get_mut(), &spec.greeting).await {
                    return;
                }
                let mut conn = Conn::default();
                let mut raw = Vec::new();
                loop {
                    raw.clear();
                    if stream.read_until(b'\n', &mut raw).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let command = String::from_utf8_lossy(&raw).trim_end().to_owned();
                    if let Some(user) = command.strip_prefix("AUTHINFO USER ") {
                        conn.user = Some(user.to_owned());
                    }
                    if let Some(pass) = command.strip_prefix("AUTHINFO PASS ") {
                        conn.pass = Some(pass.to_owned());
                    }
                    let acts = spec
                        .hook
                        .as_ref()
                        .and_then(|hook| hook(&command, &conn))
                        .unwrap_or_else(|| {
                            vec![Act::Send(default_reply(
                                &command,
                                spec.store.load(Ordering::Relaxed),
                                spec.has_articles,
                                &lookups,
                            ))]
                        });
                    if !perform(stream.get_mut(), &acts).await {
                        return;
                    }
                }
            });
        }
    });
    Ok(port)
}

// ---------------------------------------------------------------------------
// Proxy harness
// ---------------------------------------------------------------------------

struct Running {
    port: u16,
    lookups: Vec<Arc<AtomicUsize>>,
    stores: Vec<Arc<AtomicU64>>,
    check: Arc<BackboneCheck>,
    config_path: PathBuf,
    _dir: tempfile::TempDir,
}

/// The config file the harness writes when a test gives none.
fn default_file(specs: &[Spec]) -> String {
    let mut text = String::from("[proxy]\nport = 8119\n");
    for (index, spec) in specs.iter().enumerate() {
        text.push_str(&format!(
            "\n[[servers]]\nname = \"{}\"\n",
            spec.name.replace('\\', "\\\\").replace('"', "\\\"")
        ));
        if let Some(backbone) = &spec.backbone {
            text.push_str(&format!("backbone = \"{backbone}\"\n"));
        }
        text.push_str(&format!(
            "host = \"{}\"\n",
            independent_provider_host(index)
        ));
    }
    text
}

async fn start(
    specs: &[Spec],
    detection: BackboneDetection,
    file: Option<String>,
) -> Result<Running> {
    let mut servers = Vec::new();
    let mut lookups = Vec::new();
    for (index, spec) in specs.iter().enumerate() {
        let counter = Arc::new(AtomicUsize::new(0));
        let port = spawn_mock(spec, counter.clone()).await?;
        lookups.push(counter);
        let mut server = Server::builder(independent_provider_host(index), Port::try_new(port)?)
            .name(spec.name.clone())
            .tier(spec.tier)
            .max_connections(MaxConnections::try_new(2)?)
            .prewarm_connections(0)
            .compress(Some(false));
        if let Some(backbone) = &spec.backbone {
            server = server.backbone(backbone.clone());
        }
        if let Some((user, pass)) = &spec.credentials {
            server = server.username(user.clone()).password(pass.clone());
        }
        servers.push(server.build()?);
    }
    let dir = tempfile::tempdir()?;
    let config_path = dir.path().join("config.toml");
    std::fs::write(&config_path, file.unwrap_or_else(|| default_file(specs)))?;
    let config = Config {
        servers,
        routing: Routing {
            backbone_detection: detection,
            ..Routing::default()
        },
        ..Config::default()
    };
    let proxy = NntpProxy::builder(config)
        .with_routing_mode(RoutingMode::PerCommand)
        .build()
        .await?;
    let check = Arc::new(proxy.backbone_check(Some(config_path.clone())));
    let port = spawn_test_proxy_on_random_port(proxy, true).await?;
    Ok(Running {
        port,
        lookups,
        stores: specs.iter().map(|spec| spec.store.clone()).collect(),
        check,
        config_path,
        _dir: dir,
    })
}

impl Running {
    fn file(&self) -> String {
        std::fs::read_to_string(&self.config_path).unwrap_or_default()
    }

    fn asked(&self, index: usize) -> usize {
        self.lookups[index].load(Ordering::Relaxed)
    }

    /// Fetch `count` articles, one client each, and say how routing treated
    /// the `430` from the tier-0 server: grouped (sibling skipped) or apart
    /// (sibling asked and answered).
    async fn routing(&self, prefix: &str, count: usize) -> Result<Routed> {
        let before = self.asked(1);
        let codes = article_each(self.port, prefix, count).await?;
        let asked = self.asked(1) - before;
        Ok(if codes.iter().all(|c| c == "430") && asked == 0 {
            Routed::Grouped
        } else if codes.iter().all(|c| c == "220") && asked == count {
            Routed::Apart
        } else {
            Routed::Mixed { codes, asked }
        })
    }
}

#[derive(Debug, PartialEq, Eq)]
enum Routed {
    Grouped,
    Apart,
    Mixed { codes: Vec<String>, asked: usize },
}

async fn article_each(port: u16, prefix: &str, count: usize) -> Result<Vec<String>> {
    let mut codes = Vec::with_capacity(count);
    for index in 0..count {
        let (code, _) = fetch_article(port, &format!("<{prefix}-{index}@example.com>")).await?;
        codes.push(code);
    }
    Ok(codes)
}

/// One client asks for one article; the status code and, for a `220`, the
/// whole reply.
async fn fetch_article(port: u16, id: &str) -> Result<(String, String)> {
    let client = connect_and_read_greeting(port).await?;
    let mut client = BufReader::new(client);
    client
        .get_mut()
        .write_all(format!("ARTICLE {id}\r\n").as_bytes())
        .await?;
    let mut status = String::new();
    client.read_line(&mut status).await?;
    let code: String = status.chars().take(3).collect();
    let mut whole = status.clone();
    if code == "220" {
        loop {
            let mut line = String::new();
            if client.read_line(&mut line).await? == 0 {
                break;
            }
            whole.push_str(&line);
            if line == ".\r\n" {
                break;
            }
        }
    }
    Ok((code, whole))
}

async fn bounded<F: Future<Output = Result<()>>>(secs: u64, test: F) -> Result<()> {
    tokio::time::timeout(Duration::from_secs(secs), test)
        .await
        .map_err(|_| anyhow!("test did not finish within {secs}s (hang)"))?
}

/// The lines the check does not own, byte for byte.
fn foreign_lines(text: &str) -> String {
    text.split_inclusive('\n')
        .filter(|line| !line.trim_start().starts_with(MARKER))
        .collect()
}

fn notes(text: &str) -> Vec<&str> {
    text.lines()
        .filter(|line| line.trim_start().starts_with(MARKER))
        .collect()
}

/// How many servers carry a note: each note's first line is dated.
fn noted_servers(text: &str) -> usize {
    text.lines()
        .filter(|line| line.trim_start().starts_with(&format!("{MARKER} (")))
        .count()
}

/// The file still parses, to the same values, and only marker lines moved.
fn assert_intact(before: &str, after: &str) {
    let parsed_before: toml::Value = toml::from_str(before).expect("the test's own config parses");
    let parsed_after: toml::Value = toml::from_str(after)
        .unwrap_or_else(|error| panic!("annotated config no longer parses: {error}\n{after}"));
    // A final line without a newline gains one so a note cannot run into it.
    assert_eq!(
        foreign_lines(before).trim_end_matches('\n'),
        foreign_lines(after).trim_end_matches('\n'),
        "a line the check does not own changed:\n--- before\n{before}\n--- after\n{after}"
    );
    assert_eq!(
        parsed_before, parsed_after,
        "the config's values changed:\n{after}"
    );
}

/// `H` misbehaves as `hostile` describes; `S` is a well-behaved sibling on
/// the same store holding every article.
fn pair(hostile: impl FnOnce(Spec) -> Spec) -> [Spec; 2] {
    [
        hostile(Spec::new("hostile", 0, STORE_A, false)),
        Spec::new("sibling", 1, STORE_A, true),
    ]
}

/// Run one check on `specs` and report routing afterwards, after checking
/// the config survived.
async fn check_once(specs: &[Spec]) -> Result<(Running, Routed)> {
    let running = start(specs, BackboneDetection::Annotate, None).await?;
    let before = running.file();
    assert_eq!(running.routing("before", 2).await?, Routed::Apart);
    running.check.run().await;
    let routed = running.routing("after", 3).await?;
    assert_intact(&before, &running.file());
    Ok((running, routed))
}

fn list_reply(lines: &[String]) -> Vec<Act> {
    let mut body = String::from("215 list follows\r\n");
    for line in lines {
        body.push_str(line);
        body.push_str("\r\n");
    }
    body.push_str(".\r\n");
    vec![Act::Send(body.into_bytes())]
}

// ---------------------------------------------------------------------------
// LIST ACTIVE oddities
// ---------------------------------------------------------------------------

#[tokio::test]
async fn list_numbers_that_overflow_u64_are_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, _| list_reply(&[format!("{group} 99999999999999999999999 2 y")]))
        });
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Apart);
        assert!(
            running.file().contains("gave no usable line"),
            "{}",
            running.file()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_numbers_near_u64_max_do_not_overflow_the_comparison() -> Result<()> {
    bounded(QUICK, async {
        // Both servers report u64::MAX-ish numbers: the tolerance arithmetic
        // must not overflow, and identical numbers group.
        let reply = |group: &str, _| list_reply(&[format!("{group} {} 0 y", u64::MAX)]);
        let specs = [
            Spec::new("hostile", 0, STORE_A, false).list(reply),
            Spec::new("sibling", 1, STORE_A, true).list(reply),
        ];
        let (_, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_negative_numbers_are_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| list_reply(&[format!("{group} -{} -2 y", high(store, group))]))
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Apart);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_low_above_high_is_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| list_reply(&[format!("{group} 2 {} y", high(store, group))]))
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Apart);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_zero_numbers_are_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        // Two servers both reporting an empty group (0 0) must not be
        // grouped on that "match".
        let reply = |group: &str, _| list_reply(&[format!("{group} 0 0 y")]);
        let specs = [
            Spec::new("hostile", 0, STORE_A, false).list(reply),
            Spec::new("sibling", 1, STORE_A, true).list(reply),
        ];
        assert_eq!(check_once(&specs).await?.1, Routed::Apart);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_missing_fields_are_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| {
                list_reply(&[
                    group.to_owned(),
                    format!("{group} {}", high(store, group)),
                    String::new(),
                ])
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Apart);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_extra_fields_and_tabs_still_parse() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| {
                list_reply(&[format!(
                    "{group}\t{}\t2\ty\textra junk fields",
                    high(store, group)
                )])
            })
        });
        // The numbers really match the sibling's, so grouping is right.
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_reply_for_another_group_is_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        // Each reply names the next probe group, with that group's true
        // numbers: plausible, but not an answer to the question asked.
        let specs = pair(|h| {
            h.list(|group, store| {
                let index = PROBE_GROUPS.iter().position(|g| *g == group).unwrap_or(0);
                let other = PROBE_GROUPS[(index + 1) % PROBE_GROUPS.len()];
                list_reply(&[active(store, other)])
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Apart);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_group_name_in_other_case_is_accepted() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| list_reply(&[active(store, &group.to_ascii_uppercase())]))
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_with_thousands_of_lines_finds_the_group() -> Result<()> {
    bounded(QUICK, async {
        // A server that ignores the wildmat and sends its whole active file.
        let specs = pair(|h| {
            h.list(|group, store| {
                let mut lines: Vec<String> = (0..20_000)
                    .map(|i| format!("alt.filler.group{i} {} 1 y", 1_000_000 + i))
                    .collect();
                lines.push(active(store, group));
                list_reply(&lines)
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_far_over_the_capture_limit_is_refused() -> Result<()> {
    bounded(QUICK, async {
        // ~26 MB per reply, far over the 4 MiB the framer allows any other
        // captured multiline response. A hostile server can stream for the
        // whole 30 s command limit; the probe should stop reading at a cap.
        let specs = pair(|h| {
            h.list(|group, store| {
                let mut body = b"215 list follows\r\n".to_vec();
                body.extend_from_slice(&b"alt.filler.group 1000000 1 y\r\n".repeat(900_000));
                body.extend_from_slice(format!("{}\r\n.\r\n", active(store, group)).as_bytes());
                vec![Act::Send(body)]
            })
        });
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(
            routed,
            Routed::Apart,
            "a {} MB LIST ACTIVE reply was captured whole and used as evidence",
            900_000 * 30 / 1_000_000
        );
        assert!(
            running.file().contains("could not check"),
            "{}",
            running.file()
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_single_line_errors_are_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        for reply in [
            "503 program fault\r\n",
            "480 authentication required\r\n",
            "502 no permission\r\n",
        ] {
            let specs = pair(|h| h.list(move |_, _| vec![send(reply)]));
            let (running, routed) = check_once(&specs).await?;
            assert_eq!(routed, Routed::Apart, "{reply}");
            let code: String = reply.chars().take(3).collect();
            assert!(
                running
                    .file()
                    .contains(&format!("LIST ACTIVE was refused with status {code}")),
                "{}",
                running.file()
            );
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn slow_list_that_never_terminates_is_cut_off() -> Result<()> {
    bounded(SLOW, async {
        let first = Arc::new(AtomicBool::new(true));
        let specs = pair(|h| {
            h.list(move |group, store| {
                if first.swap(false, Ordering::Relaxed) {
                    vec![
                        send(format!("215 list follows\r\n{}\r\n", active(store, group))),
                        Act::Hang,
                    ]
                } else {
                    vec![send("503 later\r\n")]
                }
            })
        });
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Apart);
        assert!(running.file().contains("timed out"), "{}", running.file());
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_that_disconnects_mid_body_is_not_evidence() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| {
                let line = active(store, group);
                vec![
                    send(format!("215 list follows\r\n{}", &line[..line.len() / 2])),
                    Act::Close,
                ]
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Apart);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_dot_stuffed_lines_are_unstuffed() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| {
                list_reply(&[
                    "..".to_owned(),
                    format!("..{}", active(STORE_B, group)),
                    active(store, group),
                ])
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_non_utf8_and_control_bytes_do_not_break_anything() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.list(|group, store| {
                let mut body = b"215 list \xff\xfe\x1b[31m follows\r\n".to_vec();
                body.extend_from_slice(b"\xff\xfe\x00\x07garbage \x01\x02 3 4\r\n");
                body.extend_from_slice(
                    format!("{group}\x00 {} 2 y\r\n", high(STORE_B, group)).as_bytes(),
                );
                body.extend_from_slice(active(store, group).as_bytes());
                body.extend_from_slice(b"\r\n.\r\n");
                vec![Act::Send(body)]
            })
        });
        // The one clean line matches the sibling.
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_error_with_control_bytes_never_reaches_the_config_raw() -> Result<()> {
    bounded(QUICK, async {
        let specs =
            pair(|h| h.list(|_, _| vec![send(b"503 \x1b[2J\x00\x07\x08\x0b\x0c\x7f\xff bad\r\n")]));
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Apart);
        let file = running.file();
        assert!(
            !file.chars().any(|c| c.is_control() && c != '\n'),
            "control characters in the config:\n{file:?}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn slow_list_with_bare_lf_line_endings() -> Result<()> {
    bounded(SLOW, async {
        // Bare LF everywhere, terminator included. Whatever the framer makes
        // of it, the probe must finish and leave the sibling reachable.
        let specs = pair(|h| {
            h.list(|group, store| {
                vec![send(format!(
                    "215 list follows\n{}\n.\n",
                    active(store, group)
                ))]
            })
        });
        let (running, routed) = check_once(&specs).await?;
        eprintln!("bare-LF LIST ACTIVE -> {routed:?}\n{}", running.file());
        assert!(
            matches!(routed, Routed::Apart | Routed::Grouped),
            "{routed:?}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn list_early_terminator_with_trailing_lines_in_one_write() -> Result<()> {
    bounded(QUICK, async {
        // An empty list, then a forged line and a second terminator in the
        // same write: the forged line must not count, and the leftover bytes
        // must not poison the next command on that connection.
        let specs = pair(|h| {
            h.list(|group, store| {
                vec![send(format!(
                    "215 list follows\r\n.\r\n{}\r\n.\r\n",
                    active(store, group)
                ))]
            })
        });
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Apart, "{}", running.file());
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// HELP / STAT oddities
// ---------------------------------------------------------------------------

#[tokio::test]
async fn huge_help_does_not_stop_grouping() -> Result<()> {
    bounded(QUICK, async {
        for size in [3 * 1024 * 1024, 8 * 1024 * 1024] {
            let specs = pair(|h| {
                h.hook(move |command, _| {
                    (command == "HELP").then(|| {
                        let mut body = b"100 help\r\n".to_vec();
                        while body.len() < size {
                            body.extend_from_slice(
                                b"  ARTICLE message-id or number, a very long help line\r\n",
                            );
                        }
                        body.extend_from_slice(b".\r\n");
                        vec![Act::Send(body)]
                    })
                })
            });
            assert_eq!(
                check_once(&specs).await?.1,
                Routed::Grouped,
                "HELP of {size} bytes"
            );
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn slow_help_that_hangs_is_cut_off_and_numbers_still_group() -> Result<()> {
    bounded(SLOW, async {
        let specs = pair(|h| h.hook(|command, _| (command == "HELP").then(|| vec![Act::Hang])));
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn slow_help_status_with_no_body_is_cut_off() -> Result<()> {
    bounded(SLOW, async {
        let specs = pair(|h| {
            h.hook(|command, _| {
                (command == "HELP").then(|| vec![send("100 help follows\r\n"), Act::Hang])
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn stat_answered_with_multiline_garbage_in_one_write() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.hook(|command, _| {
                command
                    .ends_with(".invalid>")
                    .then(|| vec![send("220 0 <x@y>\r\nSubject: garbage\r\n\r\nbody\r\n.\r\n")])
            })
        });
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn late_bytes_after_a_probe_reply_never_reach_a_client() -> Result<()> {
    bounded(QUICK, async {
        // The probe STAT gets a proper 430, then a forged article arrives a
        // moment later, unasked. If that connection goes back to the pool,
        // the next client ARTICLE on it reads the forged reply.
        let specs = pair(|h| {
            h.hook(|command, _| {
                command.ends_with(".invalid>").then(|| {
                    vec![
                        send("430 no such article\r\n"),
                        Act::Pause(Duration::from_millis(300)),
                        send("220 0 <forged@evil>\r\nSubject: POISON\r\n\r\nPOISON\r\n.\r\n"),
                    ]
                })
            })
        });
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        running.check.run().await;
        tokio::time::sleep(Duration::from_millis(600)).await;
        for index in 0..4 {
            let id = format!("<real-{index}@example.com>");
            let (code, whole) = fetch_article(running.port, &id).await?;
            assert!(
                !whole.contains("POISON"),
                "client asked for {id} and got the forged article: {whole:?}"
            );
            assert!(code == "220" || code == "430", "{code}: {whole:?}");
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn server_closing_after_every_probe_command() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.hook(|command, _| {
                (command.starts_with("LIST") || command == "HELP" || command.ends_with(".invalid>"))
                    .then(|| vec![Act::Close])
            })
        });
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Apart);
        assert!(
            running.file().contains("could not check"),
            "{}",
            running.file()
        );
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// Greetings
// ---------------------------------------------------------------------------

#[tokio::test]
async fn greeting_with_control_characters() -> Result<()> {
    bounded(QUICK, async {
        let specs =
            pair(|h| h.greeting(vec![send(b"200 \x1b[2J\x07\x00\x08 Eweka \x7f ready\r\n")]));
        let (running, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Grouped);
        assert!(!running.file().chars().any(|c| c.is_control() && c != '\n'));
        Ok(())
    })
    .await
}

#[tokio::test]
async fn greeting_that_is_not_utf8() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| h.greeting(vec![send(b"200 \xff\xfe\xc3\x28 news\r\n")]));
        assert_eq!(check_once(&specs).await?.1, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn very_long_greeting_does_not_bloat_the_config() -> Result<()> {
    bounded(QUICK, async {
        let long = format!("200 {}\r\n", "x".repeat(256 * 1024));
        let specs = pair(|h| h.greeting(vec![send(long.clone())]));
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        let before = running.file();
        running.check.run().await;
        let file = running.file();
        assert_intact(&before, &file);
        let note_bytes: usize = notes(&file).iter().map(|line| line.len()).sum();
        assert!(
            note_bytes < 4096,
            "a server's greeting put {note_bytes} bytes of notes into the config"
        );
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// Flapping and lying
// ---------------------------------------------------------------------------

#[tokio::test]
async fn flapping_numbers_regroup_on_every_check() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| h);
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        let original = running.file();
        let mut previous_file = original.clone();
        for (round, (store, expected)) in [
            (STORE_A, Routed::Grouped),
            (STORE_B, Routed::Apart),
            (STORE_A, Routed::Grouped),
            (STORE_B, Routed::Apart),
            (STORE_A, Routed::Grouped),
        ]
        .into_iter()
        .enumerate()
        {
            running.stores[0].store(store, Ordering::Relaxed);
            running.check.run().await;
            assert_eq!(
                running.routing(&format!("round{round}"), 2).await?,
                expected,
                "round {round}"
            );
            let file = running.file();
            assert_intact(&original, &file);
            assert_intact(&previous_file, &file);
            previous_file = file;
        }
        Ok(())
    })
    .await
}

#[tokio::test]
async fn known_limitation_matching_numbers_but_failing_every_article() -> Result<()> {
    bounded(QUICK, async {
        // H reports S's numbers exactly but answers every article with 430
        // (copied numbers, or an account that cannot read anything). The
        // evidence groups them, so clients see 430 though S has every
        // article. Recorded, not a detection bug: numbers are the evidence.
        let specs = pair(|h| h);
        let (_, routed) = check_once(&specs).await?;
        assert_eq!(routed, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn known_limitation_liar_copies_numbers_but_holds_other_articles() -> Result<()> {
    bounded(QUICK, async {
        // A holds only "<a-…>", B holds only "<b-…>", both report one store.
        let holds = |prefix: &'static str| {
            move |command: &str, _: &Conn| {
                let (verb, id) = command.split_once(' ')?;
                if verb != "ARTICLE" || id.ends_with(".invalid>") {
                    return None;
                }
                Some(vec![send(if id.starts_with(prefix) {
                    format!("220 0 {id}\r\nSubject: x\r\n\r\nbody\r\n.\r\n")
                } else {
                    "430 no such article\r\n".to_owned()
                })])
            }
        };
        let specs = [
            Spec::new("a", 0, STORE_A, false).hook(holds("<a-")),
            Spec::new("b", 1, STORE_A, false).hook(holds("<b-")),
        ];
        let running = start(&specs, BackboneDetection::Apply, None).await?;
        let (before, _) = fetch_article(running.port, "<b-1@x>").await?;
        running.check.run().await;
        let (after, _) = fetch_article(running.port, "<b-2@x>").await?;
        eprintln!("liar pair: B's article before check -> {before}, after -> {after}");
        assert_eq!(before, "220");
        assert_eq!(
            after, "430",
            "documented limitation: grouped on copied numbers"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_server_matching_in_only_one_group_is_grouped() -> Result<()> {
    bounded(QUICK, async {
        // H answers one probe group (matching S) and refuses the rest. One
        // group of agreement is enough evidence for detection today.
        let specs = pair(|h| {
            h.list(|group, store| {
                if group == PROBE_GROUPS[0] {
                    list_reply(&[active(store, group)])
                } else {
                    vec![send("503 no such group\r\n")]
                }
            })
        });
        let (_, routed) = check_once(&specs).await?;
        eprintln!("one-group match -> {routed:?}");
        assert_eq!(routed, Routed::Grouped);
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// Concurrency
// ---------------------------------------------------------------------------

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn concurrent_checks_while_clients_fetch() -> Result<()> {
    bounded(120, async {
        let specs = pair(|h| h);
        let running = Arc::new(start(&specs, BackboneDetection::Annotate, None).await?);
        let original = running.file();
        let stop = Arc::new(AtomicBool::new(false));

        let flipper = {
            let store = running.stores[0].clone();
            let stop = stop.clone();
            tokio::spawn(async move {
                let mut flip = false;
                while !stop.load(Ordering::Relaxed) {
                    store.store(if flip { STORE_A } else { STORE_B }, Ordering::Relaxed);
                    flip = !flip;
                    tokio::time::sleep(Duration::from_millis(7)).await;
                }
            })
        };
        let checkers: Vec<_> = (0..4)
            .map(|_| {
                let check = running.check.clone();
                tokio::spawn(async move {
                    for _ in 0..5 {
                        check.run().await;
                    }
                })
            })
            .collect();
        let clients: Vec<_> = (0..8)
            .map(|client| {
                let port = running.port;
                tokio::spawn(async move {
                    let mut codes = Vec::new();
                    for index in 0..10 {
                        let (code, _) =
                            fetch_article(port, &format!("<c{client}-{index}@example.com>"))
                                .await?;
                        codes.push(code);
                    }
                    anyhow::Ok(codes)
                })
            })
            .collect();

        for checker in checkers {
            checker.await.context("a check panicked")?;
        }
        for client in clients {
            let codes = client.await.context("a client task panicked")??;
            // Every request is answered with a status. Whether that status
            // is always 220/430 is checked by
            // `a_regroup_during_requests_never_answers_503`.
            assert!(
                codes
                    .iter()
                    .all(|code| code.len() == 3 && code.chars().all(|c| c.is_ascii_digit())),
                "a client got no answer: {codes:?}"
            );
        }
        stop.store(true, Ordering::Relaxed);
        flipper.await?;

        // Settle: one more check on fixed numbers routes as they say.
        running.stores[0].store(STORE_B, Ordering::Relaxed);
        running.check.run().await;
        assert_eq!(running.routing("settled", 2).await?, Routed::Apart);
        assert_intact(&original, &running.file());
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// Config annotation against the real file writer
// ---------------------------------------------------------------------------

/// Two well-behaved servers on one store, so both get a "detected" note.
fn twins(a: &str, b: &str) -> [Spec; 2] {
    [
        Spec::new(a, 0, STORE_A, false),
        Spec::new(b, 1, STORE_A, true),
    ]
}

async fn annotate_file(specs: &[Spec], file: &str) -> Result<(Running, String)> {
    let running = start(specs, BackboneDetection::Annotate, Some(file.to_owned())).await?;
    running.check.run().await;
    let after = running.file();
    Ok((running, after))
}

fn host(index: usize) -> String {
    independent_provider_host(index)
}

#[tokio::test]
async fn config_with_crlf_line_endings() -> Result<()> {
    bounded(QUICK, async {
        let file = default_file(&twins("alpha", "beta")).replace('\n', "\r\n");
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        assert_eq!(noted_servers(&after), 2, "{after}");
        assert!(
            !after.replace("\r\n", "").contains('\n'),
            "a bare LF crept in"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_with_a_utf8_bom_before_the_first_server() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "\u{feff}[[servers]]\nname = \"alpha\"\nhost = \"{}\"\n\n[[servers]]\nname = \"beta\"\nhost = \"{}\"\n",
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        assert!(after.starts_with('\u{feff}'), "the BOM was dropped");
        assert_eq!(
            noted_servers(&after),
            2,
            "both servers should be annotated:\n{after}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_with_multiline_array_and_inline_table() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            r#"[proxy]
port = 8119

[[servers]]
name = "alpha"
host = "{}"
tls = {{ verify = true, alpn = ["nntp", "x"] }}
extra = [
    "one",
    ["nested", "array"],
    {{ inline = "table" }},
]
backbone = "auto"

[[servers]]
host = "{}"
tags = [
  ["first"],
]
name = "beta"
"#,
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        eprintln!("multi-line arrays:\n{after}");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_with_single_quoted_names_is_annotated() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "[[servers]]\nname = 'alpha'\nhost = \"{}\"\n\n[[servers]]\nname = 'beta'\nhost = \"{}\"\n",
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        assert_eq!(
            noted_servers(&after),
            2,
            "literal-string names are valid TOML and should be annotated:\n{after}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_with_odd_whitespace_and_indentation() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "  [[servers]]   \n\tname   =    \"alpha\"   # first\n\thost=\"{}\"\n\n[[servers]]\n    name=\"beta\"\n    backbone   =   \"auto\"\n    host = \"{}\"\n",
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        assert_eq!(noted_servers(&after), 2, "{after}");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_with_comments_that_look_like_servers() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "# [[servers]]\n# name = \"beta\"\n[proxy]\nport = 8119 # [[servers]] name = \"alpha\"\n\n[[servers]]\n# name = \"beta\"\nname = \"alpha\"\nhost = \"{}\"\n\n#[[servers]]\n[[servers]]\nname = \"beta\" # [[servers]]\nhost = \"{}\"\n",
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        assert_eq!(noted_servers(&after), 2, "{after}");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_with_quoted_and_unicode_server_names() -> Result<()> {
    bounded(QUICK, async {
        let specs = twins("we \"quote\"", "ñews-über ☃");
        let file = default_file(&specs);
        let (_, after) = annotate_file(&specs, &file).await?;
        assert_intact(&file, &after);
        eprintln!("quoted/unicode names:\n{after}");
        assert!(
            after.contains("name = \"ñews-über ☃\"\n# backbone-check"),
            "the unicode name is annotated:\n{after}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_server_name_with_escaped_quotes_is_annotated() -> Result<()> {
    bounded(QUICK, async {
        let specs = twins("we \"quote\"", "plain");
        let file = default_file(&specs);
        let (_, after) = annotate_file(&specs, &file).await?;
        assert_intact(&file, &after);
        assert_eq!(
            noted_servers(&after),
            2,
            "a basic-string name with escapes should be found:\n{after}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn read_only_config_is_left_alone() -> Result<()> {
    bounded(QUICK, async {
        use std::os::unix::fs::PermissionsExt;
        let specs = twins("alpha", "beta");
        let file = default_file(&specs);
        let running = start(&specs, BackboneDetection::Annotate, Some(file.clone())).await?;
        std::fs::set_permissions(&running.config_path, std::fs::Permissions::from_mode(0o444))?;
        running.check.run().await;
        running.check.run().await;
        assert_eq!(running.file(), file);
        assert_eq!(running.routing("ro", 2).await?, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn deleted_config_is_not_recreated() -> Result<()> {
    bounded(QUICK, async {
        let specs = twins("alpha", "beta");
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        std::fs::remove_file(&running.config_path)?;
        running.check.run().await;
        assert!(
            !running.config_path.exists(),
            "the check recreated a deleted config"
        );
        assert_eq!(running.routing("gone", 2).await?, Routed::Grouped);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn config_edited_between_checks_keeps_the_edits() -> Result<()> {
    bounded(QUICK, async {
        let specs = twins("alpha", "beta");
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        running.check.run().await;
        let first = running.file();
        assert_eq!(noted_servers(&first), 2, "{first}");
        // The operator moves things around and adds keys, keeping our notes.
        let edited = first
            .replace("[proxy]\nport = 8119\n", "[proxy]\nport = 9119 # edited\n")
            .replace(
                "name = \"beta\"\n",
                "name = \"beta\"\nmax_connections = 5\n",
            );
        std::fs::write(&running.config_path, &edited)?;
        running.stores[1].store(STORE_B, Ordering::Relaxed);
        running.check.run().await;
        let second = running.file();
        assert_intact(&edited, &second);
        assert_ne!(
            notes(&first),
            notes(&second),
            "the notes follow the new finding"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn name_on_the_last_line_without_a_newline() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "[[servers]]\nname = \"alpha\"\nhost = \"{}\"\n\n[[servers]]\nhost = \"{}\"\nname = \"beta\"",
            host(0),
            host(1)
        );
        let (running, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert_intact(&file, &after);
        // And a second, changed finding must replace the note, not stack.
        running.stores[1].store(STORE_B, Ordering::Relaxed);
        running.check.run().await;
        let again = running.file();
        assert_intact(&file, &again);
        Ok(())
    })
    .await
}

#[tokio::test]
async fn operator_comments_that_merely_start_like_the_marker_are_kept() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "[[servers]]\nname = \"alpha\"\n# backbone-checked by hand in 2025: keep on omicron\nhost = \"{}\"\n\n[[servers]]\nname = \"beta\"\nhost = \"{}\"\n",
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        assert!(
            after.contains("# backbone-checked by hand in 2025: keep on omicron\n"),
            "the operator's own comment was deleted:\n{after}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn multiline_string_that_looks_like_a_server_block() -> Result<()> {
    bounded(QUICK, async {
        let file = format!(
            "[[servers]]\nname = \"alpha\"\nhost = \"{}\"\nnotes = \"\"\"\ncopied from the old config:\n[[servers]]\nname = \"beta\"\n\"\"\"\n\n[[servers]]\nname = \"beta\"\nhost = \"{}\"\n",
            host(0),
            host(1)
        );
        let (_, after) = annotate_file(&twins("alpha", "beta"), &file).await?;
        // assert_intact also compares parsed values, so a comment written
        // inside the string shows up as a changed `notes` value.
        assert_intact(&file, &after);
        Ok(())
    })
    .await
}

// ---------------------------------------------------------------------------
// Credentials
// ---------------------------------------------------------------------------

const USER: &str = "alice-secret-user";
const PASS: &str = "hunter2-secret-pass";

#[tokio::test]
async fn an_echoed_username_never_lands_in_the_config() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.credentials(USER, PASS).hook(|command, conn| {
                command.starts_with("LIST ACTIVE ").then(|| {
                    vec![send(format!(
                        "480 {} may not LIST\r\n",
                        conn.user.clone().unwrap_or_default()
                    ))]
                })
            })
        });
        let (running, _) = check_once(&specs).await?;
        let file = running.file();
        assert!(
            !file.contains(USER),
            "the account name reached the config:\n{file}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn an_auth_refusal_echoing_credentials_never_lands_in_the_config() -> Result<()> {
    bounded(QUICK, async {
        // The server refuses the login and echoes what it was sent. The
        // probe's first checkout fails with that reply in its error.
        let specs = pair(|h| {
            h.credentials(USER, PASS).hook(|command, conn| {
                command.starts_with("AUTHINFO PASS").then(|| {
                    vec![send(format!(
                        "481 rejected user {} with password {}\r\n",
                        conn.user.clone().unwrap_or_default(),
                        conn.pass.clone().unwrap_or_default()
                    ))]
                })
            })
        });
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        running.check.run().await;
        let file = running.file();
        assert!(
            !file.contains(USER) && !file.contains(PASS),
            "credentials reached the config:\n{file}"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_long_auth_refusal_does_not_bloat_the_config() -> Result<()> {
    bounded(QUICK, async {
        let specs = pair(|h| {
            h.credentials(USER, PASS).hook(|command, _| {
                command
                    .starts_with("AUTHINFO PASS")
                    .then(|| vec![send(format!("481 {}\r\n", "spam ".repeat(1500)))])
            })
        });
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        running.check.run().await;
        let file = running.file();
        let note_bytes: usize = notes(&file).iter().map(|line| line.len()).sum();
        assert!(
            note_bytes < 4096,
            "a server's refusal put {note_bytes} bytes of notes into the config"
        );
        Ok(())
    })
    .await
}

#[tokio::test]
async fn a_steady_login_refusal_does_not_rewrite_the_note_every_check() -> Result<()> {
    bounded(QUICK, async {
        // Nothing about the server changes between checks, so the note (and
        // its "first seen" date) should stay as written.
        let specs = pair(|h| {
            h.credentials(USER, PASS).hook(|command, _| {
                command
                    .starts_with("AUTHINFO PASS")
                    .then(|| vec![send("481 authentication failed\r\n")])
            })
        });
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        running.check.run().await;
        let first = running.file();
        tokio::time::sleep(Duration::from_millis(50)).await;
        running.check.run().await;
        let second = running.file();
        assert_eq!(
            notes(&first),
            notes(&second),
            "an unchanged refusal rewrote the config"
        );
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 8)]
async fn concurrent_checks_never_tear_the_config() -> Result<()> {
    bounded(QUICK, async {
        // Many checks at once, each seeing different numbers, so their notes
        // differ and their writes overlap.
        let specs = pair(|h| h);
        let running = start(&specs, BackboneDetection::Annotate, None).await?;
        let original = running.file();
        for burst in 0..15 {
            let tasks: Vec<_> = (0..8)
                .map(|task| {
                    let check = running.check.clone();
                    let store = running.stores[0].clone();
                    tokio::spawn(async move {
                        store.store(
                            if (task + burst) % 2 == 0 {
                                STORE_A
                            } else {
                                STORE_B
                            },
                            Ordering::Relaxed,
                        );
                        check.run().await;
                    })
                })
                .collect();
            for task in tasks {
                task.await.context("a check panicked")?;
            }
            assert_intact(&original, &running.file());
        }
        Ok(())
    })
    .await
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn a_regroup_during_requests_never_answers_503() -> Result<()> {
    bounded(QUICK, async {
        // One check groups H with S while clients are mid-lookup. A request
        // in flight may see either grouping, so 430 (grouped) or 220 (apart)
        // are both right; "503 Backend error" is not: H did answer 430 and S
        // was either asked or legitimately skipped as H's twin.
        let mut wrong = Vec::new();
        for round in 0..30 {
            let running = start(&pair(|h| h), BackboneDetection::Apply, None).await?;
            let clients: Vec<_> = (0..8)
                .map(|client| {
                    let port = running.port;
                    tokio::spawn(async move {
                        let mut statuses = Vec::new();
                        for index in 0..3 {
                            let id = format!("<r{round}-c{client}-{index}@example.com>");
                            let (code, whole) = fetch_article(port, &id).await?;
                            if code != "220" && code != "430" {
                                statuses.push(whole.trim_end().to_owned());
                            }
                        }
                        anyhow::Ok(statuses)
                    })
                })
                .collect();
            running.check.run().await;
            for client in clients {
                wrong.extend(client.await.context("a client task panicked")??);
            }
        }
        assert!(
            wrong.is_empty(),
            "{} in-flight requests got a non-430/220 answer during a regroup: {:?}",
            wrong.len(),
            wrong
        );
        Ok(())
    })
    .await
}
