//! Bounded loopback diagnostics, not a throughput benchmark or WAN simulation.
//!
//! Reader cases inject 5/20/80 ms of service delay per ARTICLE/STAT reply, use
//! fresh IDs, and compare otherwise identical STAT 0/1 configurations. Payload
//! cases pace 16 KiB application writes/reads by 1 ms; socket buffers can overlap
//! this work. First byte includes the status line. Backend completion means
//! socket-writer acceptance, not client delivery. Content validation and cache
//! inspection happen after the measured client-completion timestamp.

use anyhow::{Context, Result, ensure};
use clap::Parser;
use nntp_proxy::config::{Cache, Config, Server};
use nntp_proxy::protocol::{RequestContext, RequestKind, StatusCode};
use nntp_proxy::session::ResponseFixture;
use nntp_proxy::types::{CacheCapacity, MaxConnections, Port};
use nntp_proxy::{NntpProxy, RoutingMode};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::time::{Duration, Instant};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};
use tokio::task::{JoinHandle, JoinSet};

const CHUNK_BYTES: usize = 16 * 1024;
const CACHE_BYTES: u64 = 16 * 1024 * 1024;
const WIRE_BUDGET_4M: usize = 4 * 1024 * 1024;

#[derive(Parser)]
struct Args {
    /// Cargo supplies this argument even when the benchmark harness is disabled.
    #[arg(long, hide = true)]
    bench: bool,
    #[arg(long)]
    self_check: bool,
    #[arg(long)]
    list: bool,
    #[arg(long)]
    exact: Option<String>,
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u8).range(1..=50))]
    requests: u8,
    #[arg(long, default_value_t = 30, value_parser = clap::value_parser!(u16).range(1..=120))]
    deadline_secs: u16,
}

fn main() -> Result<()> {
    let args = Args::parse();
    if args.list {
        for case in cases() {
            println!("{}", case.name);
        }
        return Ok(());
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()?;
    runtime.block_on(async {
        let deadline = Duration::from_secs(u64::from(args.deadline_secs));
        if args.self_check {
            return tokio::time::timeout(deadline, self_check()).await?;
        }
        let name = args.exact.context("select --exact CASE or --list")?;
        let case = cases()
            .into_iter()
            .find(|case| case.name == name)
            .context("unknown exact case; use --list")?;
        let result = tokio::time::timeout(deadline, run_case(&case, usize::from(args.requests)))
            .await
            .context("scenario deadline exceeded")??;
        result.print(&case);
        Ok(())
    })
}

async fn wait_reply_delay(delay: Duration) {
    if !delay.is_zero() {
        tokio::time::sleep(delay).await;
    }
}

async fn check_reply_delay() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let fixture = ResponseFixture::status(StatusCode::new(200), "Delayed fixture");
    let wire = fixture.wire();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        wait_reply_delay(Duration::from_millis(5)).await;
        stream.write_all(&wire).await
    });
    let start = Instant::now();
    let mut client = TcpStream::connect(address).await?;
    fixture.assert_received(&mut client).await?;
    ensure!(
        start.elapsed() >= Duration::from_millis(5),
        "modeled reply delay was omitted from the actual socket exchange"
    );
    server.await??;
    println!("PASS actual socket reply observes modeled service delay");
    Ok(())
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Outcome {
    Present,
    Missing,
    Unsupported503,
    Unknown500,
}

impl Outcome {
    fn label(self) -> &'static str {
        match self {
            Self::Present => "present",
            Self::Missing => "missing",
            Self::Unsupported503 => "unsupported503",
            Self::Unknown500 => "unknown500",
        }
    }
    fn probe_code(self) -> StatusCode {
        StatusCode::new(match self {
            Self::Present => 223,
            Self::Missing => 430,
            Self::Unsupported503 => 503,
            Self::Unknown500 => 500,
        })
    }
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum PayloadCache {
    Disabled,
    Enabled,
}

#[derive(Clone, Copy, PartialEq, Eq)]
enum Pacing {
    None,
    Upstream,
    Downstream,
    Both,
}

impl Pacing {
    fn upstream(self) -> bool {
        matches!(self, Self::Upstream | Self::Both)
    }
    fn downstream(self) -> bool {
        matches!(self, Self::Downstream | Self::Both)
    }
    fn label(self) -> &'static str {
        match self {
            Self::None => "none",
            Self::Upstream => "upstream",
            Self::Downstream => "downstream",
            Self::Both => "both",
        }
    }
}

#[derive(Clone, Copy)]
enum Scenario {
    Reader {
        delay_ms: u64,
        outcome: Outcome,
        stat: u8,
    },
    Payload {
        size: usize,
        pacing: Pacing,
        cache: PayloadCache,
    },
}

struct Case {
    name: String,
    scenario: Scenario,
}

fn cases() -> Vec<Case> {
    let mut cases = Vec::new();
    for delay_ms in [5, 20, 80] {
        for outcome in [
            Outcome::Present,
            Outcome::Missing,
            Outcome::Unsupported503,
            Outcome::Unknown500,
        ] {
            for stat in [0, 1] {
                cases.push(Case {
                    name: format!("reader-{delay_ms}ms-{}-stat{stat}", outcome.label()),
                    scenario: Scenario::Reader {
                        delay_ms,
                        outcome,
                        stat,
                    },
                });
            }
        }
    }
    for (label, size) in [
        ("64k", 64 * 1024),
        ("788k", 788 * 1024),
        ("wire4m", WIRE_BUDGET_4M),
    ] {
        for pacing in [Pacing::Upstream, Pacing::Downstream, Pacing::Both] {
            for cache in [PayloadCache::Disabled, PayloadCache::Enabled] {
                cases.push(Case {
                    name: format!(
                        "payload-{label}-{}-cache{}",
                        pacing.label(),
                        usize::from(cache == PayloadCache::Enabled)
                    ),
                    scenario: Scenario::Payload {
                        size,
                        pacing,
                        cache,
                    },
                });
            }
        }
    }
    cases
}

fn status(code: u16, detail: &str) -> ResponseFixture {
    ResponseFixture::status(StatusCode::new(code), detail)
}

fn article(id: &str, body_len: usize) -> ResponseFixture {
    ResponseFixture::reader(RequestKind::Article, 0, id)
        .header("Subject", "Controlled benchmark")
        .body(vec![b'x'; body_len])
}

fn request_id(index: usize) -> String {
    format!("<controlled-{index:06}@bench.local>")
}

fn payload_body_len(id: &str, size: usize) -> usize {
    if size == WIRE_BUDGET_4M {
        // Reserve renderer overhead numerically; never locate wire boundaries.
        let overhead = article(id, 1).wire_len() - 1;
        size - overhead
    } else {
        size
    }
}

async fn wait_pacing(delay: Duration) {
    tokio::time::sleep(delay).await;
}

async fn write_paced(stream: &mut TcpStream, wire: &[u8], pause: Duration) -> Result<()> {
    for (index, chunk) in wire.chunks(CHUNK_BYTES).enumerate() {
        if index != 0 {
            wait_pacing(pause).await;
        }
        stream.write_all(chunk).await?;
    }
    Ok(())
}

#[derive(Default)]
struct Counts {
    articles: AtomicUsize,
    stats: AtomicUsize,
}

struct PreparedReply {
    id: String,
    wire: Arc<[u8]>,
}
type CurrentReply = Arc<Mutex<Option<PreparedReply>>>;
type WriteTime = Arc<Mutex<Option<Instant>>>;

#[derive(Clone, Copy)]
enum Lookup {
    Present,
    Missing,
}

struct Backend {
    server: Server,
    counts: Arc<Counts>,
    final_write: WriteTime,
    task: JoinHandle<Result<()>>,
}
impl Drop for Backend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

struct BackendOptions {
    host: &'static str,
    tier: u8,
    stat: u8,
    lookup: Lookup,
    probe: StatusCode,
    delay: Duration,
    pacing: Pacing,
}

async fn start_backend(options: BackendOptions, current: CurrentReply) -> Result<Backend> {
    let listener = TcpListener::bind((options.host, 0)).await?;
    let server = Server::builder(options.host, Port::try_new(listener.local_addr()?.port())?)
        .name(format!("controlled-tier{}", options.tier))
        .tier(options.tier)
        .max_connections(MaxConnections::try_new(1)?)
        .stat_missing(options.stat)
        .build()?;
    let counts = Arc::new(Counts::default());
    let final_write: WriteTime = Arc::default();
    let observed = counts.clone();
    let written = final_write.clone();
    let task = tokio::spawn(async move {
        let mut clients = JoinSet::new();
        loop {
            let (stream, _) = listener.accept().await?;
            stream.set_nodelay(true)?;
            let current = current.clone();
            let counts = observed.clone();
            let final_write = written.clone();
            let lookup = options.lookup;
            let probe = options.probe;
            let delay = options.delay;
            let pacing = options.pacing;
            clients.spawn(async move {
                let mut reader = BufReader::new(stream);
                reader
                    .get_mut()
                    .write_all(&status(200, "Reader fixture").wire())
                    .await?;
                let mut line = String::new();
                loop {
                    line.clear();
                    if reader.read_line(&mut line).await? == 0 {
                        return Ok::<(), anyhow::Error>(());
                    }
                    let request = RequestContext::parse(line.as_bytes())
                        .context("invalid fixture request")?;
                    let wire: Arc<[u8]> = match request.kind() {
                        RequestKind::Article | RequestKind::Stat => {
                            let id = request
                                .message_id()
                                .context("fixture requires message-ID")?;
                            let prepared = current.lock().unwrap();
                            let reply = prepared.as_ref().context("no prepared fixture")?;
                            ensure!(reply.id == id, "request and fixture IDs differ");
                            if request.kind() == RequestKind::Stat {
                                counts.stats.fetch_add(1, Ordering::Relaxed);
                                if probe.as_u16() == 223 {
                                    ResponseFixture::reader(RequestKind::Stat, 0, id)
                                        .wire()
                                        .into()
                                } else {
                                    ResponseFixture::status(probe, "No such article")
                                        .wire()
                                        .into()
                                }
                            } else {
                                counts.articles.fetch_add(1, Ordering::Relaxed);
                                match lookup {
                                    Lookup::Present => reply.wire.clone(),
                                    Lookup::Missing => status(430, "No such article").wire().into(),
                                }
                            }
                        }
                        RequestKind::Group => status(211, "1 42 42 alt.test").wire().into(),
                        RequestKind::Mode => status(200, "Reader mode").wire().into(),
                        RequestKind::Quit => status(205, "Goodbye").wire().into(),
                        _ => status(500, "Unsupported fixture command").wire().into(),
                    };
                    if matches!(request.kind(), RequestKind::Article | RequestKind::Stat) {
                        wait_reply_delay(delay).await;
                    }
                    if request.kind() == RequestKind::Article && pacing.upstream() {
                        write_paced(reader.get_mut(), &wire, Duration::from_millis(1)).await?;
                    } else {
                        reader.get_mut().write_all(&wire).await?;
                    }
                    *final_write.lock().unwrap() = Some(Instant::now());
                }
            });
        }
    });
    Ok(Backend {
        server,
        counts,
        final_write,
        task,
    })
}

struct Lab {
    proxy: NntpProxy,
    client: TcpStream,
    backends: Vec<Backend>,
    current: CurrentReply,
    task: JoinHandle<Result<()>>,
}
impl Drop for Lab {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl Lab {
    async fn start(scenario: Scenario) -> Result<Self> {
        let current: CurrentReply = Arc::default();
        let mut backends = Vec::new();
        let (mode, payload_cache) = match scenario {
            Scenario::Reader {
                delay_ms,
                outcome,
                stat,
            } => {
                for (host, tier, lookup, probe, enabled) in [
                    ("127.0.0.1", 0, Lookup::Missing, StatusCode::new(430), 0),
                    (
                        "127.0.0.2",
                        1,
                        if outcome == Outcome::Missing {
                            Lookup::Missing
                        } else {
                            Lookup::Present
                        },
                        outcome.probe_code(),
                        stat,
                    ),
                ] {
                    backends.push(
                        start_backend(
                            BackendOptions {
                                host,
                                tier,
                                stat: enabled,
                                lookup,
                                probe,
                                delay: Duration::from_millis(delay_ms),
                                pacing: Pacing::None,
                            },
                            current.clone(),
                        )
                        .await?,
                    );
                }
                (RoutingMode::Hybrid, PayloadCache::Disabled)
            }
            Scenario::Payload { pacing, cache, .. } => {
                backends.push(
                    start_backend(
                        BackendOptions {
                            host: "127.0.0.1",
                            tier: 0,
                            stat: 0,
                            lookup: Lookup::Present,
                            probe: StatusCode::new(223),
                            delay: Duration::ZERO,
                            pacing,
                        },
                        current.clone(),
                    )
                    .await?,
                );
                (RoutingMode::PerCommand, cache)
            }
        };
        let mut config = Config {
            servers: backends
                .iter()
                .map(|backend| backend.server.clone())
                .collect(),
            cache: Some(Cache {
                article_cache_capacity: CacheCapacity::try_new(CACHE_BYTES)?,
                article_cache_ttl_secs: Duration::from_secs(300),
                store_article_bodies: payload_cache == PayloadCache::Enabled,
                adaptive_precheck: false,
                availability_index_path: None,
                disk: None,
            }),
            ..Config::default()
        };
        config.routing.reader_message_id_failover = true;
        config.memory.buffer_pool_count = 8;
        let proxy = NntpProxy::new(config, mode).await?;
        let listener = TcpListener::bind("127.0.0.1:0").await?;
        let address = listener.local_addr()?;
        let serving = proxy.clone();
        let task = tokio::spawn(async move {
            let mut clients = JoinSet::new();
            loop {
                let (stream, address) = listener.accept().await?;
                stream.set_nodelay(true)?;
                let proxy = serving.clone();
                clients.spawn(async move {
                    proxy
                        .handle_client_per_command_routing(stream, address.into())
                        .await
                });
            }
        });
        let mut client = TcpStream::connect(address).await?;
        client.set_nodelay(true)?;
        status(201, "NNTP Proxy Ready")
            .assert_status_received(&mut client)
            .await?;
        if matches!(scenario, Scenario::Reader { .. }) {
            RequestContext::parse(b"GROUP alt.test")
                .unwrap()
                .write_wire_to(&mut client)
                .await?;
            status(211, "1 42 42 alt.test")
                .assert_received(&mut client)
                .await?;
        }
        Ok(Self {
            proxy,
            client,
            backends,
            current,
            task,
        })
    }

    async fn request(&mut self, scenario: Scenario, index: usize) -> Result<Observation> {
        let id = request_id(index);
        ensure!(
            self.proxy
                .cache()
                .get_request_message_id(&id)
                .await
                .is_none(),
            "cold request reused a warm/cache-known ID"
        );
        let body_len = match scenario {
            Scenario::Reader { .. } => 64 * 1024,
            Scenario::Payload { size, .. } => payload_body_len(&id, size),
        };
        let payload = article(&id, body_len);
        let expected = match scenario {
            Scenario::Reader {
                outcome: Outcome::Missing,
                ..
            } => status(430, "No such article"),
            _ => payload.clone(),
        };
        let wire_len = expected.wire_len();
        *self.current.lock().unwrap() = Some(PreparedReply {
            id: id.clone(),
            wire: payload.wire().into(),
        });
        for backend in &self.backends {
            *backend.final_write.lock().unwrap() = None;
        }
        let command = format!("ARTICLE {id}");
        let request = RequestContext::parse(command.as_bytes()).unwrap();
        let mut actual = vec![0; wire_len];
        let started = Instant::now();
        request.write_wire_to(&mut self.client).await?;
        self.client.read_exact(&mut actual[..1]).await?;
        let first = Instant::now();
        let downstream =
            matches!(scenario, Scenario::Payload { pacing, .. } if pacing.downstream());
        let mut consumed = 1;
        while consumed < actual.len() {
            let end = if downstream {
                (consumed + CHUNK_BYTES).min(actual.len())
            } else {
                actual.len()
            };
            self.client.read_exact(&mut actual[consumed..end]).await?;
            consumed = end;
            if downstream && consumed < actual.len() {
                wait_pacing(Duration::from_millis(1)).await;
            }
        }
        let completed = Instant::now();
        expected.assert_received(&mut actual.as_slice()).await?;
        for _ in 0..128 {
            tokio::task::yield_now().await;
        }
        let admitted = self
            .proxy
            .cache()
            .get_request_message_id(&id)
            .await
            .is_some_and(|entry| entry.is_complete_article());
        let final_write = self
            .backends
            .last()
            .unwrap()
            .final_write
            .lock()
            .unwrap()
            .context("backend did not finish writing its fixture")?;
        Ok(Observation {
            first_byte: first.duration_since(started),
            complete: completed.duration_since(started),
            backend_complete: final_write.duration_since(started),
            first_before_backend_complete: first < final_write,
            admitted,
            body_len,
            wire_len,
        })
    }
}

struct Observation {
    first_byte: Duration,
    complete: Duration,
    backend_complete: Duration,
    first_before_backend_complete: bool,
    admitted: bool,
    body_len: usize,
    wire_len: usize,
}

struct Run {
    observations: Vec<Observation>,
    primary_articles: usize,
    secondary_articles: usize,
    secondary_stats: usize,
}

async fn run_case(case: &Case, requests: usize) -> Result<Run> {
    let mut lab = Lab::start(case.scenario).await?;
    let mut observations = Vec::new();
    for index in 0..requests {
        observations.push(lab.request(case.scenario, index).await?);
    }
    let primary_articles = lab.backends[0].counts.articles.load(Ordering::Relaxed);
    let secondary_articles = lab
        .backends
        .get(1)
        .map_or(0, |backend| backend.counts.articles.load(Ordering::Relaxed));
    let secondary_stats = lab
        .backends
        .get(1)
        .map_or(0, |backend| backend.counts.stats.load(Ordering::Relaxed));
    ensure!(
        primary_articles == requests,
        "fresh requests bypassed the primary backend"
    );
    match case.scenario {
        Scenario::Reader { outcome, stat, .. } => {
            let expected_articles = if outcome == Outcome::Missing && stat == 1 {
                0
            } else {
                requests
            };
            ensure!(
                secondary_articles == expected_articles,
                "unexpected fallback ARTICLE count"
            );
            ensure!(
                secondary_stats == usize::from(stat) * requests,
                "unexpected fallback STAT count"
            );
            ensure!(
                observations.iter().all(|entry| !entry.admitted),
                "reader availability-only case admitted payloads"
            );
        }
        Scenario::Payload { cache, .. } => {
            ensure!(
                observations
                    .iter()
                    .all(|entry| entry.admitted == (cache == PayloadCache::Enabled)),
                "payload capture admission differs from the selected cache policy"
            );
        }
    }
    Ok(Run {
        observations,
        primary_articles,
        secondary_articles,
        secondary_stats,
    })
}

fn quantile(values: &[Duration], percent: usize) -> f64 {
    let mut sorted = values.to_vec();
    sorted.sort_unstable();
    sorted[(sorted.len() * percent).div_ceil(100).saturating_sub(1)].as_secs_f64() * 1000.0
}

impl Run {
    fn print(&self, case: &Case) {
        println!(
            "# loopback; modeled per-reply service delay; 16KiB/1ms application pacing; first byte includes status; wire4m fits 4MiB capture budget; one client/current-thread runtime; 16MiB cache"
        );
        println!(
            "case,requests,body_bytes,wire_bytes,first_p50_ms,first_p95_ms,first_p99_ms,complete_p50_ms,complete_p95_ms,complete_p99_ms,backend_final_write_p50_ms,first_before_backend_final_write,primary_articles,secondary_articles,secondary_stats,payload_admissions"
        );
        let first: Vec<_> = self
            .observations
            .iter()
            .map(|entry| entry.first_byte)
            .collect();
        let complete: Vec<_> = self
            .observations
            .iter()
            .map(|entry| entry.complete)
            .collect();
        let backend: Vec<_> = self
            .observations
            .iter()
            .map(|entry| entry.backend_complete)
            .collect();
        let sample = &self.observations[0];
        println!(
            "{},{},{},{},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{:.3},{},{},{},{},{}",
            case.name,
            self.observations.len(),
            sample.body_len,
            sample.wire_len,
            quantile(&first, 50),
            quantile(&first, 95),
            quantile(&first, 99),
            quantile(&complete, 50),
            quantile(&complete, 95),
            quantile(&complete, 99),
            quantile(&backend, 50),
            self.observations
                .iter()
                .filter(|entry| entry.first_before_backend_complete)
                .count(),
            self.primary_articles,
            self.secondary_articles,
            self.secondary_stats,
            self.observations
                .iter()
                .filter(|entry| entry.admitted)
                .count()
        );
        println!(
            "# request,first_byte_ms,complete_ms,backend_final_write_ms,first_before_backend_final_write,payload_admitted"
        );
        for (index, entry) in self.observations.iter().enumerate() {
            println!(
                "# {},{:.3},{:.3},{:.3},{},{}",
                index,
                entry.first_byte.as_secs_f64() * 1000.0,
                entry.complete.as_secs_f64() * 1000.0,
                entry.backend_complete.as_secs_f64() * 1000.0,
                entry.first_before_backend_complete,
                entry.admitted
            );
        }
    }
}

async fn check_application_pacing() -> Result<()> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let address = listener.local_addr()?;
    let fixture = article("<pacing@bench.local>", 64 * 1024);
    let wire = fixture.wire();
    let server = tokio::spawn(async move {
        let (mut stream, _) = listener.accept().await?;
        write_paced(&mut stream, &wire, Duration::from_millis(5)).await
    });
    let started = Instant::now();
    let mut client = TcpStream::connect(address).await?;
    fixture.assert_received(&mut client).await?;
    ensure!(
        started.elapsed() >= Duration::from_millis(20),
        "application pacing was omitted from the actual socket exchange"
    );
    server.await??;
    println!("PASS actual socket transfer observes application pacing");
    Ok(())
}

async fn self_check() -> Result<()> {
    check_reply_delay().await?;
    let case = Case {
        name: "self-check-cold-payload".to_owned(),
        scenario: Scenario::Payload {
            size: 64 * 1024,
            pacing: Pacing::Both,
            cache: PayloadCache::Enabled,
        },
    };
    run_case(&case, 2).await?;
    println!("PASS two fresh IDs fetch and admit two real payloads");
    check_application_pacing().await?;
    for outcome in [
        Outcome::Present,
        Outcome::Missing,
        Outcome::Unsupported503,
        Outcome::Unknown500,
    ] {
        for stat in [0, 1] {
            let case = Case {
                name: "self-check-reader".to_owned(),
                scenario: Scenario::Reader {
                    delay_ms: 5,
                    outcome,
                    stat,
                },
            };
            run_case(&case, 2).await?;
        }
    }
    println!("PASS reader fallback/probe counter matrix and exact response identity");
    Ok(())
}
