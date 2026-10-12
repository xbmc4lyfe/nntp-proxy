//! Reader metadata requests must be able to reach another provider after GROUP.

use crate::test_helpers::{
    connect_and_read_greeting, create_test_server_config_with_max_connections,
    spawn_test_proxy_on_random_port,
};
use anyhow::Result;
use nntp_proxy::protocol::{RequestContext, RequestKind, StatusCode};
use nntp_proxy::session::ResponseFixture;
use nntp_proxy::{Config, NntpProxy, RoutingMode};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::{TcpListener, TcpStream};

const ID: &str = "<PAR2@Example.COM>";
fn status(code: u16, detail: &str) -> ResponseFixture {
    ResponseFixture::status(StatusCode::new(code), detail)
}
fn metadata(kind: RequestKind) -> ResponseFixture {
    ResponseFixture::reader(kind, 0, ID)
        .header("Subject", "Original title")
        .body("metadata bytes")
}
fn group() -> ResponseFixture {
    status(211, "1 42 42 alt.test")
}
fn numbered(number: u64) -> ResponseFixture {
    ResponseFixture::reader(RequestKind::Stat, number, "<number@primary>")
}
fn prior() -> ResponseFixture {
    ResponseFixture::reader(RequestKind::Body, 42, "<prior@primary>").body("prior bytes")
}
async fn send(client: &mut TcpStream, command: &str) -> Result<()> {
    RequestContext::parse(command.as_bytes())
        .unwrap()
        .write_wire_to(client)
        .await?;
    client.flush().await?;
    Ok(())
}
async fn exchange(client: &mut TcpStream, command: &str, expected: &ResponseFixture) -> Result<()> {
    send(client, command).await?;
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        expected.assert_received(client),
    )
    .await??;
    Ok(())
}

struct ReaderBackend {
    config: nntp_proxy::config::Server,
    commands: Arc<Mutex<Vec<String>>>,
    lookup: Arc<Mutex<ResponseFixture>>,
    probe: Arc<Mutex<Option<ResponseFixture>>>,
    probe_gate: Arc<Mutex<Option<Arc<tokio::sync::Semaphore>>>>,
    partial_probe: Arc<Mutex<Option<Arc<tokio::sync::Semaphore>>>>,
    task: tokio::task::AbortHandle,
}
impl Drop for ReaderBackend {
    fn drop(&mut self) {
        self.task.abort();
    }
}
async fn reader_backend(host: &str, tier: u8, lookup: ResponseFixture) -> Result<ReaderBackend> {
    let listener = TcpListener::bind((host, 0)).await?;
    let port = listener.local_addr()?.port();
    let commands = Arc::new(Mutex::new(Vec::new()));
    let observed = commands.clone();
    let lookup = Arc::new(Mutex::new(lookup));
    let responses = lookup.clone();
    let probe = Arc::new(Mutex::new(None::<ResponseFixture>));
    let probe_responses = probe.clone();
    let probe_gate = Arc::new(Mutex::new(None::<Arc<tokio::sync::Semaphore>>));
    let gates = probe_gate.clone();
    let partial_probe = Arc::new(Mutex::new(None::<Arc<tokio::sync::Semaphore>>));
    let partial_probes = partial_probe.clone();
    let task = tokio::spawn(async move {
        let mut clients = tokio::task::JoinSet::new();
        while let Ok((stream, _)) = listener.accept().await {
            let observed = observed.clone();
            let responses = responses.clone();
            let probe_responses = probe_responses.clone();
            let gates = gates.clone();
            let partial_probes = partial_probes.clone();
            clients.spawn(async move {
                let mut stream = BufReader::new(stream);
                stream
                    .get_mut()
                    .write_all(&status(200, "Reader fixture").wire())
                    .await
                    .unwrap();
                let mut pointer = 42;
                loop {
                    let mut line = String::new();
                    if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    let command = line.trim_end();
                    observed.lock().unwrap().push(command.into());
                    if command == "STAT <PAR2@Example.COM>" {
                        let partial = partial_probes.lock().unwrap().clone();
                        if let Some(sent) = partial {
                            metadata(RequestKind::Stat)
                                .write_partial_status(stream.get_mut())
                                .await
                                .unwrap();
                            sent.add_permits(1);
                            assert_eq!(stream.read(&mut [0]).await.unwrap(), 0);
                            break;
                        }
                        let gate = gates.lock().unwrap().clone();
                        if let Some(gate) = gate {
                            gate.acquire().await.unwrap().forget();
                        }
                    }
                    let response = match command {
                        "GROUP alt.test" => {
                            pointer = 42;
                            group()
                        }
                        "STAT 7" => {
                            pointer = 7;
                            numbered(7)
                        }
                        "STAT" => numbered(pointer),
                        "BODY 42" => {
                            pointer = 42;
                            prior()
                        }
                        "BODY <PAR2@Example.COM>"
                        | "HEAD <PAR2@Example.COM>"
                        | "ARTICLE <PAR2@Example.COM>" => responses.lock().unwrap().clone(),
                        "STAT <PAR2@Example.COM>" => probe_responses
                            .lock()
                            .unwrap()
                            .clone()
                            .unwrap_or_else(|| responses.lock().unwrap().clone()),
                        "QUIT" => status(205, "Goodbye"),
                        "MODE READER" => status(200, "Reader mode"),
                        "DATE" => status(111, "20260503120000"),
                        _ => status(502, "Unsupported"),
                    };
                    if stream.get_mut().write_all(&response.wire()).await.is_err() {
                        break;
                    }
                    if command == "QUIT" {
                        break;
                    }
                }
            });
        }
    })
    .abort_handle();
    let mut config = create_test_server_config_with_max_connections(host, port, host, 1);
    config.tier = tier;
    Ok(ReaderBackend {
        config,
        commands,
        lookup,
        probe,
        probe_gate,
        partial_probe,
        task,
    })
}
async fn reader_proxy(
    backends: &[&ReaderBackend],
    enabled: bool,
) -> Result<(NntpProxy, TcpStream)> {
    reader_proxy_with_cache(backends, enabled, false).await
}
async fn reader_proxy_with_cache(
    backends: &[&ReaderBackend],
    enabled: bool,
    cache: bool,
) -> Result<(NntpProxy, TcpStream)> {
    reader_proxy_with_cache_ttl(
        backends,
        enabled,
        cache,
        std::time::Duration::from_secs(300),
    )
    .await
}

async fn reader_proxy_with_cache_ttl(
    backends: &[&ReaderBackend],
    enabled: bool,
    cache: bool,
    ttl: std::time::Duration,
) -> Result<(NntpProxy, TcpStream)> {
    let mut config = Config {
        servers: backends
            .iter()
            .map(|backend| backend.config.clone())
            .collect(),
        ..Config::default()
    };
    if cache {
        config.cache = Some(nntp_proxy::config::Cache {
            store_article_bodies: true,
            article_cache_ttl_secs: ttl,
            ..Default::default()
        });
    }
    let mut config_value = toml::Value::try_from(config)?;
    config_value["routing"].as_table_mut().unwrap().insert(
        "reader_message_id_failover".into(),
        toml::Value::Boolean(enabled),
    );
    let mut config: Config = config_value.try_into()?;
    // Operator TOML stores whole seconds; the expiry regression uses a
    // programmatic subsecond TTL after exercising flag deserialization.
    if let Some(cache) = &mut config.cache {
        cache.article_cache_ttl_secs = ttl;
    }
    let proxy = NntpProxy::new(config, RoutingMode::Hybrid).await?;
    let port = spawn_test_proxy_on_random_port(proxy.clone(), true).await?;
    Ok((proxy, connect_and_read_greeting(port).await?))
}

#[tokio::test]
async fn reader_selected_partial_stat_times_out_to_same_tier_without_missing_fact() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut stalled = reader_backend("127.0.0.2", 0, metadata(RequestKind::Body)).await?;
    stalled.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    stalled.config.stat_missing = 1;
    let partial_sent = Arc::new(tokio::sync::Semaphore::new(0));
    *stalled.partial_probe.lock().unwrap() = Some(partial_sent.clone());
    let mut healthy = reader_backend("127.0.0.3", 0, metadata(RequestKind::Body)).await?;
    healthy.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let mut lower = reader_backend("127.0.0.4", 1, metadata(RequestKind::Body)).await?;
    lower.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    // GROUP consumes the first round-robin selection; its retry selects the
    // second of the two eligible same-tier alternates.
    let (proxy, mut client) = reader_proxy(&[&primary, &healthy, &stalled, &lower], true).await?;
    let before = proxy.metrics().snapshot(None);
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    send(&mut client, &format!("BODY {ID}")).await?;
    // Preserve the existing retry-once policy: each actual probe gets one
    // status deadline and one command/traffic charge before same-tier fallback.
    for _ in 0..2 {
        tokio::time::timeout(std::time::Duration::from_secs(3), partial_sent.acquire())
            .await
            .unwrap_or_else(|error| {
                panic!(
                    "partial STAT never sent: {error}; primary={:?}; stalled={:?}; healthy={:?}",
                    primary.commands.lock().unwrap(),
                    stalled.commands.lock().unwrap(),
                    healthy.commands.lock().unwrap()
                )
            })?
            .forget();
        tokio::time::pause();
        tokio::time::advance(nntp_proxy::constants::timeout::BACKEND_READ).await;
        tokio::time::resume();
    }
    tokio::time::timeout(std::time::Duration::from_secs(3), metadata(RequestKind::Body).assert_received(&mut client))
        .await.unwrap_or_else(|error| panic!("selected partial STAT failed recovery: {error}; stalled={:?}; healthy={:?}; pools={:?}", stalled.commands.lock().unwrap(), healthy.commands.lock().unwrap(), proxy.router().backend_provider(nntp_proxy::types::BackendId::from_index(2)).unwrap().status_counts()))?;
    assert_eq!(
        proxy
            .cache()
            .get_request_message_id(ID)
            .await
            .unwrap()
            .availability()
            .missing_bits(),
        0b0001
    );
    let stalled_commands: Vec<_> = stalled
        .commands
        .lock()
        .unwrap()
        .iter()
        .filter(|command| command.ends_with(ID))
        .cloned()
        .collect();
    assert_eq!(
        stalled_commands,
        [format!("STAT {ID}"), format!("STAT {ID}")]
    );
    assert!(
        healthy
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| command == &format!("BODY {ID}"))
    );
    assert!(
        !lower
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| command.ends_with(ID))
    );
    let backend = nntp_proxy::types::BackendId::from_index(2);
    assert_eq!(
        proxy
            .router()
            .backend_provider(backend)
            .unwrap()
            .status_counts()
            .size,
        0
    );
    exchange(&mut client, "QUIT", &status(205, "Goodbye")).await?;
    assert_eq!(client.read(&mut [0]).await?, 0);
    let after = proxy.metrics().snapshot(None);
    assert_eq!(
        after.backend_stats[2].total_commands.get() - before.backend_stats[2].total_commands.get(),
        2
    );
    assert_eq!(
        after.backend_stats[2].bytes_sent.as_u64() - before.backend_stats[2].bytes_sent.as_u64(),
        2 * format!("STAT {ID}\r\n").len() as u64
    );
    assert_eq!(
        after.backend_stats[2].bytes_received.as_u64()
            - before.backend_stats[2].bytes_received.as_u64(),
        0
    );
    assert_eq!(
        after.user_stats[0].bytes_sent.as_u64() - before.user_stats[0].bytes_sent.as_u64(),
        format!("GROUP alt.test\r\nBODY {ID}\r\nQUIT\r\n").len() as u64
    );
    assert_eq!(
        after.user_stats[0].bytes_received.as_u64() - before.user_stats[0].bytes_received.as_u64(),
        (group().wire_len()
            + metadata(RequestKind::Body).wire_len()
            + status(205, "Goodbye").wire_len()) as u64
    );
    Ok(())
}

#[tokio::test]
async fn reader_body_message_id_recovers_after_group_without_spare_primary_connection() -> Result<()>
{
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing here")).await?;
    let secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Body)).await?;
    let (_, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(
        &mut client,
        "BODY <PAR2@Example.COM>",
        &metadata(RequestKind::Body),
    )
    .await?;
    Ok(())
}
#[tokio::test]
async fn reader_failover_default_keeps_primary_miss() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let secondary = reader_backend(
        "127.0.0.2",
        1,
        ResponseFixture::reader(RequestKind::Body, 0, ID).body("secondary"),
    )
    .await?;
    let (_, mut client) = reader_proxy(&[&primary, &secondary], false).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(
        &mut client,
        "BODY <PAR2@Example.COM>",
        &status(430, "Missing primary"),
    )
    .await?;
    assert!(
        !secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "BODY <PAR2@Example.COM>")
    );
    Ok(())
}
#[tokio::test]
async fn reader_failover_primary_hit_avoids_secondary_and_preserves_pointer() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, metadata(RequestKind::Stat)).await?;
    let secondary = reader_backend("127.0.0.2", 1, status(430, "Missing secondary")).await?;
    let (_, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(&mut client, "STAT 7", &numbered(7)).await?;
    exchange(
        &mut client,
        "STAT <PAR2@Example.COM>",
        &metadata(RequestKind::Stat),
    )
    .await?;
    exchange(&mut client, "STAT", &numbered(7)).await?;
    assert!(secondary.commands.lock().unwrap().is_empty());
    Ok(())
}
#[tokio::test]
async fn reader_failover_pipeline_keeps_payload_order_pointer_case_and_metrics() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Body)).await?;
    let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(&mut client, "STAT 7", &numbered(7)).await?;
    let mut batch = Vec::new();
    for command in ["BODY 42", "BODY <PAR2@Example.COM>", "POST", "STAT", "QUIT"] {
        RequestContext::parse(command.as_bytes())
            .unwrap()
            .write_wire_to(&mut batch)
            .await?;
    }
    client.write_all(&batch).await?;
    for expected in [
        prior(),
        metadata(RequestKind::Body),
        status(440, "Posting not permitted"),
        numbered(42),
        status(205, "Goodbye"),
    ] {
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            expected.assert_received(&mut client),
        )
        .await??;
    }
    assert!(
        secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "BODY <PAR2@Example.COM>")
    );
    assert!(
        !secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "STAT" || line == "GROUP alt.test")
    );
    assert_eq!(client.read(&mut [0]).await?, 0);
    let snapshot = proxy.metrics().snapshot(None);
    assert_eq!(
        snapshot.backend_stats[1].bytes_received.as_u64(),
        metadata(RequestKind::Body).wire_len() as u64
    );
    let primary_bytes: usize = [
        group(),
        numbered(7),
        prior(),
        status(440, "Posting not permitted"),
        numbered(42),
        status(205, "Goodbye"),
    ]
    .iter()
    .map(ResponseFixture::wire_len)
    .sum();
    assert_eq!(
        snapshot.backend_stats[0].bytes_received.as_u64(),
        primary_bytes as u64
    );
    assert_eq!(
        proxy
            .router()
            .backend_load(nntp_proxy::types::BackendId::from_index(1))
            .unwrap()
            .get(),
        0
    );
    Ok(())
}
#[tokio::test]
async fn reader_failover_exhausts_same_tier_before_lower_tier() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut peer = reader_backend("127.0.0.2", 0, status(430, "Missing peer")).await?;
    peer.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let secondary = reader_backend("127.0.0.3", 1, metadata(RequestKind::Stat)).await?;
    let (_, mut client) = reader_proxy(&[&primary, &peer, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(
        &mut client,
        "STAT <PAR2@Example.COM>",
        &metadata(RequestKind::Stat),
    )
    .await?;
    assert!(
        peer.commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "STAT <PAR2@Example.COM>")
    );
    Ok(())
}
#[tokio::test]
async fn reader_failover_non_missing_status_does_not_record_missing() -> Result<()> {
    for (code, detail) in [(423, "Missing number"), (412, "No group"), (400, "Closing")] {
        let primary = reader_backend("127.0.0.1", 0, status(code, detail)).await?;
        let secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Stat)).await?;
        let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(
            &mut client,
            "STAT <PAR2@Example.COM>",
            &status(code, detail),
        )
        .await?;
        assert!(proxy.cache().get_request_message_id(ID).await.is_none());
        assert!(secondary.commands.lock().unwrap().is_empty());
    }
    Ok(())
}
#[tokio::test]
async fn reader_failover_supports_each_message_id_reader_verb() -> Result<()> {
    for (verb, kind) in [
        ("ARTICLE", RequestKind::Article),
        ("HEAD", RequestKind::Head),
        ("BODY", RequestKind::Body),
        ("STAT", RequestKind::Stat),
    ] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let expected = ResponseFixture::reader(kind, 0, ID)
            .header("Subject", "Original title")
            .body("original body");
        let secondary = reader_backend("127.0.0.2", 1, expected.clone()).await?;
        let (_, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(&mut client, "STAT 7", &numbered(7)).await?;
        exchange(&mut client, &format!("{verb} {ID}"), &expected).await?;
        exchange(&mut client, "STAT", &numbered(7)).await?;
    }
    Ok(())
}
#[tokio::test]
async fn reader_failover_unavailable_peer_does_not_unlock_lower_tier() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut peer = reader_backend(
        "127.0.0.2",
        0,
        ResponseFixture::malformed_status("malformed status"),
    )
    .await?;
    peer.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let secondary = reader_backend("127.0.0.3", 1, metadata(RequestKind::Stat)).await?;
    let (proxy, mut client) = reader_proxy(&[&primary, &peer, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(
        &mut client,
        "STAT <PAR2@Example.COM>",
        &status(430, "No such article"),
    )
    .await?;
    assert_eq!(
        proxy
            .cache()
            .get_request_message_id(ID)
            .await
            .unwrap()
            .availability()
            .missing_bits(),
        1
    );
    assert!(secondary.commands.lock().unwrap().is_empty());
    assert_eq!(
        proxy
            .router()
            .backend_load(nntp_proxy::types::BackendId::from_index(1))
            .unwrap()
            .get(),
        0
    );
    Ok(())
}
#[tokio::test]
async fn reader_failover_disconnect_releases_retry_and_pinned_guards() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let secondary = reader_backend(
        "127.0.0.2",
        1,
        ResponseFixture::reader(RequestKind::Body, 0, ID).body(vec![
            b'x';
            16 * 1024 * 1024
                - status(
                    222,
                    "0 <PAR2@Example.COM>"
                )
                .wire_len()
        ]),
    )
    .await?;
    let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    send(&mut client, &format!("BODY {ID}")).await?;
    metadata(RequestKind::Body)
        .assert_status_received(&mut client)
        .await?;
    socket2::SockRef::from(&client).set_linger(Some(std::time::Duration::ZERO))?;
    drop(client);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if (0..2).all(|index| {
                let backend = nntp_proxy::types::BackendId::from_index(index);
                let status = proxy
                    .router()
                    .backend_provider(backend)
                    .unwrap()
                    .status_counts();
                proxy.router().backend_load(backend).unwrap().get() == 0
                    && status.size == status.available
            }) {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn reader_failover_serves_each_cached_variant_after_pinned_miss_and_exhausted_backups()
-> Result<()> {
    use nntp_proxy::cache::AvailabilitySlot;
    use nntp_proxy::client::NntpClient;
    use nntp_proxy::types::MessageId;
    for (warm_verb, warm_kind, verb, kind) in [
        ("BODY", RequestKind::Body, "BODY", RequestKind::Body),
        ("ARTICLE", RequestKind::Article, "HEAD", RequestKind::Head),
        (
            "ARTICLE",
            RequestKind::Article,
            "ARTICLE",
            RequestKind::Article,
        ),
        ("STAT", RequestKind::Stat, "STAT", RequestKind::Stat),
    ] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let mut secondary = reader_backend("127.0.0.2", 1, metadata(warm_kind)).await?;
        secondary.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
        let (proxy, mut client) =
            reader_proxy_with_cache(&[&primary, &secondary], true, true).await?;
        exchange(
            &mut client,
            &format!("{warm_verb} {ID}"),
            &metadata(warm_kind),
        )
        .await?;
        tokio::time::timeout(std::time::Duration::from_secs(3), async {
            loop {
                if proxy
                    .cache()
                    .get_request_message_id(ID)
                    .await
                    .is_some_and(|entry| entry.cached_response_for(kind, ID).is_some())
                {
                    break;
                }
                tokio::task::yield_now().await;
            }
        })
        .await?;
        *secondary.lookup.lock().unwrap() = status(430, "Gone secondary");
        let direct = NntpClient::new(
            proxy.connection_providers()[1].clone(),
            proxy.buffer_pool().clone(),
        );
        assert!(!direct.stat(&MessageId::new(ID.into())?).await?);
        proxy
            .cache()
            .record_availability_missing(
                MessageId::new(ID.into())?,
                AvailabilitySlot::new(1).unwrap(),
            )
            .await;
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(&mut client, "STAT 7", &numbered(7)).await?;
        let before = proxy.metrics().snapshot(None);
        let commands = secondary.commands.lock().unwrap().len();
        exchange(&mut client, &format!("{verb} {ID}"), &metadata(kind)).await?;
        let after = proxy.metrics().snapshot(None);
        assert_eq!(
            secondary.commands.lock().unwrap().len(),
            commands,
            "cached response must require no backup traffic"
        );
        assert_eq!(
            after.backend_stats[1].bytes_received.as_u64(),
            before.backend_stats[1].bytes_received.as_u64()
        );
        assert_eq!(
            after.backend_stats[0].bytes_received.as_u64(),
            before.backend_stats[0].bytes_received.as_u64()
                + (group().wire_len() + numbered(7).wire_len()) as u64
        );
        // The miss flushes the prior GROUP/STAT replies; cache bytes belong
        // only to the user receipt and never to the pinned provider.
        assert_eq!(
            after.backend_stats[0].errors_4xx.get(),
            before.backend_stats[0].errors_4xx.get() + 1
        );
        assert_eq!(
            after.user_stats[0].bytes_received.as_u64()
                - before.user_stats[0].bytes_received.as_u64(),
            (group().wire_len() + numbered(7).wire_len() + metadata(kind).wire_len()) as u64
        );
        exchange(&mut client, "STAT", &numbered(7)).await?;
    }
    Ok(())
}

async fn wait_cached_response(proxy: &NntpProxy, kind: RequestKind) -> Result<()> {
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            if proxy
                .cache()
                .get_request_message_id(ID)
                .await
                .is_some_and(|entry| entry.cached_response_for(kind, ID).is_some())
            {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn reader_failover_body_cache_serves_body_and_stat_but_retries_head_and_article() -> Result<()>
{
    for (verb, kind) in [
        ("BODY", RequestKind::Body),
        ("STAT", RequestKind::Stat),
        ("HEAD", RequestKind::Head),
        ("ARTICLE", RequestKind::Article),
    ] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let mut secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Body)).await?;
        secondary.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
        let (proxy, mut client) =
            reader_proxy_with_cache(&[&primary, &secondary], true, true).await?;
        exchange(
            &mut client,
            &format!("BODY {ID}"),
            &metadata(RequestKind::Body),
        )
        .await?;
        wait_cached_response(&proxy, RequestKind::Body).await?;
        *secondary.lookup.lock().unwrap() = metadata(kind);
        let before = secondary.commands.lock().unwrap().len();
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(&mut client, &format!("{verb} {ID}"), &metadata(kind)).await?;
        let expected_additional =
            usize::from(matches!(kind, RequestKind::Head | RequestKind::Article));
        assert_eq!(
            secondary.commands.lock().unwrap().len(),
            before + expected_additional,
            "{verb} compatibility must be enforced without reacquiring pinned max=1"
        );
        assert!(
            proxy
                .cache()
                .get_request_message_id(ID)
                .await
                .unwrap()
                .availability()
                .is_missing_slot(nntp_proxy::cache::AvailabilitySlot::new(0).unwrap())
        );
    }
    Ok(())
}

#[tokio::test]
async fn reader_failover_negative_cache_cannot_manufacture_a_payload_hit() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut secondary = reader_backend("127.0.0.2", 1, status(430, "Missing secondary")).await?;
    secondary.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let (proxy, mut client) = reader_proxy_with_cache(&[&primary, &secondary], true, true).await?;
    exchange(
        &mut client,
        &format!("BODY {ID}"),
        &status(430, "No such article"),
    )
    .await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(
        &mut client,
        &format!("BODY {ID}"),
        &status(430, "No such article"),
    )
    .await?;
    assert!(
        !proxy
            .cache()
            .get_request_message_id(ID)
            .await
            .unwrap()
            .is_complete_article()
    );
    Ok(())
}

#[tokio::test]
async fn reader_failover_expired_payload_is_refetched_after_pinned_miss() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Body)).await?;
    secondary.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let (proxy, mut client) = reader_proxy_with_cache_ttl(
        &[&primary, &secondary],
        true,
        true,
        std::time::Duration::from_millis(10),
    )
    .await?;
    exchange(
        &mut client,
        &format!("BODY {ID}"),
        &metadata(RequestKind::Body),
    )
    .await?;
    wait_cached_response(&proxy, RequestKind::Body).await?;
    let replacement = ResponseFixture::reader(RequestKind::Body, 0, ID).body("replacement bytes");
    *secondary.lookup.lock().unwrap() = replacement.clone();
    tokio::time::sleep(std::time::Duration::from_millis(120)).await;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(&mut client, &format!("BODY {ID}"), &replacement).await?;
    assert_eq!(
        secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command.as_str() == "BODY <PAR2@Example.COM>")
            .count(),
        2
    );
    Ok(())
}

#[tokio::test]
async fn reader_recovered_backend_command_counts_user_sent_once() -> Result<()> {
    for cache_hit in [false, true] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let mut secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Body)).await?;
        secondary.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
        let (proxy, mut client) =
            reader_proxy_with_cache(&[&primary, &secondary], true, cache_hit).await?;
        if cache_hit {
            exchange(
                &mut client,
                &format!("BODY {ID}"),
                &metadata(RequestKind::Body),
            )
            .await?;
            wait_cached_response(&proxy, RequestKind::Body).await?;
        }
        let before = proxy.metrics().snapshot(None);
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(
            &mut client,
            &format!("BODY {ID}"),
            &metadata(RequestKind::Body),
        )
        .await?;
        exchange(&mut client, "QUIT", &status(205, "Goodbye")).await?;
        assert_eq!(client.read(&mut [0]).await?, 0);
        let after = proxy.metrics().snapshot(None);
        // Frontend commands are counted once even when the proxy repeats BODY.
        let frontend_bytes = b"GROUP alt.test\r\nBODY <PAR2@Example.COM>\r\nQUIT\r\n".len() as u64;
        assert_eq!(
            after.user_stats[0].bytes_sent.as_u64() - before.user_stats[0].bytes_sent.as_u64(),
            frontend_bytes,
            "cache_hit={cache_hit}"
        );
        assert_eq!(
            after.backend_stats[0].bytes_sent.as_u64()
                - before.backend_stats[0].bytes_sent.as_u64(),
            frontend_bytes
        );
        assert_eq!(
            after.backend_stats[1].bytes_sent.as_u64()
                - before.backend_stats[1].bytes_sent.as_u64(),
            if cache_hit {
                0
            } else {
                b"BODY <PAR2@Example.COM>\r\n".len() as u64
            }
        );
        assert_eq!(
            after.client_to_backend_bytes.as_u64() - before.client_to_backend_bytes.as_u64(),
            frontend_bytes
                + if cache_hit {
                    0
                } else {
                    b"BODY <PAR2@Example.COM>\r\n".len() as u64
                }
        );
        assert_eq!(
            after.user_stats[0].bytes_received.as_u64()
                - before.user_stats[0].bytes_received.as_u64(),
            (group().wire_len()
                + metadata(RequestKind::Body).wire_len()
                + status(205, "Goodbye").wire_len()) as u64
        );
    }
    Ok(())
}

#[tokio::test]
async fn reader_retry_honors_first_alternate_stat_missing_probe() -> Result<()> {
    for (enabled, probe_code, expected) in [
        (0, 430, vec!["BODY <PAR2@Example.COM>"]),
        (
            1,
            223,
            vec!["STAT <PAR2@Example.COM>", "BODY <PAR2@Example.COM>"],
        ),
        (1, 430, vec!["STAT <PAR2@Example.COM>"]),
        (
            1,
            503,
            vec!["STAT <PAR2@Example.COM>", "BODY <PAR2@Example.COM>"],
        ),
    ] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let mut secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Body)).await?;
        secondary.config.stat_missing = enabled;
        *secondary.probe.lock().unwrap() = Some(if probe_code == 223 {
            metadata(RequestKind::Stat)
        } else {
            status(probe_code, "Probe outcome")
        });
        let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(
            &mut client,
            &format!("BODY {ID}"),
            &if enabled != 0 && probe_code == 430 {
                status(430, "No such article")
            } else {
                metadata(RequestKind::Body)
            },
        )
        .await?;
        let commands: Vec<String> = secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command.ends_with(ID))
            .cloned()
            .collect();
        assert_eq!(
            commands, expected,
            "stat_missing={enabled}, probe={probe_code}"
        );
        assert_eq!(
            proxy
                .cache()
                .get_request_message_id(ID)
                .await
                .unwrap()
                .availability()
                .missing_bits(),
            if enabled != 0 && probe_code == 430 {
                0b11
            } else {
                0b01
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn hybrid_default_and_stateful_count_all_frontend_command_bytes() -> Result<()> {
    for (mode, prelude) in [
        (RoutingMode::Hybrid, false),
        (RoutingMode::Hybrid, true),
        (RoutingMode::Stateful, false),
        (RoutingMode::Stateful, true),
    ] {
        let primary = reader_backend("127.0.0.1", 0, metadata(RequestKind::Body)).await?;
        let config = Config {
            servers: vec![primary.config.clone()],
            ..Default::default()
        };
        let proxy = NntpProxy::new(config, mode).await?;
        let port =
            spawn_test_proxy_on_random_port(proxy.clone(), mode == RoutingMode::Hybrid).await?;
        let mut client = connect_and_read_greeting(port).await?;
        let before = proxy.metrics().snapshot(None);
        if prelude {
            exchange(&mut client, "DATE", &status(111, "20260503120000")).await?;
        }
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(
            &mut client,
            &format!("BODY {ID}"),
            &metadata(RequestKind::Body),
        )
        .await?;
        exchange(&mut client, "QUIT", &status(205, "Goodbye")).await?;
        assert_eq!(client.read(&mut [0]).await?, 0);
        let after = proxy.metrics().snapshot(None);
        let frontend_bytes = b"GROUP alt.test\r\nBODY <PAR2@Example.COM>\r\nQUIT\r\n".len() as u64
            + if prelude { b"DATE\r\n".len() as u64 } else { 0 };
        assert_eq!(
            after.user_stats[0].bytes_sent.as_u64() - before.user_stats[0].bytes_sent.as_u64(),
            frontend_bytes,
            "mode={mode:?}"
        );
        assert_eq!(
            after.client_to_backend_bytes.as_u64() - before.client_to_backend_bytes.as_u64(),
            frontend_bytes
        );
        let response_bytes = (group().wire_len()
            + metadata(RequestKind::Body).wire_len()
            + status(205, "Goodbye").wire_len()
            + if prelude {
                status(111, "20260503120000").wire_len()
            } else {
                0
            }) as u64;
        assert_eq!(
            after.backend_stats[0].bytes_received.as_u64()
                - before.backend_stats[0].bytes_received.as_u64(),
            response_bytes
        );
        assert_eq!(
            after.backend_to_client_bytes.as_u64() - before.backend_to_client_bytes.as_u64(),
            response_bytes
        );
        assert_eq!(
            after.backend_stats[0].bytes_sent.as_u64()
                - before.backend_stats[0].bytes_sent.as_u64(),
            frontend_bytes,
            "mode={mode:?}"
        );
    }
    Ok(())
}

#[tokio::test]
async fn reader_retry_probes_do_not_unlock_lower_body_without_same_tier_430() -> Result<()> {
    for missing in [false, true] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let mut peer = reader_backend("127.0.0.2", 0, status(412, "No group peer")).await?;
        peer.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
        peer.config.stat_missing = 1;
        *peer.probe.lock().unwrap() = Some(status(if missing { 430 } else { 503 }, "Peer probe"));
        let mut lower = reader_backend("127.0.0.3", 1, metadata(RequestKind::Body)).await?;
        lower.config.stat_missing = 1;
        *lower.probe.lock().unwrap() = Some(metadata(RequestKind::Stat));
        let (proxy, mut client) = reader_proxy(&[&primary, &peer, &lower], true).await?;
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        exchange(
            &mut client,
            &format!("BODY {ID}"),
            &if missing {
                metadata(RequestKind::Body)
            } else {
                status(412, "No group peer")
            },
        )
        .await?;
        assert_eq!(
            peer.commands
                .lock()
                .unwrap()
                .iter()
                .filter(|command| command.as_str() == "BODY <PAR2@Example.COM>")
                .count(),
            usize::from(!missing),
            "selected503 permits the original same-tier BODY but never a lower-tier BODY"
        );
        assert_eq!(
            lower
                .commands
                .lock()
                .unwrap()
                .iter()
                .filter(|command| command.as_str() == "BODY <PAR2@Example.COM>")
                .count(),
            usize::from(missing)
        );
        assert_eq!(
            proxy
                .cache()
                .get_request_message_id(ID)
                .await
                .unwrap()
                .availability()
                .missing_bits(),
            if missing { 0b11 } else { 0b01 }
        );
    }
    Ok(())
}

#[tokio::test]
async fn reader_retry_stat_lookup_does_not_probe_stat_again() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut secondary = reader_backend("127.0.0.2", 1, metadata(RequestKind::Stat)).await?;
    secondary.config.stat_missing = 1;
    let (_, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    exchange(
        &mut client,
        &format!("STAT {ID}"),
        &metadata(RequestKind::Stat),
    )
    .await?;
    assert_eq!(
        secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command.as_str() == "STAT <PAR2@Example.COM>")
            .count(),
        1
    );
    Ok(())
}

#[tokio::test]
async fn reader_internal_stat_traffic_is_provider_only() -> Result<()> {
    let mut body_user_commands = None;
    for (verb, kind, stat_missing, probe_code, fetched, provider_commands) in [
        ("BODY", RequestKind::Body, 1, 223, true, 3),
        ("ARTICLE", RequestKind::Article, 1, 430, false, 1),
        ("BODY", RequestKind::Body, 1, 503, true, 3),
        ("BODY", RequestKind::Body, 0, 430, true, 2),
        ("STAT", RequestKind::Stat, 1, 223, true, 2),
    ] {
        let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
        let mut secondary = reader_backend("127.0.0.2", 1, metadata(kind)).await?;
        secondary.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
        secondary.config.stat_missing = stat_missing;
        let probe = if probe_code == 223 {
            metadata(RequestKind::Stat)
        } else {
            status(probe_code, "Probe outcome")
        };
        *secondary.probe.lock().unwrap() = Some(probe.clone());
        let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
        let before = proxy.metrics().snapshot(None);
        exchange(&mut client, "GROUP alt.test", &group()).await?;
        let response = if fetched {
            metadata(kind)
        } else {
            status(430, "No such article")
        };
        exchange(&mut client, &format!("{verb} {ID}"), &response).await?;
        exchange(&mut client, "QUIT", &status(205, "Goodbye")).await?;
        assert_eq!(client.read(&mut [0]).await?, 0);
        let after = proxy.metrics().snapshot(None);
        // Retain ordinary lookup counter semantics; internal STAT adds one.
        assert_eq!(
            after.backend_stats[1].total_commands.get()
                - before.backend_stats[1].total_commands.get(),
            provider_commands
        );
        let commands: Vec<String> = secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .filter(|command| command.ends_with(ID))
            .cloned()
            .collect();
        let internal_probe = stat_missing != 0 && kind != RequestKind::Stat;
        let expected_commands = match (internal_probe, fetched) {
            (true, true) => vec![format!("STAT {ID}"), format!("{verb} {ID}")],
            (true, false) => vec![format!("STAT {ID}")],
            (false, true) => vec![format!("{verb} {ID}")],
            (false, false) => unreachable!(),
        };
        assert_eq!(
            commands, expected_commands,
            "verb={verb}, probe={probe_code}"
        );
        let provider_sent = expected_commands
            .iter()
            .map(|command| command.len() as u64 + 2)
            .sum::<u64>();
        let provider_received = if internal_probe {
            probe.wire_len() as u64
                + if fetched {
                    metadata(kind).wire_len() as u64
                } else {
                    0
                }
        } else {
            metadata(kind).wire_len() as u64
        };
        assert_eq!(
            after.backend_stats[1].bytes_sent.as_u64()
                - before.backend_stats[1].bytes_sent.as_u64(),
            provider_sent,
            "verb={verb}, probe={probe_code}"
        );
        assert_eq!(
            after.backend_stats[1].bytes_received.as_u64()
                - before.backend_stats[1].bytes_received.as_u64(),
            provider_received,
            "verb={verb}, probe={probe_code}"
        );
        let frontend_sent = format!("GROUP alt.test\r\n{verb} {ID}\r\nQUIT\r\n").len() as u64;
        assert_eq!(
            after.user_stats[0].bytes_sent.as_u64() - before.user_stats[0].bytes_sent.as_u64(),
            frontend_sent
        );
        let frontend_received =
            (group().wire_len() + response.wire_len() + status(205, "Goodbye").wire_len()) as u64;
        assert_eq!(
            after.user_stats[0].bytes_received.as_u64()
                - before.user_stats[0].bytes_received.as_u64(),
            frontend_received
        );
        if kind == RequestKind::Body && fetched {
            let commands = after.user_stats[0].total_commands.get()
                - before.user_stats[0].total_commands.get();
            if let Some(previous) = body_user_commands {
                assert_eq!(
                    commands, previous,
                    "internal probes must not add USER commands"
                );
            }
            body_user_commands = Some(commands);
        }
        assert_eq!(
            after.client_to_backend_bytes.as_u64() - before.client_to_backend_bytes.as_u64(),
            frontend_sent + provider_sent
        );
        assert_eq!(
            after.backend_to_client_bytes.as_u64() - before.backend_to_client_bytes.as_u64(),
            (group().wire_len() + status(205, "Goodbye").wire_len()) as u64 + provider_received
        );
        assert_eq!(
            proxy
                .cache()
                .get_request_message_id(ID)
                .await
                .unwrap()
                .availability()
                .missing_bits(),
            if internal_probe && probe_code == 430 {
                0b11
            } else {
                0b01
            }
        );
    }
    Ok(())
}

#[tokio::test]
async fn reader_current_tier_hit_does_not_wait_for_held_lower_stat() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, status(430, "Missing primary")).await?;
    let mut current = reader_backend("127.0.0.2", 0, metadata(RequestKind::Body)).await?;
    current.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    current.config.stat_missing = 1;
    *current.probe.lock().unwrap() = Some(metadata(RequestKind::Stat));
    let mut lower = reader_backend("127.0.0.3", 1, metadata(RequestKind::Body)).await?;
    lower.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    lower.config.stat_missing = 1;
    *lower.probe.lock().unwrap() = Some(metadata(RequestKind::Stat));
    let held_reply = Arc::new(tokio::sync::Semaphore::new(0));
    *lower.probe_gate.lock().unwrap() = Some(held_reply.clone());
    let (_proxy, mut client) = reader_proxy(&[&primary, &current, &lower], true).await?;
    exchange(&mut client, "GROUP alt.test", &group()).await?;
    send(&mut client, &format!("BODY {ID}")).await?;
    // This is a bounded liveness guard, not a throughput threshold: the actual
    // lower reply stays held until the complete current-tier response arrives.
    let recovered = tokio::time::timeout(
        std::time::Duration::from_secs(10),
        metadata(RequestKind::Body).assert_received(&mut client),
    )
    .await;
    assert_eq!(held_reply.available_permits(), 0);
    assert!(
        matches!(recovered, Ok(Ok(()))),
        "current-tier response waited for held lower STAT; current={:?}, lower={:?}",
        current.commands.lock().unwrap(),
        lower.commands.lock().unwrap()
    );
    held_reply.add_permits(1);
    let current_commands: Vec<String> = current
        .commands
        .lock()
        .unwrap()
        .iter()
        .filter(|command| command.ends_with(ID))
        .cloned()
        .collect();
    assert_eq!(
        current_commands,
        [format!("STAT {ID}"), format!("BODY {ID}")]
    );
    assert!(
        !lower
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|command| command == &format!("BODY {ID}"))
    );
    Ok(())
}
