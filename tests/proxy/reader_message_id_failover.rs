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
    let task = tokio::spawn(async move {
        let mut clients = tokio::task::JoinSet::new();
        while let Ok((stream, _)) = listener.accept().await {
            let observed = observed.clone();
            let responses = responses.clone();
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
                        | "ARTICLE <PAR2@Example.COM>"
                        | "STAT <PAR2@Example.COM>" => responses.lock().unwrap().clone(),
                        "QUIT" => status(205, "Goodbye"),
                        "MODE READER" => status(200, "Reader mode"),
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
