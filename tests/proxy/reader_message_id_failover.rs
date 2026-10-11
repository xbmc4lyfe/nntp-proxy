//! Reader metadata requests must be able to reach another provider after GROUP.

use anyhow::Result;
use nntp_proxy::{Config, NntpProxy, RoutingMode};
use std::sync::{Arc, Mutex};
use tokio::io::{AsyncBufReadExt, AsyncReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use crate::test_helpers::{
    MockNntpServer, connect_and_read_greeting, create_test_server_config_with_max_connections,
    send_command_read_line, spawn_proxy_with_config, spawn_test_proxy_on_random_port,
};

#[tokio::test]
async fn reader_body_message_id_recovers_after_group_without_spare_primary_connection() -> Result<()>
{
    let (primary, primary_task) = MockNntpServer::new()
        .on_command("GROUP", "211 1 42 42 alt.test\r\n")
        .on_command("BODY", "430 Missing here\r\n")
        .spawn_on_random_port()
        .await?;
    let (secondary, secondary_task) = MockNntpServer::new()
        .on_command(
            "BODY",
            "222 0 <PAR2@Example.COM>\r\nmetadata bytes\r\n.\r\n",
        )
        .spawn_on_random_port()
        .await?;
    let mut secondary_config =
        create_test_server_config_with_max_connections("localhost", secondary, "secondary", 1);
    secondary_config.tier = 1;
    let config = Config {
        servers: vec![
            create_test_server_config_with_max_connections("127.0.0.1", primary, "primary", 1),
            secondary_config,
        ],
        ..Config::default()
    };
    // Exercise the operator-facing setting through deserialization.
    let mut config_value = toml::Value::try_from(config)?;
    config_value["routing"].as_table_mut().unwrap().insert(
        "reader_message_id_failover".into(),
        toml::Value::Boolean(true),
    );
    let config: Config = config_value.try_into()?;
    let proxy = spawn_proxy_with_config(config, RoutingMode::Hybrid).await?;
    let mut client = connect_and_read_greeting(proxy).await?;
    assert_eq!(
        send_command_read_line(&mut client, "GROUP alt.test").await?,
        "211 1 42 42 alt.test\r\n"
    );
    assert_eq!(
        send_command_read_line(&mut client, "BODY <PAR2@Example.COM>").await?,
        "222 0 <PAR2@Example.COM>\r\n"
    );
    let mut body = [0; b"metadata bytes\r\n.\r\n".len()];
    client.read_exact(&mut body).await?;
    assert_eq!(&body, b"metadata bytes\r\n.\r\n");
    primary_task.abort();
    secondary_task.abort();
    Ok(())
}

struct ReaderBackend {
    config: nntp_proxy::config::Server,
    commands: Arc<Mutex<Vec<String>>>,
    task: tokio::task::AbortHandle,
}

impl Drop for ReaderBackend {
    fn drop(&mut self) {
        self.task.abort();
    }
}

async fn reader_backend(host: &str, tier: u8, lookup: &[u8]) -> Result<ReaderBackend> {
    let listener = TcpListener::bind((host, 0)).await?;
    let port = listener.local_addr()?.port();
    let commands = Arc::new(Mutex::new(Vec::new()));
    let observed = commands.clone();
    let lookup: Arc<[u8]> = lookup.to_vec().into();
    let task = tokio::spawn(async move {
        let mut clients = tokio::task::JoinSet::new();
        while let Ok((stream, _)) = listener.accept().await {
            let observed = observed.clone();
            let lookup = lookup.clone();
            clients.spawn(async move {
                let mut stream = BufReader::new(stream);
                stream
                    .get_mut()
                    .write_all(b"200 Reader fixture\r\n")
                    .await
                    .unwrap();
                let mut pointer = 42;
                loop {
                    let mut line = String::new();
                    if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                        break;
                    }
                    observed.lock().unwrap().push(line.clone());
                    let response = match line.trim_end() {
                        "GROUP alt.test" => {
                            pointer = 42;
                            b"211 1 42 42 alt.test\r\n".to_vec()
                        }
                        "STAT 7" => {
                            pointer = 7;
                            b"223 7 <number@primary>\r\n".to_vec()
                        }
                        "STAT" => format!("223 {pointer} <number@primary>\r\n").into_bytes(),
                        "BODY 42" => {
                            pointer = 42;
                            b"222 42 <prior@primary>\r\nprior bytes\r\n.\r\n".to_vec()
                        }
                        "BODY <PAR2@Example.COM>"
                        | "HEAD <PAR2@Example.COM>"
                        | "ARTICLE <PAR2@Example.COM>"
                        | "STAT <PAR2@Example.COM>" => lookup.to_vec(),
                        "QUIT" => b"205 Goodbye\r\n".to_vec(),
                        "MODE READER" => b"200 Reader mode\r\n".to_vec(),
                        _ => b"502 Unsupported\r\n".to_vec(),
                    };
                    if stream.get_mut().write_all(&response).await.is_err() {
                        break;
                    }
                    if line.trim_end() == "QUIT" {
                        break;
                    }
                }
            });
        }
    })
    .abort_handle();
    let config = create_test_server_config_with_max_connections(host, port, host, 1);
    let mut config = config;
    config.tier = tier;
    Ok(ReaderBackend {
        config,
        commands,
        task,
    })
}

async fn reader_proxy(
    backends: &[&ReaderBackend],
    enabled: bool,
) -> Result<(NntpProxy, tokio::net::TcpStream)> {
    let mut config = Config {
        servers: backends
            .iter()
            .map(|backend| backend.config.clone())
            .collect(),
        ..Config::default()
    };
    config.routing.reader_message_id_failover = enabled;
    let proxy = NntpProxy::new(config, RoutingMode::Hybrid).await?;
    let port = spawn_test_proxy_on_random_port(proxy.clone(), true).await?;
    let client = connect_and_read_greeting(port).await?;
    Ok((proxy, client))
}

#[tokio::test]
async fn reader_failover_default_keeps_primary_miss() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, b"430 Missing primary\r\n").await?;
    let secondary = reader_backend(
        "127.0.0.2",
        1,
        b"222 0 <PAR2@Example.COM>\r\nsecondary\r\n.\r\n",
    )
    .await?;
    let (_, mut client) = reader_proxy(&[&primary, &secondary], false).await?;
    send_command_read_line(&mut client, "GROUP alt.test").await?;
    assert_eq!(
        send_command_read_line(&mut client, "BODY <PAR2@Example.COM>").await?,
        "430 Missing primary\r\n"
    );
    assert!(
        !secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "BODY <PAR2@Example.COM>\r\n")
    );
    Ok(())
}

#[tokio::test]
async fn reader_failover_primary_hit_avoids_secondary_and_preserves_pointer() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, b"223 0 <PAR2@Example.COM>\r\n").await?;
    let secondary = reader_backend("127.0.0.2", 1, b"430 Missing secondary\r\n").await?;
    let (_, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    send_command_read_line(&mut client, "GROUP alt.test").await?;
    send_command_read_line(&mut client, "STAT 7").await?;
    assert_eq!(
        send_command_read_line(&mut client, "STAT <PAR2@Example.COM>").await?,
        "223 0 <PAR2@Example.COM>\r\n"
    );
    assert_eq!(
        send_command_read_line(&mut client, "STAT").await?,
        "223 7 <number@primary>\r\n"
    );
    assert!(secondary.commands.lock().unwrap().is_empty());
    Ok(())
}

#[tokio::test]
async fn reader_failover_pipeline_keeps_payload_order_pointer_case_and_metrics() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, b"430 Missing primary\r\n").await?;
    let secondary = reader_backend(
        "127.0.0.2",
        1,
        b"222 0 <PAR2@Example.COM>\r\nmetadata bytes\r\n.\r\n",
    )
    .await?;
    let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    send_command_read_line(&mut client, "GROUP alt.test").await?;
    send_command_read_line(&mut client, "STAT 7").await?;
    client
        .write_all(b"BODY 42\r\nBODY <PAR2@Example.COM>\r\nPOST\r\nSTAT\r\nQUIT\r\n")
        .await?;
    let expected = b"222 42 <prior@primary>\r\nprior bytes\r\n.\r\n222 0 <PAR2@Example.COM>\r\nmetadata bytes\r\n.\r\n440 Posting not permitted\r\n223 42 <number@primary>\r\n205 Goodbye\r\n";
    let mut response = vec![0; expected.len()];
    tokio::time::timeout(
        std::time::Duration::from_secs(3),
        client.read_exact(&mut response),
    )
    .await??;
    assert_eq!(response, expected);
    assert!(
        secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "BODY <PAR2@Example.COM>\r\n")
    );
    assert!(
        !secondary
            .commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "STAT\r\n" || line == "GROUP alt.test\r\n")
    );
    let mut eof = [0];
    assert_eq!(client.read(&mut eof).await?, 0);
    let snapshot = proxy.metrics().snapshot(None);
    assert_eq!(
        snapshot.backend_stats[1].bytes_received.as_u64(),
        b"222 0 <PAR2@Example.COM>\r\nmetadata bytes\r\n.\r\n".len() as u64
    );
    let primary_bytes = b"211 1 42 42 alt.test\r\n223 7 <number@primary>\r\n222 42 <prior@primary>\r\nprior bytes\r\n.\r\n440 Posting not permitted\r\n223 42 <number@primary>\r\n205 Goodbye\r\n".len() as u64;
    assert_eq!(
        snapshot.backend_stats[0].bytes_received.as_u64(),
        primary_bytes
    );
    assert_eq!(
        proxy
            .router()
            .backend_load(nntp_proxy::types::BackendId::from_index(1))
            .map(|count| count.get()),
        Some(0)
    );
    Ok(())
}

#[tokio::test]
async fn reader_failover_exhausts_same_tier_before_lower_tier() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, b"430 Missing primary\r\n").await?;
    let mut peer = reader_backend("127.0.0.2", 0, b"430 Missing peer\r\n").await?;
    peer.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let secondary = reader_backend("127.0.0.3", 1, b"223 0 <PAR2@Example.COM>\r\n").await?;
    let (_, mut client) = reader_proxy(&[&primary, &peer, &secondary], true).await?;
    send_command_read_line(&mut client, "GROUP alt.test").await?;
    assert_eq!(
        send_command_read_line(&mut client, "STAT <PAR2@Example.COM>").await?,
        "223 0 <PAR2@Example.COM>\r\n"
    );
    assert!(
        peer.commands
            .lock()
            .unwrap()
            .iter()
            .any(|line| line == "STAT <PAR2@Example.COM>\r\n")
    );
    Ok(())
}

#[tokio::test]
async fn reader_failover_non_missing_status_does_not_record_missing() -> Result<()> {
    for response in [
        b"423 Missing number\r\n".as_slice(),
        b"412 No group\r\n",
        b"400 Closing\r\n",
    ] {
        let primary = reader_backend("127.0.0.1", 0, response).await?;
        let secondary = reader_backend("127.0.0.2", 1, b"223 0 <PAR2@Example.COM>\r\n").await?;
        let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
        send_command_read_line(&mut client, "GROUP alt.test").await?;
        assert_eq!(
            send_command_read_line(&mut client, "STAT <PAR2@Example.COM>")
                .await?
                .as_bytes(),
            response
        );
        assert!(
            proxy
                .cache()
                .get_request_message_id("<PAR2@Example.COM>")
                .await
                .is_none()
        );
        assert!(secondary.commands.lock().unwrap().is_empty());
    }
    Ok(())
}

#[tokio::test]
async fn reader_failover_supports_each_message_id_reader_verb() -> Result<()> {
    for (verb, response) in [
        (
            "ARTICLE",
            b"220 0 <PAR2@Example.COM>\r\nSubject: Original title\r\n\r\noriginal body\r\n.\r\n"
                .as_slice(),
        ),
        (
            "HEAD",
            b"221 0 <PAR2@Example.COM>\r\nSubject: Original title\r\n.\r\n",
        ),
        (
            "BODY",
            b"222 0 <PAR2@Example.COM>\r\noriginal body\r\n.\r\n",
        ),
        ("STAT", b"223 0 <PAR2@Example.COM>\r\n"),
    ] {
        let primary = reader_backend("127.0.0.1", 0, b"430 Missing primary\r\n").await?;
        let secondary = reader_backend("127.0.0.2", 1, response).await?;
        let (_, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
        send_command_read_line(&mut client, "GROUP alt.test").await?;
        send_command_read_line(&mut client, "STAT 7").await?;
        client
            .write_all(format!("{verb} <PAR2@Example.COM>\r\n").as_bytes())
            .await?;
        let mut returned = vec![0; response.len()];
        tokio::time::timeout(
            std::time::Duration::from_secs(3),
            client.read_exact(&mut returned),
        )
        .await??;
        assert_eq!(
            returned, response,
            "{verb} must forward the provider's bytes"
        );
        assert_eq!(
            send_command_read_line(&mut client, "STAT").await?,
            "223 7 <number@primary>\r\n"
        );
    }
    Ok(())
}

#[tokio::test]
async fn reader_failover_unavailable_peer_does_not_unlock_lower_tier() -> Result<()> {
    let primary = reader_backend("127.0.0.1", 0, b"430 Missing primary\r\n").await?;
    let mut peer = reader_backend("127.0.0.2", 0, b"malformed status\r\n").await?;
    peer.config.stateful_sessions = nntp_proxy::config::StatefulSessions::Excluded;
    let secondary = reader_backend("127.0.0.3", 1, b"223 0 <PAR2@Example.COM>\r\n").await?;
    let (proxy, mut client) = reader_proxy(&[&primary, &peer, &secondary], true).await?;
    send_command_read_line(&mut client, "GROUP alt.test").await?;
    assert_eq!(
        send_command_read_line(&mut client, "STAT <PAR2@Example.COM>").await?,
        "430 No such article\r\n"
    );
    let cached = proxy
        .cache()
        .get_request_message_id("<PAR2@Example.COM>")
        .await
        .unwrap();
    assert_eq!(
        cached.availability().missing_bits(),
        1,
        "only primary actually returned 430"
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
    let primary = reader_backend("127.0.0.1", 0, b"430 Missing primary\r\n").await?;
    let mut response = b"222 0 <PAR2@Example.COM>\r\n".to_vec();
    response.resize(16 * 1024 * 1024, b'x');
    response.extend_from_slice(b"\r\n.\r\n");
    let secondary = reader_backend("127.0.0.2", 1, &response).await?;
    let (proxy, mut client) = reader_proxy(&[&primary, &secondary], true).await?;
    send_command_read_line(&mut client, "GROUP alt.test").await?;
    assert_eq!(
        send_command_read_line(&mut client, "BODY <PAR2@Example.COM>").await?,
        "222 0 <PAR2@Example.COM>\r\n"
    );
    socket2::SockRef::from(&client).set_linger(Some(std::time::Duration::ZERO))?;
    drop(client);
    tokio::time::timeout(std::time::Duration::from_secs(3), async {
        loop {
            let idle = (0..2).all(|index| {
                let backend = nntp_proxy::types::BackendId::from_index(index);
                let status = proxy
                    .router()
                    .backend_provider(backend)
                    .unwrap()
                    .status_counts();
                proxy.router().backend_load(backend).unwrap().get() == 0
                    && status.size == status.available
            });
            if idle {
                break;
            }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    })
    .await?;
    Ok(())
}
