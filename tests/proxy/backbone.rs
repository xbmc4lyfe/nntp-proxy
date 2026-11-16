//! Servers that name the same `backbone` share `430` facts.
//!
//! Resellers of one backbone serve the same article store, so once one of
//! them answers `430` the proxy must not ask its siblings. Any other failure
//! (a refused lookup, a server error, a refused login) is about that server,
//! not the article, so a sibling on the same backbone is still tried.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
use tokio::net::TcpListener;

use nntp_proxy::config::{Routing, Server};
use nntp_proxy::types::{MaxConnections, Port};
use nntp_proxy::{Config, NntpProxy, RoutingMode};

use crate::test_helpers::{
    connect_and_read_greeting, independent_provider_host, spawn_test_proxy_on_random_port,
};

#[derive(Clone, Copy)]
enum Behaviour {
    Has,
    Missing,
    /// Refuses lookups with a non-430 error, as some providers do with `451`.
    Refuses,
    /// Answers lookups with a server error.
    ServerError,
    /// Refuses every login at the greeting: the account is full.
    TooManyConnections,
}

struct Backend {
    name: &'static str,
    tier: u8,
    backbone: Option<&'static str>,
    behaviour: Behaviour,
}

/// A backend that answers every message-ID lookup the same way. `lookups`
/// counts lookups, or for a full account the logins it refused.
async fn spawn_backend(behaviour: Behaviour, lookups: Arc<AtomicUsize>) -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let lookups = lookups.clone();
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                if matches!(behaviour, Behaviour::TooManyConnections) {
                    lookups.fetch_add(1, Ordering::Relaxed);
                    let _ = stream
                        .get_mut()
                        .write_all(b"502 too many connections\r\n")
                        .await;
                    return;
                }
                if stream.get_mut().write_all(b"200 ready\r\n").await.is_err() {
                    return;
                }
                let mut line = String::new();
                loop {
                    line.clear();
                    if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let command = line.trim_end().to_string();
                    let reply = match command.split_once(' ') {
                        Some((verb @ ("ARTICLE" | "STAT"), id)) => {
                            lookups.fetch_add(1, Ordering::Relaxed);
                            match behaviour {
                                Behaviour::Has if verb == "STAT" => format!("223 0 {id}\r\n"),
                                Behaviour::Has => {
                                    format!("220 0 {id}\r\nSubject: x\r\n\r\nbody\r\n.\r\n")
                                }
                                Behaviour::Missing => "430 no such article\r\n".to_string(),
                                Behaviour::Refuses => format!("451 0 {id}\r\n"),
                                Behaviour::ServerError => "503 internal error\r\n".to_string(),
                                Behaviour::TooManyConnections => unreachable!(),
                            }
                        }
                        _ => match command.as_str() {
                            "MODE READER" => "200 reader\r\n".to_string(),
                            "DATE" => "111 20260101000000\r\n".to_string(),
                            "QUIT" => "205 bye\r\n".to_string(),
                            _ => "500 unknown command\r\n".to_string(),
                        },
                    };
                    if stream.get_mut().write_all(reply.as_bytes()).await.is_err() {
                        return;
                    }
                }
            });
        }
    });
    Ok(port)
}

async fn start_proxy(backends: &[Backend]) -> Result<(u16, Vec<Arc<AtomicUsize>>)> {
    start_proxy_with(backends, Routing::default()).await
}

async fn start_proxy_with(
    backends: &[Backend],
    routing: Routing,
) -> Result<(u16, Vec<Arc<AtomicUsize>>)> {
    let mut servers = Vec::new();
    let mut counters = Vec::new();
    for (index, backend) in backends.iter().enumerate() {
        let lookups = Arc::new(AtomicUsize::new(0));
        let port = spawn_backend(backend.behaviour, lookups.clone()).await?;
        counters.push(lookups);
        // Distinct hosts: only the backbone may join two backends.
        let mut server = Server::builder(independent_provider_host(index), Port::try_new(port)?)
            .name(backend.name)
            .tier(backend.tier)
            .max_connections(MaxConnections::try_new(2)?)
            .prewarm_connections(0)
            .compress(Some(false));
        if let Some(backbone) = backend.backbone {
            server = server.backbone(backbone);
        }
        servers.push(server.build()?);
    }
    let config = Config {
        servers,
        routing,
        ..Config::default()
    };
    let proxy = NntpProxy::builder(config)
        .with_routing_mode(RoutingMode::PerCommand)
        .build()
        .await?;
    let port = spawn_test_proxy_on_random_port(proxy, true).await?;
    Ok((port, counters))
}

/// One `ARTICLE` per distinct message-ID, each on a fresh client
/// connection; returns the status codes.
async fn article_each(port: u16, count: usize) -> Result<Vec<String>> {
    let mut codes = Vec::with_capacity(count);
    for index in 0..count {
        let client = connect_and_read_greeting(port).await?;
        let mut client = BufReader::new(client);
        client
            .get_mut()
            .write_all(format!("ARTICLE <bb-{index}@example.com>\r\n").as_bytes())
            .await?;
        let mut line = String::new();
        client.read_line(&mut line).await?;
        codes.push(line.chars().take(3).collect());
    }
    Ok(codes)
}

fn lookups(counter: &AtomicUsize) -> usize {
    counter.load(Ordering::Relaxed)
}

const fn backend(
    name: &'static str,
    tier: u8,
    backbone: Option<&'static str>,
    behaviour: Behaviour,
) -> Backend {
    Backend {
        name,
        tier,
        backbone,
        behaviour,
    }
}

#[tokio::test]
async fn a_430_from_one_reseller_skips_the_rest_of_its_backbone() -> Result<()> {
    let (port, counters) = start_proxy(&[
        backend("reseller-a", 0, Some("omicron"), Behaviour::Missing),
        backend("reseller-b", 1, Some("Omicron"), Behaviour::Has),
    ])
    .await?;

    let codes = article_each(port, 6).await?;

    assert!(codes.iter().all(|code| code == "430"), "{codes:?}");
    assert_eq!(lookups(&counters[0]), 6);
    assert_eq!(
        lookups(&counters[1]),
        0,
        "a sibling on the same backbone must not be asked after a 430"
    );
    Ok(())
}

#[tokio::test]
async fn without_a_backbone_the_next_server_is_still_asked_after_a_430() -> Result<()> {
    let (port, counters) = start_proxy(&[
        backend("reseller-a", 0, None, Behaviour::Missing),
        backend("reseller-b", 1, None, Behaviour::Has),
    ])
    .await?;

    let codes = article_each(port, 6).await?;

    assert!(codes.iter().all(|code| code == "220"), "{codes:?}");
    assert_eq!(lookups(&counters[1]), 6);
    Ok(())
}

#[tokio::test]
async fn a_430_on_one_backbone_still_tries_a_different_backbone() -> Result<()> {
    let (port, counters) = start_proxy(&[
        backend("omicron-a", 0, Some("omicron"), Behaviour::Missing),
        backend("omicron-b", 0, Some("omicron"), Behaviour::Missing),
        backend("eweka", 1, Some("eweka"), Behaviour::Has),
    ])
    .await?;

    let codes = article_each(port, 6).await?;

    assert!(codes.iter().all(|code| code == "220"), "{codes:?}");
    assert_eq!(
        lookups(&counters[0]) + lookups(&counters[1]),
        6,
        "exactly one omicron server per article"
    );
    assert_eq!(lookups(&counters[2]), 6);
    Ok(())
}

async fn sibling_answers_after(failure: Behaviour) -> Result<()> {
    let (port, counters) = start_proxy(&[
        backend("failing", 0, Some("omicron"), failure),
        backend("sibling", 0, Some("omicron"), Behaviour::Has),
    ])
    .await?;

    let codes = article_each(port, 8).await?;

    assert!(
        codes.iter().all(|code| code == "220"),
        "a sibling on the same backbone must answer: {codes:?}"
    );
    assert_eq!(lookups(&counters[1]), 8);
    assert!(
        lookups(&counters[0]) > 0,
        "the failing server must have been tried for the test to mean anything"
    );
    Ok(())
}

#[tokio::test]
async fn the_adaptive_precheck_skips_a_backbone_sibling_in_a_lower_tier() -> Result<()> {
    let routing = Routing {
        adaptive_precheck: true,
        ..Routing::default()
    };
    let (port, counters) = start_proxy_with(
        &[
            backend("reseller-a", 0, Some("omicron"), Behaviour::Missing),
            backend("reseller-b", 1, Some("omicron"), Behaviour::Has),
        ],
        routing,
    )
    .await?;

    let mut codes = Vec::new();
    for index in 0..6 {
        let client = connect_and_read_greeting(port).await?;
        let mut client = BufReader::new(client);
        client
            .get_mut()
            .write_all(format!("STAT <pre-{index}@example.com>\r\n").as_bytes())
            .await?;
        let mut line = String::new();
        client.read_line(&mut line).await?;
        codes.push(line.chars().take(3).collect::<String>());
    }

    assert!(codes.iter().all(|code| code == "430"), "{codes:?}");
    assert!(lookups(&counters[0]) > 0);
    assert_eq!(
        lookups(&counters[1]),
        0,
        "the precheck must not ask a lower-tier server on the same backbone"
    );
    Ok(())
}

#[tokio::test]
async fn a_refused_lookup_still_tries_a_sibling_on_the_backbone() -> Result<()> {
    sibling_answers_after(Behaviour::Refuses).await
}

#[tokio::test]
async fn a_server_error_still_tries_a_sibling_on_the_backbone() -> Result<()> {
    sibling_answers_after(Behaviour::ServerError).await
}

#[tokio::test]
async fn a_full_account_still_tries_a_sibling_on_the_backbone() -> Result<()> {
    sibling_answers_after(Behaviour::TooManyConnections).await
}
