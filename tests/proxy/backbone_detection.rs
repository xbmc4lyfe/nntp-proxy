//! Backbone detection regroups servers from what they report.
//!
//! Each mock server answers `LIST ACTIVE` with article-number ranges. Servers
//! reporting the same ranges read one store, so after a check a `430` from
//! one skips the other; servers whose ranges differ are kept apart even when
//! the config puts them on one backbone. Findings land in the config file as
//! comments.

use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};

use anyhow::Result;
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

struct Mock {
    name: &'static str,
    tier: u8,
    backbone: Option<&'static str>,
    /// Base of the highest article numbers this server reports.
    store: u64,
    has_articles: bool,
}

/// A server that reports `store`-based article ranges and answers every
/// message-ID lookup the same way; `lookups` counts `ARTICLE`/`STAT` by
/// message-ID (the probe's own `STAT` uses a reserved `.invalid` ID and is
/// not counted).
async fn spawn_mock(mock: &Mock, lookups: Arc<AtomicUsize>) -> Result<u16> {
    let listener = TcpListener::bind("127.0.0.1:0").await?;
    let port = listener.local_addr()?.port();
    let (store, has_articles) = (mock.store, mock.has_articles);
    tokio::spawn(async move {
        while let Ok((stream, _)) = listener.accept().await {
            let lookups = lookups.clone();
            tokio::spawn(async move {
                let mut stream = BufReader::new(stream);
                if stream
                    .get_mut()
                    .write_all(b"200 mock ready\r\n")
                    .await
                    .is_err()
                {
                    return;
                }
                let mut line = String::new();
                loop {
                    line.clear();
                    if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                        return;
                    }
                    let command = line.trim_end().to_string();
                    let reply = if let Some(group) = command.strip_prefix("LIST ACTIVE ") {
                        let index = PROBE_GROUPS.iter().position(|g| *g == group).unwrap_or(0);
                        let high = store * (index as u64 + 1) + 17;
                        format!("215 list follows\r\n{group} {high} 2 y\r\n.\r\n")
                    } else if command == "HELP" {
                        "100 help\r\n  ARTICLE\r\n  STAT\r\n.\r\n".to_string()
                    } else if let Some((verb @ ("ARTICLE" | "STAT"), id)) = command.split_once(' ')
                    {
                        if !id.ends_with(".invalid>") {
                            lookups.fetch_add(1, Ordering::Relaxed);
                        }
                        match (has_articles && !id.ends_with(".invalid>"), verb) {
                            (true, "STAT") => format!("223 0 {id}\r\n"),
                            (true, _) => format!("220 0 {id}\r\nSubject: x\r\n\r\nbody\r\n.\r\n"),
                            (false, _) => "430 no such article\r\n".to_string(),
                        }
                    } else {
                        match command.as_str() {
                            "MODE READER" => "200 reader\r\n".to_string(),
                            "DATE" => "111 20260101000000\r\n".to_string(),
                            "QUIT" => "205 bye\r\n".to_string(),
                            _ => "500 unknown command\r\n".to_string(),
                        }
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

struct Running {
    port: u16,
    lookups: Vec<Arc<AtomicUsize>>,
    check: BackboneCheck,
    config_file: tempfile::NamedTempFile,
}

async fn start(mocks: &[Mock], detection: BackboneDetection) -> Result<Running> {
    let mut servers = Vec::new();
    let mut lookups = Vec::new();
    let mut file_text = String::from("[proxy]\nport = 8119\n");
    for (index, mock) in mocks.iter().enumerate() {
        let counter = Arc::new(AtomicUsize::new(0));
        let port = spawn_mock(mock, counter.clone()).await?;
        lookups.push(counter);
        let mut server = Server::builder(independent_provider_host(index), Port::try_new(port)?)
            .name(mock.name)
            .tier(mock.tier)
            .max_connections(MaxConnections::try_new(2)?)
            .prewarm_connections(0)
            .compress(Some(false));
        file_text.push_str(&format!("\n[[servers]]\nname = \"{}\"\n", mock.name));
        if let Some(backbone) = mock.backbone {
            server = server.backbone(backbone);
            file_text.push_str(&format!("backbone = \"{backbone}\"\n"));
        }
        file_text.push_str(&format!(
            "host = \"{}\"\n",
            independent_provider_host(index)
        ));
        servers.push(server.build()?);
    }
    let config_file = tempfile::NamedTempFile::new()?;
    std::fs::write(config_file.path(), &file_text)?;
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
    let check = proxy.backbone_check(Some(config_file.path().to_path_buf()));
    let port = spawn_test_proxy_on_random_port(proxy, true).await?;
    Ok(Running {
        port,
        lookups,
        check,
        config_file,
    })
}

async fn article_each(port: u16, prefix: &str, count: usize) -> Result<Vec<String>> {
    let mut codes = Vec::with_capacity(count);
    for index in 0..count {
        let client = connect_and_read_greeting(port).await?;
        let mut client = BufReader::new(client);
        client
            .get_mut()
            .write_all(format!("ARTICLE <{prefix}-{index}@example.com>\r\n").as_bytes())
            .await?;
        let mut line = String::new();
        client.read_line(&mut line).await?;
        codes.push(line.chars().take(3).collect());
    }
    Ok(codes)
}

fn count(counter: &AtomicUsize) -> usize {
    counter.load(Ordering::Relaxed)
}

#[tokio::test]
async fn servers_reporting_one_store_are_grouped_while_running() -> Result<()> {
    let running = start(
        &[
            Mock {
                name: "reseller-a",
                tier: 0,
                backbone: None,
                store: STORE_A,
                has_articles: false,
            },
            Mock {
                name: "reseller-b",
                tier: 1,
                backbone: None,
                store: STORE_A,
                has_articles: true,
            },
        ],
        BackboneDetection::Annotate,
    )
    .await?;

    let before = article_each(running.port, "before", 3).await?;
    assert!(before.iter().all(|code| code == "220"), "{before:?}");
    let asked_before = count(&running.lookups[1]);
    assert_eq!(
        asked_before, 3,
        "ungrouped: the other server rescues every miss"
    );

    running.check.run().await;

    let after = article_each(running.port, "after", 3).await?;
    assert!(after.iter().all(|code| code == "430"), "{after:?}");
    assert_eq!(
        count(&running.lookups[1]),
        asked_before,
        "grouped: a 430 from one server skips the other without a restart"
    );

    let text = std::fs::read_to_string(running.config_file.path())?;
    assert!(
        text.contains("# backbone-check (") && text.contains("detected"),
        "the finding is written above the server:\n{text}"
    );
    Ok(())
}

#[tokio::test]
async fn a_configured_backbone_the_numbers_contradict_is_split() -> Result<()> {
    let running = start(
        &[
            Mock {
                name: "reseller-a",
                tier: 0,
                backbone: Some("omicron"),
                store: STORE_A,
                has_articles: false,
            },
            Mock {
                name: "reseller-b",
                tier: 1,
                backbone: Some("omicron"),
                store: STORE_B,
                has_articles: true,
            },
        ],
        BackboneDetection::Annotate,
    )
    .await?;

    let before = article_each(running.port, "before", 3).await?;
    assert!(before.iter().all(|code| code == "430"), "{before:?}");
    assert_eq!(count(&running.lookups[1]), 0, "configured as one backbone");

    running.check.run().await;

    let after = article_each(running.port, "after", 3).await?;
    assert!(after.iter().all(|code| code == "220"), "{after:?}");
    assert_eq!(
        count(&running.lookups[1]),
        3,
        "split: the other server is asked again after a 430"
    );

    let text = std::fs::read_to_string(running.config_file.path())?;
    assert!(
        text.contains("config says \"omicron\""),
        "the misconfiguration is written into the file:\n{text}"
    );
    assert!(
        text.contains("backbone = \"omicron\""),
        "the operator's own value is left in place"
    );
    Ok(())
}

#[tokio::test]
async fn apply_regroups_without_touching_the_file() -> Result<()> {
    let running = start(
        &[
            Mock {
                name: "reseller-a",
                tier: 0,
                backbone: None,
                store: STORE_B,
                has_articles: false,
            },
            Mock {
                name: "reseller-b",
                tier: 1,
                backbone: None,
                store: STORE_B,
                has_articles: true,
            },
        ],
        BackboneDetection::Apply,
    )
    .await?;
    let original = std::fs::read_to_string(running.config_file.path())?;

    running.check.run().await;

    let after = article_each(running.port, "after", 2).await?;
    assert!(after.iter().all(|code| code == "430"), "{after:?}");
    assert_eq!(
        std::fs::read_to_string(running.config_file.path())?,
        original
    );
    Ok(())
}

#[tokio::test]
async fn off_never_probes_or_regroups() -> Result<()> {
    let running = start(
        &[
            Mock {
                name: "reseller-a",
                tier: 0,
                backbone: None,
                store: STORE_A,
                has_articles: false,
            },
            Mock {
                name: "reseller-b",
                tier: 1,
                backbone: None,
                store: STORE_A,
                has_articles: true,
            },
        ],
        BackboneDetection::Off,
    )
    .await?;

    running.check.run().await;

    let after = article_each(running.port, "after", 2).await?;
    assert!(after.iter().all(|code| code == "220"), "{after:?}");
    Ok(())
}

#[tokio::test]
async fn audits_split_servers_whose_numbers_match_but_articles_differ() -> Result<()> {
    let running = start(
        &[
            Mock {
                name: "reseller-a",
                tier: 0,
                backbone: None,
                store: STORE_A,
                has_articles: false,
            },
            Mock {
                name: "reseller-b",
                tier: 1,
                backbone: None,
                store: STORE_A,
                has_articles: true,
            },
        ],
        BackboneDetection::Apply,
    )
    .await?;
    running.check.run().await;
    let grouped = article_each(running.port, "grouped", 3).await?;
    assert!(grouped.iter().all(|code| code == "430"), "{grouped:?}");

    // Enough shared-backbone misses for the sampler to keep several.
    let misses = article_each(running.port, "miss", 200).await?;
    assert!(misses.iter().all(|code| code == "430"));

    running.check.audit_now().await;

    let after = article_each(running.port, "after", 3).await?;
    assert!(
        after.iter().all(|code| code == "220"),
        "after audits catch the sibling holding the 'missing' articles, it is asked again: {after:?}"
    );
    Ok(())
}
