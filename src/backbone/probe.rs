//! Reading the evidence a server gives about the backbone behind it.

use crate::pool::{BufferPool, DeadpoolConnectionProvider};
use crate::protocol::RequestContext;
use crate::types::BackendId;
use anyhow::{Context, Result};
use std::collections::BTreeMap;
use std::hash::Hasher;
use std::time::Duration;
use twox_hash::XxHash64;

/// How long one probe command may wait for a pooled connection.
const CHECKOUT_LIMIT: Duration = Duration::from_secs(30);
/// How long one probe command may take once sent.
const COMMAND_LIMIT: Duration = Duration::from_secs(30);
/// A message-ID no server holds, to read how the software words a `430`.
const MISSING_ARTICLE: &str = "<backbone-probe.missing@nntp-proxy.invalid>";

/// A group's article-number range on one server.
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub(crate) struct Watermarks {
    pub(crate) low: u64,
    pub(crate) high: u64,
}

/// What one server said about itself.
#[derive(Clone, Debug, Default, Eq, PartialEq)]
pub(crate) struct Fingerprint {
    /// The latest greeting line, for brand hints.
    pub(crate) greeting: Option<String>,
    /// Article-number ranges of the probe groups the server answered for.
    pub(crate) watermarks: BTreeMap<String, Watermarks>,
    /// Hash of the server software's own wording (its `HELP` text and how
    /// it words a missing article). Resellers on one backbone often run
    /// different front ends, so a mismatch is weak evidence; a match is a
    /// supporting check.
    pub(crate) software: Option<u64>,
    /// Why the server gave no article numbers, when it gave none.
    pub(crate) no_numbers_reason: Option<String>,
}

/// Ask one server for its fingerprint over its own connection pool.
///
/// Each command uses a pooled connection the normal way and hands it back,
/// so the probe costs a few commands, not a login of its own, when the pool
/// already has a connection.
pub(crate) async fn fingerprint(
    provider: &DeadpoolConnectionProvider,
    buffer_pool: &BufferPool,
    backend: BackendId,
    groups: &[String],
) -> Fingerprint {
    let mut fingerprint = Fingerprint::default();
    let mut refusals = Vec::new();
    for group in groups {
        let request = RequestContext::from_verb_args(b"LIST", format!("ACTIVE {group}").as_bytes());
        match command(provider, buffer_pool, backend, &request).await {
            Ok(Reply::Lines(lines)) => {
                if let Some(watermarks) = lines.iter().find_map(|line| parse_active(line, group)) {
                    fingerprint.watermarks.insert(group.clone(), watermarks);
                } else {
                    refusals.push(format!("LIST ACTIVE {group} gave no usable line"));
                }
            }
            Ok(Reply::Line(line)) => refusals.push(format!(
                "LIST ACTIVE answered \"{}\"",
                line.chars().take(60).collect::<String>()
            )),
            Err(error) => refusals.push(format!("LIST ACTIVE failed: {error:#}")),
        }
    }
    if fingerprint.watermarks.is_empty() {
        refusals.dedup();
        fingerprint.no_numbers_reason = Some(refusals.join("; "));
    }

    let mut software = XxHash64::default();
    let mut any = false;
    for request in [
        RequestContext::from_verb_args(b"HELP", b""),
        RequestContext::from_verb_args(b"STAT", MISSING_ARTICLE.as_bytes()),
    ] {
        match command(provider, buffer_pool, backend, &request).await {
            Ok(Reply::Lines(lines)) => {
                any = true;
                for line in lines {
                    software.write(line.trim().as_bytes());
                    software.write_u8(b'\n');
                }
            }
            Ok(Reply::Line(line)) => {
                any = true;
                software.write(line.trim().as_bytes());
            }
            Err(_) => {}
        }
        software.write_u8(0);
    }
    fingerprint.software = any.then(|| software.finish());
    fingerprint.greeting = provider.last_greeting();
    fingerprint
}

/// Whether a server has an article: `Some` for a clear answer, `None` when
/// the lookup was refused or failed, which says nothing about the article.
pub(crate) async fn has_article(
    provider: &DeadpoolConnectionProvider,
    buffer_pool: &BufferPool,
    backend: BackendId,
    message_id: &str,
) -> Option<bool> {
    for verb in [b"STAT".as_slice(), b"HEAD".as_slice()] {
        let request = RequestContext::from_verb_args(verb, message_id.as_bytes());
        match command(provider, buffer_pool, backend, &request)
            .await
            .ok()?
        {
            Reply::Lines(_) => return Some(true),
            Reply::Line(line) => {
                match crate::protocol::StatusCode::parse(line.as_bytes()).map(|code| code.as_u16())
                {
                    Some(223 | 221) => return Some(true),
                    Some(430) => return Some(false),
                    // Some servers refuse STAT; HEAD answers the same question.
                    Some(500 | 501) if verb == b"STAT" => {}
                    _ => return None,
                }
            }
        }
    }
    None
}

enum Reply {
    /// A single-line reply, status included.
    Line(String),
    /// The text lines of a multiline reply.
    Lines(Vec<String>),
}

async fn command(
    provider: &DeadpoolConnectionProvider,
    buffer_pool: &BufferPool,
    backend: BackendId,
    request: &RequestContext,
) -> Result<Reply> {
    let conn = provider
        .checkout_connection_guard_within(CHECKOUT_LIMIT)
        .await
        .context("no connection")?
        .activate();
    let exchange = async {
        let mut exchange = crate::session::backend::execute_request_exchange(
            conn,
            request,
            buffer_pool.acquire(),
            buffer_pool,
            backend,
        )
        .await?;
        let Some(status) = exchange.status_code() else {
            exchange.fail_backend();
            anyhow::bail!("unreadable reply");
        };
        if request.has_response_body(status) {
            return Ok(Reply::Lines(exchange.capture_text_lines_and_reuse().await?));
        }
        let line = exchange
            .single_line_bytes()
            .map(|bytes| String::from_utf8_lossy(bytes).trim_end().to_owned())
            .unwrap_or_default();
        exchange.receiving()?.complete_single_line()?;
        let _reusable = exchange.into_connection().complete_success();
        Ok(Reply::Line(line))
    };
    tokio::time::timeout(COMMAND_LIMIT, exchange)
        .await
        .context("timed out")?
}

/// `<group> <high> <low> <flag>`, only for the group asked about: some
/// servers answer with a line that leaves the group name out.
fn parse_active(line: &str, group: &str) -> Option<Watermarks> {
    let mut fields = line.split_whitespace();
    if !fields.next()?.eq_ignore_ascii_case(group) {
        return None;
    }
    let high = fields.next()?.parse().ok()?;
    let low = fields.next()?.parse().ok()?;
    (high > 0 && low <= high).then_some(Watermarks { low, high })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn an_active_line_gives_the_groups_range() {
        assert_eq!(
            parse_active("alt.binaries.test 166274849578 2 y", "alt.binaries.test"),
            Some(Watermarks {
                low: 2,
                high: 166_274_849_578
            })
        );
    }

    #[test]
    fn an_active_line_for_another_or_no_group_is_ignored() {
        assert_eq!(parse_active("0 46682260 71 y", "alt.binaries.test"), None);
        assert_eq!(parse_active("alt.other 10 1 y", "alt.binaries.test"), None);
        assert_eq!(
            parse_active("alt.binaries.test 1 10 y", "alt.binaries.test"),
            None
        );
    }
}
