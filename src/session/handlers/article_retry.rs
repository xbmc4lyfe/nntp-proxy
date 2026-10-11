//! Availability-aware routing for article lookup commands.
//!
//! ARTICLE, BODY, HEAD, and STAT requests with a message-id share the same
//! negative availability facts: a backend that returned 430 for that article is
//! skipped until the entry expires.

use super::BackendLease;
use super::per_command::BatchBackendConnection;

use crate::cache::ArticleAvailability;
use crate::router::{BackendSelector, SuppressedBackends};
use crate::session::ClientSession;
use crate::session::SessionError;
use crate::session::handlers::cache_operations::{
    CacheLookupResult, write_cached_article_response,
};
use crate::session::handlers::command_execution::{
    ArticleAttemptState, AuthoritativeArticleMissing, BackendAttemptResult, StatPrefetch,
};
use crate::types::{BackendToClientBytes, ClientToBackendBytes};
use anyhow::Result;
use std::sync::Arc;
use tokio::io::{AsyncWrite, AsyncWriteExt};
use tracing::{debug, trace, warn};

use crate::protocol::RequestContext;
use crate::session::precheck;

/// Client-side write state shared across cache, precheck, and direct routing paths.
pub(super) struct RequestExecutionIo<'a, W> {
    pub(super) client_writer: &'a mut W,
    pub(super) backend_connection: &'a mut Option<BackendLease>,
    pub(super) client_to_backend_bytes: &'a mut ClientToBackendBytes,
    pub(super) backend_to_client_bytes: &'a mut BackendToClientBytes,
}

/// Result of preparing a request before pipeline/direct backend execution.
pub(super) enum PreparedRequest {
    /// The response was already written to the client.
    Served,
    /// Continue with backend routing using resolved availability state.
    Continue {
        availability: Option<ArticleAvailability>,
        stat_prefetch: Option<StatPrefetch>,
    },
}

impl ClientSession {
    /// Continue a pinned reader lookup only after its provider proved the ID missing.
    pub(super) async fn retry_reader_message_id<W: AsyncWrite + Unpin>(
        &self,
        request: &mut RequestContext,
        client_writer: &mut W,
        pinned_backend: crate::types::BackendId,
    ) -> Result<crate::types::TransferMetrics, SessionError> {
        let router = self.router.as_ref().ok_or_else(|| {
            SessionError::Backend(anyhow::anyhow!("reader failover requires a router"))
        })?;
        let message_id = request
            .message_id_value()
            .expect("framer retry belongs to a validated message-ID lookup");
        let mut availability = self
            .cache
            .get_request_message_id(request.message_id().unwrap())
            .await
            .map(|entry| entry.to_availability())
            .unwrap_or_default();
        let slot = router.availability_slot(pinned_backend).ok_or_else(|| {
            SessionError::Backend(anyhow::anyhow!("pinned backend has no availability slot"))
        })?;
        availability.record_missing_slot(slot);
        self.cache
            .record_availability_missing(message_id, slot)
            .await;
        self.metrics.record_error_4xx(pinned_backend);
        let mut backend_connection = BatchBackendConnection::default();
        let mut client_to_backend_bytes = ClientToBackendBytes::zero();
        let mut backend_to_client_bytes = BackendToClientBytes::zero();
        let result = self
            .execute_article_retry_loop(
                router,
                request,
                Some(availability),
                None,
                &mut RequestExecutionIo {
                    client_writer,
                    backend_connection: backend_connection.slot(),
                    client_to_backend_bytes: &mut client_to_backend_bytes,
                    backend_to_client_bytes: &mut backend_to_client_bytes,
                },
            )
            .await;
        result?;
        backend_connection.complete_success();
        if request.response_metadata().is_none() {
            // Synthetic exhaustion/error replies have no supplying provider.
            self.metrics
                .user_bytes_received(self.username(), backend_to_client_bytes.as_u64());
        }
        Ok(crate::types::TransferMetrics {
            client_to_backend: client_to_backend_bytes,
            backend_to_client: backend_to_client_bytes,
        })
    }

    /// Route a single lookup request to a backend and execute it.
    ///
    /// This function is `pub(super)` to allow reuse of per-command routing logic by sibling handler modules
    /// (such as `hybrid.rs`) that also need to route commands.
    pub(super) async fn route_and_execute_request<W: AsyncWrite + Unpin>(
        &self,
        router: Arc<BackendSelector>,
        request: &mut RequestContext,
        client_writer: &mut W,
        backend_connection: &mut Option<BackendLease>,
        client_to_backend_bytes: &mut ClientToBackendBytes,
        backend_to_client_bytes: &mut BackendToClientBytes,
    ) -> Result<(), SessionError> {
        self.log_route_request(request);

        let mut io = RequestExecutionIo {
            client_writer,
            backend_connection,
            client_to_backend_bytes,
            backend_to_client_bytes,
        };
        let preloaded_cache = match request.message_id() {
            Some(message_id) => self.cache.get_request_message_id(message_id).await,
            None => None,
        };
        let preloaded_availability = preloaded_cache
            .as_ref()
            .map(|cached| cached.to_availability());
        let (availability, stat_prefetch) = match self
            .prepare_request_execution(
                &router,
                request,
                &mut io,
                preloaded_cache,
                preloaded_availability,
            )
            .await?
        {
            PreparedRequest::Served => return Ok(()),
            PreparedRequest::Continue {
                availability,
                stat_prefetch,
            } => (availability, stat_prefetch),
        };

        self.execute_article_retry_loop(&router, request, availability, stat_prefetch, &mut io)
            .await
    }

    fn log_route_request(&self, request: &RequestContext) {
        debug!(
            "Client {} ENTERED route_and_execute_request: kind={:?}, verb={:?}",
            self.client_addr,
            request.kind(),
            request.verb()
        );
        debug!(
            "Client {} msg_id={:?}, payload_policy={:?}",
            self.client_addr,
            request.message_id(),
            self.cache.payload_policy()
        );
    }

    pub(super) async fn prepare_request_execution<W: AsyncWrite + Unpin>(
        &self,
        router: &Arc<BackendSelector>,
        request: &mut RequestContext,
        io: &mut RequestExecutionIo<'_, W>,
        preloaded_cache: Option<crate::cache::CachedArticle>,
        preloaded_availability: Option<ArticleAvailability>,
    ) -> Result<PreparedRequest, SessionError> {
        let availability = {
            let client_write = &mut *io.client_writer;
            match self
                .try_serve_from_cached(
                    request,
                    router,
                    client_write,
                    io.backend_to_client_bytes,
                    preloaded_cache,
                )
                .await?
            {
                CacheLookupResult::Hit => return Ok(PreparedRequest::Served),
                CacheLookupResult::PartialHit => request
                    .cache_availability()
                    .map(Self::request_cache_availability),
                CacheLookupResult::Miss => None,
            }
        };
        let availability = availability.or(preloaded_availability).or_else(|| {
            request
                .message_id_value()
                .is_some()
                .then(ArticleAvailability::new)
        });
        let stat_prefetch = availability.as_ref().and_then(|availability| {
            self.spawn_non_primary_tier_stat_prefetch(
                router,
                request,
                availability,
                SuppressedBackends::empty(),
            )
        });
        if self
            .try_adaptive_precheck(router, request, io, availability.as_ref())
            .await?
        {
            return Ok(PreparedRequest::Served);
        }

        Ok(PreparedRequest::Continue {
            availability,
            stat_prefetch,
        })
    }

    const fn request_cache_availability(
        availability: crate::protocol::RequestCacheAvailability,
    ) -> ArticleAvailability {
        ArticleAvailability::from_missing_bits(availability.missing_bits())
    }

    async fn try_adaptive_precheck<W: AsyncWrite + Unpin>(
        &self,
        router: &Arc<BackendSelector>,
        request: &RequestContext,
        io: &mut RequestExecutionIo<'_, W>,
        availability: Option<&ArticleAvailability>,
    ) -> Result<bool, SessionError> {
        if !self.adaptive_precheck || !(request.is_stat() || request.is_head()) {
            return Ok(false);
        }
        if request.is_head() && !self.cache.stores_payload_responses() {
            return Ok(false);
        }
        let Some(msg_id) = request.message_id_value() else {
            return Ok(false);
        };
        let Some(availability) = availability else {
            return Ok(false);
        };

        let deps = self.precheck_deps(router);
        let Some(response) = precheck::precheck(&deps, request, &msg_id, availability).await else {
            return Ok(false);
        };

        let client_write = &mut *io.client_writer;
        let bytes_written = match response {
            precheck::PrecheckResponse::Cached(entry) => {
                if let Some(write) = write_cached_article_response(
                    &mut *client_write,
                    &entry,
                    request.kind(),
                    msg_id.as_str(),
                )
                .await
                .map_err(|e| SessionError::from(anyhow::Error::from(e)))?
                {
                    write.wire_len.get()
                } else {
                    return Ok(false);
                }
            }
            precheck::PrecheckResponse::Direct(response) => {
                Self::write_precheck_direct_response(&mut *client_write, response).await?
            }
        };
        *io.backend_to_client_bytes = io.backend_to_client_bytes.add(bytes_written);
        Ok(true)
    }

    async fn write_precheck_direct_response<W>(
        client_write: &mut W,
        response: precheck::DirectResponse,
    ) -> Result<usize, SessionError>
    where
        W: AsyncWrite + Unpin,
    {
        let len = match response {
            precheck::DirectResponse::Framed(response) => {
                let len = response.len();
                response
                    .write_to(client_write)
                    .await
                    .map_err(|e| SessionError::from(anyhow::Error::from(e)))?;
                len
            }
            precheck::DirectResponse::SingleLine(response) => {
                let len = response.len();
                client_write
                    .write_all(&response)
                    .await
                    .map_err(|e| SessionError::from(anyhow::Error::from(e)))?;
                len
            }
        };
        client_write
            .flush()
            .await
            .map(|()| len)
            .map_err(|e| SessionError::from(anyhow::Error::from(e)))
    }

    async fn write_backend_error_response<W>(client_write: &mut W) -> Result<usize, SessionError>
    where
        W: AsyncWrite + Unpin,
    {
        client_write
            .write_all(crate::protocol::BACKEND_ERROR)
            .await
            .map_err(|e| SessionError::from(anyhow::Error::from(e)))?;
        client_write
            .flush()
            .await
            .map(|()| crate::protocol::BACKEND_ERROR.len())
            .map_err(|e| SessionError::from(anyhow::Error::from(e)))
    }

    async fn execute_article_retry_loop<W: AsyncWrite + Unpin>(
        &self,
        router: &Arc<BackendSelector>,
        request: &mut RequestContext,
        availability: Option<ArticleAvailability>,
        mut stat_prefetch: Option<StatPrefetch>,
        io: &mut RequestExecutionIo<'_, W>,
    ) -> Result<(), SessionError> {
        trace!(
            "Client {} starting availability routing for request kind={:?}, verb={:?}",
            self.client_addr,
            request.kind(),
            request.verb()
        );

        let mut availability = availability.unwrap_or_default();
        let mut unavailable_backends = SuppressedBackends::empty();
        let mut is_retry_attempt = false;
        let mut retry_stat_sweep_done = false;
        trace!(
            "Client {} availability routing: missing_bits={:08b}, backend_count={}",
            self.client_addr,
            availability.missing_bits(),
            router.backend_count().get()
        );

        while !availability.all_exhausted(router.availability_mask()) {
            if is_retry_attempt && !retry_stat_sweep_done {
                self.parallel_retry_stat_sweep(
                    router,
                    request,
                    &mut ArticleAttemptState {
                        availability: &mut availability,
                        client_to_backend_bytes: io.client_to_backend_bytes,
                        backend_connection: io.backend_connection,
                        unavailable_backends: &mut unavailable_backends,
                    },
                    stat_prefetch.take(),
                )
                .await?;
                retry_stat_sweep_done = true;
            }

            let attempt = self
                .try_backend_for_article(
                    router,
                    request,
                    io.client_writer,
                    &mut ArticleAttemptState {
                        availability: &mut availability,
                        client_to_backend_bytes: io.client_to_backend_bytes,
                        backend_connection: io.backend_connection,
                        unavailable_backends: &mut unavailable_backends,
                    },
                    is_retry_attempt,
                )
                .await;
            match attempt {
                Ok(BackendAttemptResult::Success) => {
                    let response = request
                        .response_metadata()
                        .expect("successful direct attempt records response metadata");
                    *io.backend_to_client_bytes =
                        io.backend_to_client_bytes.add(response.wire_len().get());
                    return Ok(());
                }
                Ok(BackendAttemptResult::ArticleNotFound { missing }) => {
                    is_retry_attempt = true;
                    let backend_id = missing.backend_id();
                    trace!(
                        "Client {} backend {:?} returned 430 during retry",
                        self.client_addr, backend_id
                    );
                }
                Ok(BackendAttemptResult::BackendUnavailable) => {
                    is_retry_attempt = true;
                }
                Ok(BackendAttemptResult::NoRetryableBackend) => {
                    // Some backend answered 430 and the rest were only
                    // transiently unavailable (busy or refusing logins): the
                    // article is most likely gone, and clients handle 430
                    // (try another source) far better than 503 (retry here).
                    if availability.missing_bits() != 0 {
                        trace!(
                            "Client {} remaining backends unavailable after 430s for {:?}, sending 430",
                            self.client_addr,
                            request.message_id_value()
                        );
                        let client_write = &mut *io.client_writer;
                        self.send_430_to_client(client_write, io.backend_to_client_bytes)
                            .await?;
                        return Ok(());
                    }
                    let bytes_written = {
                        let client_write = &mut *io.client_writer;
                        Self::write_backend_error_response(client_write).await?
                    };
                    *io.backend_to_client_bytes = io.backend_to_client_bytes.add(bytes_written);
                    return Ok(());
                }
                Err(e @ SessionError::ClientDisconnect(_)) => {
                    debug!(
                        "Client {} disconnected during article retry for {:?}",
                        self.client_addr,
                        request.message_id_value()
                    );
                    return Err(e);
                }
                Err(SessionError::Backend(e)) => {
                    warn!(
                        client = %self.client_addr,
                        error = %e,
                        "Backend error during article retry (will try next backend)"
                    );
                }
            }
        }

        trace!(
            "Client {} all backends exhausted for {:?}, sending 430",
            self.client_addr,
            request.message_id_value()
        );
        {
            let client_write = &mut *io.client_writer;
            self.send_430_to_client(client_write, io.backend_to_client_bytes)
                .await?;
        }

        Ok(())
    }

    /// Record 430 response in availability tracker.
    ///
    /// Note: `complete_command` is handled by [`crate::router::CommandGuard`] RAII, not here.
    pub(super) fn record_authoritative_article_missing(
        &self,
        missing: &AuthoritativeArticleMissing,
        availability: &mut crate::cache::ArticleAvailability,
    ) {
        let backend_id = missing.backend_id();
        availability.record_missing_slot(missing.availability_slot());

        // Track 430 responses in 4xx metrics for visibility in TUI
        // While 430 is normal retry behavior (not a failure), users want to see
        // these counted to understand backend article distribution
        self.metrics.record_error_4xx(backend_id);
    }
}

#[cfg(test)]
mod tests {
    use crate::cache::UnifiedCache;
    use crate::protocol::{RequestCacheAvailability, StatusCode};
    use crate::session::ClientSession;
    use crate::types::{BackendId, MessageId};
    use std::time::Duration;

    #[tokio::test]
    async fn cancelled_reader_retry_preserves_capacity_of_cached_completed_lease() {
        use crate::auth::AuthHandler;
        use crate::config::{RoutingMode, Server};
        use crate::metrics::MetricsCollector;
        use crate::pool::{BufferPool, DeadpoolConnectionProvider};
        use crate::protocol::{RequestContext, RequestKind};
        use crate::router::BackendSelector;
        use crate::types::{BufferSize, ClientAddress, MaxConnections, Port, ServerName};
        use std::sync::Arc;
        use tokio::io::{AsyncBufReadExt, AsyncWriteExt, BufReader};
        use tokio::net::TcpListener;

        let (probe_started, mut probes) = tokio::sync::mpsc::unbounded_channel();
        let mut router = BackendSelector::new();
        // The framer has already proved provider 0 missing before this helper.
        router.add_backend(
            ServerName::try_new("pinned").unwrap(),
            DeadpoolConnectionProvider::new("127.0.0.1".into(), 1, "pinned".into(), 2, None, None),
            0,
        );
        let mut servers = Vec::new();
        for index in 1..=3 {
            let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
            let port = listener.local_addr().unwrap().port();
            let signal = probe_started.clone();
            servers.push(tokio::spawn(async move {
                let mut clients = tokio::task::JoinSet::new();
                while let Ok((stream, _)) = listener.accept().await {
                    let signal = signal.clone();
                    clients.spawn(async move {
                        let mut stream = BufReader::new(stream);
                        stream.get_mut().write_all(b"200 Ready\r\n").await.unwrap();
                        loop {
                            let mut line = String::new();
                            if stream.read_line(&mut line).await.unwrap_or(0) == 0 {
                                break;
                            }
                            let request = RequestContext::parse(line.as_bytes()).unwrap();
                            match request.kind() {
                                RequestKind::Body => stream
                                    .get_mut()
                                    .write_all(b"430 Missing secondary\r\n")
                                    .await
                                    .unwrap(),
                                RequestKind::Stat => {
                                    signal.send(index).unwrap();
                                    std::future::pending::<()>().await;
                                }
                                RequestKind::Mode => stream
                                    .get_mut()
                                    .write_all(b"200 Reader mode\r\n")
                                    .await
                                    .unwrap(),
                                _ => stream
                                    .get_mut()
                                    .write_all(b"502 Unsupported\r\n")
                                    .await
                                    .unwrap(),
                            }
                        }
                    });
                }
            }));
            let tier = u8::from(index != 1);
            let config = Server::builder("127.0.0.1", Port::try_new(port).unwrap())
                .name(format!("provider-{index}"))
                .max_connections(MaxConnections::try_new(2).unwrap())
                .compress(Some(false))
                .stat_missing(u8::from(index != 1))
                .replacement_cooldown(Duration::from_secs(60))
                .tier(tier)
                .build()
                .unwrap();
            let provider =
                DeadpoolConnectionProvider::from_server_config(&config, 8192, 8192).unwrap();
            router.add_backend(config.name.clone(), provider, tier);
        }
        let router = Arc::new(router);
        let cached_provider = router
            .backend_provider(BackendId::from_index(1))
            .unwrap()
            .clone();
        let session = ClientSession::builder(
            ClientAddress::from("127.0.0.1:9999".parse::<std::net::SocketAddr>().unwrap()),
            BufferPool::new(BufferSize::try_new(8192).unwrap(), 4),
            Arc::new(AuthHandler::new(None, None).unwrap()),
            MetricsCollector::new(4),
        )
        .with_router(router)
        .with_routing_mode(RoutingMode::Hybrid)
        .build();
        let cache = session.cache.clone();
        let retry = tokio::spawn(async move {
            let mut request = RequestContext::parse(b"BODY <cancel@reader>\r\n").unwrap();
            session
                .retry_reader_message_id(
                    &mut request,
                    &mut tokio::io::sink(),
                    BackendId::from_index(0),
                )
                .await
        });
        for _ in 0..2 {
            tokio::time::timeout(Duration::from_secs(3), probes.recv())
                .await
                .unwrap()
                .unwrap();
        }
        assert_eq!(
            cache
                .get_request_message_id("<cancel@reader>")
                .await
                .unwrap()
                .availability()
                .missing_bits(),
            0b11
        );
        let cached = cached_provider.status_counts();
        assert_eq!(
            (cached.size, cached.available, cached.max_size),
            (1, 0, 2),
            "secondary's completed 430 lease must still be cached during probes"
        );
        retry.abort();
        assert!(retry.await.unwrap_err().is_cancelled());
        assert_eq!(
            cached_provider.status_counts().max_size,
            2,
            "cancelling a completed cached lease must not trigger replacement cooldown"
        );
        for server in servers {
            server.abort();
        }
    }

    #[tokio::test]
    async fn status_fact_preserves_missing_backend_without_payload() {
        let cache = UnifiedCache::memory(1_000_000, Duration::from_secs(60));
        let msg_id = MessageId::from_borrowed("<ordered-success@example.com>").unwrap();

        cache
            .record_availability_missing(
                msg_id.clone(),
                crate::cache::AvailabilitySlot::new(0).unwrap(),
            )
            .await;
        cache
            .record_backend_has_status(
                msg_id.clone(),
                StatusCode::new(222),
                BackendId::from_index(1),
                crate::cache::ttl::CacheTier::new(0),
            )
            .await;
        cache.sync().await;

        let entry = cache
            .get(&msg_id)
            .await
            .expect("cache facts must preserve mixed success/missing availability");
        assert_eq!(entry.status_code(), StatusCode::new(222));
        assert_eq!(entry.payload_len().get(), 0);
        assert!(!entry.should_try_slot(crate::cache::AvailabilitySlot::new(0).unwrap()));
        assert!(entry.should_try_slot(crate::cache::AvailabilitySlot::new(1).unwrap()));
        assert_eq!(entry.availability().missing_bits(), 0b0000_0001);
    }

    #[test]
    fn request_cache_availability_discards_positive_checked_bits_for_retry() {
        let availability = ClientSession::request_cache_availability(
            RequestCacheAvailability::from_bits(0b0000_0110, 0b0000_0010),
        );

        assert!(availability.should_try_slot(crate::cache::AvailabilitySlot::new(0).unwrap()));
        assert!(!availability.should_try_slot(crate::cache::AvailabilitySlot::new(1).unwrap()));
        assert!(availability.should_try_slot(crate::cache::AvailabilitySlot::new(2).unwrap()));
        assert_eq!(availability.missing_bits(), 0b0000_0010);
    }
}
