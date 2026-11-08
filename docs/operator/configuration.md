# Configuration

## Config sources and precedence

`nntp-proxy` loads configuration in this order:

1. `--config <FILE>` / `NNTP_PROXY_CONFIG` if the file exists
2. `NNTP_SERVER_*` backend variables override `[[servers]]` from the file
3. `NNTP_PROXY_CREDENTIALS_FILE` can overlay backend and client-auth secrets
4. Selected CLI flags override the loaded config

If no config file exists and no backend environment variables are present, the proxy creates a default `config.toml`.

## Canonical section order

Keep the config ordered from general settings to concrete identities:

1. `[proxy]`
2. `[routing]`
3. `[routing.queue.backpressure]`
4. `[memory]`
5. `[cache]`
6. `[cache.disk]`
7. `[health_check]`
8. `[client_auth]`
9. `[[client_auth.users]]`
10. `[[servers]]`

See [../../config.full.toml](../../config.full.toml) for a complete example.

## Section reference

### `[proxy]`

| Field | Default | Notes |
| --- | --- | --- |
| `host` | `0.0.0.0` | Listen host |
| `port` | `8119` | Listen port |
| `threads` | `1` | Use `0` for CPU-count worker threads |
| `validate_yenc` | `true` | Validate yEnc structure/checksums |
| `log_file_level` | `"warn"` | Filter for the optional local `debug.log` appender |
| `response_write_metrics_secs` | unset | Optional interval for response write metrics logging |
| `stats_file` | unset | If unset, metrics persistence defaults to `stats.json` next to the config file |

### `[routing]`

| Field | Default | Notes |
| --- | --- | --- |
| `mode` | `hybrid` | `hybrid` is the default; `per-command` and `stateful` remain available explicit modes |
| `backend_selection` | `least-loaded` | Or `weighted-round-robin` |
| `adaptive_precheck` | `false` | Adaptive availability precheck for `STAT`/`HEAD` |

### `[routing.queue.backpressure]`

| Field | Default | Notes |
| --- | --- | --- |
| `enabled` | `true` | Enable queue-pressure-aware backend filtering |
| `soft_waiters_per_connection_percent` | `25` | Penalize backends above this queued-requests-per-connection threshold |
| `hard_waiters_per_connection_percent` | `50` | Filter backends above this threshold while alternatives exist in the tier |
| `all_busy_sleep_ms` | `1` | Sleep before retrying when every eligible backend in the preferred tier is hard-saturated |

### `[routing.article]`

Time limits for `ARTICLE`, `BODY`, `HEAD`, and `STAT` lookups by message-id.

| Field | Default | Notes |
| --- | --- | --- |
| `deadline_ms` | `20000` | Total time one client request may spend across all backend attempts. When it runs out the proxy answers with `unavailable_reply`. `0` disables the deadline. Set the client's article timeout a little above this |
| `checkout_wait_ms` | `3000` | Longest one attempt waits for a free connection on a backend, also cut to what is left of `deadline_ms` |
| `backlog_skip` | `true` | Skip a backend at once, without queueing, when its estimated connection wait is too long |
| `backlog_skip_percent` | `100` | Skip when the estimated wait is longer than this percentage of the attempt's wait. Lower values skip sooner |
| `unavailable_reply` | `"503"` | `"503"` or `"430"`: the reply when a request ends before every eligible backend returned `430`, either because `deadline_ms` ran out or because every remaining backend was busy, skipped, or failed. A request that every eligible backend answered with `430` always gets `430` |
| `unfinished_replies_before_430` | `2` | How many `503`s one message-id may get, once at least one backend returned `430` for it, before the next unfinished lookup answers `430`. `0` keeps answering `unavailable_reply`. Ignored when `unavailable_reply` is `"430"` |
| `backlog_last_resort` | `"wait-shortest"` | What a request does when backlog skipping leaves no other backend to try in the tier that should serve it. `"wait-shortest"` queues on the skipped backend with the shortest estimated wait, bounded by `checkout_wait_ms` and what is left of `deadline_ms`; `"give-up"` ends the request with `unavailable_reply` |

The estimated wait is `(requests ahead in the pool queue) × (average time one
command holds a connection) / (pool size)`. The average is a running mean of
recent commands on that backend, so the estimate is zero until the backend has
finished a command, and zero while it has an idle connection or room to open
one. Background `STAT` probes use the same skip rule with their fixed 2-second
wait.

A skipped or timed-out backend is unavailable for that request only. It is not
recorded as missing the article, and the next request tries it again. As with
any busy backend, a skip does not move the lookup to a lower-priority tier
while another backend in the current tier has not answered `430`.

Skipping saves time only when another backend can answer instead. When every
backend left in the tier was skipped as backlogged, the default
`backlog_last_resort = "wait-shortest"` queues on the one expected to free a
connection first rather than failing the request. That wait is bounded like any
other checkout, and if it times out the request ends with `unavailable_reply`
as before. A backend that
answered `430` or failed is never a last-resort candidate, and background
`STAT` probes always skip.

The proxy answers `430` only when every backend that may serve the request
returned `430`, either during this request or as a cached availability fact.
Downloaders take `430` as final: a typical downloader stops trying an article
on a server after `430` but retries it later after other errors. A request that
ended because a backend was busy, skipped, or failed, or because the deadline
ran out, has not established that the article is missing, so it gets
`503 Backend error` by default and the downloader retries.
Set `unavailable_reply = "430"` to fail such requests fast instead.

Answering only `503` makes a truly dead article slow to fail when a small,
busy backend can never confirm the miss in time: the downloader retries it
again and again. So once a message-id has had `unfinished_replies_before_430`
`503`s, the next unfinished lookup answers `430` if at least one backend has
returned `430` for it, during this request or as a cached fact. A backend that
was busy, skipped, failed, or refused with another error (such as `451`) says
nothing either way, so it does not block the `430`. The count lives in a
fixed 64K-slot table (512 KiB); a slot collision only restarts a count.

| How the request ended | Reply |
| --- | --- |
| A backend returned the article (or `223` for `STAT`) | the backend's reply |
| Every eligible backend returned `430` (or has a cached `430` fact) | `430` |
| `deadline_ms` ran out | `unavailable_reply` (`503`) |
| Every backend left was busy, skipped, or failed, even if others returned `430` | `unavailable_reply` (`503`) |
| Either of the two above, after `unfinished_replies_before_430` earlier `503`s for this message-id, with at least one `430` | `430` |

Skips and checkout timeouts are summarised at `info` once per backend every
10 seconds instead of logged per request.

### `[memory]`

These settings control transport memory, not stored article bodies.

| Field | Default |
| --- | ---: |
| `socket_recv_buffer_size` | `16777216` |
| `socket_send_buffer_size` | `16777216` |
| `buffer_pool_size` | `1048576` |
| `buffer_pool_count` | `100` |
| `capture_pool_size` | `1048576` |
| `capture_pool_count` | `16` |

### `[cache]`

`[cache]` is optional. If you omit it entirely, the runtime still uses availability tracking internally.

| Field | Default | Notes |
| --- | --- | --- |
| `article_cache_capacity` | `64mb` | Memory tier capacity |
| `article_cache_ttl_secs` | `3600` | Base TTL |
| `store_article_bodies` | `false` | Availability tracking stays on either way |
| `availability_index_path` | unset | When unset in availability-only mode, defaults to `availability.idx` next to the config file |

The availability-only index has a fixed capacity. It stores exact message IDs,
compact fingerprints, and bits identifying providers that returned `430`.
Rotation and replacement can remove old entries, causing previous misses to be
retried. A fingerprint match alone cannot mark an article as missing.

### `[cache.disk]`

Optional disk tier for article bodies evicted from memory. It is only active when `store_article_bodies = true`.

| Field | Default |
| --- | --- |
| `path` | `/var/cache/nntp-proxy` |
| `capacity` | `10gb` |
| `compression` | `lz4` |
| `shards` | `4` |

### `[health_check]`

| Field | Default |
| --- | --- |
| `interval` | `30` seconds |
| `timeout` | `5` seconds |
| `unhealthy_threshold` | `3` failures |

### `[client_auth]` and `[[client_auth.users]]`

Client auth is optional and applies to the proxy itself, not the backend servers.

```toml
[client_auth]
greeting = "200 NNTP proxy ready"

[[client_auth.users]]
username = "reader"
password = "reader-password"
```

### `[[servers]]`

| Field | Default | Notes |
| --- | --- | --- |
| `host` | required | Backend hostname or IP; canonical hostname identity used for availability sharing |
| `port` | required | `119` for plain NNTP, `563` for NNTPS |
| `name` | required | Friendly name used in logs and the TUI |
| `username` / `password` | unset | Backend auth |
| `max_connections` | `10` | Per-backend pool size |
| `stat_missing` | `0` | Probe missing articles with `STAT` before `ARTICLE`/`BODY`/`HEAD` on this backend. Enable it on backends that correctly return `430` to speed up retrying missing articles. |
| `tier` | `0` | Lower tiers are preferred first |
| `use_tls` | `false` | TLS to the backend |
| `tls_verify_cert` | `true` | Keep enabled in production |
| `tls_cert_path` | unset | Additional PEM CA certificate |
| `connection_keepalive` | unset | Sends `DATE` on idle backend connections |
| `replacement_cooldown` | `30` seconds | Set `0` to disable |
| `health_check_max_per_cycle` | `5` | Per-backend health-check acquisition cap |
| `health_check_pool_timeout` | `100ms` | Wait time when health-checking the pool |
| `compress` | auto-detect | RFC 8054 backend `COMPRESS DEFLATE` |
| `compress_level` | unset | Level `0`-`9` when compression is enabled |
| `backend_idle_timeout` | `600` seconds | Clears idle backend connections after proxy-wide inactivity |

Availability identity is derived from the configured `host` value only; the
proxy does not resolve DNS to infer provider identity. Hostnames are compared
case-insensitively after removing one trailing DNS dot. Ports, credentials,
TLS settings, tiers, and display names do not create separate provider slots.

## Environment-based backend configuration

Indexed `NNTP_SERVER_<N>_*` variables can define backends without a TOML server list.

Currently supported backend environment fields:

- `HOST`
- `PORT`
- `NAME`
- `USERNAME`
- `PASSWORD`
- `MAX_CONNECTIONS`
- `STAT_MISSING`
- `USE_TLS`
- `TLS_VERIFY_CERT`
- `TLS_CERT_PATH`
- `CONNECTION_KEEPALIVE`
- `HEALTH_CHECK_MAX_PER_CYCLE`
- `HEALTH_CHECK_POOL_TIMEOUT`
- `TIER`

Example:

```bash
NNTP_SERVER_0_HOST=news.example.com \
NNTP_SERVER_0_PORT=563 \
NNTP_SERVER_0_NAME=Primary \
NNTP_SERVER_0_USE_TLS=true \
nntp-proxy
```

Indices must be contiguous. Loading stops at the first missing `NNTP_SERVER_<N>_HOST`.

## Credentials overlay

Set `NNTP_PROXY_CREDENTIALS_FILE` to a TOML overlay file when you want to keep secrets out of the main config.

Example:

```toml
[[servers]]
name = "Primary"
username = "backend-user"
password = "backend-pass"

[client_auth]

[[client_auth.users]]
username = "reader"
password = "reader-password"
```

Server entries in the overlay are matched by `name`.
