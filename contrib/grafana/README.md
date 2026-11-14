# Grafana dashboard and alert rules

Starting points for monitoring nntp-proxy through its Prometheus endpoint
(`[metrics] listen`, see [Metrics](../../docs/operator/metrics.md)).

| File | What it is |
| --- | --- |
| `nntp-proxy-dashboard.json` | Grafana dashboard. Import it and pick your Prometheus or VictoriaMetrics data source; the `job`, `instance`, and `backend` variables filter every panel. |
| `gen_dashboard.py` | Generates the dashboard JSON. Edit this, run `python3 gen_dashboard.py`, and commit both. `--local` also writes a variant pinned to a `victoriametrics` data source with a VictoriaLogs log row. |
| `nntp-proxy-alerts.rules.yml` | Alerting rules for Prometheus or vmalert. Adjust the `job` selector, thresholds, and `for:` durations to your deployment. |
| `nntp-proxy-alerts.test.yml` | `promtool test rules` cases for the rules. |

Check the rules after editing them:

```sh
promtool check rules nntp-proxy-alerts.rules.yml
promtool test rules nntp-proxy-alerts.test.yml
```

or, without a local promtool:

```sh
docker run --rm -v "$PWD":/w -w /w --entrypoint promtool prom/prometheus:v3.5.0 \
  test rules nntp-proxy-alerts.test.yml
```

## Severities

`critical` and `warning` alerts describe something an operator should act on.
`NntpBackendStandingLow` is `info`: a backend whose article standing stayed
below 0.1 for half an hour while the proxy was downloading, which is usually a
takedown of the current download on that provider. With
`[routing.article] standing = "adaptive"` the proxy already routes around it,
so route `severity="info"` to a receiver that records it without notifying
anyone (for example an Alertmanager route with no receivers configured, or a
Grafana notification policy with an always-active mute timing).
