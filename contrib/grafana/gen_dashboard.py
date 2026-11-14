#!/usr/bin/env python3
"""Generate the nntp-proxy Grafana dashboard.

    python3 gen_dashboard.py            # writes nntp-proxy-dashboard.json
    python3 gen_dashboard.py --local    # also writes nntp-proxy-dashboard.local.json, pinned
                                        # to a VictoriaMetrics data source with a VictoriaLogs row

Edit this file, not the JSON, and commit both.
"""
import json, sys, copy
SEL = 'job=~"$job",instance=~"$instance"'
B = SEL + ',backend=~"$backend"'
RI = "$__rate_interval"
panels, y = [], 0
pid = [0]
def nid():
    pid[0] += 1; return pid[0]
def row(title):
    global y
    panels.append({"type": "row", "title": title, "id": nid(), "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y}, "panels": []}); y += 1
X = [0]
def place(w, h):
    global y
    if X[0] + w > 24:
        X[0] = 0; y += LASTH[0]
    g = {"h": h, "w": w, "x": X[0], "y": y}; X[0] += w; LASTH[0] = h
    return g
LASTH = [0]
def flush():
    global y
    if X[0]:
        y += LASTH[0]; X[0] = 0
def ts(title, targets, unit="short", w=12, h=8, stack=False, desc="", minv=None, maxv=None):
    fc = {"defaults": {"unit": unit, "custom": {"lineWidth": 1, "fillOpacity": 25 if stack else 8, "stacking": {"mode": "normal" if stack else "none"}, "showPoints": "never"}}, "overrides": []}
    if minv is not None: fc["defaults"]["min"] = minv
    if maxv is not None: fc["defaults"]["max"] = maxv
    panels.append({"type": "timeseries", "title": title, "description": desc, "id": nid(), "gridPos": place(w, h),
                   "datasource": {"type": "prometheus", "uid": "${datasource}"}, "fieldConfig": fc,
                   "options": {"legend": {"displayMode": "table", "placement": "right", "calcs": ["mean", "max", "lastNotNull"]}, "tooltip": {"mode": "multi", "sort": "desc"}},
                   "targets": [{"refId": chr(65 + i), "expr": e, "legendFormat": l, "datasource": {"type": "prometheus", "uid": "${datasource}"}} for i, (e, l) in enumerate(targets)]})
def stat(title, expr, unit="short", w=4, h=4, desc="", thresholds=None):
    steps = thresholds or [{"color": "green", "value": None}]
    panels.append({"type": "stat", "title": title, "description": desc, "id": nid(), "gridPos": place(w, h),
                   "datasource": {"type": "prometheus", "uid": "${datasource}"},
                   "fieldConfig": {"defaults": {"unit": unit, "thresholds": {"mode": "absolute", "steps": steps}}, "overrides": []},
                   "options": {"reduceOptions": {"calcs": ["lastNotNull"]}, "colorMode": "value", "graphMode": "area"},
                   "targets": [{"refId": "A", "expr": expr, "datasource": {"type": "prometheus", "uid": "${datasource}"}}]})

row("Overview")
stat("Download throughput", f'sum(rate(nntp_proxy_backend_article_bytes_total{{{SEL}}}[{RI}]))', "Bps", desc="Article bytes received from all backends.")
stat("Articles / s", f'sum(rate(nntp_proxy_backend_articles_total{{{SEL}}}[{RI}]))', "reqps")
stat("Missing (430) share", f'sum(rate(nntp_proxy_backend_article_missing_total{{{SEL}}}[{RI}])) / clamp_min(sum(rate(nntp_proxy_backend_commands_total{{{SEL}}}[{RI}])), 1e-9)', "percentunit",
     desc="430 replies divided by all backend commands. A 430 on one backend is normal; this is how often lookups miss.")
stat("Client sessions", f'sum(nntp_proxy_client_sessions_active{{{SEL}}})')
stat("Proxy CPU", f'sum(rate(nntp_proxy_process_cpu_seconds_total{{{SEL}}}[{RI}]))', "percentunit", desc="CPU cores used (1.0 = one core).")
stat("Resident memory", f'sum(nntp_proxy_process_resident_memory_bytes{{{SEL}}})', "bytes")
flush()
ts("Throughput by backend", [(f'sum by (backend) (rate(nntp_proxy_backend_article_bytes_total{{{B}}}[{RI}]))', "{{backend}}")], "Bps", stack=True)
ts("Client commands by class", [(f'sum by (command) (rate(nntp_proxy_client_commands_total{{{SEL}}}[{RI}]))', "{{command}}")], "reqps", stack=True)
flush()

row("Backend health")
ts("Missing (430) share by backend", [(f'sum by (backend) (rate(nntp_proxy_backend_article_missing_total{{{B}}}[{RI}])) / clamp_min(sum by (backend) (rate(nntp_proxy_backend_commands_total{{{B}}}[{RI}])), 1e-9)', "{{backend}}")], "percentunit", w=8, minv=0, maxv=1,
   desc="Share of commands answered 430. A backend that suddenly misses much more than usual may have lost retention or be routing to a bad server.")
ts("Article standing by backend", [(f'min by (backend) (nntp_proxy_backend_article_standing{{{B}}})', "{{backend}}")], "percentunit", w=8, minv=0, maxv=1,
   desc="Falls by 1/8 on every 430 and climbs by 1/128 of the gap on every hit. With routing.article.standing = \"adaptive\" it scales the backend's share of article lookups within its tier, so a backend hit by a takedown drops to the bottom and works its way back. A backend near 0 while its tier peers stay near 1 is missing that download.")
ts("Article transfer rate per backend", [(f'sum by (backend) (rate(nntp_proxy_backend_articles_total{{{B}}}[{RI}]))', "{{backend}}")], "reqps", w=8)
flush()
ts("Refusals and server errors by backend", [(f'sum by (backend) (rate(nntp_proxy_backend_error_replies_total{{{B},code_class="4xx"}}[{RI}])) - sum by (backend) (rate(nntp_proxy_backend_article_missing_total{{{B}}}[{RI}]))', "{{backend}} 4xx (not 430)"),
   (f'sum by (backend) (rate(nntp_proxy_backend_error_replies_total{{{B},code_class="5xx"}}[{RI}]))', "{{backend}} 5xx")], "reqps", w=8)
ts("Transport errors by backend", [(f'sum by (backend) (rate(nntp_proxy_backend_errors_total{{{B}}}[{RI}])) - sum by (backend) (rate(nntp_proxy_backend_error_replies_total{{{B}}}[{RI}]))', "{{backend}}")], "reqps", w=8,
   desc="Connection-level failures (resets, timeouts, TLS errors): all backend errors minus 4xx/5xx replies.")
ts("Time to first byte p50 / p95", [(f'histogram_quantile(0.95, sum by (backend, le) (rate(nntp_proxy_backend_first_byte_seconds_bucket{{{B}}}[{RI}])))', "{{backend}} p95"),
   (f'histogram_quantile(0.5, sum by (backend, le) (rate(nntp_proxy_backend_first_byte_seconds_bucket{{{B}}}[{RI}])))', "{{backend}} p50")], "s", w=8,
   desc="Only measured for commands timed individually (retries, failovers, prechecks).")
flush()
ts("Exchange duration p95", [(f'histogram_quantile(0.95, sum by (backend, le) (rate(nntp_proxy_backend_exchange_duration_seconds_bucket{{{B}}}[{RI}])))', "{{backend}}")], "s", w=24,
   desc="How long one exchange held a backend connection.")
flush()

row("Connection pools")
ts("Pool utilisation (in use / max)", [(f'sum by (backend) (nntp_proxy_backend_pool_connections{{{B},state="in_use"}}) / clamp_min(sum by (backend) (nntp_proxy_backend_pool_max_connections{{{B}}}), 1)', "{{backend}}")], "percentunit", w=8, minv=0, maxv=1)
ts("Requests waiting for a connection", [(f'sum by (backend) (nntp_proxy_backend_pool_waiters{{{B}}})', "{{backend}}")], w=8)
ts("Connection attempts by outcome", [(f'sum by (backend, outcome) (rate(nntp_proxy_backend_connect_attempts_total{{{B}}}[{RI}]))', "{{backend}} {{outcome}}")], "ops", w=8,
   desc="login_refused usually means the provider account's connection limit is reached.")
flush()
ts("Connections closed by reason", [(f'sum by (backend, reason) (rate(nntp_proxy_backend_connections_closed_total{{{B}}}[{RI}]))', "{{backend}} {{reason}}")], "ops", w=8)
ts("Health check failures", [(f'sum by (backend) (rate(nntp_proxy_backend_health_check_failures_total{{{B}}}[{RI}]))', "{{backend}}")], "ops", w=8)
ts("Login backoff active", [(f'max by (backend) (nntp_proxy_backend_login_backoff{{{B}}})', "{{backend}}")], w=8, minv=0, maxv=1)
flush()
ts("Connection cap", [(f'max by (backend) (nntp_proxy_backend_connection_cap{{{B}}})', "{{backend}} cap"),
   (f'sum by (backend) (nntp_proxy_backend_pool_connections{{{B}}})', "{{backend}} open")], w=24,
   desc="Connections each backend may hold. With connection_cap = \"adaptive\" it drops to the accepted count on a refused login and climbs back one a minute, and past max_connections one every 10 minutes while saturated.")
flush()

row("Article admission")
ts("Backlog skips and busy timeouts", [(f'sum by (backend) (rate(nntp_proxy_backend_backlog_skips_total{{{B}}}[{RI}]))', "{{backend}} skipped"),
   (f'sum by (backend) (rate(nntp_proxy_backend_busy_timeouts_total{{{B}}}[{RI}]))', "{{backend}} timed out")], "ops", w=8,
   desc="Requests that passed over a backend because its queue was too long, or waited and gave up.")
ts("Last-resort waits", [(f'sum by (backend) (rate(nntp_proxy_backend_last_resort_waits_total{{{B}}}[{RI}]))', "{{backend}} waits"),
   (f'sum by (backend) (rate(nntp_proxy_backend_last_resort_timeouts_total{{{B}}}[{RI}]))', "{{backend}} timeouts")], "ops", w=8)
ts("Deadline expiries and error-reply failovers", [(f'sum by (backend) (rate(nntp_proxy_backend_deadline_expiries_total{{{B}}}[{RI}]))', "{{backend}} deadline"),
   (f'sum by (backend) (rate(nntp_proxy_backend_error_reply_failovers_total{{{B}}}[{RI}]))', "{{backend}} failover")], "ops", w=8)
flush()
ts("Service time (EWMA)", [(f'max by (backend) (nntp_proxy_backend_service_time_seconds{{{B}}})', "{{backend}}")], "s", w=12)
ts("Estimated connection wait", [(f'max by (backend) (nntp_proxy_backend_estimated_wait_seconds{{{B}}})', "{{backend}}")], "s", w=12)
flush()

row("Process and cache")
ts("CPU cores", [(f'sum by (instance) (rate(nntp_proxy_process_cpu_seconds_total{{{SEL}}}[{RI}]))', "{{instance}}")], "short", w=8)
ts("Resident memory", [(f'sum by (instance) (nntp_proxy_process_resident_memory_bytes{{{SEL}}})', "{{instance}}")], "bytes", w=8)
ts("Open file descriptors / threads", [(f'sum by (instance) (nntp_proxy_process_open_fds{{{SEL}}})', "{{instance}} fds"), (f'sum by (instance) (nntp_proxy_process_threads{{{SEL}}})', "{{instance}} threads")], w=8)
flush()
ts("Cache hit ratio", [(f'sum(rate(nntp_proxy_cache_lookups_total{{{SEL},result="hit"}}[{RI}])) / clamp_min(sum(rate(nntp_proxy_cache_lookups_total{{{SEL}}}[{RI}])), 1e-9)', "hit ratio")], "percentunit", w=12, minv=0, maxv=1)
ts("Client bytes", [(f'sum by (direction) (rate(nntp_proxy_client_bytes_total{{{SEL}}}[{RI}]))', "{{direction}}")], "Bps", w=12,
   desc="Updated when client sessions end; use backend article bytes for live throughput.")
flush()

def var_query(name, q, multi=True, label=None):
    return {"name": name, "label": label or name, "type": "query", "datasource": {"type": "prometheus", "uid": "${datasource}"},
            "query": {"query": q, "refId": name}, "definition": q, "refresh": 2, "includeAll": True, "multi": multi,
            "current": {"selected": True, "text": "All", "value": "$__all"}, "allValue": ".*", "sort": 1}
dash = {
    "title": "NNTP Proxy", "uid": "nntp-proxy", "tags": ["nntp", "usenet"], "timezone": "browser", "schemaVersion": 39,
    "refresh": "30s", "time": {"from": "now-6h", "to": "now"}, "editable": True, "graphTooltip": 1,
    "templating": {"list": [
        {"name": "datasource", "label": "Data source", "type": "datasource", "query": "prometheus", "current": {}, "hide": 0},
        var_query("job", 'label_values(nntp_proxy_build_info, job)'),
        var_query("instance", 'label_values(nntp_proxy_build_info{job=~"$job"}, instance)'),
        var_query("backend", 'label_values(nntp_proxy_backend_commands_total{job=~"$job",instance=~"$instance"}, backend)'),
    ]},
    "annotations": {"list": []},
    "panels": panels,
}
with open("nntp-proxy-dashboard.json", "w") as f:
    json.dump(dash, f, indent=2)
    f.write("\n")
if "--local" not in sys.argv[1:]:
    print(len(panels), "panels")
    sys.exit(0)

# local variant: pinned datasource, env variable, plus a VictoriaLogs row
local = copy.deepcopy(dash)
local["uid"] = "nntp-proxy-local"; local["title"] = "NNTP Proxy (server)"
local["templating"]["list"][0]["current"] = {"text": "VictoriaMetrics", "value": "victoriametrics"}
y2 = max(p["gridPos"]["y"] + p["gridPos"]["h"] for p in panels)
local["panels"].append({"type": "row", "title": "Proxy logs (VictoriaLogs)", "id": 900, "collapsed": False, "gridPos": {"h": 1, "w": 24, "x": 0, "y": y2}, "panels": []})
local["panels"].append({"type": "logs", "title": "Warnings and errors", "id": 901, "gridPos": {"h": 12, "w": 24, "x": 0, "y": y2 + 1},
    "datasource": {"type": "victoriametrics-logs-datasource", "uid": "victorialogs"},
    "options": {"showTime": True, "wrapLogMessage": True, "sortOrder": "Descending", "enableLogDetails": True},
    "targets": [{"refId": "A", "datasource": {"type": "victoriametrics-logs-datasource", "uid": "victorialogs"},
                 "expr": 'container:~"^nntp-proxy" (WARN OR ERROR) | fields _time, container, _msg', "queryType": "instant"}]})
local["panels"].append({"type": "timeseries", "title": "Warning/error log lines per minute", "id": 902, "gridPos": {"h": 8, "w": 24, "x": 0, "y": y2 + 13},
    "datasource": {"type": "victoriametrics-logs-datasource", "uid": "victorialogs"},
    "targets": [{"refId": "A", "datasource": {"type": "victoriametrics-logs-datasource", "uid": "victorialogs"},
                 "expr": 'container:~"^nntp-proxy" (WARN OR ERROR) | stats by (container) count() as lines', "queryType": "statsRange", "step": "1m"}]})
with open("nntp-proxy-dashboard.local.json", "w") as f:
    json.dump(local, f, indent=2)
    f.write("\n")
print(len(panels), "panels")
