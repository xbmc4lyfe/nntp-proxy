#!/usr/bin/env python3
"""Measure end-to-end download throughput through nntp-proxy with NZBGet.

Queues one NZB in NZBGet (unpack off), samples NZBGet's download rate, the
host NIC receive rate, and per-backend proxy metrics every few seconds, then
deletes the download unfinished. Nothing is post-processed, so back-to-back
runs measure the network path only.

usage: nzbget_speed_run.py NZB [--seconds 180] [--warmup 45] [--label NAME]

Environment:
  NZBGET_URL        JSON-RPC endpoint (default http://127.0.0.1:6789/jsonrpc)
  NZBGET_USER/PASS  NZBGet control credentials (required)
  PROXY_METRICS     proxy /metrics URL (default http://127.0.0.1:9469/metrics)
  NIC               interface whose rx bytes to sample (optional)
  RESULTS           JSON-lines file to append each run's summary to (optional)

The mean of NZBGet's rate after the warm-up is the headline number. Compare
configurations on the same release, alternating, and look at per-backend
MB/s, in-use connections and waiters to see which backend limits the run.
Use a large release whose articles all exist (check a sample with STAT first)
so missing-article handling doesn't dominate.
"""
import base64
import json
import os
import re
import statistics
import sys
import time
import urllib.request

NZBGET_URL = os.environ.get("NZBGET_URL", "http://127.0.0.1:6789/jsonrpc")
METRICS_URL = os.environ.get("PROXY_METRICS", "http://127.0.0.1:9469/metrics")
NIC = os.environ.get("NIC")
PREFIX = "SPEEDTEST-"
SAMPLE_SECS = 5
LINE = re.compile(r"^nntp_proxy_backend_([a-z_]+)\{([^}]*)\} ([0-9.eE+-]+)$")


def rpc(method, params=()):
    auth = base64.b64encode(f"{os.environ['NZBGET_USER']}:{os.environ['NZBGET_PASS']}".encode()).decode()
    req = urllib.request.Request(
        NZBGET_URL,
        data=json.dumps({"method": method, "params": list(params)}).encode(),
        headers={"Authorization": "Basic " + auth},
    )
    return json.load(urllib.request.urlopen(req, timeout=60))["result"]


def scrape():
    """Per-backend metric values keyed like `pool_connections[in_use]`."""
    out = {}
    for line in urllib.request.urlopen(METRICS_URL, timeout=10).read().decode().splitlines():
        m = LINE.match(line)
        if not m:
            continue
        labels = dict(re.findall(r'(\w+)="([^"]*)"', m.group(2)))
        backend = labels.pop("backend", None)
        labels.pop("tier", None)
        if backend is None:
            continue
        key = m.group(1) + "".join(f"[{v}]" for _, v in sorted(labels.items()))
        out.setdefault(backend, {})[key] = float(m.group(3))
    return out


def nic_rx():
    if not NIC:
        return None
    with open(f"/sys/class/net/{NIC}/statistics/rx_bytes") as f:
        return int(f.read())


def delete_test_downloads():
    ids = [g["NZBID"] for g in rpc("listgroups", [0]) if g["NZBName"].startswith(PREFIX)]
    if ids:
        rpc("editqueue", ["GroupFinalDelete", "", ids])


def arg(name, default):
    return type(default)(sys.argv[sys.argv.index(name) + 1]) if name in sys.argv else default


def main():
    if len(sys.argv) < 2 or sys.argv[1].startswith("-"):
        sys.exit(__doc__)
    nzb = sys.argv[1]
    seconds, warmup, label = arg("--seconds", 180.0), arg("--warmup", 45.0), arg("--label", "run")

    # Fail on a bad metrics URL or NIC before anything is queued.
    scrape()
    nic_rx()
    delete_test_downloads()
    name = f"{PREFIX}{int(time.time())}-{label}"
    with open(nzb, "rb") as f:
        content = base64.b64encode(f.read()).decode()
    nzb_id = rpc("append", [name + ".nzb", content, "", 900, True, False, "", 0, "FORCE",
                            [{"Name": "*Unpack:", "Value": "no"}]])
    if nzb_id <= 0:
        sys.exit(f"NZBGet rejected the NZB ({nzb_id})")

    start = time.time()
    samples = []
    try:
        prev, prev_t, prev_rx = scrape(), time.time(), nic_rx()
        while time.time() - start < seconds:
            time.sleep(SAMPLE_SECS)
            status, cur, now, rx = rpc("status"), scrape(), time.time(), nic_rx()
            dt = now - prev_t
            per = {}
            for backend, m in cur.items():
                p = prev.get(backend, {})
                per[backend] = {
                    "mbps": (m.get("article_bytes_total", 0) - p.get("article_bytes_total", 0)) / dt / 1e6,
                    "in_use": m.get("pool_connections[in_use]", 0),
                    "waiters": m.get("pool_waiters", 0),
                    "missing": m.get("article_missing_total", 0) - p.get("article_missing_total", 0),
                }
            nic = (rx - prev_rx) / dt / 1e6 if rx is not None else None
            samples.append({"t": now - start, "nzbget": status["DownloadRate"] / 1e6, "nic": nic, "backends": per})
            busy = " ".join(f"{b[:8]}:{v['mbps']:.0f}/{v['in_use']:.0f}w{v['waiters']:.0f}"
                            for b, v in per.items() if v["mbps"] > 0.5 or v["in_use"])
            nic_text = f" nic {nic:6.1f}" if nic is not None else ""
            print(f"{now - start:5.0f}s nzbget {status['DownloadRate'] / 1e6:6.1f}{nic_text} MB/s | {busy}", flush=True)
            prev, prev_t, prev_rx = cur, now, rx
            if not any(g["NZBID"] == nzb_id for g in rpc("listgroups", [0])):
                break
    finally:
        delete_test_downloads()

    steady = [s for s in samples if s["t"] >= warmup]
    if not steady:
        sys.exit("run ended before the warm-up finished")
    rates = [s["nzbget"] for s in steady]
    result = {"label": label, "nzb": os.path.basename(nzb), "start": start,
              "mean": statistics.mean(rates), "median": statistics.median(rates)}
    if NIC:
        result["nic"] = statistics.mean(s["nic"] for s in steady)
    result["backends"] = {
        b: {k: round(statistics.mean(s["backends"][b][k] for s in steady if b in s["backends"]), 2)
            for k in ("mbps", "in_use", "waiters")}
        for b in steady[-1]["backends"]
    }
    if os.environ.get("RESULTS"):
        with open(os.environ["RESULTS"], "a") as f:
            f.write(json.dumps(result) + "\n")
    nic_text = f", NIC {result['nic']:.1f}" if NIC else ""
    print(f"\nRESULT {label}: mean {result['mean']:.1f} MB/s, median {result['median']:.1f}{nic_text}")
    for backend, d in result["backends"].items():
        if d["mbps"] or d["in_use"]:
            print(f"  {backend:14} {d['mbps']:6.1f} MB/s  in_use {d['in_use']:6.1f}  waiters {d['waiters']:5.1f}")


if __name__ == "__main__":
    main()
