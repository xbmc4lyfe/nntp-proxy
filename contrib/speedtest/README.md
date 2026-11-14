# NZBGet throughput runs

`nzbget_speed_run.py` measures real download throughput through the proxy. It queues one NZB in NZBGet with unpack off, samples for a fixed time, then deletes the download unfinished. Nothing is post-processed, so runs can go back to back.

```sh
NZBGET_USER=... NZBGET_PASS=... PROXY_METRICS=http://127.0.0.1:9469/metrics NIC=eth0 \
  RESULTS=results.jsonl contrib/speedtest/nzbget_speed_run.py big-release.nzb --seconds 180
```

Each sample prints NZBGet's rate and the NIC receive rate. It also prints, per backend, `MB/s / in_use connections w waiters`. The result is the mean after the warm-up (default 45 s).

## Method

- Use large releases whose articles all exist. STAT a random sample of segments on each provider first; missing-article handling otherwise dominates.
- Compare configurations on the same release, alternating A/B/A/B. Releases differ: article (segment) size matters as much as age.
- Read the NIC rate against your line rate. When the NIC sits near the line (about 122 MB/s of frames on 1 Gbit/s), the link is the limit and proxy changes will not show.
- Check the per-backend columns to find the limit:
  - every pool at `in_use = max` with waiters: backends are the constraint, and more connections or a faster provider help;
  - spare connections while the client waits: the client or the link is the constraint.

## What it found (2026-11-14, 1 Gbit/s link)

- **The proxy was not the bottleneck.** Direct probes without the proxy gave the same per-provider split, and socket queues were empty.
- **Per-article think time.** A provider that pauses about 0.37 s before each article delivers about 1.5 MB/s per connection on 740 KB segments, but about 2.7 MB/s on 4 MB segments. Large-segment releases fill the link with fewer connections.
- **Per-address throttling.** One provider throttled per server address: 100 connections on one address gave 75 MB/s, while 50 on each of two addresses gave 99.5 MB/s. Listing all of its addresses (for example in `/etc/hosts`) and raising `max_connections` took typical releases from about 100 to 110–114 MB/s, at the line.
