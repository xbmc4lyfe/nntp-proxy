#!/usr/bin/env bash
# Run the benchmark subset that CI tracks with Bencher.
#
# The suites are kept small enough for a GitHub-hosted runner and cover the
# proxy hot paths: multiline framing, request classification, backend
# selection, yEnc line decoding, retained/pooled buffers and the cache.
#
#   divan     Wall-clock Divan benches, converted to Bencher Metric Format JSON
#             (Bencher has no Divan adapter). Tracked as the `latency` Measure.
#   callgrind Gungraun (Callgrind) benches. Instruction counts do not depend on
#             runner noise, so they gate small regressions reliably. Parsed by
#             Bencher's `rust_gungraun` adapter. Needs valgrind and a
#             `gungraun-runner` matching the gungraun version in Cargo.lock.
#
# Usage: scripts/bench-ci.sh <divan|callgrind> <output-file>

set -euo pipefail

usage() {
    echo "usage: $0 <divan|callgrind> <output-file>" >&2
    exit 2
}

[ "$#" -eq 2 ] || usage
suite="$1"
case "$2" in
    /*) output="$2" ;;
    *) output="$PWD/$2" ;;
esac

repo_root="$(git rev-parse --show-toplevel)"
cd "$repo_root"

# Each entry is "<bench target>[ <divan filter args>...]". Filters keep only
# benches of production code: yenc_decoding also measures the external yenc
# crate and multiline_framing a stateless control. They also drop the
# per-verb command_parsing matrix (the callgrind suite covers classification
# deterministically) and thread-contention runs, which are too noisy on shared
# runners. Every extra wall-clock bench raises the chance of a false alert.
divan_benches=(
    "multiline_framing --skip stateless"
    "retained_buffer"
    "client_stat"
    "router_selection --skip contention"
    "yenc_decoding (wire_stuffed|unstuffed)_line"
    "command_parsing realistic_workload"
    "response_parsing"
    "cache_ingest"
    "cache_response_generation"
)

callgrind_benches=(
    "request_classifier_callgrind"
    "multiline_framing_callgrind"
    "availability_index_callgrind"
    "cache_miss_roundtrip_callgrind"
)

bench_target_args() {
    local entry target
    for entry in "$@"; do
        target="${entry%% *}"
        printf -- '--bench\n%s\n' "$target"
    done
}

build_benches() {
    local -a targets
    mapfile -t targets < <(bench_target_args "$@")
    cargo bench --no-run --features framing-bench "${targets[@]}"
}

run_divan() {
    local entry target
    local -a filters
    raw="$(mktemp)"
    trap 'rm -f "$raw"' EXIT

    build_benches "${divan_benches[@]}"
    for entry in "${divan_benches[@]}"; do
        read -r -a filters <<<"$entry"
        target="${filters[0]}"
        filters=("${filters[@]:1}")
        echo "### divan bench: $target" | tee -a "$raw"
        cargo bench --features framing-bench --bench "$target" -- --color never "${filters[@]}" \
            | tee -a "$raw"
    done

    python3 - "$raw" >"$output" <<'PY'
"""Convert Divan's tree output into Bencher Metric Format (BMF) JSON.

Each leaf row reads `<tree><name>  fastest │ slowest │ median │ mean │ samples │ iters`.
Tree prefixes are three characters per level, so the depth of a row identifies
its parents. The benchmark name is the bench target followed by that path.
"""
import json
import re
import sys

UNITS_NS = {"ps": 1e-3, "ns": 1.0, "µs": 1e3, "us": 1e3, "ms": 1e6, "s": 1e9, "m": 6e10, "h": 3.6e12}
TIME = r"([0-9][0-9.]*)\s*(ps|ns|µs|us|ms|s|m|h)"
LEAF = re.compile(
    r"^(?P<prefix>(?:[│ ]  )*)(?:├─|╰─) (?P<name>\S(?:.*?\S)?)\s{2,}"
    + TIME + r"\s+│\s+" + TIME + r"\s+│\s+" + TIME + r"\s+│\s+" + TIME
    + r"\s+│\s+\d+\s+│\s+\d+\s*$"
)
PARENT = re.compile(r"^(?P<prefix>(?:[│ ]  )*)(?:├─|╰─) (?P<name>[^\s│](?:[^│]*?[^\s│])?)[\s│]*$")
TOP = re.compile(r"^(?P<name>[^\s│├╰]\S*)(?:\s.*)?$")
TARGET = re.compile(r"^### divan bench: (?P<target>\S+)$")


def ns(value, unit):
    return round(float(value) * UNITS_NS[unit], 3)


results = {}
target = None
stack = []
with open(sys.argv[1], encoding="utf-8") as raw:
    for line in raw:
        line = line.rstrip("\n")
        if match := TARGET.match(line):
            target, stack = match["target"], []
            continue
        if target is None or not line.strip() or line.startswith("Timer precision"):
            continue
        if match := LEAF.match(line):
            depth = len(match["prefix"]) // 3 + 1
            path = stack[:depth] + [match["name"]]
            if path[0] == target:
                path = path[1:]
            fastest, slowest, median = (ns(*match.groups()[i:i + 2]) for i in (2, 4, 6))
            name = "::".join([target] + path)
            if name in results:
                sys.exit(f"duplicate benchmark name: {name}")
            results[name] = {
                "latency": {"value": median, "lower_value": fastest, "upper_value": slowest}
            }
            continue
        if match := PARENT.match(line):
            depth = len(match["prefix"]) // 3 + 1
            stack = stack[:depth] + [match["name"]]
            continue
        if match := TOP.match(line):
            stack = [match["name"]]

if not results:
    sys.exit("no Divan results were parsed")
json.dump(results, sys.stdout, indent=2, ensure_ascii=False)
print(f"parsed {len(results)} Divan benchmarks", file=sys.stderr)
PY
}

run_callgrind() {
    local -a targets
    mapfile -t targets < <(bench_target_args "${callgrind_benches[@]}")
    build_benches "${callgrind_benches[@]}"
    cargo bench --features framing-bench "${targets[@]}" 2>&1 | tee "$output"
}

case "$suite" in
    divan) run_divan ;;
    callgrind) run_callgrind ;;
    *) usage ;;
esac
