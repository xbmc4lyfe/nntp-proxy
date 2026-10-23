#!/usr/bin/env bash
# Bencher falls back to the latest version when a start-point hash is absent.
# Fail closed unless this suite has results at the exact base on the active head.
set -euo pipefail

if [ "$#" -ne 4 ]; then
    echo "usage: $0 <project> <base-branch> <base-sha> <adapter>" >&2
    exit 2
fi
project="$1"
branch="$2"
base_sha="$3"
adapter="$4"
reports="$(mktemp)"
trap 'rm -f "$reports"' EXIT

# Bound the lookup; an older baseline must be refreshed rather than guessed.
for ((page = 1; page <= 10; page++)); do
    bencher report list "$project" --branch "$branch" --testbed ubuntu-latest \
        --per-page 255 --page "$page" --direction desc --attempts 1 > "$reports"
    jq -e 'type == "array"' "$reports" > /dev/null
    if jq -e --arg hash "$base_sha" --arg adapter "$adapter" --arg branch "$branch" '
        any(.[];
            .branch.name == $branch and
            .branch.head.replaced == null and
            .branch.head.version.hash == $hash and
            .testbed.name == "ubuntu-latest" and
            .adapter == $adapter and
            any(.counts.results[]?; .benchmarks > 0)
        )
    ' "$reports" > /dev/null; then
        echo "Verified exact base benchmark report for $adapter."
        exit 0
    fi
    if [ "$(jq length "$reports")" -lt 255 ]; then
        break
    fi
done

echo "::error::Exact $adapter baseline for $branch at $base_sha is missing. Finish the base benchmark upload, then rerun this job; refusing Bencher's latest-version fallback." >&2
exit 1
