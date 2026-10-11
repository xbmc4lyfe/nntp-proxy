"""Exercise the baseline gate without a Bencher account or network access."""

import json
import os
from pathlib import Path
import subprocess
import tempfile
import unittest


SCRIPT = Path(__file__).resolve().parents[1] / "scripts/bench-require-baseline.sh"
BASE_HASH = "a" * 40


def report(*, commit=BASE_HASH, adapter="json", replaced=None, benchmarks=41):
    # Fields consumed from Bencher v0.6.13 JsonReport/JsonHead/JsonReportCounts.
    return {
        "branch": {"name": "main", "head": {
            "version": {"number": 1, "hash": commit}, "replaced": replaced,
        }},
        "testbed": {"name": "ubuntu-latest"},
        "adapter": adapter,
        "counts": {"results": [{"benchmarks": benchmarks, "measures": 1}]},
    }


class BencherBaselineTests(unittest.TestCase):
    def run_gate(self, pages, *, adapter="json", failure=False):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "pages.json").write_text(json.dumps(pages))
            bencher = root / "bencher"
            bencher.write_text('''#!/usr/bin/env python3
import json
import os
from pathlib import Path
import sys
args = sys.argv[1:]
assert args[:3] == ["report", "list", "example-project"], args
assert args[args.index("--branch") + 1] == "main", args
assert args[args.index("--testbed") + 1] == "ubuntu-latest", args
assert args[args.index("--per-page") + 1] == "255", args
if os.environ["FAIL_QUERY"] == "1":
    sys.exit(9)
page = int(args[args.index("--page") + 1])
pages = json.loads(Path(__file__).with_name("pages.json").read_text())
print(json.dumps(pages[page - 1] if page <= len(pages) else []))
''')
            bencher.chmod(0o755)
            return subprocess.run(
                ["bash", str(SCRIPT), "example-project", "main", BASE_HASH, adapter],
                env={**os.environ, "PATH": f"{root}:{os.environ['PATH']}",
                     "FAIL_QUERY": "1" if failure else "0"},
                text=True, capture_output=True, timeout=10,
            )

    def test_accepts_exact_hash_with_results(self):
        result = self.run_gate([[report()]])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_latest_report_from_another_commit(self):
        result = self.run_gate([[report(commit="b" * 40)]])
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("baseline", result.stderr)

    def test_requires_results_from_the_requested_suite(self):
        result = self.run_gate([[report()]], adapter="rust_gungraun")
        self.assertNotEqual(result.returncode, 0)

    def test_accepts_callgrind_report(self):
        result = self.run_gate([[report(adapter="rust_gungraun")]], adapter="rust_gungraun")
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_rejects_replaced_branch_head(self):
        result = self.run_gate([[report(replaced="2026-10-11T00:00:00Z")]])
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_empty_report(self):
        result = self.run_gate([[report(benchmarks=0)]])
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_no_reports(self):
        result = self.run_gate([[]])
        self.assertNotEqual(result.returncode, 0)

    def test_finds_exact_hash_on_later_page(self):
        result = self.run_gate([[report(commit="b" * 40)] * 255, [report()]])
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_query_failure_is_not_a_valid_baseline(self):
        result = self.run_gate([[report()]], failure=True)
        self.assertNotEqual(result.returncode, 0)

    def test_rejects_malformed_response(self):
        result = self.run_gate([{"error": "not a report list"}])
        self.assertNotEqual(result.returncode, 0)


if __name__ == "__main__":
    unittest.main()
