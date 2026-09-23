#!/usr/bin/env python3
"""Alternate identical benchmark profiles across baseline and candidate trees."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
import csv
import json
from pathlib import Path
import subprocess
import sys
import time

import checks


def measure_hashes(repo, selected, cache_only):
    """Hashes the measurement-only source that must match in both trees."""
    files = []
    if cache_only:
        files.append("benches/cache_churn.rs")
    else:
        files.extend([
            "benches/catalog/case.rs",
            "benches/catalog/reads.rs",
            "benches/catalog/main.rs",
        ])
    if "B15" in selected and not cache_only:
        files.extend([
            "benches/allocation.rs",
            "benches/fixture.rs",
            "benches/read_path_memory.rs",
            "benches/sparql/hot_path.rs",
            "benches/support.rs",
        ])
    if set(selected) & {"B09", "B10"}:
        files.append("src/internal/search/bench.rs")
    if set(selected) & {"B14", "B17"}:
        files.append("src/internal/store_bench.rs")
    return {path: checks.file_hash(repo / path) for path in files}


def prepare(repo, log, selected, cache_only, catalog):
    """Builds benchmark and internal-test executables before timing starts."""
    targets = {"cache_churn"} if cache_only else {"consistency_catalog"}
    if not cache_only:
        for benchmark in selected:
            for command in catalog[benchmark]["commands"]:
                if "--bench" in command["argv"]:
                    targets.add(command["argv"][command["argv"].index("--bench") + 1])
    commands = [["cargo", "bench", "--locked", "--all-features", "--bench", target,
                 "--no-run"] for target in sorted(targets)]
    if set(selected) & {"B09", "B10", "B14", "B17"}:
        commands.append(["cargo", "test", "--locked", "--all-features", "--lib", "--no-run"])
    with log.open("x") as stream:
        for command in commands:
            code = subprocess.run(command, cwd=repo, stdout=stream,
                                  stderr=subprocess.STDOUT, check=False).returncode
            if code:
                return code
    return 0


def semantic_result(command):
    """Extracts correctness and completion state while excluding measured work."""
    native = command.get("native", {})
    result = native.get("result", {})
    state_keys = (
        "completed", "fingerprint", "fingerprints", "hits", "output_digest", "result_rows", "source_rows",
        "indexed_rows", "first", "second", "rejected", "unchanged_on_reject",
        "restart_equal", "remaining_debt", "expired",
        "present_after_restart", "failed_repair", "duplicates_avoided",
    )
    state = {key: result[key] for key in state_keys if key in result}
    if "traces" in result:
        state["traces"] = [{
            "trace": trace["trace"],
            "samples": trace["samples"],
            "exact_checksum": trace["exact"]["checksum"],
            "actual_checksum": trace["actual"]["checksum"],
        } for trace in result["traces"]]
    return {
        "status": native.get("status", command.get("status")),
        "case": native.get("case"),
        "state": state,
        "completion_boundary": command.get("completion_boundary"),
        "exit": command.get("exit"),
    }


def compare_pair(baseline, candidate):
    """Returns exact semantic mismatches for one paired command sequence."""
    left = json.loads((baseline / "complete.json").read_text())["commands"]
    right = json.loads((candidate / "complete.json").read_text())["commands"]
    mismatches = []
    if len(left) != len(right):
        return [{"reason": "command-count", "baseline": len(left), "candidate": len(right)}]
    for index, (before, after) in enumerate(zip(left, right)):
        before_state = semantic_result(before)
        after_state = semantic_result(after)
        if before_state != after_state:
            mismatches.append({"command": index, "baseline": before_state, "candidate": after_state})
        if (before.get("native") or after.get("native")) \
                and (not before.get("binary_sha256") or not after.get("binary_sha256")):
            mismatches.append({"command": index, "reason": "missing-binary-identity"})
    return mismatches


def native_work(command):
    """Returns top-level work or summed production-cache trace work."""
    result = command.get("native", {}).get("result", {})
    if "work_ns" in result:
        return result["work_ns"]
    if "traces" in result:
        return sum(trace["actual"]["work_ns"] for trace in result["traces"])
    return None


def cache_metrics(command):
    """Returns cache trace hit, miss, eviction, byte, and work observations."""
    traces = command.get("native", {}).get("result", {}).get("traces", [])
    return [{
        "trace": trace["trace"],
        "samples": trace["samples"],
        "exact": trace["exact"],
        "actual": trace["actual"],
        "exact_retained_bytes_estimate": trace["exact_retained_bytes_estimate"],
        "actual_retained_bytes_estimate": trace["actual_retained_bytes_estimate"],
    } for trace in traces]


def pair_metrics(baseline, candidate, pair):
    """Returns paired time, CPU, memory, I/O, and native-work observations."""
    left = json.loads((baseline / "complete.json").read_text())["commands"]
    right = json.loads((candidate / "complete.json").read_text())["commands"]
    rows = []
    for index, (before, after) in enumerate(zip(left, right)):
        before_work = native_work(before)
        after_work = native_work(after)
        rows.append({
            "pair": pair,
            "command": index,
            "label": before.get("label"),
            "case": json.dumps(before.get("native", {}).get("case"), sort_keys=True),
            "baseline_seconds": before.get("seconds"),
            "candidate_seconds": after.get("seconds"),
            "baseline_cpu": before.get("cpu_seconds"),
            "candidate_cpu": after.get("cpu_seconds"),
            "baseline_rss": before.get("peak_rss_bytes"),
            "candidate_rss": after.get("peak_rss_bytes"),
            "baseline_inputs": before.get("fs_inputs"),
            "candidate_inputs": after.get("fs_inputs"),
            "baseline_outputs": before.get("fs_outputs"),
            "candidate_outputs": after.get("fs_outputs"),
            "baseline_work_ns": before_work,
            "candidate_work_ns": after_work,
            "baseline_cache": json.dumps(cache_metrics(before), sort_keys=True),
            "candidate_cache": json.dumps(cache_metrics(after), sort_keys=True),
        })
    return rows


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("baseline", type=Path)
    parser.add_argument("candidate", type=Path)
    parser.add_argument("output", type=Path)
    parser.add_argument("--runs", type=int, default=5)
    parser.add_argument("--only", nargs="*", default=[])
    parser.add_argument("--cache-only", action="store_true")
    args = parser.parse_args()
    baseline = args.baseline.resolve(strict=True)
    candidate = args.candidate.resolve(strict=True)
    output = args.output.resolve()
    if args.runs < 5:
        raise ValueError("paired release evidence requires at least five runs")
    selected = args.only or [f"B{index:02}" for index in range(1, 21)]
    if args.cache_only and selected != ["B15"]:
        parser.error("--cache-only requires --only B15")
    baseline_id = checks.identity(baseline)
    candidate_id = checks.identity(candidate)
    catalog = json.loads((candidate / "scripts" / "benchmark_catalog.json").read_text())
    if baseline_id["cargo_lock_sha256"] != candidate_id["cargo_lock_sha256"]:
        raise ValueError("baseline and candidate Cargo.lock identities differ")
    if baseline_id["fixtures_sha256"] != candidate_id["fixtures_sha256"]:
        raise ValueError("baseline and candidate fixture identities differ")
    if measure_hashes(baseline, selected, args.cache_only) != measure_hashes(candidate, selected, args.cache_only):
        raise ValueError("measurement source differs between baseline and candidate")
    output.mkdir(parents=True, exist_ok=False)
    checks.save(output / "identities.json", {
        "baseline": baseline_id,
        "candidate": candidate_id,
        "measurement_sha256": measure_hashes(candidate, selected, args.cache_only),
        "driver_sha256": {
            "benchmarks.py": checks.file_hash(candidate / "scripts" / "benchmarks.py"),
            "benchmark_axes.json": checks.file_hash(candidate / "scripts" / "benchmark_axes.json"),
            "benchmark_catalog.json": checks.file_hash(candidate / "scripts" / "benchmark_catalog.json"),
        },
    })
    for role, repo in (("baseline", baseline), ("candidate", candidate)):
        code = prepare(repo, output / f"prepare-{role}.log", selected, args.cache_only, catalog)
        if code:
            raise RuntimeError(f"{role} measurement build failed with exit {code}")
    fields = ["pair", "order", "role", "repo", "exit", "started_ns", "ended_ns", "results"]
    failed = False
    last_end = 0
    mismatches = []
    metrics = []
    with (output / "PAIR_RESULTS.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=fields)
        writer.writeheader()
        for pair in range(args.runs):
            order = [("baseline", baseline), ("candidate", candidate)]
            if pair % 2:
                order.reverse()
            paired = {}
            for position, (role, repo) in enumerate(order):
                results = output / f"pair-{pair + 1:02}-{position + 1}-{role}"
                command = [sys.executable, "-B", str(candidate / "scripts" / "benchmarks.py"),
                           str(results), "--repo", str(repo), "--profile", "bounded"]
                if args.only:
                    command.extend(["--only", *args.only])
                if args.cache_only:
                    command.append("--cache-only")
                started_ns = time.monotonic_ns()
                if started_ns < last_end:
                    raise RuntimeError("paired timing intervals overlapped")
                code = subprocess.run(command, cwd=repo, check=False).returncode
                ended_ns = time.monotonic_ns()
                last_end = ended_ns
                failed |= code != 0
                writer.writerow({"pair": pair + 1, "order": position + 1, "role": role,
                                 "repo": repo, "exit": code, "started_ns": started_ns,
                                 "ended_ns": ended_ns, "results": results})
                stream.flush()
                paired[role] = results
            pair_mismatches = compare_pair(paired["baseline"], paired["candidate"])
            mismatches.extend({"pair": pair + 1, **item} for item in pair_mismatches)
            metrics.extend(pair_metrics(paired["baseline"], paired["candidate"], pair + 1))
            failed |= bool(pair_mismatches)
    checks.save(output / "PAIR_MISMATCHES.json", mismatches)
    with (output / "PAIRED_METRICS.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=list(metrics[0]) if metrics else ["pair"])
        writer.writeheader()
        writer.writerows(metrics)
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
