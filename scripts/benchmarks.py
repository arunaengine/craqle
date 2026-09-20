#!/usr/bin/env python3
"""Run the native B01-B20 workload catalog and preserve honest raw evidence."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
import csv
import hashlib
import itertools
import json
import os
from pathlib import Path
import re
import signal
import subprocess
import sys
import time

import checks


RESULT_FIELDS = [
    "benchmark_id", "run_id", "revision", "measurement_revision", "status",
    "fixture_hash", "seed", "feature_set", "persist_mode", "completion_boundary",
    "writers", "readers", "stores", "graphs", "preloaded_rows", "changed_rows",
    "offered_per_second", "completed", "rejected", "timed_out",
    "throughput_per_second", "latency_samples", "p50_us", "p95_us", "p99_us",
    "max_us", "cpu_seconds", "peak_rss_bytes", "allocated_bytes",
    "storage_write_bytes", "persist_calls", "lock_wait_ns", "lock_hold_ns",
    "max_queue_bytes", "oldest_pending_ms", "retries", "records_visited",
    "cache_keys_inspected", "recency_rebuilds", "docs_decoded", "prepared_bytes",
    "qv_fallbacks", "remaining_debt", "raw_samples_path", "notes",
]


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--repo", type=Path)
    parser.add_argument("--profile", choices=("bounded", "release"), default="bounded")
    parser.add_argument("--only", nargs="*", default=[])
    parser.add_argument("--require-complete", action="store_true")
    parser.add_argument("--cache-only", action="store_true")
    args = parser.parse_args()
    if args.cache_only and args.only != ["B15"]:
        parser.error("--cache-only requires --only B15")
    script_root = Path(__file__).resolve().parent.parent
    repo = args.repo.resolve(strict=True) if args.repo else script_root
    catalog = json.loads((script_root / "scripts" / "benchmark_catalog.json").read_text())
    axes = json.loads((script_root / "scripts" / "benchmark_axes.json").read_text())
    return run(repo, args.output.resolve(), catalog, axes, args)


def parse_time(path):
    """Read portable fields emitted by GNU time when it is available."""
    if not path.is_file():
        return {}
    text = path.read_text(errors="replace")
    values = {}
    patterns = {
        "user": r"User time \(seconds\): ([0-9.]+)",
        "system": r"System time \(seconds\): ([0-9.]+)",
        "rss": r"Maximum resident set size \(kbytes\): ([0-9]+)",
        "fs_inputs": r"File system inputs: ([0-9]+)",
        "fs_outputs": r"File system outputs: ([0-9]+)",
    }
    for key, pattern in patterns.items():
        match = re.search(pattern, text)
        if match:
            values[key] = float(match.group(1))
    return values


def resolve_binary(repo, argv):
    """Uses Cargo JSON to resolve and hash the exact executable target."""
    if "--bench" in argv:
        name = argv[argv.index("--bench") + 1]
        command = ["cargo", "bench", "--locked", "--all-features", "--bench", name,
                   "--no-run", "--message-format=json"]
    elif "--test" in argv:
        name = argv[argv.index("--test") + 1]
        command = ["cargo", "test", "--locked", "--all-features", "--test", name,
                   "--no-run", "--message-format=json"]
    elif "--lib" in argv:
        name = "craqle"
        command = ["cargo", "test", "--locked", "--all-features", "--lib", "--no-run",
                   "--message-format=json"]
    else:
        return {}
    process = subprocess.run(command, cwd=repo, capture_output=True, text=True, check=False)
    if process.returncode:
        raise RuntimeError(f"binary resolution failed for {name}: {process.stderr.strip()}")
    executables = []
    for line in process.stdout.splitlines():
        try:
            message = json.loads(line)
        except json.JSONDecodeError:
            continue
        executable = message.get("executable")
        target = message.get("target", {})
        if executable and target.get("name") == name:
            executables.append(Path(executable))
    if not executables:
        return {}
    selected = executables[-1]
    return {selected.relative_to(repo).as_posix(): checks.file_hash(selected)}


def fixture_hash(identity):
    """Combines the exact fixture manifest into one stable evidence digest."""
    payload = json.dumps(identity["fixtures_sha256"], sort_keys=True, separators=(",", ":"))
    return hashlib.sha256(payload.encode()).hexdigest()


def stop_group(process):
    try:
        os.killpg(process.pid, signal.SIGKILL)
    except ProcessLookupError:
        pass
    process.wait()


def run_command(repo, output, benchmark, index, command, workdir):
    """Run one native workload and return its evidence row."""
    label = f"{benchmark}-{index:02}"
    log = output / f"{label}.log"
    time_log = output / f"{label}.time"
    argv = [part.replace("{workdir}", str(workdir)) for part in command["argv"]]
    env = os.environ.copy()
    env.update({key: value.replace("{workdir}", str(workdir))
                for key, value in command.get("env", {}).items()})
    binary_sha256 = resolve_binary(repo, argv)
    timed = Path("/usr/bin/time").is_file()
    launch = ["/usr/bin/time", "-v", "-o", str(time_log), *argv] if timed else argv
    started = time.monotonic()
    started_ns = time.monotonic_ns()
    process = None
    code = None
    reason = ""
    with log.open("x") as stream:
        stream.write(json.dumps({"argv": argv, "env": command.get("env", {})}) + "\n")
        stream.flush()
        try:
            process = subprocess.Popen(launch, cwd=repo, env=env, stdout=stream,
                                       stderr=subprocess.STDOUT, start_new_session=True)
            code = process.wait(timeout=14_400)
            if code:
                reason = f"child exit {code}"
        except subprocess.TimeoutExpired:
            reason = "four-hour workload timeout"
        finally:
            if process is not None and process.poll() is None:
                stop_group(process)
                code = process.returncode
    if not reason and argv[:2] == ["cargo", "test"]:
        text = log.read_text(errors="replace")
        counts = re.findall(r"test result: ok\. (\d+) passed;", text)
        if not any(int(count) for count in counts):
            reason = "test workload executed zero tests"
    metrics = parse_time(time_log)
    native = {}
    for line in reversed(log.read_text(errors="replace").splitlines()):
        candidate = line[line.find("{"):] if "{" in line else line
        try:
            value = json.loads(candidate)
        except json.JSONDecodeError:
            continue
        if isinstance(value, dict) and value.get("benchmark_id") == benchmark:
            native = value
            break
    if not reason and command.get("env", {}).get("CRAQLE_BENCH_ID") and not native:
        reason = "native workload emitted no structured result"
    ended_ns = time.monotonic_ns()
    native_case = native.get("case", {})
    if "cache_churn" in argv:
        boundary = "cache-trace-complete"
    else:
        boundary = "source+qv+search" if native_case.get("search", True) else "source+qv"
    return {
        "label": label,
        "argv": argv,
        "exit": code,
        "status": "failed" if reason else "passed",
        "reason": reason,
        "seconds": time.monotonic() - started,
        "started_ns": started_ns,
        "ended_ns": ended_ns,
        "cpu_seconds": metrics.get("user", 0.0) + metrics.get("system", 0.0),
        "peak_rss_bytes": int(metrics.get("rss", 0.0) * 1024),
        "fs_inputs": int(metrics.get("fs_inputs", 0.0)),
        "fs_outputs": int(metrics.get("fs_outputs", 0.0)),
        "binary_sha256": binary_sha256,
        "completion_boundary": boundary,
        "raw_samples_path": log.relative_to(output).as_posix(),
        "native": native,
    }


def axis_cases(axes, profile):
    """Return bounded covering cases or the complete Cartesian release matrix."""
    names = list(axes)
    if profile == "release":
        return [dict(zip(names, values)) for values in itertools.product(*(axes[name] for name in names))]
    baseline = {name: values[0] for name, values in axes.items()}
    cases = [baseline]
    for name, values in axes.items():
        for value in values[1:]:
            case = baseline.copy()
            case[name] = value
            cases.append(case)
    return cases


def catalog_command(benchmark, case):
    """Build the native catalog command for one exact axis case."""
    internal = {
        "B09": "search::search_bench::catalog_b09",
        "B10": "search::search_bench::catalog_b10",
        "B14": "store::store_bench::catalog_b14",
        "B17": "store::store_bench::catalog_b17",
    }
    if benchmark in internal:
        argv = ["cargo", "test", "--locked", "--all-features", "--lib",
                internal[benchmark], "--", "--exact", "--ignored", "--nocapture",
                "--test-threads=1"]
    else:
        argv = ["cargo", "bench", "--locked", "--all-features", "--bench",
                "consistency_catalog"]
    return {
        "argv": argv,
        "env": {
            "CRAQLE_BENCH_ID": benchmark,
            "CRAQLE_BENCH_CASE": json.dumps(case, sort_keys=True, separators=(",", ":")),
        },
    }


def run(repo, output, catalog, axes, args):
    target = os.environ.get("CARGO_TARGET_DIR")
    if target and Path(target).resolve() != repo / "target":
        raise ValueError("CARGO_TARGET_DIR must resolve to this checkout's target directory")
    selected = args.only or sorted(catalog)
    unknown = sorted(set(selected) - set(catalog))
    if unknown:
        raise ValueError(f"unknown benchmark IDs: {unknown}")
    baseline = checks.identity(repo)
    output.mkdir(parents=True, exist_ok=False)
    run_id = time.strftime("%Y%m%dT%H%M%SZ", time.gmtime())
    repetitions = 1 if args.profile == "bounded" else 5
    checks.save(output / "source.json", baseline)
    checks.save(output / "catalog.json", {key: catalog[key] for key in selected})
    checks.save(output / "axes.json", {key: axes[key] for key in selected})
    rows = []
    failed = False
    with (output / "BENCHMARK_RESULTS.csv").open("x", newline="") as stream:
        writer = csv.DictWriter(stream, fieldnames=RESULT_FIELDS)
        writer.writeheader()
        for benchmark in selected:
            entry = catalog[benchmark]
            if args.require_complete and entry["coverage"] != "bounded":
                failed = True
            workdir = output / "work" / benchmark
            workdir.mkdir(parents=True)
            index = 0
            for repetition in range(repetitions):
                cases = axis_cases(axes[benchmark], args.profile)
                if args.cache_only:
                    if benchmark != "B15":
                        raise ValueError("--cache-only requires --only B15")
                    commands = [command for command in entry["commands"]
                                if "cache_churn" in command["argv"]]
                else:
                    commands = [catalog_command(benchmark, case) for case in cases]
                    commands.extend(entry["commands"])
                for command in commands:
                    if checks.identity(repo) != baseline:
                        raise RuntimeError("source changed during benchmark run")
                    result = run_command(repo, output, benchmark, index, command, workdir)
                    index += 1
                    native_status = result["native"].get("status")
                    failed |= result["status"] != "passed"
                    if native_status == "capacity_blocked" and args.profile == "release":
                        failed = True
                    row = {field: "" for field in RESULT_FIELDS}
                    row.update({
                        "benchmark_id": benchmark,
                        "run_id": run_id,
                        "revision": baseline["head"],
                        "measurement_revision": baseline["source_sha256"],
                        "fixture_hash": fixture_hash(baseline),
                        "status": (native_status or ("measured_partial"
                                   if result["status"] == "passed" and entry["coverage"] == "partial"
                                   else result["status"])),
                        "cpu_seconds": f"{result['cpu_seconds']:.6f}",
                        "peak_rss_bytes": result["peak_rss_bytes"],
                        "feature_set": "all" if "--all-features" in result["argv"] else "",
                        "raw_samples_path": result["raw_samples_path"],
                        "notes": (f"coverage={entry['coverage']}; repetition={repetition + 1}; "
                                  f"{entry['notes']}; {result['reason']}").rstrip("; "),
                    })
                    native_result = result["native"].get("result", {})
                    native_case = result["native"].get("case", {})
                    for field in ("writers", "readers", "stores", "graphs"):
                        row[field] = native_case.get(field, "")
                    row["preloaded_rows"] = native_result.get("preloaded_rows", native_case.get("rows", ""))
                    row["changed_rows"] = native_case.get("changed", "")
                    row["persist_mode"] = ("" if "cache_churn" in result["argv"]
                                           else native_case.get("persist", "sync-all"))
                    row["completion_boundary"] = result["completion_boundary"]
                    row["completed"] = native_result.get("completed", "")
                    row["rejected"] = native_result.get("failures", "")
                    source_keys = native_result.get("source_keys_read")
                    qv_keys = native_result.get("qv_keys_read")
                    if source_keys is not None or qv_keys is not None:
                        row["records_visited"] = (source_keys or 0) + (qv_keys or 0)
                    elif "receipt_lookups" in native_result:
                        row["records_visited"] = native_result["receipt_lookups"]
                    row["qv_fallbacks"] = int(bool(native_result.get("qv_fallback")))
                    row["remaining_debt"] = native_result.get("remaining_debt", "")
                    row["retries"] = native_result.get("retries", "")
                    row["prepared_bytes"] = native_result.get("prepared_bytes", "")
                    row["max_queue_bytes"] = native_result.get("max_queue_bytes", "")
                    row["lock_wait_ns"] = native_result.get("lock_wait_ns", "")
                    row["lock_hold_ns"] = native_result.get("binding_hold_ns", "")
                    row["persist_calls"] = native_result.get("persist_calls", "")
                    latencies = sorted(native_result.get("writer_ns", native_result.get("latency_ns", [])))
                    if latencies:
                        row["latency_samples"] = len(latencies)
                        row["p50_us"] = latencies[len(latencies) // 2] // 1_000
                        row["p95_us"] = latencies[min(len(latencies) - 1, len(latencies) * 95 // 100)] // 1_000
                        row["p99_us"] = latencies[min(len(latencies) - 1, len(latencies) * 99 // 100)] // 1_000
                        row["max_us"] = latencies[-1] // 1_000
                    writer.writerow(row)
                    stream.flush()
                    rows.append(result)
    final = checks.identity(repo)
    failed |= final != baseline
    checks.save(output / "complete.json", {
        "status": "failed" if failed else "passed",
        "profile": args.profile,
        "require_complete": args.require_complete,
        "source_unchanged": final == baseline,
        "commands": rows,
    })
    return int(failed)


if __name__ == "__main__":
    sys.exit(main())
