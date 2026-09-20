#!/usr/bin/env python3
"""Build once and retain independent local query benchmark evidence."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
import hashlib
import json
import os
from pathlib import Path
import shutil
import signal
import subprocess
import tarfile
import time


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("--runs", type=int, default=3)
    parser.add_argument("--target", default="query_costs")
    parser.add_argument("--features", default="")
    parser.add_argument("--no-default-features", action="store_true")
    parser.add_argument("--plain", action="store_true")
    parser.add_argument("--reuse", type=Path)
    parser.add_argument("--filter")
    args = parser.parse_args()
    if args.runs < 1:
        parser.error("--runs must be positive")
    repo = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    output.mkdir(parents=True, exist_ok=False)
    origin = args.reuse.resolve(strict=True) if args.reuse else None
    source = (json.loads((origin / "source.json").read_text())
              if origin else source_identity(repo))
    write_json(output / "source.json", source)
    if origin:
        if args.features or args.no_default_features:
            parser.error("a reused binary keeps its original compiled features")
        shutil.copy2(origin / "source.tar.gz", output / "source.tar.gz")
        binary = origin / args.target
        expected = json.loads((origin / "binary.json").read_text())["sha256"]
        if file_hash(binary) != expected:
            raise RuntimeError("reused benchmark binary hash differs from its evidence manifest")
        write_json(output / "build.json", {"reused": str(origin), "exit": 0})
    else:
        with tarfile.open(output / "source.tar.gz", "w:gz") as archive:
            for name in source["files_sha256"]:
                archive.add(repo / name, arcname=name, recursive=False)
        command = ["cargo", "bench", "--locked", "--bench", args.target,
                   "--no-run", "--message-format=json"]
        if args.features:
            command.extend(["--features", args.features])
        if args.no_default_features:
            command.append("--no-default-features")
        built = launch(command, output / "build.log", repo)
        write_json(output / "build.json", built)
        if built["exit"]:
            return built["exit"]
        binary = resolve_binary(output / "build.log", args.target)
    os.environ["CRAQLE_GIT_COMMIT"] = source["head"]
    identity = {"path": str(binary), "sha256": file_hash(binary),
                "environment": {key: value for key, value in os.environ.items()
                                if key.startswith("CRAQLE_") or key in (
                                    "CARGO_BUILD_JOBS", "RUST_TEST_THREADS",
                                    "CARGO_INCREMENTAL", "CARGO_PROFILE_DEV_DEBUG")}}
    write_json(output / "binary.json", identity)
    shutil.copy2(binary, output / args.target)
    for index in range(args.runs):
        if not origin and build_identity(source_identity(repo), args.target) != build_identity(source, args.target):
            raise RuntimeError("source changed after build; evidence run stopped")
        run = output / f"run-{index + 1:02}"
        run.mkdir()
        label = f"local-query-{os.getpid()}-{index}"
        command = [str(binary)] if args.plain else [
            str(binary), "--bench", "--noplot", "--save-baseline", label]
        if args.filter and not args.plain:
            command.append(args.filter)
        result = launch(command, run / "stdout.log", repo)
        result["binary_sha256"] = identity["sha256"]
        result["process_wall_includes_setup"] = True
        write_json(run / "process.json", result)
        records = extract_records(run / "stdout.log")
        with (run / "work.jsonl").open("x") as stream:
            for record in records:
                stream.write(json.dumps(record, sort_keys=True) + "\n")
        samples = copy_samples(repo / "target" / "criterion", run, label)
        write_json(run / "samples.json", samples)
        if result["exit"]:
            return result["exit"]
        if not records or (not args.plain and not samples):
            raise RuntimeError("successful process emitted no work records or timing samples")
    return 0


def file_hash(path):
    digest = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def write_json(path, value):
    with path.open("x") as stream:
        json.dump(value, stream, indent=2, sort_keys=True)
        stream.write("\n")


def source_identity(repo):
    files = subprocess.check_output(
        ["git", "ls-files", "-co", "--exclude-standard", "-z"], cwd=repo
    ).decode().split("\0")
    hashes = {name: file_hash(repo / name) for name in sorted(set(files))
              if name and (repo / name).is_file()}
    return {"head": subprocess.check_output(["git", "rev-parse", "HEAD"], cwd=repo)
            .decode().strip(), "files_sha256": hashes,
            "status": subprocess.check_output(["git", "status", "--porcelain"], cwd=repo)
            .decode()}


def build_identity(source, target):
    excluded = ("benches/text/", "benches/text_costs.rs") if target in (
        "query_costs", "planning") else ()
    return {"files_sha256": {
        name: digest for name, digest in source["files_sha256"].items()
        if not name.startswith(excluded)}}


def launch(command, log, repo):
    started = time.monotonic_ns()
    with log.open("x") as stream:
        process = subprocess.Popen(command, cwd=repo, stdout=stream,
                                   stderr=subprocess.STDOUT, start_new_session=True)
        try:
            code = process.wait(timeout=3600)
        except (subprocess.TimeoutExpired, KeyboardInterrupt):
            os.killpg(process.pid, signal.SIGKILL)
            process.wait()
            raise
    return {"argv": command, "exit": code,
            "wall_ns": time.monotonic_ns() - started}


def extract_records(path):
    records = []
    for line in path.read_text().splitlines():
        try:
            record = json.loads(line)
        except json.JSONDecodeError:
            continue
        if isinstance(record, dict):
            records.append(record)
    return records


def resolve_binary(path, target):
    for record in reversed(extract_records(path)):
        if record.get("target", {}).get("name") == target and record.get("executable"):
            return Path(record["executable"])
    raise RuntimeError(f"Cargo did not report the executable for {target}")


def copy_samples(criterion, output, label):
    samples = []
    for path in sorted(criterion.rglob("sample.json")):
        if path.parent.name != label:
            continue
        relative = path.parent.parent.relative_to(criterion)
        destination = output / "criterion" / relative
        destination.mkdir(parents=True)
        for name in ("sample.json", "estimates.json", "benchmark.json"):
            source = path.parent / name
            if source.is_file():
                shutil.copyfile(source, destination / name)
        data = json.loads(path.read_text())
        samples.append({"case": str(relative), "sample_count": len(data["times"]),
                        "query_ns": [elapsed / count for elapsed, count in
                                     zip(data["times"], data["iters"])],
                        "raw": str(destination.relative_to(output) / "sample.json")})
    return samples


if __name__ == "__main__":
    raise SystemExit(main())
