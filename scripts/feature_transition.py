#!/usr/bin/env python3
"""Runs the enabled, disabled, and repaired search feature transition."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys

import checks


def inspect_phase(log, test):
    """Requires one exact executed test and one successful test summary."""
    text = log.read_text(errors="replace")
    names = re.findall(r"^test (\S+) \.\.\. (?:ok|FAILED|ignored)$", text, re.MULTILINE)
    summaries = re.findall(
        r"^test result: ok\. (\d+) passed; (\d+) failed; (\d+) ignored;",
        text,
        re.MULTILINE,
    )
    if names != [test] or summaries != [("1", "0", "0")]:
        raise RuntimeError(f"{test} cardinality mismatch: names={names}, summaries={summaries}")


def run_phase(context, phase):
    """Runs one exact phase and returns its immutable evidence record."""
    repo, output, database = context
    name, features, selector = phase
    log = output / f"{name}.log"
    argv = ["cargo", "test", "--locked", "--no-default-features"]
    if features:
        argv.extend(["--features", features])
    argv.extend(["--test", "search_recovery", selector, "--", "--exact", "--nocapture",
                 "--test-threads=1"])
    env = os.environ.copy()
    env.update({"CRAQLE_SEARCH_PHASE": name, "CRAQLE_SEARCH_DIR": str(database)})
    before = checks.identity(repo)
    with log.open("x") as stream:
        stream.write(json.dumps({"argv": argv, "phase": name, "source": before}) + "\n")
        stream.flush()
        process = subprocess.run(argv, cwd=repo, env=env, stdout=stream,
                                 stderr=subprocess.STDOUT, timeout=7_200, check=False)
    if process.returncode:
        raise RuntimeError(f"{name} exited {process.returncode}")
    inspect_phase(log, selector)
    after = checks.identity(repo)
    if after != before:
        raise RuntimeError(f"source changed during {name}")
    return {
        "phase": name,
        "test": selector,
        "argv": argv,
        "source_sha256": before["source_sha256"],
        "log": log.name,
        "log_sha256": hashlib.sha256(log.read_bytes()).hexdigest(),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("repo", type=Path)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    repo = args.repo.resolve(strict=True)
    output = args.output.resolve()
    if repo not in output.parents:
        raise ValueError("transition evidence must stay inside the repository")
    output.mkdir(parents=True, exist_ok=False)
    database = output / "database"
    phases = [
        ("write", "search", "phase_writes_index"),
        ("mutate", "", "phase_mutates_source"),
        ("verify", "search", "phase_repairs_index"),
    ]
    source = checks.identity(repo)
    context = (repo, output, database)
    evidence = [run_phase(context, phase) for phase in phases]
    if checks.identity(repo) != source:
        raise RuntimeError("source changed during feature transition")
    checks.save(output / "manifest.json", {
        "source": source,
        "database": str(database.relative_to(repo)),
        "phases": evidence,
    })
    for phase in evidence:
        print(f"{phase['phase']}: passed {phase['test']}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
