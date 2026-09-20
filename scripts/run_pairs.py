#!/usr/bin/env python3
"""Run paired baseline and candidate workloads inside one resource-capped scope."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

from pathlib import Path
import subprocess
import sys


def main():
    if len(sys.argv) < 4:
        raise ValueError("usage: run_pairs.py BASELINE CANDIDATE OUTPUT [PAIR OPTIONS]")
    baseline = Path(sys.argv[1]).resolve(strict=True)
    candidate = Path(sys.argv[2]).resolve(strict=True)
    output = Path(sys.argv[3]).resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    resource_log = output.with_name(output.name + "-resources.jsonl")
    command = [
        sys.executable,
        str(candidate / "scripts" / "guard.py"),
        str(candidate),
        str(resource_log),
        sys.executable,
        str(candidate / "scripts" / "pair_benchmarks.py"),
        str(baseline),
        str(candidate),
        str(output),
        *sys.argv[4:],
    ]
    return subprocess.run(command, cwd=candidate, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
