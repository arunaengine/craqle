#!/usr/bin/env python3
"""Run the B01-B20 dispatcher under the Craqle resource guard."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
from pathlib import Path
import subprocess
import sys


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    parser.add_argument("arguments", nargs=argparse.REMAINDER)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    resource_log = output.with_name(output.name + "-resources.jsonl")
    command = [
        sys.executable,
        str(repo / "scripts" / "guard.py"),
        str(repo),
        str(resource_log),
        sys.executable,
        str(repo / "scripts" / "benchmarks.py"),
        str(output),
        *args.arguments,
    ]
    return subprocess.run(command, cwd=repo, check=False).returncode


if __name__ == "__main__":
    sys.exit(main())
