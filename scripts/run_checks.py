#!/usr/bin/env python3
"""Run the complete serial Craqle verification matrix under resource limits."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
import json
from pathlib import Path
import subprocess
import sys
import tempfile

import matrix


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("output", type=Path)
    args = parser.parse_args()
    repo = Path(__file__).resolve().parent.parent
    output = args.output.resolve()
    output.parent.mkdir(parents=True, exist_ok=True)
    if output.exists():
        raise ValueError(f"output already exists: {output}")
    resource_log = output.with_name(output.name + "-resources.jsonl")
    if resource_log.exists():
        raise ValueError(f"resource log already exists: {resource_log}")

    temporary = tempfile.NamedTemporaryFile(
        mode="w", prefix="craqle-checks-", suffix=".json", dir=output.parent, delete=False
    )
    manifest = Path(temporary.name)
    try:
        with temporary:
            json.dump(matrix.cases(), temporary, indent=2)
            temporary.write("\n")
        command = [
            sys.executable,
            str(repo / "scripts" / "guard.py"),
            str(repo),
            str(resource_log),
            sys.executable,
            str(repo / "scripts" / "checks.py"),
            str(repo),
            str(output),
            str(manifest),
        ]
        return subprocess.run(command, cwd=repo, check=False).returncode
    finally:
        manifest.unlink(missing_ok=True)


if __name__ == "__main__":
    sys.exit(main())
