#!/usr/bin/env python3
"""Builds the serial verification manifest consumed by the guarded runner."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import json
import sys


def main():
    print(json.dumps(cases(), indent=2))



def cargo_subcommand(args):
    """Return the cargo subcommand, skipping a leading toolchain override."""
    return next(arg for arg in args if not arg.startswith("+"))


def cases():
    feature_sets = [
        ("none", []),
        ("search", ["search"]),
        ("iroh", ["iroh"]),
        ("shacl", ["shacl-core"]),
        ("search-iroh", ["search", "iroh"]),
        ("search-shacl", ["search", "shacl-core"]),
        ("iroh-shacl", ["iroh", "shacl-core"]),
        ("all", ["search", "iroh", "shacl-core"]),
    ]
    commands = []
    for label, features in feature_sets:
        args = ["test", "--locked", "--no-default-features"]
        if features:
            args.extend(["--features", ",".join(features)])
        commands.append((f"test-{label}", args))
    commands.extend([
        ("test-doc", ["test", "--locked", "--all-features", "--doc"]),
        ("doc", ["doc", "--locked", "--all-features", "--no-deps"]),
        ("fmt-nightly", ["+nightly", "fmt", "--all", "--", "--check"]),
        ("clippy-nightly", ["+nightly", "clippy", "--locked", "--all-features",
                            "--all-targets", "--", "-D", "warnings"]),
        ("check-msrv", ["+1.97.1", "check", "--locked", "--all-features", "--all-targets"]),
    ])
    result = [{"label": name,
               "argv": (["env", "RUSTDOCFLAGS=-D warnings"] if cargo_subcommand(args) == "doc" else [])
                       + ["cargo", *args], "timeout": 7200,
               **({"allow_empty": True} if "--doc" in args else {})}
              for name, args in commands]
    result.insert(0, {"label": "style-unit", "timeout": 120,
                      "argv": [sys.executable, "-B", "scripts/style/tests.py"]})
    result.insert(1, {"label": "style", "timeout": 120,
                      "argv": [sys.executable, "-B", "scripts/style/checker.py"]})
    transition = next(index for index, case in enumerate(result) if case["label"] == "test-doc")
    result.insert(transition, {
        "label": "feature-transition",
        "timeout": 21_600,
        "argv": [sys.executable, "-B", "scripts/feature_transition.py", ".",
                 "{output}/feature-transition"],
    })
    return result


if __name__ == "__main__":
    main()
