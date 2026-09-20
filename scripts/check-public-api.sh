#!/usr/bin/env bash
# Compares the current public API with the retained release snapshot.
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

set -euo pipefail

snapshot="api/craqle-0.2.0-rc.1.txt"
actual="$(mktemp)"
trap 'rm -f "${actual}"' EXIT

cargo public-api --all-features -sss --color never > "${actual}"
diff -u "${snapshot}" "${actual}"
