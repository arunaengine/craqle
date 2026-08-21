#!/usr/bin/env bash
set -euo pipefail

snapshot="api/craqle-0.2.0-rc.1.txt"
actual="$(mktemp)"
trap 'rm -f "${actual}"' EXIT

cargo public-api --all-features -sss --color never > "${actual}"
diff -u "${snapshot}" "${actual}"
