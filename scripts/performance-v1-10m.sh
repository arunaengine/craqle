#!/usr/bin/env bash
set -euo pipefail

if [[ "${CRAQLE_ALLOW_10M_RUN:-}" != "yes" ]]; then
    echo "Refusing to start: set CRAQLE_ALLOW_10M_RUN=yes only after explicit authorization." >&2
    exit 2
fi

repo_root="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_root"

cutoff_seconds="${CRAQLE_10M_CUTOFF_SECONDS:-3600}"
if [[ ! "$cutoff_seconds" =~ ^[1-9][0-9]*$ ]]; then
    echo "CRAQLE_10M_CUTOFF_SECONDS must be a positive integer." >&2
    exit 2
fi

if [[ -n "$(git status --porcelain=v1)" ]]; then
    echo "Refusing to start from a dirty worktree." >&2
    exit 2
fi

run_id="$(date -u +%Y%m%dT%H%M%SZ)-$(git rev-parse --short=12 HEAD)"
evidence_parent="${CRAQLE_10M_EVIDENCE_ROOT:-target/performance-v1-10m}"
run_root="$evidence_parent/$run_id"
mkdir -p "$evidence_parent"
mkdir "$run_root"

completed=0
on_exit() {
    status=$?
    if [[ "$completed" -ne 1 ]]; then
        printf 'status=stopped-or-incomplete\nexit_status=%s\n' "$status" >"$run_root/STOPPED"
    fi
}
trap on_exit EXIT
trap 'exit 130' INT
trap 'exit 143' TERM

commit="$(git rev-parse HEAD)"
{
    printf 'commit=%s\n' "$commit"
    printf 'started_utc=%s\n' "$(date -u --iso-8601=seconds)"
    printf 'cutoff_seconds=%s\n' "$cutoff_seconds"
    printf 'corpus_quads=10000000\n'
    printf 'corpus_graphs=32\n'
    printf 'duplicate_percent=25\n'
    printf 'sample_size=10\n'
    printf 'warmup_seconds=1\n'
    printf 'measurement_seconds=5\n'
    printf 'features=shacl-core\n'
    rustc --version
    cargo --version
} >"$run_root/provenance-before-timing.txt"

# Compile before the cutoff and hash the exact newest benchmark executable.
# The benchmark process then constructs and settles one deterministic fixture
# before Criterion timing and reuses that fixture for every validation and
# checked-write case in the process.
cargo bench --locked --features shacl-core --bench shacl_incremental --no-run \
    >"$run_root/build.log" 2>&1

benchmark_binary="$(
    find target/release/deps -maxdepth 1 -type f -name 'shacl_incremental-*' -executable \
        -printf '%T@ %p\n' | sort -nr | head -n 1 | cut -d' ' -f2-
)"
if [[ -z "$benchmark_binary" ]]; then
    echo "Could not locate the compiled shacl_incremental benchmark binary." >&2
    exit 3
fi
sha256sum "$benchmark_binary" >"$run_root/binary.sha256"
printf 'binary=%s\n' "$benchmark_binary" >>"$run_root/provenance-before-timing.txt"

printf '%s\n' \
    "timeout --foreground --signal=INT --kill-after=30 ${cutoff_seconds}s env CRAQLE_GIT_COMMIT=$commit CRAQLE_BENCH_QUADS=10000000 CRAQLE_BENCH_GRAPHS=32 CRAQLE_BENCH_DUPLICATE_PERCENT=25 CRAQLE_BENCH_SAMPLE_SIZE=10 CRAQLE_BENCH_WARMUP_SECS=1 CRAQLE_BENCH_MEASUREMENT_SECS=5 cargo bench --locked --features shacl-core --bench shacl_incremental" \
    >"$run_root/command.txt"

set +e
timeout --foreground --signal=INT --kill-after=30 "${cutoff_seconds}s" \
    env CRAQLE_GIT_COMMIT="$commit" \
    CRAQLE_BENCH_QUADS=10000000 \
    CRAQLE_BENCH_GRAPHS=32 \
    CRAQLE_BENCH_DUPLICATE_PERCENT=25 \
    CRAQLE_BENCH_SAMPLE_SIZE=10 \
    CRAQLE_BENCH_WARMUP_SECS=1 \
    CRAQLE_BENCH_MEASUREMENT_SECS=5 \
    cargo bench --locked --features shacl-core --bench shacl_incremental \
    >"$run_root/shacl-incremental-10m.log" 2>&1
status=$?
set -e

if [[ "$status" -ne 0 ]]; then
    echo "10M run stopped or failed with status $status; no performance claim is valid." >&2
    exit "$status"
fi

log="$run_root/shacl-incremental-10m.log"
grep -q 'shacl_incremental provenance:' "$log"
grep -q 'operation=validation' "$log"
grep -q 'operation=checked_write' "$log"
grep -q 'operation=mutation_recompile_full_settle' "$log"

{
    printf 'status=complete\n'
    printf 'commit=%s\n' "$commit"
    printf 'completed_utc=%s\n' "$(date -u --iso-8601=seconds)"
    printf 'log=%s\n' "$log"
} >"$run_root/COMPLETE"
completed=1
echo "Complete 10M evidence root: $run_root"
