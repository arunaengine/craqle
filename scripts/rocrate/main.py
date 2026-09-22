#!/usr/bin/env python3
"""Generate RO-Crate fixtures and compare Craqle with a local Virtuoso on validated answers."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import argparse
import hashlib
import json
import os
from pathlib import Path
import platform
import random
import statistics
import subprocess
import sys
import time

sys.path.insert(0, str(Path(__file__).resolve().parent))
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

import catalog
import checks
import fixture
import virtuoso

REPO = Path(__file__).resolve().parent.parent.parent
# Craqle's MAX_SEARCH_LIMIT; one search cannot return a larger complete set.
SEARCH_LIMIT = 10_000


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--profile", choices=sorted(fixture.PROFILES), default="smoke")
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--samples", type=int, default=10)
    parser.add_argument("--cpus", default="4-7")
    parser.add_argument("--tuned", action="store_true")
    parser.add_argument("--engines", default="craqle,virtuoso")
    args = parser.parse_args()
    out = args.out.resolve()
    out.mkdir(parents=True, exist_ok=False)
    model = fixture.generate(args.profile)
    fixture_dir = out / "fixture"
    manifest = fixture.write_fixture(model, fixture_dir)
    crates = [{"file": f"crates/{crate.index:05d}.json", "graph": crate.base}
              for crate in model.crates]
    (fixture_dir / "crates.json").write_text(json.dumps(crates))
    cases = catalog.build(model)
    (fixture_dir / "cases.json").write_text(json.dumps(cases, ensure_ascii=False))
    # Build before hashing, so the recorded source is the source the binary came from.
    binary = build_craqle() if "craqle" in args.engines else None
    identity = {k: v for k, v in checks.identity(REPO).items() if k != "fixtures_sha256"}
    context = REPO / "src" / "rocrate" / "1_2.jsonld"
    environment = {
        "source": identity,
        "fixture": manifest,
        "context_sha256": hashlib.sha256(context.read_bytes()).hexdigest(),
        "cases": len(cases),
        "samples": args.samples,
        "cpus": args.cpus,
        "tuned": args.tuned,
        "machine": {"system": platform.platform(), "processor": platform.processor(),
                    "cpu_count": os.cpu_count()},
    }
    results = []
    engines = args.engines.split(",")
    random.Random(model.seed).shuffle(engines)
    environment["engine_order"] = engines
    for engine in engines:
        if engine == "craqle":
            environment["craqle"], records = run_craqle(fixture_dir, binary, args)
        else:
            environment["virtuoso"], records = run_virtuoso(fixture_dir, cases, args)
        for record in records:
            record["engine"] = engine
        results.extend(records)
    by_id = {case["id"]: case for case in cases}
    for record in results:
        if record.get("record") == "case":
            record["validity"] = validate(by_id[record["case"]], record)
    (out / "environment.json").write_text(json.dumps(environment, indent=1))
    with (out / "results.jsonl").open("w") as stream:
        for record in results:
            stream.write(json.dumps(record, ensure_ascii=False) + "\n")
    summary = summarize(cases, results)
    (out / "summary.json").write_text(json.dumps(summary, indent=1))
    invalid = [row for row in summary if any(v not in ("valid", "normalized", None)
                                             and not v.startswith("limit:")
                                             for v in row["validity"].values())]
    print(json.dumps({"out": str(out), "cases": len(cases), "invalid": invalid}, indent=1))
    return 1 if invalid else 0


def build_craqle():
    """Build the in-process adapter and return its executable path."""
    build = subprocess.run(
        ["nice", "-n", "19", "cargo", "build", "--release", "--locked", "--all-features",
         "--bench", "rocrate_craqle", "--message-format=json"],
        cwd=REPO, capture_output=True, text=True, check=True,
        env={**os.environ, "CARGO_BUILD_JOBS": os.environ.get("CARGO_BUILD_JOBS", "2")})
    return next(json.loads(line)["executable"] for line in build.stdout.splitlines()
                if '"executable":"' in line and "rocrate_craqle" in line)


def run_craqle(fixture_dir, binary, args):
    """Run the adapter on the declared CPUs and keep its records and identity."""
    started = time.monotonic()
    run = subprocess.run(["taskset", "-c", args.cpus, binary], capture_output=True, text=True,
                         env={**os.environ, "CRAQLE_ROCRATE_FIXTURE": str(fixture_dir),
                              "CRAQLE_ROCRATE_SAMPLES": str(args.samples)})
    records = [json.loads(line) for line in run.stdout.splitlines() if line.startswith("{")]
    info = {"binary": binary,
            "binary_sha256": hashlib.sha256(Path(binary).read_bytes()).hexdigest(),
            "exit": run.returncode, "wall_ns": int((time.monotonic() - started) * 1e9),
            "stderr_tail": run.stderr[-2000:]}
    return info, records


def run_virtuoso(fixture_dir, cases, args):
    server = virtuoso.Virtuoso(args.cpus, args.tuned)
    records = []
    info = {"image": virtuoso.IMAGE, "tag": virtuoso.IMAGE_TAG, "settings": server.settings,
            "client": "persistent HTTP/1.1 keep-alive on loopback; ODBC driver not on host"}
    try:
        server.start(fixture_dir / "dataset.nq")
        info["version"] = server.version()
        load = server.load()
        expected = json.loads((fixture_dir / "manifest.json").read_text())["quads"]
        if load["loaded_quads"] != expected:
            raise RuntimeError(f"loaded {load['loaded_quads']} quads, expected {expected}")
        records.append({"record": "load", **load})
        order = list(cases)
        random.Random(len(cases)).shuffle(order)
        for case in order:
            if "virtuoso" in case.get("engines", ["virtuoso"]):
                records.append(virtuoso_case(server, case, args.samples))
        records.append(virtuoso_update(server, cases))
        records.append({"record": "footprint", "bytes": server.footprint()})
    finally:
        server.stop()
    return info, records


def virtuoso_query(case):
    """The Virtuoso form of a case: portable SPARQL, or bif:contains for text cases."""
    if case["kind"] != "text":
        return virtuoso.dataset_query(case)
    match = virtuoso.text_filter(case["terms"])
    if case["mode"] == "controlled":
        return f"SELECT DISTINCT ?g ?s WHERE {{ GRAPH ?g {{ ?s ?p ?o . {match} }} }}"
    # Virtuoso reserves the variable name "score" in free-text patterns.
    return (f"SELECT ?g ?s (MAX(?weight) AS ?best) WHERE {{ GRAPH ?g {{ ?s ?p ?o . "
            f"{match} OPTION (score ?weight) }} }} GROUP BY ?g ?s "
            f"ORDER BY DESC(?best) ?g ?s LIMIT {case['limit']}")


def virtuoso_case(server, case, samples):
    query = virtuoso_query(case)
    try:
        answer = server.select(query)
    except RuntimeError as error:
        return {"record": "case", "case": case["id"], "status": "failed", "error": str(error),
                "query": query}
    if case["kind"] == "text":
        rows = [[row["g"]["value"], row["s"]["value"]]
                for row in answer["results"]["bindings"]]
    else:
        rows = virtuoso.canonical(answer, case["variables"])
    timings = []
    for _ in range(samples):
        started = time.perf_counter_ns()
        server.select(query)
        timings.append(time.perf_counter_ns() - started)
    return {"record": "case", "case": case["id"], "status": "measured", "rows": rows,
            "samples_ns": timings, "query": query}


def virtuoso_update(server, cases):
    """Rename the target crate, then time query and text-index visibility."""
    target = next(case for case in cases if case["id"].startswith("RC02"))["sparql"]
    graph = target.split("GRAPH <", 1)[1].split(">", 1)[0]
    schema_name = "<http://schema.org/name>"
    started = time.perf_counter_ns()
    server.update(f"WITH <{graph}> DELETE {{ <{graph}> {schema_name} ?old }} "
                  f"INSERT {{ <{graph}> {schema_name} \"Renamed zephyr crate\" }} "
                  f"WHERE {{ <{graph}> {schema_name} ?old }}")
    acknowledged = time.perf_counter_ns() - started
    answer = server.select(f"SELECT ?name WHERE {{ GRAPH <{graph}> {{ <{graph}> "
                           f"{schema_name} ?name }} }}")
    queried = time.perf_counter_ns() - started
    barrier = server.text_barrier()
    hits = server.select(f"SELECT DISTINCT ?g ?s WHERE {{ GRAPH ?g {{ ?s ?p ?o . "
                         f"{virtuoso.text_filter('zephyr')} }} }}")
    return {"record": "update", "case": "RC14-rename", "graph": graph,
            "acknowledged_ns": acknowledged, "query_visible_ns": queried,
            "text_barrier_ns": barrier, "search_visible_ns": queried + barrier,
            "rows": virtuoso.canonical(answer, ["name"]),
            "hits": [[row["g"]["value"], row["s"]["value"]]
                     for row in hits["results"]["bindings"]]}


def normalize(case, rows):
    base = case.get("descriptor")
    if not base:
        return rows
    names = (f"<{fixture.DESCRIPTOR}>", f"<{base}{fixture.DESCRIPTOR}>")
    for name in names:
        rows = [row.replace(f"={name}", "=<DESCRIPTOR>") for row in rows]
    return rows


def lenient(rows):
    """Integer-family datatypes read as plain integers, for count results only."""
    return [row.replace("^^<http://www.w3.org/2001/XMLSchema#int>",
                        "^^<http://www.w3.org/2001/XMLSchema#integer>") for row in rows]


def validate(case, record):
    """'valid', 'normalized' (datatype difference only), or a reason the answer is wrong."""
    if record.get("status") != "measured":
        return record.get("error", "not measured")
    rows = record["rows"]
    if case["kind"] == "text":
        hits = [tuple(hit) for hit in rows]
        if len(set(hits)) != len(hits):
            return "duplicate hits"
        if case["mode"] == "controlled":
            if sorted(hits) == sorted(map(tuple, case["expected"])):
                return "valid"
            if record["engine"] == "craqle" and len(hits) == SEARCH_LIMIT < len(case["expected"]):
                return f"limit: one search returns at most {SEARCH_LIMIT} hits"
            return f"matched {len(hits)} resources, expected {len(case['expected'])}"
        pool = set(map(tuple, case["pool"]))
        if len(hits) != case["size"]:
            return f"{len(hits)} hits, expected {case['size']}"
        return "valid" if all(hit in pool for hit in hits) else "ineligible hit"
    rows = normalize(case, rows)
    expected = case["expected"]
    if case["compare"] == "subset":
        # A bounded answer may omit matches, but every row must be one and appear once.
        valid = len(set(rows)) == len(rows) and set(rows) <= set(expected)
        return "valid" if valid else "row outside the expected answer or repeated"
    ordered = case["compare"] == "ordered"
    if (rows if ordered else sorted(rows)) == (expected if ordered else sorted(expected)):
        return "valid"
    loose = lenient(rows)
    if (loose if ordered else sorted(loose)) == (expected if ordered else sorted(expected)):
        return "normalized"
    return f"{len(rows)} rows differ from {len(expected)} expected"


def summarize(cases, results):
    summary = []
    for case in cases:
        row = {"case": case["id"], "validity": {}, "median_ms": {}, "p90_ms": {}}
        for record in results:
            if record.get("record") != "case" or record["case"] != case["id"]:
                continue
            engine = record["engine"]
            row["validity"][engine] = record["validity"]
            samples = sorted(record.get("samples_ns") or [])
            if samples:
                row["median_ms"][engine] = round(statistics.median(samples) / 1e6, 4)
                row["p90_ms"][engine] = round(samples[(len(samples) - 1) * 9 // 10] / 1e6, 4)
        summary.append(row)
    return summary


if __name__ == "__main__":
    sys.exit(main())
