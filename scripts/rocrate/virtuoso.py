"""Run a disposable, localhost-only Virtuoso instance and query it over persistent HTTP."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import http.client
import json
from pathlib import Path
import secrets
import shutil
import subprocess
import tempfile
import time
import urllib.parse

IMAGE = ("openlink/virtuoso-opensource-7@sha256:"
         "0dbe1ab4fa0cb7bbafc1f6c0c2b0a5d6f22d918dbd17672f2ddb24580aa6756a")
IMAGE_TAG = "7.2.17-r25.1-g2850f18-ubuntu"
LABEL = "craqle-rocrate-bench"
# The image's /database is a symlink to this directory.
DATABASE = "/opt/virtuoso-opensource/database"
MARKER = ".craqle-rocrate-bench"
TEXT_PREDICATES = ("name", "description", "keywords", "identifier")
# Result caps and time limits would allow silent partial answers.
SETTINGS = {
    "VIRT_SPARQL_ResultSetMaxRows": "100000000",
    "VIRT_SPARQL_MaxQueryExecutionTime": "0",
    "VIRT_SPARQL_MaxQueryCostEstimationTime": "0",
    "VIRT_SPARQL_MaxSortedTopRows": "100000000",
    "VIRT_Parameters_DirsAllowed": f".,../vad,/usr/share/proj,{DATABASE}/load",
}
# A modest tuned profile from the RDF performance guide's 2 GiB row.
TUNED = {"VIRT_Parameters_NumberOfBuffers": "170000",
         "VIRT_Parameters_MaxDirtyBuffers": "130000"}


class Virtuoso:
    """One disposable server; only runner-owned containers and directories are removed."""

    def __init__(self, cpus, tuned):
        self.name = f"{LABEL}-{secrets.token_hex(4)}"
        self.password = secrets.token_urlsafe(18)
        self.data = Path(tempfile.mkdtemp(prefix="craqle-virtuoso-"))
        (self.data / MARKER).write_text(self.name)
        self.cpus = cpus
        self.settings = {**SETTINGS, **(TUNED if tuned else {})}
        self.connection = None
        self.port = None

    def start(self, dataset):
        """Start the container with the dataset already staged in its data directory."""
        # The container takes ownership of the mounted directory, so mount a child of ours.
        (self.data / "db" / "load").mkdir(parents=True)
        shutil.copy(dataset, self.data / "db" / "load" / "dataset.nq")
        command = ["docker", "run", "-d", "--name", self.name, "--label", f"{LABEL}=1",
                   "--cpuset-cpus", self.cpus, "-p", "127.0.0.1::8890",
                   "-e", f"DBA_PASSWORD={self.password}", "-e", "SPARQL_UPDATE=true",
                   "-v", f"{self.data / 'db'}:{DATABASE}"]
        for key, value in self.settings.items():
            command += ["-e", f"{key}={value}"]
        subprocess.run(command + [IMAGE], check=True, capture_output=True)
        mapped = subprocess.run(["docker", "port", self.name, "8890/tcp"], check=True,
                                capture_output=True, text=True).stdout.split()[0]
        self.port = int(mapped.rsplit(":", 1)[1])
        deadline = time.monotonic() + 300
        while time.monotonic() < deadline:
            if self.isql("status();", check=False).returncode == 0:
                # The disposable endpoint user may update, as the freshness case requires.
                self.isql('GRANT SPARQL_UPDATE TO "SPARQL";')
                self.isql("DB.DBA.RDF_DEFAULT_USER_PERMS_SET ('nobody', 7);")
                return
            time.sleep(1)
        raise RuntimeError("Virtuoso did not accept SQL within 300 seconds")

    def isql(self, statement, check=True):
        """Run SQL through the container's own isql client; isql exits 0 on SQL errors."""
        script = f"{statement}\n"
        result = subprocess.run(["docker", "exec", "-i", self.name, "isql", "1111", "dba",
                                 self.password], input=script, text=True, capture_output=True)
        if check and (result.returncode != 0 or "*** Error" in result.stdout):
            raise RuntimeError(f"isql failed: {statement}: {result.stdout[-500:]}")
        return result

    def version(self):
        output = self.isql("select sys_stat('st_dbms_ver'), sys_stat('st_build_date');").stdout
        return " ".join(line.strip() for line in output.splitlines() if "7." in line)

    def load(self):
        """Bulk load the staged N-Quads, then build the RDF literal text index."""
        timings = {}
        started = time.monotonic()
        self.isql("DB.DBA.RDF_OBJ_FT_RULE_ADD(null, null, 'craqle_bench');")
        self.isql(f"ld_dir('{DATABASE}/load', 'dataset.nq', 'urn:bench:unused');")
        self.isql("rdf_loader_run();")
        self.isql("checkpoint;")
        timings["load_ns"] = int((time.monotonic() - started) * 1e9)
        started = time.monotonic()
        self.isql("DB.DBA.VT_INC_INDEX_DB_DBA_RDF_OBJ();")
        timings["text_index_ns"] = int((time.monotonic() - started) * 1e9)
        errors = self.isql("select count(*) from DB.DBA.LOAD_LIST where ll_error is not null;")
        if not any(line.strip() == "0" for line in errors.stdout.splitlines()):
            raise RuntimeError(f"bulk load reported errors: {errors.stdout}")
        loaded = self.select("SELECT (COUNT(*) AS ?n) WHERE { GRAPH ?g { ?s ?p ?o } "
                             "FILTER(STRSTARTS(STR(?g), \"https://bench.example/crate/\")) }")
        timings["loaded_quads"] = int(loaded["results"]["bindings"][0]["n"]["value"])
        return timings

    def text_barrier(self):
        """Apply pending text-index updates; returns elapsed nanoseconds."""
        started = time.monotonic()
        self.isql("DB.DBA.VT_INC_INDEX_DB_DBA_RDF_OBJ();")
        return int((time.monotonic() - started) * 1e9)

    def request(self, body, accept):
        """POST one SPARQL request on the persistent connection; rejects partial results."""
        if self.connection is None:
            self.connection = http.client.HTTPConnection("127.0.0.1", self.port, timeout=3600)
        self.connection.request("POST", "/sparql", body=urllib.parse.urlencode(body),
                                headers={"Content-Type": "application/x-www-form-urlencoded",
                                         "Accept": accept})
        response = self.connection.getresponse()
        payload = response.read()
        state = response.getheader("X-SQL-State")
        if response.status != 200 or state:
            raise RuntimeError(f"incomplete or failed answer: status {response.status}, "
                               f"state {state}, {payload[:300]!r}")
        return payload

    def select(self, query):
        payload = self.request({"query": query, "timeout": "0"},
                               "application/sparql-results+json")
        return json.loads(payload)

    def update(self, statement):
        self.request({"query": statement}, "application/sparql-results+json")

    def footprint(self):
        """Bytes of the database directory, measured inside the container that owns it."""
        output = self.isql_shell(f"du -sb {DATABASE}")
        return int(output.split()[0])

    def isql_shell(self, command):
        return subprocess.run(["docker", "exec", self.name, "sh", "-c", command],
                              capture_output=True, text=True, check=True).stdout

    def stop(self):
        """Remove only this runner's labelled container and marked temporary directory."""
        if self.connection is not None:
            self.connection.close()
        labels = subprocess.run(["docker", "inspect", "--format",
                                 f"{{{{index .Config.Labels \"{LABEL}\"}}}}", self.name],
                                capture_output=True, text=True).stdout.strip()
        if labels == "1":
            subprocess.run(["docker", "rm", "-f", self.name], capture_output=True, check=True)
        owned = (self.data / MARKER).exists() and (self.data / MARKER).read_text() == self.name
        if owned and self.data.parent == Path(tempfile.gettempdir()):
            # Container-written files belong to its user; remove them from inside first.
            subprocess.run(["docker", "run", "--rm", "-v", f"{self.data / 'db'}:{DATABASE}",
                            "--entrypoint", "find", IMAGE, DATABASE, "-mindepth", "1",
                            "-delete"], capture_output=True, check=True)
            # The emptied child belongs to the container user but sits in our directory.
            (self.data / "db").rmdir()
            (self.data / MARKER).unlink()
            self.data.rmdir()


def term(binding):
    """One SPARQL JSON result term in the N-Triples form both adapters report."""
    if binding is None:
        return "UNBOUND"
    value = binding["value"]
    if binding["type"] == "uri":
        return f"<{value}>"
    if binding["type"] == "bnode":
        return f"_:{value}"
    text = json.dumps(value, ensure_ascii=False)
    if "xml:lang" in binding:
        return f"{text}@{binding['xml:lang']}"
    if "datatype" in binding and binding["datatype"] != "http://www.w3.org/2001/XMLSchema#string":
        return f"{text}^^<{binding['datatype']}>"
    return text


def canonical(results, variables):
    bindings = results["results"]["bindings"]
    return [" ".join(f"{name}={term(row.get(name))}" for name in variables) for row in bindings]


def text_filter(terms):
    words = " OR ".join(f"'{word}'" for word in terms.split())
    predicates = ", ".join(f"<http://schema.org/{name}>" for name in TEXT_PREDICATES)
    return f"FILTER(?p IN ({predicates})) ?o bif:contains \"{words}\""


def dataset_query(case):
    """The case's SPARQL with its explicit dataset or readable-graph restriction applied."""
    query = case.get("virtuoso_sparql") or case["sparql"]
    if case.get("graphs"):
        head, body = query.split(" WHERE ", 1)
        query = f"{head} {' '.join(f'FROM <{graph}>' for graph in case['graphs'])} WHERE {body}"
    if case.get("readable") is not None:
        values = " ".join(f"<{graph}>" for graph in case["readable"])
        query = query.replace(" WHERE { ", f" WHERE {{ VALUES ?g {{ {values} }} ", 1)
    return query
