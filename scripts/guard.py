#!/usr/bin/env python3
"""Run a command in a capped Linux user scope with a headroom watchdog."""
# Copyright (c) 2026 ArunaStorage Team @ JLU Giessen
# SPDX-License-Identifier: MIT

import json
import os
from pathlib import Path
import shutil
import resource
import signal
import subprocess
import sys
import time
import uuid


def main():
    if len(sys.argv) < 4:
        raise ValueError(__doc__)
    repo, log = Path(sys.argv[1]).resolve(strict=True), Path(sys.argv[2])
    memory_max = os.environ.get("CRAQLE_MEMORY_MAX", "12G")
    memory_high = os.environ.get("CRAQLE_MEMORY_HIGH", "10G")
    memory_swap = os.environ.get("CRAQLE_MEMORY_SWAP_MAX", "0")
    cpu_quota = os.environ.get("CRAQLE_CPU_QUOTA", "200%")
    tasks_max = os.environ.get("CRAQLE_TASKS_MAX", "256")
    memory_floor = int(os.environ.get("CRAQLE_MEMORY_FLOOR_BYTES", str(8 * 1024**3)))
    disk_floor = int(os.environ.get("CRAQLE_DISK_FLOOR_BYTES", str(20 * 1024**3)))
    unit = "craqle-check-" + uuid.uuid4().hex + ".scope"
    command = ["systemd-run", "--user", "--scope", "--quiet", f"--unit={unit}",
               "-p", f"MemoryMax={memory_max}", "-p", f"MemoryHigh={memory_high}",
               "-p", f"MemorySwapMax={memory_swap}", "-p", f"CPUQuota={cpu_quota}",
               "-p", f"TasksMax={tasks_max}", "env", "CARGO_BUILD_JOBS=2",
               "RUST_TEST_THREADS=2", "CARGO_INCREMENTAL=0", "CARGO_PROFILE_DEV_DEBUG=0",
               "CARGO_SAFE_ACTIVE=1",
               "PYTHONDONTWRITEBYTECODE=1", "nice", "-n", "19", "ionice", "-c3", *sys.argv[3:]]
    resource.setrlimit(resource.RLIMIT_CORE, (0, 0))
    process = None
    with log.open("x") as output:
        def record(**event):
            output.write(json.dumps({"utc": time.strftime("%FT%TZ", time.gmtime()), **event}) + "\n")
            output.flush()

        def stop(signum, _frame):
            raise InterruptedError(f"signal {signum}")

        signal.signal(signal.SIGTERM, stop)
        signal.signal(signal.SIGINT, stop)
        try:
            sample = resources(repo, None)
            record(unit=unit, command=command, **sample)
            if not healthy(sample, memory_floor, disk_floor):
                raise RuntimeError("insufficient initial resource headroom")
            process = subprocess.Popen(command, cwd=repo)
            cgroup = None
            while process.poll() is None:
                cgroup = cgroup or scope_path(unit)
                sample = resources(repo, cgroup)
                record(**sample)
                if not healthy(sample, memory_floor, disk_floor):
                    raise RuntimeError("resource headroom floor crossed")
                try:
                    process.wait(timeout=1)
                except subprocess.TimeoutExpired:
                    pass
            record(exit=process.returncode, **resources(repo, cgroup))
            return process.returncode if process.returncode >= 0 else 128 - process.returncode
        except (OSError, RuntimeError) as error:
            record(failure=str(error))
            if process is not None:
                subprocess.run(["systemctl", "--user", "kill", "--kill-whom=all", "--signal=SIGKILL", unit],
                               check=False, timeout=30)
                process.wait(timeout=30)
            return 1



def scope_path(unit):
    """Returns the user scope's cgroup v2 path when systemd exposes it."""
    result = subprocess.run(
        ["systemctl", "--user", "show", unit, "--property=ControlGroup", "--value"],
        capture_output=True, text=True, check=False,
    )
    value = result.stdout.strip()
    if result.returncode or not value:
        return None
    path = Path("/sys/fs/cgroup") / value.lstrip("/")
    return path if path.is_dir() else None


def read_stat(path):
    """Reads a cgroup counter file without inventing unavailable values."""
    try:
        return path.read_text().strip()
    except OSError:
        return None


def cgroup_stats(path):
    """Returns available scope counters and an explicit availability marker."""
    if path is None:
        return {"available": False}
    counters = {"available": True, "path": str(path)}
    for name in ("memory.current", "memory.peak", "cpu.stat", "io.stat"):
        value = read_stat(path / name)
        if value is not None:
            counters[name.replace(".", "_")] = value
    return counters


def resources(repo, cgroup):
    memory = {line.split(":")[0]: int(line.split()[1]) * 1024
              for line in Path("/proc/meminfo").read_text().splitlines()}
    return {"available_memory": memory["MemAvailable"],
            "filesystem_free": shutil.disk_usage(repo).free,
            "load": Path("/proc/loadavg").read_text().strip(),
            "cgroup": cgroup_stats(cgroup)}


def healthy(sample, memory_floor, disk_floor):
    return (sample["available_memory"] >= memory_floor
            and sample["filesystem_free"] >= disk_floor)




if __name__ == "__main__":
    sys.exit(main())
