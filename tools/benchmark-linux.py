"""Bounded offline A/B driver. Requires isolated networking (e.g. PrivateNetwork).

Each run owns a fresh output directory. It never modifies the input fixture or
uses production credentials. Server CPU/heap/IO are separate from this driver.
"""
import argparse
import concurrent.futures
from collections import Counter, defaultdict
from datetime import datetime, timezone, timedelta
import hashlib
import http.client
import json
import math
import os
from pathlib import Path
import re
import subprocess
import threading
import time
from urllib.parse import urlencode, urlsplit


def percentile(values, percent):
    values = sorted(values)
    if not values:
        return None
    return values[min(len(values) - 1, math.ceil(percent * len(values)) - 1)]


def event(kind, sequence, consumers, epoch, admin):
    index = sequence
    if kind == "contact" and sequence % 5 in (1, 2):
        index -= sequence % 5
    if kind == "cooldown" and sequence % 5 == 1:
        index -= 1
    consumer = consumers[index % len(consumers)]
    headers = {"Authorization": "Bearer " + consumer["Key"],
               "X-Gwpool-Account-Tag": consumer["Tag"], "X-Gwpool-Cooldown-Max-Seconds": "86400"}
    query = urlencode({"account": consumer["Account"], "model": consumer["Model"]})
    if kind == "gateways":
        return "GET", "/gateways?" + query, None, headers, consumer
    if kind == "cookie":
        return "GET", "/cookie?" + urlencode({"account": consumer["Account"], "gateway": consumer["Gateway"]}), None, headers, consumer
    if kind == "page":
        return "GET", "/admin/", None, {"Authorization": "Bearer " + admin}, consumer
    at = (epoch - timedelta(minutes=1) + timedelta(milliseconds=index)).isoformat()
    report = {"id": hashlib.sha256(f"offline-{kind}-{index}".encode()).hexdigest(),
              "account_tag": consumer["Tag"], "gateway": consumer["Gateway"]}
    if kind == "contact":
        report.update({"model": consumer["Model"], "criterion": "state-echo-v1", "source": "foreground",
                       "first": "repeat", "at": at, "gap_known": True, "elapsed_seconds": 7200,
                       "outcome": "refreshed" if index % 5 == 3 else "full"})
        if sequence % 5 == 2:
            report.update({"window_final": True, "full_window_ms": 1000})
    else:
        report.update({"window_seconds": 7200, "elapsed_seconds": 7200, "result": "full", "observed_at": at})
    headers["Content-Type"] = "application/json"
    return "POST", f"/{kind}/report", json.dumps(report, separators=(",", ":")).encode(), headers, consumer


def validate(kind, status, raw, consumer):
    if status != 200:
        return "http_" + str(status)
    if kind == "page":
        return None if b"<!doctype html" in raw.lower() else "invalid_html"
    try:
        body = json.loads(raw)
        if kind == "gateways":
            rows = body["gateways"]
            names = [r["name"] for r in rows]
            if body["account"] != consumer["Account"] or names != sorted(set(names)):
                return "catalog_identity_or_order"
            if any(not r["pair_ready"] or r["valid_for_s"] < body["deliver_floor_s"] for r in rows):
                return "catalog_invalid_inventory"
        elif kind == "cookie":
            if body["gateway"] != consumer["Gateway"] or "__oailb=" not in body["cookie"] or body["valid_for_s"] <= 0:
                return "invalid_ticket"
        elif body.get("ok") is not True:
            return "invalid_acknowledgement"
    except (KeyError, ValueError, TypeError):
        return "invalid_response"
    return None


def run(args):
    output = Path(args.output).resolve()
    output.mkdir(mode=0o700)  # refuse reusing a partial or completed run
    fixture = Path(args.fixture).resolve()
    binary = Path(args.binary).resolve()
    env = {**os.environ, "GWPOOL_MIX_FIXTURE": str(fixture), "GWPOOL_MIX_OUTPUT": str(output),
           "GWPOOL_MIX_BACKGROUND": str(int(args.background)), "GWPOOL_MIX_CHECKPOINT": str(int(args.checkpoint)),
           "GWPOOL_MIX_PROFILE": str(int(args.profile)), "GOMAXPROCS": "2"}
    log = (output / "server.log").open("wb")
    process = subprocess.Popen([str(binary), "-test.run=^TestPortableProductionBenchmark$", "-test.timeout=60s",
                                "-test.v"], env=env, stdout=log, stderr=subprocess.STDOUT)
    try:
        deadline = time.monotonic() + 20
        ready_path = output / "ready.json"
        while not ready_path.exists():
            if process.poll() is not None:
                raise RuntimeError("benchmark server failed before ready; inspect server.log")
            if time.monotonic() >= deadline:
                raise RuntimeError("benchmark setup exceeded 20 seconds")
            time.sleep(0.05)
        ready = json.loads(ready_path.read_bytes())
        # Python 3.10 accepts microseconds, while Go emits RFC3339 nanoseconds.
        timestamp = re.sub(r"\.(\d{6})\d+", r".\1", ready["started_at"])
        epoch = datetime.fromisoformat(timestamp.replace("Z", "+00:00"))
        address = urlsplit(ready["url"])
        local = threading.local()
        schedule = []
        rates = {"gateways": args.rate, "cookie": 8, "contact": 2, "cooldown": 1, "page": 0.2}
        for kind, rate in rates.items():
            for index in range(int(rate * args.duration)):
                schedule.append((index / rate, kind, index))
        schedule.sort()
        start = time.monotonic()

        def request(plan):
            offset, kind, sequence = plan
            began = time.monotonic()
            method, path, payload, headers, consumer = event(kind, sequence, ready["consumers"], epoch, ready["admin_key"])
            try:
                if not hasattr(local, "connection"):
                    local.connection = http.client.HTTPConnection(address.hostname, address.port, timeout=10)
                local.connection.request(method, path, payload, headers)
                response = local.connection.getresponse()
                raw = response.read()
                status = response.status
                error = validate(kind, status, raw, consumer)
            except (OSError, http.client.HTTPException) as e:
                if hasattr(local, "connection"):
                    local.connection.close()
                    del local.connection
                status, raw, error = 0, b"", type(e).__name__
            finished = time.monotonic()
            return {"kind": kind, "sequence": sequence, "offset": offset, "status": status,
                    "service_ms": (finished - began) * 1000,
                    "scheduled_ms": (finished - start - offset) * 1000,
                    "queue_ms": max(0, (began - start - offset) * 1000), "bytes": len(raw), "error": error}

        samples = []
        sampling_stop = threading.Event()

        def sample():
            ticks = os.sysconf("SC_CLK_TCK")
            while not sampling_stop.wait(0.1):
                try:
                    stat = Path(f"/proc/{process.pid}/stat").read_text().split()
                    status = Path(f"/proc/{process.pid}/status").read_text()
                    rss = next(int(line.split()[1]) for line in status.splitlines() if line.startswith("VmRSS:"))
                    samples.append({"at": time.monotonic() - start, "cpu_seconds": (int(stat[13]) + int(stat[14])) / ticks,
                                    "rss_kib": rss})
                except (OSError, StopIteration):
                    break

        sampler = threading.Thread(target=sample, daemon=True)
        sampler.start()
        futures = []
        with concurrent.futures.ThreadPoolExecutor(max_workers=32) as executor:
            for plan in schedule:
                wait = start + plan[0] - time.monotonic()
                if wait > 0:
                    time.sleep(wait)
                futures.append(executor.submit(request, plan))
            results = [f.result(timeout=12) for f in futures]
        ended = time.monotonic()
        (output / "stop").write_text("load complete")
        sampling_stop.set()
        sampler.join()
        process.wait(timeout=15)
        if process.returncode:
            raise RuntimeError("benchmark server did not finish cleanly; inspect server.log")
        server = json.loads((output / "server.json").read_bytes())
        grouped = defaultdict(list)
        for row in results:
            grouped[row["kind"]].append(row)
        endpoints = {}
        for kind, rows in grouped.items():
            endpoints[kind] = {"planned": sum(k == kind for _, k, _ in schedule), "completed": len(rows),
                               "statuses": dict(Counter(str(r["status"]) for r in rows)),
                               "errors": dict(Counter(r["error"] for r in rows if r["error"])),
                               "bytes": sum(r["bytes"] for r in rows)}
            for metric in ("service_ms", "scheduled_ms", "queue_ms"):
                values = [r[metric] for r in rows]
                endpoints[kind][metric] = {"p50": percentile(values, .5), "p95": percentile(values, .95),
                                           "p99": percentile(values, .99), "max": max(values)}
        deltas = {key: server["after"][key] - server["before"][key] for key in
                  ("cpu_seconds", "allocated_bytes", "mallocs", "gc_cycles", "gc_pause_ns",
                   "rchar", "wchar", "read_bytes", "write_bytes", "syscr", "syscw")}
        delta = {"schema": 1, "binary_sha256": hashlib.sha256(binary.read_bytes()).hexdigest(),
                 "fixture_state_sha256": ready["manifest"]["state_sha256"], "endpoints": endpoints,
                 "duration_planned": args.duration, "elapsed_load_seconds": ended - start, "rates": rates,
                 "server_delta": deltas, "server_peak_rss_kib": max((s["rss_kib"] for s in samples), default=0),
                 "fake_mint_calls": server["fake_mint_calls"], "forced_checkpoint_seconds": server["checkpoint_seconds"],
                 "warm_interval_seconds": ready["warm_interval_seconds"], "profile": args.profile,
                 "total_validation_errors": sum(bool(r["error"]) for r in results),
                 "planned": len(schedule), "completed": len(results), "server": server}
        (output / "requests.json").write_text(json.dumps(results))
        (output / "resources.json").write_text(json.dumps(samples))
        (output / "result.json").write_text(json.dumps(delta, indent=2))
        print(json.dumps({"output": str(output), "completed": len(results),
                          "errors": delta["total_validation_errors"], "server_delta": deltas,
                          "peak_rss_mib": delta["server_peak_rss_kib"] / 1024,
                          "catalog_p95_ms": endpoints["gateways"]["service_ms"]["p95"],
                          "cookie_p95_ms": endpoints["cookie"]["service_ms"]["p95"]}), flush=True)
        if delta["total_validation_errors"]:
            raise RuntimeError("responses failed correctness checks; not a passing benchmark")
    finally:
        if process.poll() is None:
            process.terminate()
            try:
                process.wait(timeout=5)
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        log.close()


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--binary", required=True)
    parser.add_argument("--fixture", required=True)
    parser.add_argument("--output", required=True)
    parser.add_argument("--duration", type=float, default=22)
    parser.add_argument("--rate", type=float, default=100)
    parser.add_argument("--background", action="store_true")
    parser.add_argument("--checkpoint", action="store_true")
    parser.add_argument("--profile", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.duration <= 30 or not 1 <= args.rate <= 1000:
        parser.error("duration must be 1..30 seconds and catalog rate 1..1000/s")
    run(args)
