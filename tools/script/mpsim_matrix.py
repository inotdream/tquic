#!/usr/bin/env python3
# Copyright (c) 2025 The TQUIC Authors.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Run tquic_mpsim experiment matrices and aggregate the results.

Usage:
    cargo build --release -p tquic_tools --bin tquic_mpsim
    python3 tools/script/mpsim_matrix.py run --out results --matrix A B
    python3 tools/script/mpsim_matrix.py summarize --out results

Every run writes <out>/<matrix>/<name>.json (and a per-frame CSV). Existing
results are skipped, so an interrupted matrix can be resumed.
"""

import argparse
import csv
import itertools
import json
import os
import re
import subprocess
import sys
from concurrent.futures import ThreadPoolExecutor

ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), "..", ".."))
DEFAULT_BIN = os.path.join(ROOT, "target", "release", "tquic_mpsim")

# A WiFi-like primary path and an LTE-like secondary path.
WIFI = "bw=20,delay=15,queue=100"
LTE = "bw=10,delay=30,queue=100"

MODES = ["single", "minrtt", "redundant", "roundrobin"]
CCS = ["bbr", "cubic"]
SEEDS = [1, 2, 3]
LOSSES = [0, 1, 2, 5, 10, 20]


def run_specs(matrices):
    """Return the list of runs of the selected matrices."""
    runs = []

    def add(matrix, name, mode, cc, seed, paths, extra=None, **meta):
        runs.append(
            dict(
                matrix=matrix,
                name=name,
                mode=mode,
                cc=cc,
                seed=seed,
                paths=paths,
                extra=extra or [],
                meta=meta,
            )
        )

    # A: random loss on the WiFi path only, the LTE path is clean.
    if "A" in matrices:
        for loss, mode, cc, seed in itertools.product(LOSSES, MODES, CCS, SEEDS):
            add("A", f"A_l{loss}_{mode}_{cc}_s{seed}", mode, cc, seed,
                [f"{WIFI},loss={loss}", LTE], loss=loss)

    # B: the same random loss rate on both paths.
    if "B" in matrices:
        for loss, mode, cc, seed in itertools.product([2, 5, 10, 20], MODES, CCS, SEEDS):
            add("B", f"B_l{loss}_{mode}_{cc}_s{seed}", mode, cc, seed,
                [f"{WIFI},loss={loss}", f"{LTE},loss={loss}"], loss=loss)

    # C: bursty loss on WiFi. Loss bursts last 20 ms on average and the
    # share of time in a burst equals the loss rate.
    if "C" in matrices:
        for loss, mode, cc, seed in itertools.product([2, 5, 10], MODES, CCS, SEEDS):
            add("C", f"C_l{loss}_{mode}_{cc}_s{seed}", mode, cc, seed,
                [f"{WIFI},loss={loss},burstms=20", LTE], loss=loss, burst_ms=20)

    # Cpkt: per-packet Gilbert-Elliott bursts (mean 10 packets, as netem's
    # gemodel). Bursts last longer in time when a path carries little traffic.
    if "Cpkt" in matrices:
        for loss, mode, cc, seed in itertools.product([2, 5, 10], MODES, CCS, SEEDS):
            add("Cpkt", f"C_l{loss}_{mode}_{cc}_s{seed}", mode, cc, seed,
                [f"{WIFI},loss={loss},burst=10", LTE], loss=loss, burst=10)

    # D: WiFi outage, either for 3 seconds or until the end of the stream.
    if "D" in matrices:
        outages = {"3s": "10-13", "perm": "10-inf"}
        for (label, window), mode, cc, seed in itertools.product(
            outages.items(), MODES, CCS, SEEDS
        ):
            add("D", f"D_{label}_{mode}_{cc}_s{seed}", mode, cc, seed,
                [f"{WIFI},outage={window}", LTE],
                extra=["timeline", "cc"], outage=label)

    return runs


def run_one(binary, out_dir, spec, duration):
    mdir = os.path.join(out_dir, spec["matrix"])
    os.makedirs(mdir, exist_ok=True)
    base = os.path.join(mdir, spec["name"])
    if os.path.exists(base + ".json"):
        return spec["name"], "skipped"

    cmd = [
        binary,
        "--mode", spec["mode"],
        "--cc", spec["cc"],
        "--seed", str(spec["seed"]),
        "--duration", str(duration),
        "--label", json.dumps(spec["meta"]),
        "--out", base + ".json.tmp",
        "--frames-csv", base + ".frames.csv",
    ]
    for p in spec["paths"]:
        cmd += ["--path", p]
    if "timeline" in spec["extra"]:
        cmd += ["--timeline-csv", base + ".timeline.csv"]
    if "cc" in spec["extra"]:
        cmd += ["--cc-csv", base + ".cc.csv"]

    try:
        subprocess.run(cmd, check=True, timeout=duration + 120,
                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE)
    except subprocess.CalledProcessError as e:
        return spec["name"], "failed: " + e.stderr.decode(errors="replace")[-300:]
    except subprocess.TimeoutExpired:
        return spec["name"], "timeout"
    os.replace(base + ".json.tmp", base + ".json")
    return spec["name"], "done"


def cmd_run(args):
    specs = run_specs(set(args.matrix))
    if args.filter:
        pattern = re.compile(args.filter)
        specs = [s for s in specs if pattern.search(s["name"])]
    print(f"{len(specs)} runs, {args.jobs} in parallel", flush=True)
    with ThreadPoolExecutor(max_workers=args.jobs) as pool:
        futures = [
            pool.submit(run_one, args.bin, args.out, s, args.duration) for s in specs
        ]
        for i, f in enumerate(futures, 1):
            name, status = f.result()
            print(f"[{i}/{len(specs)}] {name}: {status}", flush=True)


def flatten(d):
    """Extract the metrics of one run summary."""
    meta = json.loads(d["label"]) if d["label"] else {}
    frames = d["frames"]
    lat = d["latency_ms"]
    row = {
        "mode": d["mode"],
        "cc": d["cc"],
        "seed": d["seed"],
        "loss": meta.get("loss", 0),
        "burst": meta.get("burst", 1),
        "outage": meta.get("outage", ""),
        "aborted": d["aborted"] or "",
        "frames": frames["generated"],
        "recv_ratio": frames["received"] / max(frames["generated"], 1),
        "lat_mean": lat["mean_received"],
        "lat_p50": lat["p50"],
        "lat_p90": lat["p90"],
        "lat_p95": lat["p95"],
        "lat_p99": lat["p99"],
        "lat_max": lat["max"],
        "goodput_mbps": d["goodput_mbps"],
        "wire_overhead": d["wire_overhead"],
    }
    for k, v in d["late_ratio"].items():
        row[f"late_{k}"] = v
    for k, v in d["rebuffer"].items():
        row[f"stall_ratio_{k}"] = v["stall_ratio"]
        row[f"stalls_{k}"] = v["stalls"]
    # A path that one endpoint never validated can stall the connection.
    states = [p["state"] for side in ("client_paths", "server_paths")
              for p in (d.get(side) or [])]
    row["paths_validated"] = int(all(s == "Validated" for s in states))
    lost = sum(p["lost_count"] for p in d["sender_paths"])
    sent = sum(p["sent_count"] for p in d["sender_paths"])
    row["sender_lost_pkts"] = lost
    row["sender_sent_pkts"] = sent
    row["sender_pto_final"] = max([p["pto_count"] for p in d["sender_paths"]] or [0])
    return row


def cmd_summarize(args):
    rows = []
    for matrix in sorted(os.listdir(args.out)):
        mdir = os.path.join(args.out, matrix)
        if not os.path.isdir(mdir):
            continue
        for fn in sorted(os.listdir(mdir)):
            if not fn.endswith(".json"):
                continue
            with open(os.path.join(mdir, fn)) as f:
                d = json.load(f)
            row = {"matrix": matrix, "name": fn[:-5]}
            row.update(flatten(d))
            rows.append(row)
    if not rows:
        print("no results", file=sys.stderr)
        return
    keys = list(rows[0].keys())
    for r in rows:
        for k in r:
            if k not in keys:
                keys.append(k)
    path = os.path.join(args.out, "summary.csv")
    with open(path, "w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=keys)
        w.writeheader()
        w.writerows(rows)
    print(f"wrote {len(rows)} rows to {path}")


def main():
    parser = argparse.ArgumentParser(description=__doc__,
                                     formatter_class=argparse.RawDescriptionHelpFormatter)
    sub = parser.add_subparsers(dest="cmd", required=True)

    p = sub.add_parser("run", help="run experiment matrices")
    p.add_argument("--matrix", nargs="+", default=["A", "B", "C", "D"])
    p.add_argument("--out", default="mpsim_results")
    p.add_argument("--bin", default=DEFAULT_BIN)
    p.add_argument("--jobs", type=int, default=6)
    p.add_argument("--duration", type=float, default=30.0)
    p.add_argument("--filter", help="only run the specs whose name matches this regex")
    p.set_defaults(func=cmd_run)

    p = sub.add_parser("summarize", help="aggregate results into summary.csv")
    p.add_argument("--out", default="mpsim_results")
    p.set_defaults(func=cmd_summarize)

    args = parser.parse_args()
    args.func(args)


if __name__ == "__main__":
    main()
