#!/usr/bin/env python3
import argparse
import concurrent.futures
import json
import math
import os
import random
import subprocess
import threading
import time
from pathlib import Path

import requests

API_URL = "http://127.0.0.1:8080"
METRICS_URL = f"{API_URL}/metrics"
REGIONS = ["us-east", "us-west", "eu-west", "eu-central", "ap-east"]
BENCHMARK_REGION_WEIGHTS = [0.55, 0.2, 0.15, 0.05, 0.05]
TOTAL_PLAYERS = 500
MAX_ENQUEUE_RETRIES = 5
METRICS_POLL_INTERVAL = 0.5

enqueue_times = {}
match_times = {}
lock = threading.Lock()


def read_match_logs(proc):
    for line in proc.stdout:
        line = line.strip()
        if not line:
            continue
        try:
            payload = json.loads(line)
        except json.JSONDecodeError:
            continue
        ids = payload.get("matched_ids")
        if not isinstance(ids, list):
            continue
        now = time.time()
        with lock:
            for pid in ids:
                if pid not in match_times:
                    match_times[pid] = now


def wait_for_server(timeout_s=15):
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        try:
            resp = requests.get(METRICS_URL, timeout=1)
            if resp.status_code == 200:
                return
        except requests.RequestException:
            pass
        time.sleep(0.2)
    raise RuntimeError("server did not start in time")


def pick_region(profile):
    if profile == "benchmark":
        return random.choices(REGIONS, weights=BENCHMARK_REGION_WEIGHTS, k=1)[0]
    return random.choice(REGIONS)


def enqueue_player(pid, profile):
    rating = random.gauss(1500, 300)
    rating = max(0.0, min(3000.0, rating))
    region = pick_region(profile)
    payload = {"id": pid, "skill_rating": rating, "ping_region": region}
    start = time.time()
    for attempt in range(MAX_ENQUEUE_RETRIES):
        try:
            resp = requests.post(f"{API_URL}/queue", json=payload, timeout=5)
            resp.raise_for_status()
            break
        except requests.RequestException:
            if attempt == MAX_ENQUEUE_RETRIES - 1:
                raise
            time.sleep(0.05)
    with lock:
        enqueue_times[pid] = start


def percentile(values, pct):
    if not values:
        return 0.0
    values = sorted(values)
    k = (len(values) - 1) * (pct / 100.0)
    f = math.floor(k)
    c = math.ceil(k)
    if f == c:
        return values[int(k)]
    return values[f] + (values[c] - values[f]) * (k - f)


def parse_args():
    parser = argparse.ArgumentParser(description="Matchmaker load simulation")
    parser.add_argument(
        "--profile",
        choices=["spec", "benchmark"],
        default="spec",
        help="Region distribution profile (default: spec)",
    )
    parser.add_argument(
        "--fast-cross-region",
        action="store_true",
        help="Enable immediate cross-region matching in the server",
    )
    return parser.parse_args()


def main(args):
    binary = Path("target/release/matchmaker")
    if not binary.exists():
        raise RuntimeError("binary not found; run cargo build --release first")

    env = dict(os.environ)
    if args.fast_cross_region:
        env["MATCHMAKER_FAST_CROSS_REGION"] = "1"

    proc = subprocess.Popen(
        [str(binary)],
        stdout=subprocess.PIPE,
        stderr=subprocess.DEVNULL,
        text=True,
        bufsize=1,
        env=env,
    )

    log_thread = threading.Thread(target=read_match_logs, args=(proc,), daemon=True)
    log_thread.start()

    wait_for_server()
    start_time = time.time()

    with concurrent.futures.ThreadPoolExecutor(max_workers=TOTAL_PLAYERS) as executor:
        futures = [
            executor.submit(enqueue_player, pid, args.profile)
            for pid in range(1, TOTAL_PLAYERS + 1)
        ]
        for future in concurrent.futures.as_completed(futures):
            future.result()

    metrics = {}
    while True:
        try:
            resp = requests.get(METRICS_URL, timeout=2)
            resp.raise_for_status()
            metrics = resp.json()
        except requests.RequestException:
            time.sleep(METRICS_POLL_INTERVAL)
            continue
        if metrics.get("total_players_queued", 0) >= TOTAL_PLAYERS and metrics.get(
            "queue_depth", 1
        ) == 0:
            break
        time.sleep(METRICS_POLL_INTERVAL)

    deadline = time.time() + 10
    while True:
        with lock:
            done = len(match_times) >= TOTAL_PLAYERS
        if done or time.time() > deadline:
            break
        time.sleep(0.1)

    end_time = time.time()
    elapsed = end_time - start_time

    with lock:
        waits_ms = []
        missing = 0
        for pid, start in enqueue_times.items():
            end = match_times.get(pid)
            if end is None:
                missing += 1
                continue
            waits_ms.append((end - start) * 1000.0)

    matches = metrics.get("total_matches_formed", 0)
    avg_wait_ms = sum(waits_ms) / len(waits_ms) if waits_ms else 0.0
    throughput = matches / elapsed if elapsed > 0 else 0.0

    p50 = percentile(waits_ms, 50)
    p95 = percentile(waits_ms, 95)
    p99 = percentile(waits_ms, 99)

    print("Final report")
    print(f"Total time elapsed: {elapsed:.2f} s")
    print(f"Matches formed: {matches}")
    print(f"Average wait time (ms): {avg_wait_ms:.0f}")
    print(f"Throughput (matches/sec): {throughput:.2f}")
    print(f"P50/P95/P99 wait times (ms): {p50:.0f} / {p95:.0f} / {p99:.0f}")
    if missing:
        print(f"Warning: {missing} players missing match timestamps")

    proc.terminate()
    try:
        proc.wait(timeout=5)
    except subprocess.TimeoutExpired:
        proc.kill()


if __name__ == "__main__":
    main(parse_args())
