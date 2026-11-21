#!/usr/bin/env python3
"""Worker-exit resilience test against a running Offline Tile Packer.

Usage: worker_exit_test.py BASE_URL TILES_DIR [DATA_DIR]

SIGKILLs the worker process that is mid-conversion of one batch's tile and
verifies the scheduler's post-exit behaviour:
  * only the batch that lost a conversion result fails, naming the tile;
  * the surviving worker is kept (same pid) and a replacement worker
    restores the configured process count;
  * the other active batch — previously stalled behind the dead worker's
    held staging quota — keeps packing in submission order and its tar
    becomes downloadable; a queued batch takes the freed slot and completes;
  * staging quota is fully handed back (used returns to 0, no remnants).
"""
from __future__ import annotations

import io
import os
import signal
import sys
import tarfile
import time
from pathlib import Path

import httpx

FAILURES: list[str] = []


def check(cond: bool, label: str, detail: str = "") -> None:
    mark = "PASS" if cond else "FAIL"
    print(f"    [{mark}] {label}" + (f" -- {detail}" if detail else ""))
    if not cond:
        FAILURES.append(label)


def submit(client: httpx.Client, tiles_dir: Path, names: list[str]) -> dict:
    handles = [open(tiles_dir / n, "rb") for n in names]
    try:
        files = [("files", (n, h, "image/png")) for n, h in zip(names, handles)]
        r = client.post("/batches", files=files)
        r.raise_for_status()
        return r.json()
    finally:
        for h in handles:
            h.close()


def wait_terminal(client: httpx.Client, batch_id: str, timeout: float = 60) -> dict:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        info = client.get(f"/batches/{batch_id}").json()
        if info["state"] in ("done", "failed"):
            return info
        time.sleep(0.05)
    raise TimeoutError(f"batch {batch_id} did not finish in {timeout}s")


def tar_names(content: bytes) -> list[str]:
    with tarfile.open(fileobj=io.BytesIO(content)) as tf:
        return tf.getnames()


def test_mid_conversion_exit(client: httpx.Client, tiles_dir: Path,
                             data_dir: Path | None) -> None:
    print("TEST 1: worker SIGKILLed mid-conversion")
    status = client.get("/status").json()
    print(f"  service: workers={status['workers']} alive={status['workers_alive']} "
          f"limit={status['ppm_staging_limit_bytes']:,} B")
    check(status["workers_alive"] == status["workers"],
          "all configured workers alive at start")
    initial_pids = {w["pid"] for w in status["worker_processes"]}

    # A: victim batch. Its second tile (big) converts slowly (~200ms), giving
    #    a wide window to kill its worker mid-conversion. big's 15.6MB plus
    #    small1's 3.1MB nearly fill the 20MiB quota, so B cannot even start
    #    until A's held quota comes back — the exact stall this fix addresses.
    # B: concurrent active batch that must ride through and complete.
    # C: queued behind the two active slots; must take A's freed slot.
    a = submit(client, tiles_dir, ["small1.png", "big.png", "small2.png"])
    b = submit(client, tiles_dir, ["small3.png", "medium.png", "small4.png"])
    c = submit(client, tiles_dir, ["small1.png", "small2.png"])
    aid, bid, cid = a["batch_id"], b["batch_id"], c["batch_id"]
    print(f"  batches: A={aid} (victim)  B={bid}  C={cid} (queued)")

    # Wait until A's tile #1 (big.png) is converting on a specific worker,
    # then SIGKILL that worker mid-conversion.
    victim = None
    st = None
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        st = client.get("/status").json()
        for w in st["worker_processes"]:
            cur = w["current"]
            if cur and cur["batch_id"] == aid and cur["tile_index"] == 1:
                victim = w
                break
        if victim:
            break
        time.sleep(0.01)
    check(victim is not None, "located the worker converting A's big tile")
    if victim is None:
        sys.exit(1)
    survivor_pids = {w["pid"] for w in st["worker_processes"]} - {victim["pid"]}
    b_at_kill = client.get(f"/batches/{bid}").json()
    print(f"  SIGKILL worker pid={victim['pid']} (A tile#1 big.png); "
          f"survivor pids={sorted(survivor_pids)}; "
          f"B at kill: started={b_at_kill['tiles_started']} "
          f"packed={b_at_kill['tiles_packed']} held={b_at_kill['staging_bytes_held']:,}")
    os.kill(victim["pid"], signal.SIGKILL)

    fa = wait_terminal(client, aid)
    check(fa["state"] == "failed", "batch that lost a result fails",
          f"state={fa['state']}")
    check(fa["failed_tile"] == "big.png", "failure names the lost tile",
          f"failed_tile={fa['failed_tile']!r}")
    check("worker" in (fa["error"] or "") and "exited" in (fa["error"] or ""),
          "error reports the worker exit", fa["error"])

    fb = wait_terminal(client, bid)
    fc = wait_terminal(client, cid)
    check(fb["state"] == "done", "other active batch completes", f"state={fb['state']}")
    check(fc["state"] == "done", "queued batch activated and completes",
          f"state={fc['state']}")

    r = client.get(f"/batches/{aid}/download")
    check(r.status_code == 409 and "big.png" in r.text,
          "failed batch download refused, naming the tile", f"HTTP {r.status_code}")

    r = client.get(f"/batches/{bid}/download")
    want_b = ["000000_small3.ppm", "000001_medium.ppm", "000002_small4.ppm"]
    got_b = tar_names(r.content) if r.status_code == 200 else None
    check(r.status_code == 200 and got_b == want_b,
          "other batch tar downloadable and ordered", f"{got_b}")

    r = client.get(f"/batches/{cid}/download")
    want_c = ["000000_small1.ppm", "000001_small2.ppm"]
    got_c = tar_names(r.content) if r.status_code == 200 else None
    check(r.status_code == 200 and got_c == want_c,
          "queued batch tar downloadable and ordered", f"{got_c}")

    # Worker pool: the survivor must be untouched, the dead one replaced.
    st = client.get("/status").json()
    now_pids = {w["pid"] for w in st["worker_processes"]}
    check(st["workers_alive"] == st["workers"] == len(initial_pids),
          "worker count replenished to the configured value",
          f"alive={st['workers_alive']}/{st['workers']}")
    check(survivor_pids <= now_pids, "surviving worker kept (not restarted)",
          f"survivor={sorted(survivor_pids)} now={sorted(now_pids)}")
    check(victim["pid"] not in now_pids, "dead worker is gone from the pool")
    check(len(now_pids - initial_pids) == 1, "exactly one replacement spawned",
          f"new={sorted(now_pids - initial_pids)}")

    # Quota: everything the failed batch held must come back once no tile of
    # it can still be written and remnants are cleaned.
    deadline = time.monotonic() + 10
    while time.monotonic() < deadline:
        st = client.get("/status").json()
        fa2 = client.get(f"/batches/{aid}").json()
        if st["ppm_staging_used_bytes"] == 0 and fa2["staging_bytes_held"] == 0:
            break
        time.sleep(0.05)
    check(st["ppm_staging_used_bytes"] == 0, "staging quota fully released",
          f"used={st['ppm_staging_used_bytes']:,}")
    check(fa2["staging_bytes_held"] == 0, "failed batch holds no quota",
          f"held={fa2['staging_bytes_held']:,}")

    if data_dir is not None:
        stage_dir = data_dir / "stage"
        leftovers = list(stage_dir.rglob("*.ppm")) if stage_dir.exists() else []
        check(not leftovers, "no staged PPM remnants left", f"{leftovers}")
        check(not (data_dir / "out" / f"{aid}.tar").exists(),
              "failed batch left no partial tar")
        check((data_dir / "out" / f"{bid}.tar").exists()
              and (data_dir / "out" / f"{cid}.tar").exists(),
              "finished tars present on disk")


def test_reclaim_unstarted_task(client: httpx.Client, tiles_dir: Path) -> None:
    """A worker killed *before picking up* its task lost nothing: the task
    must be reclaimed and re-dispatched, and the batch must NOT fail."""
    print("\nTEST 2: worker SIGKILLed before starting its task (reclaim path)")
    st = client.get("/status").json()
    # Freeze one worker so it cannot pick up the task it is about to be given.
    frozen = st["worker_processes"][0]
    os.kill(frozen["pid"], signal.SIGSTOP)
    try:
        d = submit(client, tiles_dir, ["small1.png", "small2.png"])
        did = d["batch_id"]
        # Wait until the frozen worker owns a queued-but-unstarted tile.
        deadline = time.monotonic() + 10
        owned = None
        while time.monotonic() < deadline:
            st = client.get("/status").json()
            for w in st["worker_processes"]:
                cur = w["current"]
                if cur and cur["batch_id"] == did and w["pid"] == frozen["pid"]:
                    owned = cur
                    break
            if owned:
                break
            time.sleep(0.01)
        check(owned is not None, "frozen worker holds a queued task",
              f"tile={owned}")
        os.kill(frozen["pid"], signal.SIGKILL)  # never started the task
        fd = wait_terminal(client, did)
        check(fd["state"] == "done",
              "batch completes: unstarted task reclaimed and re-dispatched",
              f"state={fd['state']} failed_tile={fd['failed_tile']!r}")
        r = client.get(f"/batches/{did}/download")
        want = ["000000_small1.ppm", "000001_small2.ppm"]
        got = tar_names(r.content) if r.status_code == 200 else None
        check(r.status_code == 200 and got == want,
              "reclaimed batch tar downloadable and ordered", f"{got}")
        st = client.get("/status").json()
        check(st["workers_alive"] == st["workers"],
              "worker count replenished after the kill",
              f"alive={st['workers_alive']}/{st['workers']}")
        check(st["conversions_in_flight"] == 0,
              "no in-flight conversion leaked",
              f"inflight={st['conversions_in_flight']}")
    finally:
        try:
            os.kill(frozen["pid"], signal.SIGKILL)  # ensure no STOPPED corpse
        except ProcessLookupError:
            pass


def main() -> None:
    base_url, tiles_dir = sys.argv[1], Path(sys.argv[2])
    data_dir = Path(sys.argv[3]) if len(sys.argv) > 3 else None
    client = httpx.Client(base_url=base_url, timeout=30)

    test_mid_conversion_exit(client, tiles_dir, data_dir)
    test_reclaim_unstarted_task(client, tiles_dir)

    print()
    if FAILURES:
        print(f"{len(FAILURES)} CHECK(S) FAILED:")
        for f in FAILURES:
            print(f"  - {f}")
        sys.exit(1)
    print("ALL WORKER-EXIT CHECKS PASSED")


if __name__ == "__main__":
    main()
