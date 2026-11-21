"""Batch scheduler: shared PPM staging quota, in-order dispatch, in-order tar packing.

Design invariants
-----------------
* Tiles of a batch are *dispatched* strictly in submission order. A later
  tile therefore never holds staging quota before the next tile that must
  enter the tar has started, so staged later tiles can never crowd out the
  space needed to start that next tile.
* A tile holds quota at its full PPM size from the moment it is dispatched
  (converting) until its staged PPM has been appended to the tar and
  deleted (packed). Only then is the quota released.
* Tar entries are appended strictly in submission order. A tile converted
  early stays staged until every preceding tile has been packed.
* At most ``max_active_batches`` batches pack concurrently; the rest queue.
* Any single tile whose PPM exceeds the quota limit, or any conversion
  failure, fails the whole batch: the tile name is reported, the batch's
  staged bytes and partial tar are cleaned up, and other batches continue.
* Converter processes are supervised. Each worker owns a private task queue,
  so the scheduler always knows which tile (if any) dies with a process.
  If a worker exits unexpectedly, only batches whose conversion results were
  lost with it fail — the lost tile is named. Results the worker handed back
  before exiting are processed normally, and a task it never picked up is
  reclaimed and re-dispatched rather than failed. A fresh worker is spawned
  to restore the configured pool size; surviving workers are kept. A failed
  batch's quota is handed back per tile only once that tile can no longer be
  written to staging and its staged bytes have been removed.
"""
from __future__ import annotations

import asyncio
import logging
import multiprocessing
import shutil
import tarfile
import time
import uuid
from collections import deque
from dataclasses import dataclass, field
from pathlib import Path

from .config import Settings
from .worker import run_worker

log = logging.getLogger("tiler")

# tile states
PENDING = "pending"        # not dispatched yet
CONVERTING = "converting"  # dispatched to a worker, holding quota
CONVERTED = "converted"    # PPM staged, waiting for its tar turn, holding quota
PACKED = "packed"          # in the tar, staging removed, quota released
ABORTED = "aborted"        # batch failed before/while this tile was in flight

# batch states
QUEUED = "queued"    # waiting for a free active slot
PACKING = "packing"  # active: converting / packing
DONE = "done"        # tar complete, downloadable
FAILED = "failed"    # aborted, staging cleaned

_WATCHDOG_INTERVAL_S = 0.2  # how often worker processes are checked for exits
_LOSS_GRACE_S = 1.0         # wait for an in-transit result before declaring a loss


@dataclass
class Tile:
    index: int
    name: str             # original (sanitized) upload name, used in error reports
    arcname: str          # entry name inside the tar
    png_path: Path
    ppm_path: Path
    ppm_size: int | None  # exact PPM size predicted from the PNG header
    state: str = PENDING
    reserved: int = 0     # staging quota bytes currently held


@dataclass
class Batch:
    id: str
    tiles: list[Tile]
    tar_path: Path
    created_at: float
    state: str = QUEUED
    next_to_start: int = 0
    next_to_pack: int = 0
    failed_tile: str | None = None
    error: str | None = None
    tar: tarfile.TarFile | None = None
    pack_lock: asyncio.Lock = field(default_factory=asyncio.Lock)

    @property
    def held_bytes(self) -> int:
        return sum(t.reserved for t in self.tiles)


@dataclass
class _Worker:
    """One supervised converter process with its own task queue.

    A dedicated queue per worker (instead of one shared queue) is what lets
    the scheduler know exactly which tile — if any — is lost when the
    process exits unexpectedly. ``started`` is set once the worker signals
    it picked the task up; without that signal a dead worker never touched
    the task, so it can simply be re-dispatched instead of failing a batch.
    """

    wid: int
    proc: multiprocessing.Process
    task_q: multiprocessing.Queue
    current: tuple[str, int] | None = None  # (batch_id, tile index) in flight
    started: bool = False                   # worker acked picking current up


class Scheduler:
    """Owns all mutable scheduling state. Every mutating method runs on the
    API process's event loop, so no extra locking is needed beyond the
    per-batch pack lock that serializes tar appends."""

    def __init__(self, settings: Settings):
        self.settings = settings
        self.uploads_dir = settings.data_dir / "uploads"
        self.stage_dir = settings.data_dir / "stage"
        self.out_dir = settings.data_dir / "out"
        self.batches: dict[str, Batch] = {}
        self.active: list[Batch] = []        # batches currently packing (FIFO)
        self.waiting: deque[Batch] = deque()  # submitted but not yet active
        self.used = 0                         # staging bytes currently reserved
        self.inflight = 0                     # conversions dispatched, not yet resolved
        self._mp_ctx: multiprocessing.context.BaseContext | None = None
        self._result_q: multiprocessing.Queue | None = None
        self._workers: list[_Worker] = []             # alive, dispatchable
        self._workers_by_id: dict[int, _Worker] = {}  # incl. dead (late results)
        self._next_wid = 0
        self._reader_task: asyncio.Task | None = None
        self._watchdog_task: asyncio.Task | None = None

    # ------------------------------------------------------------------
    # lifecycle
    # ------------------------------------------------------------------
    def start(self) -> None:
        # In-memory state is authoritative; drop leftovers from previous runs
        # so a restart never resurrects half-packed staging or stale tars.
        for d in (self.uploads_dir, self.stage_dir, self.out_dir):
            d.mkdir(parents=True, exist_ok=True)
            for child in d.iterdir():
                if child.is_dir():
                    shutil.rmtree(child, ignore_errors=True)
                else:
                    child.unlink(missing_ok=True)
        try:
            ctx = multiprocessing.get_context("fork")
        except ValueError:  # pragma: no cover - non-POSIX fallback
            ctx = multiprocessing.get_context("spawn")
        self._mp_ctx = ctx
        self._result_q = ctx.Queue()
        for _ in range(self.settings.workers):
            self._spawn_worker()
        self._reader_task = asyncio.create_task(self._result_reader())
        self._watchdog_task = asyncio.create_task(self._worker_watchdog())
        log.info(
            "scheduler up: workers=%d staging_limit=%d bytes max_active_batches=%d data_dir=%s",
            self.settings.workers, self.settings.ppm_staging_bytes,
            self.settings.max_active_batches, self.settings.data_dir,
        )

    def _spawn_worker(self) -> _Worker:
        """Start one converter process and register it. Used both at startup
        and to replenish the pool after an unexpected exit."""
        wid = self._next_wid
        self._next_wid += 1
        task_q = self._mp_ctx.Queue()
        proc = self._mp_ctx.Process(
            target=run_worker,
            args=(task_q, self._result_q, wid),
            name=f"ppm-worker-{wid}",
            daemon=True,
        )
        proc.start()
        worker = _Worker(wid=wid, proc=proc, task_q=task_q)
        self._workers.append(worker)
        self._workers_by_id[wid] = worker
        log.info(
            "worker=%d pid=%d STARTED alive=%d/%d",
            wid, proc.pid, len(self._workers), self.settings.workers,
        )
        return worker

    async def stop(self) -> None:
        for task in (self._watchdog_task, self._reader_task):
            if task:
                task.cancel()
        for w in self._workers:
            try:
                w.task_q.put(None)
            except Exception:  # noqa: BLE001 - shutdown best effort
                pass
        for w in self._workers:
            w.proc.join(timeout=3)
            if w.proc.is_alive():
                w.proc.terminate()
        self._workers.clear()

    # ------------------------------------------------------------------
    # intake
    # ------------------------------------------------------------------
    def new_batch(self, tiles: list[Tile], batch_id: str | None = None) -> Batch:
        batch = Batch(
            id=batch_id or uuid.uuid4().hex[:12],
            tiles=tiles,
            tar_path=self.out_dir / "pending.tar",  # replaced below
            created_at=time.time(),
        )
        batch.tar_path = self.out_dir / f"{batch.id}.tar"
        self.batches[batch.id] = batch
        self.waiting.append(batch)
        self._activate()
        self._try_dispatch()
        return batch

    def _activate(self) -> None:
        while self.waiting and len(self.active) < self.settings.max_active_batches:
            batch = self.waiting.popleft()
            batch.state = PACKING
            self.active.append(batch)
            log.info(
                "batch=%s ACTIVATED tiles=%d active=%d queued=%d",
                batch.id, len(batch.tiles), len(self.active), len(self.waiting),
            )

    # ------------------------------------------------------------------
    # dispatch (quota gate)
    # ------------------------------------------------------------------
    def _try_dispatch(self) -> None:
        """Start conversions while workers are idle and quota allows.

        Batches are scanned oldest-first; each batch offers its next
        unstarted tile (strict in-order start per batch). A tile starts only
        if its full PPM size fits in the remaining shared quota. Dispatch is
        gated on idle workers rather than a counter, so a worker that died
        mid-task can never wedge the pipeline.
        """
        while True:
            idle = [w for w in self._workers if w.current is None]
            if not idle:
                return
            progressed = False
            for batch in list(self.active):
                if not idle:
                    break
                if batch.next_to_start >= len(batch.tiles):
                    continue
                tile = batch.tiles[batch.next_to_start]
                if tile.ppm_size is None:
                    self._fail_batch(batch, tile, "invalid PNG: cannot parse signature/IHDR")
                    progressed = True
                    break  # active list changed; rescan from the oldest batch
                if tile.ppm_size > self.settings.ppm_staging_bytes:
                    self._fail_batch(
                        batch, tile,
                        f"single tile PPM is {tile.ppm_size} bytes, exceeds the "
                        f"{self.settings.ppm_staging_bytes} byte staging limit",
                    )
                    progressed = True
                    break
                if self.used + tile.ppm_size > self.settings.ppm_staging_bytes:
                    continue  # quota blocked; give other batches a chance
                self._dispatch(batch, tile, idle.pop())
                progressed = True
            if not progressed:
                return

    def _dispatch(self, batch: Batch, tile: Tile, worker: _Worker) -> None:
        tile.state = CONVERTING
        tile.reserved = tile.ppm_size or 0
        batch.next_to_start += 1
        self.used += tile.reserved
        self._assign(worker, batch, tile)
        log.info(
            "batch=%s tile=%d name=%s DISPATCH worker=%d reserve=%d used=%d/%d inflight=%d",
            batch.id, tile.index, tile.name, worker.wid, tile.reserved,
            self.used, self.settings.ppm_staging_bytes, self.inflight,
        )

    def _assign(self, worker: _Worker, batch: Batch, tile: Tile) -> None:
        """Hand one tile to one specific worker (its quota is already reserved)."""
        worker.current = (batch.id, tile.index)
        worker.started = False
        self.inflight += 1
        worker.task_q.put((batch.id, tile.index, tile.name,
                           str(tile.png_path), str(tile.ppm_path)))

    # ------------------------------------------------------------------
    # worker results
    # ------------------------------------------------------------------
    async def _result_reader(self) -> None:
        loop = asyncio.get_running_loop()
        while True:
            msg = await loop.run_in_executor(None, self._result_q.get)
            try:
                if msg[0] == "started":
                    self._on_started(msg)
                else:
                    await self._on_result(msg)
            except Exception:  # pragma: no cover - defensive
                log.exception("failed to handle worker message %r", msg)

    def _on_started(self, msg) -> None:
        """A worker picked up its assigned task: from here on, its death
        means the task's conversion result is lost."""
        _, worker_id, batch_id, index = msg
        worker = self._workers_by_id.get(worker_id)
        if worker is not None and worker.current == (batch_id, index):
            worker.started = True

    async def _on_result(self, result) -> None:
        _, worker_id, batch_id, index, name, size, error, duration = result
        worker = self._workers_by_id.get(worker_id)
        if worker is not None and worker.current == (batch_id, index):
            worker.current = None
            self.inflight -= 1
        else:
            # The watchdog already resolved this task as lost; the result is
            # late. It is still cleaned up below, but must not be counted twice.
            log.warning(
                "batch=%s tile=%d name=%s result from worker=%d has no live assignment",
                batch_id, index, name, worker_id,
            )
        batch = self.batches.get(batch_id)
        if batch is None or batch.state != PACKING:
            # Batch already failed while this tile was in flight: drop any
            # file the worker just wrote. The worker has finished writing
            # (that is what a result means), so the tile's quota can go back
            # now if it was still reserved (worker-exit failures defer it).
            if batch is not None:
                tile = batch.tiles[index]
                try:
                    tile.ppm_path.unlink(missing_ok=True)
                except OSError:
                    pass
                if tile.reserved:
                    self.used -= tile.reserved
                    tile.reserved = 0
            log.warning(
                "batch=%s tile=%d name=%s result DISCARDED (batch state=%s)",
                batch_id, index, name, batch.state if batch else "unknown",
            )
        elif error is not None:
            self._fail_batch(batch, batch.tiles[index], f"conversion failed: {error}")
        else:
            tile = batch.tiles[index]
            tile.state = CONVERTED
            if size != tile.reserved:  # keep accounting exact if size differed
                self.used += size - tile.reserved
                tile.reserved = size
            log.info(
                "batch=%s tile=%d name=%s CONVERTED size=%d duration=%.0fms used=%d/%d",
                batch.id, index, name, size, duration * 1000,
                self.used, self.settings.ppm_staging_bytes,
            )
            await self._pack_ready(batch)
        self._try_dispatch()

    # ------------------------------------------------------------------
    # packing (strict tar order)
    # ------------------------------------------------------------------
    async def _pack_ready(self, batch: Batch) -> None:
        async with batch.pack_lock:
            while batch.state == PACKING and batch.next_to_pack < len(batch.tiles):
                tile = batch.tiles[batch.next_to_pack]
                if tile.state != CONVERTED:
                    break  # earlier tile not ready: later ones keep waiting
                await asyncio.to_thread(self._append_and_clear, batch, tile)
                self.used -= tile.reserved
                tile.reserved = 0
                tile.state = PACKED
                batch.next_to_pack += 1
                log.info(
                    "batch=%s tile=%d name=%s PACKED as=%s used=%d/%d",
                    batch.id, tile.index, tile.name, tile.arcname,
                    self.used, self.settings.ppm_staging_bytes,
                )
            if batch.state == PACKING and batch.next_to_pack == len(batch.tiles):
                await asyncio.to_thread(self._close_tar, batch)
                batch.state = DONE
                self.active.remove(batch)
                shutil.rmtree(self.stage_dir / batch.id, ignore_errors=True)
                log.info("batch=%s DONE tar=%s", batch.id, batch.tar_path)
                self._activate()

    @staticmethod
    def _append_and_clear(batch: Batch, tile: Tile) -> None:
        if batch.tar is None:
            batch.tar_path.parent.mkdir(parents=True, exist_ok=True)
            batch.tar = tarfile.open(batch.tar_path, "w")
        batch.tar.add(tile.ppm_path, arcname=tile.arcname)
        tile.ppm_path.unlink()  # staging cleared; quota is released by the caller

    @staticmethod
    def _close_tar(batch: Batch) -> None:
        if batch.tar is not None:
            batch.tar.close()
            batch.tar = None

    # ------------------------------------------------------------------
    # worker supervision
    # ------------------------------------------------------------------
    async def _worker_watchdog(self) -> None:
        """Reap unexpectedly exited workers and repair the schedule."""
        while True:
            await asyncio.sleep(_WATCHDOG_INTERVAL_S)
            for worker in list(self._workers):
                if not worker.proc.is_alive():
                    await self._on_worker_death(worker)

    async def _on_worker_death(self, worker: _Worker) -> None:
        if worker not in self._workers:
            return  # already handled
        self._workers.remove(worker)
        worker.proc.join()  # reap: the process can no longer write to staging
        exitcode = worker.proc.exitcode
        lost = worker.current
        log.error(
            "worker=%d pid=%d EXITED unexpectedly exitcode=%s in_flight=%s",
            worker.wid, worker.proc.pid, exitcode, lost,
        )
        # Restore the configured worker count first; surviving workers are
        # kept as they are. Capacity is back even while the lost tile (if
        # any) is still being resolved below.
        self._spawn_worker()
        self._try_dispatch()
        if lost is None:
            return  # the worker was idle: nothing was lost, nobody fails
        batch_id, index = lost
        # Messages the worker sent just before exiting (its pickup ack or its
        # result) may still be in transit; give the reader a grace window to
        # deliver them — a result that arrives is processed normally.
        deadline = asyncio.get_running_loop().time() + _LOSS_GRACE_S
        while worker.current is not None:
            if asyncio.get_running_loop().time() >= deadline:
                break
            await asyncio.sleep(0.02)
        try:
            worker.task_q.close()
        except Exception:  # noqa: BLE001 - best effort
            pass
        if worker.current is None:
            log.info(
                "worker=%d result for batch=%s tile=%d arrived before loss was declared",
                worker.wid, batch_id, index,
            )
            self._try_dispatch()
            return
        started = worker.started
        worker.current = None
        self.inflight -= 1  # the dead worker's assignment is resolved here
        batch = self.batches.get(batch_id)
        tile = batch.tiles[index] if batch is not None and index < len(batch.tiles) else None
        if (not started and batch is not None and tile is not None
                and batch.state == PACKING):
            # The dead worker never picked this task up: hand it to a live
            # worker. The tile's quota stayed reserved the whole time.
            idle = next((w for w in self._workers if w.current is None), None)
            if idle is not None:
                self._assign(idle, batch, tile)  # re-counts the moved task
                log.warning(
                    "batch=%s tile=%d name=%s REDISPATCHED to worker=%d: "
                    "worker=%d died before starting it",
                    batch.id, index, tile.name, idle.wid, worker.wid,
                )
                self._try_dispatch()
                return
            log.error("batch=%s tile=%d no live worker for redispatch", batch.id, index)
        # The process has been reaped, so the tile can no longer be written
        # to staging: remove the partial PPM remnant first, and only then
        # hand the tile's quota back.
        if tile is not None:
            try:
                tile.ppm_path.unlink(missing_ok=True)
            except OSError:
                pass
            if tile.reserved:
                self.used -= tile.reserved
                log.error(
                    "batch=%s tile=%d name=%s LOST remnant removed, released=%d used=%d/%d",
                    batch_id, index, tile.name, tile.reserved,
                    self.used, self.settings.ppm_staging_bytes,
                )
                tile.reserved = 0
        if batch is not None and tile is not None and batch.state == PACKING:
            self._fail_batch(
                batch, tile,
                f"conversion worker exited unexpectedly (exitcode={exitcode}); "
                "the tile's conversion result was lost",
                deferred_release=True,
            )
        self._try_dispatch()

    # ------------------------------------------------------------------
    # failure handling
    # ------------------------------------------------------------------
    def _fail_batch(self, batch: Batch, tile: Tile, error: str, *,
                    deferred_release: bool = False) -> None:
        batch.state = FAILED
        batch.failed_tile = tile.name
        batch.error = error
        released = 0
        for t in batch.tiles:
            if t.state in (PENDING, CONVERTING, CONVERTED):
                t.state = ABORTED
            if t.reserved and not deferred_release:
                released += t.reserved
                self.used -= t.reserved
                t.reserved = 0
            # With deferred_release (worker-exit failures) each tile's quota
            # goes back only once the tile can no longer be written to
            # staging and its staged bytes are gone: staged tiles are
            # released by _cleanup_failed after unlinking, in-flight tiles
            # when their late result is discarded or their worker's own
            # exit is handled.
        if batch in self.active:
            self.active.remove(batch)
        else:
            try:
                self.waiting.remove(batch)
            except ValueError:
                pass
        log.error(
            "batch=%s FAILED tile=%s error=%r released=%d deferred=%s used=%d/%d",
            batch.id, tile.name, error, released, deferred_release,
            self.used, self.settings.ppm_staging_bytes,
        )
        # File cleanup runs under the pack lock: a tar append for this batch
        # may be in flight in a worker thread right now.
        asyncio.get_running_loop().create_task(self._cleanup_failed(batch))
        self._activate()

    async def _cleanup_failed(self, batch: Batch) -> None:
        """Remove the failed batch's staged PPMs and partial tar.

        Tiles still being converted by a live worker are left alone: their
        file and quota are released when the late result is discarded (or
        when that worker's own exit is handled).
        """
        try:
            async with batch.pack_lock:
                if batch.tar is not None:
                    batch.tar.close()
                    batch.tar = None
                batch.tar_path.unlink(missing_ok=True)
                released = 0
                for t in batch.tiles:
                    if t.reserved and self._in_flight(batch.id, t.index):
                        continue  # a live worker may still be writing this file
                    try:
                        t.ppm_path.unlink(missing_ok=True)
                    except OSError:
                        pass
                    if t.reserved:
                        self.used -= t.reserved
                        released += t.reserved
                        t.reserved = 0
                shutil.rmtree(self.stage_dir / batch.id, ignore_errors=True)
                if released:
                    log.info(
                        "batch=%s CLEANUP released=%d used=%d/%d",
                        batch.id, released, self.used, self.settings.ppm_staging_bytes,
                    )
        except Exception:  # pragma: no cover - defensive
            log.exception("batch=%s cleanup failed", batch.id)
        self._try_dispatch()  # freed quota may unblock waiting tiles

    def _in_flight(self, batch_id: str, index: int) -> bool:
        return any(w.current == (batch_id, index) for w in self._workers)

    # ------------------------------------------------------------------
    # introspection for the API
    # ------------------------------------------------------------------
    def status(self) -> dict:
        return {
            "ppm_staging_limit_bytes": self.settings.ppm_staging_bytes,
            "ppm_staging_used_bytes": self.used,
            "workers": self.settings.workers,
            "workers_alive": len(self._workers),
            "worker_processes": [
                {
                    "worker_id": w.wid,
                    "pid": w.proc.pid,
                    "busy": w.current is not None,
                    "current": (
                        {"batch_id": w.current[0], "tile_index": w.current[1]}
                        if w.current is not None else None
                    ),
                }
                for w in self._workers
            ],
            "conversions_in_flight": self.inflight,
            "active_batches": [b.id for b in self.active],
            "queued_batches": [b.id for b in self.waiting],
        }

    def batch_info(self, batch: Batch) -> dict:
        return {
            "batch_id": batch.id,
            "state": batch.state,
            "tiles_total": len(batch.tiles),
            "tiles_started": batch.next_to_start,
            "tiles_packed": batch.next_to_pack,
            "staging_bytes_held": batch.held_bytes,
            "failed_tile": batch.failed_tile,
            "error": batch.error,
            "download_url": f"/batches/{batch.id}/download" if batch.state == DONE else None,
            "tiles": [
                {
                    "index": t.index,
                    "name": t.name,
                    "state": t.state,
                    "ppm_size": t.ppm_size,
                    "arcname": t.arcname,
                }
                for t in batch.tiles
            ],
        }
