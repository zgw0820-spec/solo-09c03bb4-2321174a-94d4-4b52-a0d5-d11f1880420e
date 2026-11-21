"""Converter worker process.

Runs as a separate local process (multiprocessing). Receives conversion
tasks on its own private queue, decodes PNG with Pillow, writes a binary
P6 PPM, and reports back on the shared message queue. Kept side-effect
free at import time so it is safe to use with both fork and spawn start
methods.

Message protocol on the shared queue (first element is the tag):
  ("started", worker_id, batch_id, index)
      sent the moment a task is picked up — lets the scheduler tell a
      task the worker died *with* apart from one it never started;
  ("result", worker_id, batch_id, index, name, ppm_size, error, duration_s)
      sent once per finished/failed conversion.
"""
from __future__ import annotations

import os
import time

from .pngutil import ppm_header


def convert_png_to_ppm(png_path: str, ppm_path: str) -> int:
    """Decode PNG at ``png_path`` and write an 8-bit RGB P6 PPM.

    Returns the exact PPM file size. Raises on any decode failure.
    """
    from PIL import Image  # imported lazily inside the worker process

    with Image.open(png_path) as im:
        rgb = im.convert("RGB")
        width, height = rgb.size
        data = rgb.tobytes()
    with open(ppm_path, "wb") as f:
        f.write(ppm_header(width, height))
        f.write(data)
    return os.path.getsize(ppm_path)


def run_worker(task_q, result_q, worker_id: int) -> None:
    """Worker loop: pull (batch_id, index, name, png, ppm) tasks forever.

    A None task is the shutdown sentinel.
    """
    while True:
        task = task_q.get()
        if task is None:
            return
        batch_id, index, name, png_path, ppm_path = task
        result_q.put(("started", worker_id, batch_id, index))
        started = time.monotonic()
        try:
            size = convert_png_to_ppm(png_path, ppm_path)
            result_q.put(("result", worker_id, batch_id, index, name, size,
                          None, time.monotonic() - started))
        except Exception as exc:  # noqa: BLE001 - report any conversion failure
            try:
                os.unlink(ppm_path)
            except OSError:
                pass
            result_q.put(
                ("result", worker_id, batch_id, index, name, 0,
                 f"{type(exc).__name__}: {exc}", time.monotonic() - started)
            )
