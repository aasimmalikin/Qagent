"""Observability helpers (Concept 10).

Concurrent bugs are non-deterministic and miserable to reproduce, so we make
timing and failures visible by default:

* `setup_logging` — structured-ish logs with the coroutine/task name.
* `timed` — async context manager that logs how long a block took. Used to
  catch accidental serialization (a block you thought was parallel taking the
  sum of its parts).
* `guard_task` — attaches a done-callback to a task so that if a
  "fire-and-forget" task raises, the exception is logged instead of vanishing
  silently (the classic asyncio silent-failure trap).
"""

from __future__ import annotations

import asyncio
import logging
import time
from contextlib import asynccontextmanager

log = logging.getLogger("qagent")


def setup_logging(level: int = logging.INFO) -> None:
    logging.basicConfig(
        level=level,
        format="%(asctime)s.%(msecs)03d | %(levelname)-7s | %(name)s | %(message)s",
        datefmt="%H:%M:%S",
    )


@asynccontextmanager
async def timed(label: str):
    start = time.perf_counter()
    try:
        yield
    finally:
        elapsed_ms = (time.perf_counter() - start) * 1000
        log.debug("timing | %-28s %7.1f ms", label, elapsed_ms)


def guard_task(task: asyncio.Task, *, name: str) -> asyncio.Task:
    """Ensure a detached task's exception is logged, not swallowed."""

    def _done(t: asyncio.Task) -> None:
        if t.cancelled():
            return
        exc = t.exception()
        if exc is not None:
            log.error("task %r failed: %r", name, exc)

    task.add_done_callback(_done)
    return task
