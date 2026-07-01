"""The orchestration pipeline — the centerpiece that ties everything together.

Shape: a bounded work queue feeds a pool of worker coroutines; workers run the
per-question agent and push results onto a results queue; the orchestrator
yields those results to the caller as they complete.

Concepts exercised here:
* Concept 4 — `asyncio.Queue` (bounded => backpressure) for producer/consumer
  handoff, plus an `asyncio.Event` for graceful shutdown.
* Concept 2 — a `TaskGroup` supervises the producer and worker pool with
  structured lifetimes (if one crashes, siblings are cancelled and cleaned up).
* Concept 5 — results are streamed out via an async generator (`async for`).
* Concept 6/9 — cancellation-safe cleanup if the consumer stops early.
"""

from __future__ import annotations

import asyncio
import contextlib
from collections.abc import AsyncIterator

from .agent import answer_question
from .config import Settings
from .llm_client import LLMClient
from .models import AnswerResult, Question
from .observability import guard_task, log

_DONE = object()  # sentinel pushed onto results_q when processing is complete


async def run_questionnaire(
    questions: list[Question],
    llm: LLMClient,
    settings: Settings,
    stop_event: asyncio.Event | None = None,
) -> AsyncIterator[AnswerResult]:
    work_q: asyncio.Queue = asyncio.Queue(maxsize=settings.queue_maxsize)
    results_q: asyncio.Queue = asyncio.Queue()
    stop_event = stop_event or asyncio.Event()

    async def producer() -> None:
        for q in questions:
            # Concept 4: graceful shutdown — stop enqueuing if asked to.
            if stop_event.is_set():
                log.info("producer | stop requested, halting enqueue")
                break
            # Bounded put => backpressure: pauses if workers fall behind.
            await work_q.put(q)
        # One poison pill per worker so each exits cleanly.
        for _ in range(settings.worker_count):
            await work_q.put(None)

    async def worker(wid: int) -> None:
        while True:
            item = await work_q.get()
            try:
                if item is None:  # poison pill
                    return
                result = await answer_question(item, llm, settings)
                await results_q.put(result)
            finally:
                work_q.task_done()

    async def supervise() -> None:
        # Concept 2: structured concurrency. If any child raises an unexpected
        # error, the group cancels the rest and surfaces an ExceptionGroup.
        try:
            async with asyncio.TaskGroup() as tg:
                tg.create_task(producer(), name="producer")
                for i in range(settings.worker_count):
                    tg.create_task(worker(i), name=f"worker-{i}")
        finally:
            # Always signal the consumer that no more results are coming,
            # even if the group failed — otherwise the `async for` hangs.
            await results_q.put(_DONE)

    supervisor = guard_task(
        asyncio.create_task(supervise(), name="supervisor"), name="supervisor"
    )

    try:
        # Concept 5: stream results to the caller as they arrive.
        while True:
            item = await results_q.get()
            if item is _DONE:
                break
            yield item
        # Surface any ExceptionGroup from the worker pool (e.g. a real bug,
        # not a per-question failure, which the agent already absorbs).
        await supervisor
    finally:
        # Concept 6/9: if the caller breaks early or is cancelled, tear the
        # supervisor down cleanly instead of leaking the worker pool.
        if not supervisor.done():
            supervisor.cancel()
            with contextlib.suppress(asyncio.CancelledError):
                await supervisor
