"""Entrypoint.

Wires the lifecycle together and demonstrates the consumer side:

* Concept 9 — the LLM client is opened/closed via `async with`.
* Concept 5 — results are consumed with `async for` as they stream in.
* Concept 7 — `except*` handles an ExceptionGroup from the worker pool.
* Concept 6 — SIGINT sets a stop Event for graceful shutdown.
"""

from __future__ import annotations

import asyncio
import json
import logging
import signal
from pathlib import Path

from .config import settings
from .llm_client import LLMClient
from .models import AnswerResult, Confidence, Question
from .observability import log, setup_logging
from .pipeline import run_questionnaire

DATA = Path(__file__).parent / "data" / "questionnaire.json"
OUT = Path(__file__).parent.parent / "results.json"


def load_questions() -> list[Question]:
    return [Question(**row) for row in json.loads(DATA.read_text())]


def install_signal_handler(stop_event: asyncio.Event) -> None:
    loop = asyncio.get_running_loop()

    def _request_stop() -> None:
        log.warning("SIGINT received -> graceful shutdown")
        stop_event.set()

    with contextlib_suppress():
        loop.add_signal_handler(signal.SIGINT, _request_stop)


def contextlib_suppress():
    import contextlib
    # add_signal_handler is unsupported on some platforms (e.g. Windows).
    return contextlib.suppress(NotImplementedError)


def summarize(results: list[AnswerResult]) -> None:
    by_conf = {c: 0 for c in Confidence}
    review = errors = 0
    for r in results:
        by_conf[r.confidence] += 1
        review += r.needs_human_review
        errors += r.error is not None
    log.info("=" * 60)
    log.info("Processed %d questions", len(results))
    log.info("  high=%d  medium=%d  low=%d",
             by_conf[Confidence.HIGH], by_conf[Confidence.MEDIUM],
             by_conf[Confidence.LOW])
    log.info("  needs human review: %d", review)
    log.info("  hard errors:        %d", errors)
    log.info("=" * 60)


async def main() -> None:
    setup_logging(logging.DEBUG if settings.asyncio_debug else logging.INFO)
    if settings.asyncio_debug:
        asyncio.get_running_loop().set_debug(True)

    questions = load_questions()
    stop_event = asyncio.Event()
    install_signal_handler(stop_event)

    results: list[AnswerResult] = []

    # Concept 9: pooled client lifecycle bounds the whole run.
    async with LLMClient(settings) as llm:
        try:
            # Concept 5: consume the streamed results.
            async for result in run_questionnaire(questions, llm, settings, stop_event):
                tag = "OK " if result.error is None else "ERR"
                flag = "  [REVIEW]" if result.needs_human_review else ""
                log.info("%s %s | %-6s | %5.0fms%s | %s",
                         tag, result.question_id, result.confidence.value,
                         result.elapsed_ms or 0, flag, result.question_text)
                results.append(result)
        # Concept 7: a TaskGroup raises an ExceptionGroup; handle with except*.
        except* Exception as eg:
            for exc in eg.exceptions:
                log.error("pipeline-level failure: %r", exc)

    summarize(results)
    OUT.write_text(json.dumps([r.model_dump() for r in results], indent=2))
    log.info("wrote %s", OUT)


if __name__ == "__main__":
    asyncio.run(main())
