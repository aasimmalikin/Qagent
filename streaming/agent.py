"""The per-question agent.

This is the unit of work the pipeline runs concurrently. For one question it:

  1. retrieves grounding docs (tool call),
  2. drafts an answer with the LLM, consuming the token stream (Concept 5),
  3. parses a confidence signal,
  4. if low-confidence, runs ONE extra retrieval round with expanded queries
     and re-drafts — a small agentic loop,
  5. returns an AnswerResult.

Crucially it handles errors so that one bad question never sinks the batch
(Concept 7): any unexpected exception becomes a LOW-confidence result flagged
for human review, while `CancelledError` is re-raised untouched (Concept 6).
"""

from __future__ import annotations

import asyncio
import time

from .config import Settings
from .llm_client import LLMClient
from .models import AnswerResult, Confidence, Question, RetrievedDoc
from .observability import log, timed
from .retriever import multi_retrieve, retrieve

_CONTEXT_LINE = "- [relevance={score:.2f}] ({doc_id}) {text}"


def build_prompt(question: Question, docs: list[RetrievedDoc]) -> str:
    context = "\n".join(
        _CONTEXT_LINE.format(score=d.score, doc_id=d.doc_id, text=d.text)
        for d in docs
    ) or "(no relevant context found)"
    return (
        "You answer vendor security questionnaires using ONLY the context "
        "below. Cite the policy ids you used. If the context is insufficient, "
        "say so and mark confidence low.\n\n"
        f"Question {question.id}: {question.text}\n\n"
        f"Retrieved context (with relevance scores):\n{context}\n"
    )


def parse_answer(raw: str) -> tuple[str, Confidence, list[str]]:
    answer, conf, cites = "", Confidence.LOW, []
    if "ANSWER:" in raw:
        head, answer = raw.split("ANSWER:", 1)
        answer = answer.strip()
    else:
        head = raw
    for line in head.splitlines():
        low = line.lower()
        if low.startswith("confidence:"):
            val = line.split(":", 1)[1].strip().lower()
            conf = {"high": Confidence.HIGH, "medium": Confidence.MEDIUM}.get(
                val, Confidence.LOW
            )
        elif low.startswith("citations:"):
            cites = [c.strip() for c in line.split(":", 1)[1].split(",") if c.strip()]
    return answer, conf, cites


async def _draft(
    question: Question, docs: list[RetrievedDoc], llm: LLMClient, settings: Settings
) -> str:
    prompt = build_prompt(question, docs)
    # Concept 5: consume the model output as a token stream. We accumulate
    # here, but the same async generator could forward tokens to a UI live.
    chunks: list[str] = []
    async for chunk in llm.stream(
        prompt, request_id=question.id, timeout=settings.llm_timeout_s
    ):
        chunks.append(chunk)
    return "".join(chunks)


async def answer_question(
    question: Question, llm: LLMClient, settings: Settings
) -> AnswerResult:
    start = time.perf_counter()
    try:
        async with timed(f"answer:{question.id}"):
            # 1. first-pass retrieval
            docs = await retrieve(question.text, settings)

            # 2. draft + 3. score
            raw = await _draft(question, docs, llm, settings)
            answer, confidence, citations = parse_answer(raw)

            # 4. agentic loop: low confidence -> widen the search once
            if confidence == Confidence.LOW and settings.low_confidence_retry:
                log.info("low-confidence retry | %s", question.id)
                extra_queries = [question.text, question.category or question.text,
                                 f"{question.text} policy compliance"]
                docs = await multi_retrieve(extra_queries, settings)
                raw = await _draft(question, docs, llm, settings)
                answer, confidence, citations = parse_answer(raw)

        elapsed = (time.perf_counter() - start) * 1000
        return AnswerResult(
            question_id=question.id,
            question_text=question.text,
            answer=answer,
            confidence=confidence,
            citations=citations,
            needs_human_review=(confidence == Confidence.LOW),
            elapsed_ms=elapsed,
        )

    except asyncio.CancelledError:
        # Concept 6: NEVER swallow cancellation. Clean up (nothing to do here)
        # and re-raise so the pipeline can shut down promptly.
        raise
    except Exception as exc:
        # Concept 7: isolate the failure as a flagged result.
        elapsed = (time.perf_counter() - start) * 1000
        log.warning("question %s failed: %r", question.id, exc)
        return AnswerResult(
            question_id=question.id,
            question_text=question.text,
            answer=None,
            confidence=Confidence.LOW,
            needs_human_review=True,
            error=str(exc),
            elapsed_ms=elapsed,
        )
