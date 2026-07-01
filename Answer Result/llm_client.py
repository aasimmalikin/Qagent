"""Async LLM client.

This one module exercises several concepts at once because in real systems they
all converge on the call to the model:

* Concept 9  — async context manager owning a single POOLED httpx client.
* Concept 3  — a Semaphore bounding concurrent in-flight calls (rate limits).
* Concept 6  — a per-attempt `asyncio.timeout` deadline.
* Concept 7  — retries with exponential backoff AND jitter for transient errors.
* Concept 5  — token streaming exposed as an async generator (`stream`).

In fake mode it simulates latency, transient 429-style failures, slow calls
(to trip the timeout), and one permanently failing request id (to demonstrate
partial-failure isolation downstream).
"""

from __future__ import annotations

import asyncio
import hashlib
import random
from collections.abc import AsyncIterator

import httpx

from .config import Settings
from .observability import log


class LLMError(Exception):
    """Raised when retries are exhausted."""


class TransientLLMError(Exception):
    """A retryable upstream error (e.g. 429/503)."""


def _fake_behavior(request_id: str, attempt: int) -> str:
    """Deterministic simulated outcome so the demo is reproducible."""
    if request_id == "Q14":
        return "permanent"  # always fails -> exhausts retries -> hard error
    roll = int(hashlib.sha256(request_id.encode()).hexdigest(), 16) % 100
    if attempt == 0:
        if roll < 18:
            return "transient"  # fails once, succeeds on retry
        if roll < 36:
            return "slow"       # exceeds the timeout once, succeeds on retry
    return "normal"


def _chunkify(text: str, size: int = 24) -> list[str]:
    return [text[i:i + size] for i in range(0, len(text), size)]


class LLMClient:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Concept 3: one semaphore shared across all calls bounds concurrency.
        self._semaphore = asyncio.Semaphore(settings.max_concurrent_llm)
        self._client: httpx.AsyncClient | None = None

    # --- Concept 9: lifecycle as an async context manager ------------------
    async def __aenter__(self) -> "LLMClient":
        # ONE pooled client reused for every request. Creating a client per
        # call would leak connections and destroy throughput.
        self._client = httpx.AsyncClient(timeout=None, http2=False)
        return self

    async def __aexit__(self, *exc) -> None:
        if self._client is not None:
            await self._client.aclose()
            self._client = None

    def _backoff(self, attempt: int) -> float:
        base = min(self._settings.backoff_base_s * (2 ** (attempt - 1)),
                   self._settings.backoff_cap_s)
        # Full jitter: spreads concurrent retries so they don't re-hammer the
        # provider in lockstep (Concept 7).
        return base * (0.5 + random.random())

    # --- Concept 5: streaming exposed as an async generator ----------------
    async def stream(
        self, prompt: str, *, request_id: str, timeout: float
    ) -> AsyncIterator[str]:
        # Concept 3: acquire a slot before doing any work. Held for the whole
        # call (including retries) so a retrying call doesn't exceed the bound.
        async with self._semaphore:
            attempt = 0
            while True:
                try:
                    # Concept 6: each attempt gets its own deadline.
                    async with asyncio.timeout(timeout):
                        async for chunk in self._raw_stream(prompt, request_id, attempt):
                            yield chunk
                    return
                except (TransientLLMError, TimeoutError) as exc:
                    attempt += 1
                    if attempt >= self._settings.max_retries:
                        raise LLMError(
                            f"exhausted {attempt} attempts for {request_id}: {exc!r}"
                        ) from exc
                    delay = self._backoff(attempt)
                    log.info("retry | %s attempt=%d after %.2fs (%s)",
                             request_id, attempt, delay, type(exc).__name__)
                    await asyncio.sleep(delay)

    async def complete(
        self, prompt: str, *, request_id: str, timeout: float
    ) -> str:
        parts: list[str] = []
        async for chunk in self.stream(prompt, request_id=request_id, timeout=timeout):
            parts.append(chunk)
        return "".join(parts)

    # --- The actual (or simulated) network call ----------------------------
    async def _raw_stream(
        self, prompt: str, request_id: str, attempt: int
    ) -> AsyncIterator[str]:
        if self._settings.use_fake_llm:
            async for chunk in self._fake_stream(prompt, request_id, attempt):
                yield chunk
        else:
            async for chunk in self._real_stream(prompt, request_id):
                yield chunk

    async def _fake_stream(
        self, prompt: str, request_id: str, attempt: int
    ) -> AsyncIterator[str]:
        behavior = _fake_behavior(request_id, attempt)
        # Simulate failure BEFORE emitting any token, so a retry restarts
        # cleanly. (Real streaming retries must handle partial output.)
        if behavior in ("transient", "permanent"):
            await asyncio.sleep(0.02)
            raise TransientLLMError("simulated 503 from upstream")
        if behavior == "slow":
            await asyncio.sleep(5.0)  # the outer asyncio.timeout cancels this

        text = _fake_answer(prompt)
        for chunk in _chunkify(text):
            await asyncio.sleep(0.01)  # simulate token-by-token arrival
            yield chunk

    async def _real_stream(
        self, prompt: str, request_id: str
    ) -> AsyncIterator[str]:
        # Reference shape for a real provider call. Left unused in fake mode.
        assert self._client is not None
        payload = {
            "model": "claude-sonnet-4-6",
            "max_tokens": 1024,
            "messages": [{"role": "user", "content": prompt}],
            "stream": True,
        }
        async with self._client.stream(
            "POST", "https://api.anthropic.com/v1/messages", json=payload,
        ) as resp:
            if resp.status_code == 429 or resp.status_code >= 500:
                raise TransientLLMError(f"HTTP {resp.status_code}")
            resp.raise_for_status()
            async for line in resp.aiter_lines():
                # parse SSE 'data:' lines into text deltas here
                if line.startswith("data:"):
                    yield line[len("data:"):].strip()


# --- Simulated model output -------------------------------------------------
def _fake_answer(prompt: str) -> str:
    """Build a plausible structured answer from the context in the prompt.

    The prompt embeds each retrieved doc as a line:
        - [relevance=0.62] (POL-007) <text>
    A real model would just read the context; here we parse the relevance
    scores to decide confidence, which keeps the demo deterministic.
    """
    import re

    rows = re.findall(r"\[relevance=([0-9.]+)\]\s*\(([A-Z0-9-]+)\)\s*(.*)", prompt)
    if not rows:
        return "CONFIDENCE: low\nCITATIONS:\nANSWER:\nNo relevant policy found."

    rows = [(float(s), cid, txt) for s, cid, txt in rows]
    rows.sort(reverse=True)
    best = rows[0][0]
    if best >= 0.55:
        conf = "high"
    elif best >= 0.32:
        conf = "medium"
    else:
        conf = "low"

    cites = [cid for s, cid, _ in rows if s >= 0.30][:2]
    top_text = rows[0][2].strip()
    answer = f"Based on our security policies: {top_text}"
    return (
        f"CONFIDENCE: {conf}\n"
        f"CITATIONS: {', '.join(cites)}\n"
        f"ANSWER:\n{answer}"
    )
