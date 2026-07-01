"""Central configuration.

Every knob that governs concurrency, timeouts, retries, and backpressure lives
here so the runtime behaviour of the whole pipeline is visible in one place.
The demo values are deliberately small so a full run finishes in a few seconds.
"""

from __future__ import annotations

from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    # --- Concurrency control (Concept 3) -----------------------------------
    # Hard ceiling on concurrent in-flight LLM calls. This is your defence
    # against provider rate limits (429s). Set it at or just under the
    # provider's requests-per-minute / tokens-per-minute budget.
    max_concurrent_llm: int = 5

    # --- Worker pool (Concept 4) -------------------------------------------
    # Number of consumer coroutines draining the work queue. More workers =
    # more questions processed at once, but the semaphore above still caps
    # how many actually hit the LLM simultaneously.
    worker_count: int = 4

    # --- Backpressure (Concept 4) ------------------------------------------
    # Bounded work queue. When full, the producer awaits (is paused) instead
    # of buffering unbounded work in memory.
    queue_maxsize: int = 32

    # --- Timeouts (Concept 6) ----------------------------------------------
    # Per-attempt deadlines. Small here so the "slow call" simulation trips
    # the timeout quickly. In production these are seconds-to-tens-of-seconds.
    llm_timeout_s: float = 2.0
    retrieval_timeout_s: float = 1.0

    # --- Retries (Concept 7) -----------------------------------------------
    max_retries: int = 3          # total attempts per LLM call
    backoff_base_s: float = 0.1   # exponential backoff base
    backoff_cap_s: float = 2.0    # max backoff before jitter

    # --- Agentic behaviour -------------------------------------------------
    # If a first-pass answer is low-confidence, run one extra retrieval round
    # with expanded queries (a small agentic loop) before giving up.
    low_confidence_retry: bool = True

    # --- Retrieval ---------------------------------------------------------
    top_k: int = 3

    # --- Simulation --------------------------------------------------------
    # When True, the LLM client returns simulated responses (with realistic
    # latency, transient failures, and slow calls) so the project runs with
    # no API key and no network. Flip to False and fill in _real_stream() to
    # talk to a real provider.
    use_fake_llm: bool = True

    # --- Observability (Concept 10) ----------------------------------------
    asyncio_debug: bool = False   # set True (or PYTHONASYNCIODEBUG=1) to surface
                                  # un-awaited coroutines and slow callbacks.


settings = Settings()
