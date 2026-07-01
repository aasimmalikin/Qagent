# Vendor Security Questionnaire Agent

A runnable, production-shaped agentic system that auto-drafts answers to vendor
security questionnaires — and, more to the point, a worked example of **every
asyncio concept on the "production floor" syllabus**, each used where it
naturally belongs rather than bolted on.

```
python -m qagent.main      # runs end-to-end with a simulated LLM, no API key needed
```

---

## 1. What it does & the business problem

**The problem.** Every B2B software company that sells to mid-market and
enterprise gets hit with security/vendor-risk questionnaires — SIG, CAIQ, or a
prospect's bespoke spreadsheet — with anywhere from 50 to 400 questions like
*"Do you encrypt data at rest?"*, *"What is your RTO/RPO?"*, *"Do you hold a SOC
2 report?"*. Today a security or sales engineer answers these by hand, copy-
pasting from past responses and policy docs. It takes days per questionnaire and
is a direct bottleneck on closing deals. It's repetitive, grounded in a stable
body of internal knowledge, and every question is independent — an almost
perfect fit for an agent.

**What this agent does.** For each question it:

1. **retrieves** grounding snippets from the company's security knowledge base
   (policies, the SOC 2 report, past answers) — a *tool call*;
2. **drafts** a grounded answer with an LLM, citing the policy ids it used;
3. **scores its own confidence**;
4. if confidence is low, runs **one more retrieval round with widened queries**
   and re-drafts — a small *agentic loop*;
5. emits an `AnswerResult` that is either ready to use or **flagged for human
   review**.

The output is a first draft for every question plus a clear "needs a human"
list — turning days of work into minutes of review.

**Why this shape stresses asyncio.** It's a fan-out/fan-in workload: many
independent questions, each doing concurrent I/O (retrieval + LLM), all sharing
one rate-limited provider, where some calls will be slow or fail and you must
not let one bad question sink the batch. That is exactly the surface every
production agentic system has to handle.

---

## 2. Architecture & how the pieces interrelate

```
 questionnaire.json
        │  load_questions()
        ▼
 ┌─────────────┐   bounded work_q   ┌──────────────────────────────┐
 │  producer   │ ─────────────────► │  worker pool (N coroutines)  │
 └─────────────┘   (backpressure)   │   each runs answer_question  │
        ▲                           └──────────────┬───────────────┘
        │ stop_event (graceful stop)               │ per question:
        │                                          ▼
 supervised by a TaskGroup            retrieve ─► draft(stream) ─► score
        │                                          │   └─ low? widen+redraft
        ▼                                          ▼
   results_q  ───────  async generator  ───►  caller (main) consumes
                       (yields results          with `async for`,
                        as they finish)         handles ExceptionGroup
```

The flow of control, module by module:

- **`main.py`** opens the pooled `LLMClient` (`async with`), then consumes
  `run_questionnaire(...)` with `async for`, printing each result as it
  streams in and writing `results.json` at the end.
- **`pipeline.py`** is the orchestrator. A **producer** puts questions on a
  **bounded `asyncio.Queue`**; a pool of **workers** drains it; each worker
  calls the agent and pushes results to a second queue. A **`TaskGroup`**
  supervises producer + workers. The orchestrator is an **async generator**
  that yields results to `main` as they complete.
- **`agent.py`** is the per-question logic: retrieve → stream-draft → score →
  (maybe) widen-and-redraft. It converts any hard error into a flagged result
  so the batch survives.
- **`retriever.py`** turns the blocking knowledge-base search into safe async
  work (`to_thread` + timeout) and fans out multi-query retrieval with
  `gather`.
- **`llm_client.py`** owns the pooled HTTP client and concentrates the
  semaphore, per-attempt timeout, and retry-with-jitter logic, plus token
  streaming.
- **`knowledge_base.py`** is the deliberately synchronous, CPU-bound search
  (stands in for FAISS / a local embedder).
- **`config.py`**, **`models.py`**, **`observability.py`** are the supporting
  cast: tunables, typed contracts, and logging/timing.

---

## 3. Concept-by-concept: why it's here and what breaks without it

> Each entry: **why we use it**, **what happens if you don't**, and **where**
> it lives.

### Concept 1 — Core concurrency model (event loop, coroutines, tasks)
**Why:** the entire system is one event loop running many coroutines; the whole
point is that while one question waits on the LLM, others make progress.
**Without it:** if you don't understand what yields vs. blocks, you write code
that looks async but runs serially — see Concept 8 for the classic trap.
**Where:** everywhere; `asyncio.run(main())` in `main.py` starts the loop.

### Concept 2 — Structured concurrency (`gather`, `TaskGroup`)
**Why:** this is where throughput comes from. `TaskGroup` supervises the worker
pool with structured lifetimes; `gather` fans out multi-query retrieval.
**Without it:** with bare `create_task` and no supervision, a crashing worker
becomes an orphaned/leaked task and its exception may vanish; processing
questions one-by-one instead of as a pool makes the run N times slower.
**Where:** `pipeline.supervise()` (`TaskGroup`), `retriever.multi_retrieve()`
(`gather`).

### Concept 3 — Concurrency control (`Semaphore`)
**Why:** all workers share one LLM provider with a rate limit. The semaphore
caps concurrent in-flight calls so we saturate the limit without exceeding it.
**Without it:** 14 questions (or 400) hit the provider at once → a wall of 429s,
and the retry logic then amplifies the storm. This is the single most common
naive-agent production incident.
**Where:** `LLMClient._semaphore`, acquired in `LLMClient.stream()`.

### Concept 4 — Coordination primitives (`Queue`, `Event`)
**Why:** the bounded `Queue` is the producer/consumer handoff *and* the
backpressure mechanism — when workers fall behind, the producer pauses instead
of buffering everything in memory. The `Event` is the graceful-stop signal.
**Without it:** an unbounded hand-off lets a 10k-question job balloon memory
until the process dies; without the stop event, Ctrl-C either does nothing
graceful or kills work mid-flight.
**Where:** `pipeline.run_questionnaire()` (`work_q`, `results_q`, `stop_event`),
signal wired in `main.install_signal_handler()`.

### Concept 5 — Async iteration & streaming (async generators, `async for`)
**Why:** two places. (a) The LLM client exposes token streaming as an async
generator, so answers can surface as they're produced. (b) The pipeline itself
is an async generator yielding finished results to the caller as they complete
— the UI/caller sees progress immediately instead of waiting for all 400.
**Without it:** you buffer the entire batch and hand it back at the end; a long
questionnaire shows a frozen screen for minutes, and you can't stream a single
answer's tokens to a reviewer.
**Where:** `LLMClient.stream()`, `agent._draft()` (`async for chunk`),
`pipeline.run_questionnaire()` (yields results), `main` (`async for result`).

### Concept 6 — Timeouts & cancellation (`asyncio.timeout`, `CancelledError`)
**Why:** external tools and LLM calls hang. Every call gets a deadline so one
stuck request can't freeze a worker forever; cancellation must be handled so a
cut-off task cleans up.
**Without it:** a single hung LLM call ties up a worker (and its semaphore slot)
indefinitely; throughput silently collapses as workers get stuck. Swallowing
`CancelledError` would break shutdown and leak zombie tasks.
**Where:** `asyncio.timeout` in `LLMClient.stream()` and `retriever.retrieve()`;
`answer_question()` re-raises `CancelledError` and absorbs everything else.
*Run Q13 to watch a `TimeoutError` fire on the first attempt and recover on
retry.*

### Concept 7 — Error handling at scale (`ExceptionGroup`/`except*`, partial failure, retries+jitter)
**Why:** at fan-out scale, multiple things fail at once. Transient 429/503s get
retried with **jitter** (so concurrent retries don't sync up and re-hammer the
provider); per-question hard failures are isolated into flagged results; genuine
pipeline-level bugs surface as an `ExceptionGroup` handled with `except*`.
**Without it:** no retries → every transient blip becomes a failed answer; no
jitter → retry storms; no per-question isolation → one exception kills the whole
batch and you lose 399 good answers because of 1 bad question.
**Where:** retry loop + `_backoff` jitter in `LLMClient`; `gather(return_
exceptions=True)` in `multi_retrieve`; `try/except Exception` → flagged result
in `answer_question`; `except* Exception` in `main`. *Run Q14 to see retries
exhaust into an isolated, flagged hard error while every other question
succeeds.*

### Concept 8 — Mixing sync and async (`to_thread`)
**Why:** the vector search is synchronous, CPU-bound work. Offloading it to a
thread keeps the event loop free to service every other coroutine.
**Without it:** calling `search_sync()` directly in the loop blocks *every*
worker for the duration of each search — the whole system serializes behind one
CPU-bound call and your carefully-built concurrency evaporates. This is *the*
trap from Concept 1 made concrete.
**Where:** `retriever.retrieve()` → `await asyncio.to_thread(search_sync, ...)`.
(For true CPU-bound work like local inference you'd use a process pool; the
knowledge base notes this.)

### Concept 9 — Resource & lifecycle (async context managers, pooled client, shutdown)
**Why:** one pooled `httpx.AsyncClient` is reused across every call; its
lifetime is bounded by `async with`. Cleanup lives in `finally`/context managers
so it survives cancellation.
**Without it:** a new client per call leaks connections and throttles throughput;
resources created without context-manager cleanup leak when a task is cancelled
mid-flight.
**Where:** `LLMClient.__aenter__/__aexit__`, used as `async with LLMClient(...)`
in `main`; the pipeline's `finally` block tears down the supervisor cleanly.

### Concept 10 — Observability & debugging
**Why:** concurrent bugs are non-deterministic, so timing and failures are
visible by default. `guard_task` ensures a detached task's exception is logged
rather than silently swallowed; `timed` catches accidental serialization;
debug mode surfaces un-awaited coroutines.
**Without it:** a fire-and-forget task fails silently and you never know; an
accidentally-serial section looks fine until you measure it.
**Where:** `observability.py` (`setup_logging`, `timed`, `guard_task`); used on
the supervisor task in `pipeline.py` and via `--debug`-style `asyncio_debug` in
`config.py`.

---

## 4. Running it & what you'll see

```
python -m qagent.main
```

In the logs you'll observe, all interleaved (proof of concurrency):

- several questions **retrying** transient failures with jittered backoff,
- **Q13** hitting a **timeout** on attempt 0 and recovering on retry (~2.1s),
- a couple of **low-confidence retries** widening their search,
- **Q14** exhausting retries into an **isolated hard error** flagged
  `[REVIEW]`, while every other question still completes,
- a final summary and `results.json`.

## 5. Going to production (what's simulated here)

- **`use_fake_llm=True`** returns deterministic simulated responses with
  realistic latency/failures so the project runs with no API key. Set it to
  `False` and complete `LLMClient._real_stream()` (the request shape is already
  there) to talk to a real provider.
- The **knowledge base** is a bag-of-words cosine search over ~13 snippets,
  standing in for FAISS + a real embedding model. Confidence is driven by
  retrieval relevance, so a true knowledge *gap* should surface as low
  confidence; the toy scorer is approximate, but the retrieve→score→escalate
  mechanics are the real thing.
- Real deployments would add: persistent run state, a human-review UI consuming
  the streamed results, structured tracing (OpenTelemetry), and a process pool
  if you run embeddings locally.

## 6. Layout

```
qagent/
  config.py           tunables: concurrency, timeouts, retries, queue size
  models.py           Pydantic contracts: Question, RetrievedDoc, AnswerResult
  observability.py    logging, timing context manager, task guard
  knowledge_base.py   SYNC, CPU-bound vector search (to be offloaded)
  llm_client.py       pooled client + semaphore + timeout + retries + streaming
  retriever.py        to_thread offload + gather multi-query
  agent.py            per-question: retrieve -> draft -> score -> escalate
  pipeline.py         Queue + worker pool + TaskGroup + async-generator output
  main.py             lifecycle, consume stream, except*, graceful shutdown
  data/questionnaire.json
```
