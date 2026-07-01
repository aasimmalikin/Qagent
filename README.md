# qagent — a vendor security questionnaire agent, in pure Python

![Python](https://img.shields.io/badge/python-3.11%2B-blue)
![Dependencies](https://img.shields.io/badge/dependencies-httpx%20%2B%20pydantic-brightgreen)
![Agent frameworks](https://img.shields.io/badge/agent%20frameworks-none-critical)
![Runs offline](https://img.shields.io/badge/runs%20offline-no%20API%20key-informational)

A multi-agent pipeline that auto-drafts answers to vendor **security questionnaires** — and,
just as importantly, a worked, runnable example of the concurrency machinery that agent
frameworks hide from you.

> **Built in pure Python. No LangChain. No LangGraph. No CrewAI.**
> The only third-party libraries are `httpx` and `pydantic`. Every piece of the agent —
> the concurrency, retries, rate limiting, timeouts, streaming, and tool-call loop — is
> hand-written with the standard library's `asyncio`, so you can read *exactly* how an
> agent works under the hood instead of trusting a black box.

---

## Why pure Python? (the whole point)

Spinning up an agent with a framework takes an afternoon — until something leaks: it crawls
when it should parallelize, quietly trips a rate limit, or hangs forever on a stuck call. When
that happens, the bug lives in the async layer the framework was hiding, and framework fluency
won't save you.

This project deliberately rebuilds that layer by hand so the mechanics are visible and
touchable:

- a **`Semaphore`** as a bulkhead bounding concurrent LLM calls (rate-limit protection)
- **retries with exponential backoff *and jitter*** for transient failures
- a **per-attempt `asyncio.timeout`** so a hung call can't freeze a worker
- **`asyncio.TaskGroup`** supervising a worker pool with structured concurrency
- a **bounded `asyncio.Queue`** for producer/consumer handoff and backpressure
- **`asyncio.to_thread`** to keep blocking CPU work off the event loop
- **async generators** streaming results out as they complete
- **partial-failure isolation** — one bad question never sinks the batch

It runs **completely offline with no API key** (the LLM is simulated with realistic latency and
failures), so anyone can clone it and watch all of the above work in one command.

---

## Quickstart

**Requirements:** Python **3.11 or newer** (it uses `TaskGroup`, `except*`, and
`asyncio.timeout`, all introduced in 3.11).

```bash
# 1. clone
git clone https://github.com/<your-username>/qagent.git
cd qagent

# 2. create and activate a virtual environment
python -m venv .venv
# macOS / Linux:
source .venv/bin/activate
# Windows (PowerShell):
.venv\Scripts\Activate.ps1

# 3. install the two dependencies
python -m pip install httpx pydantic

# 4. run it  (note: run as a MODULE, not the file directly — see below)
python -m qagent.main
```

That's it — no API key, no network. You'll see logs stream by and a `results.json` appear.

> ⚠️ **Run it as `python -m qagent.main`, from the project root.**
> Do **not** run `qagent/main.py` directly (or hit "Run" in your editor). The project is a
> package that uses relative imports (`from .config import ...`), so it must be launched as a
> module. Running the file on its own gives
> `ImportError: attempted relative import with no known parent package`.

---

## What you'll see

The pipeline answers 14 sample security questions concurrently. In the interleaved logs you'll
watch the resilience machinery actually fire:

- several questions **retrying** transient failures with jittered backoff,
- one question hitting a **timeout** and recovering on retry,
- a low-confidence answer triggering a **second, widened retrieval round** (the agentic loop),
- one question **failing permanently** and being **isolated and flagged for human review** —
  while every other question still completes,
- a final summary written to `results.json`.

```
OK  Q01 | medium |   177ms | Do you encrypt customer data at rest?
retry | Q13 attempt=1 after 0.08s (TimeoutError)
OK  Q13 | medium |  2174ms | What is your stance on post-quantum cryptography readiness?
ERR Q14 | low    |   264ms | [REVIEW] Describe your runtime memory isolation between tenants.
...
Processed 14 questions
  high=1  medium=12  low=1
  needs human review: 1
  hard errors:        1
```

---

## How it works

Each question flows through a small agent loop — **retrieve → draft → score → escalate if
unsure** — and many questions run concurrently through a supervised worker pool:

```
 questionnaire.json
        │
        ▼
   producer ──▶ [ bounded work queue ] ──▶ worker pool (×N)
        ▲            (backpressure)             │  each worker runs one question:
        │                                       │    retrieve ─▶ draft ─▶ score
   stop_event                                   │       └─ low confidence? widen + redraft
   (graceful stop)                              ▼
        supervised by a TaskGroup ──▶ [ results queue ] ──▶ streamed to the caller
```

The whole system is deliberately split so the boundaries are clear: the two files that touch
the outside world (`llm_client.py` for the model, `knowledge_base.py` for search) are isolated
at the edges, and everything between them is pure, testable logic.

### Project layout

```
qagent/
  config.py           tunables: concurrency, timeouts, retries, queue size
  models.py           typed contracts: Question, RetrievedDoc, AnswerResult
  observability.py    logging, timing, task-failure guard
  knowledge_base.py   synchronous, CPU-bound vector search (stands in for FAISS)
  llm_client.py       pooled client + semaphore + timeout + retries + streaming
  retriever.py        to_thread offload + gather multi-query retrieval
  agent.py            per-question loop: retrieve → draft → score → escalate
  pipeline.py         queue + worker pool + TaskGroup + async-generator output
  main.py             entrypoint: lifecycle, streaming, graceful shutdown
  data/
    questionnaire.json   the sample questions
```

---

## The production armor (what each guard prevents)

Almost every line outside the core logic exists to survive calling a flaky external service at
scale. A sample of the guards and the real-world hazard each one stops:

| Guard | File | Without it |
|---|---|---|
| `Semaphore` concurrency cap | `llm_client.py` | 400 calls fire at once → a wall of `429` rate-limit errors |
| Retries + **jitter** | `llm_client.py` | every transient blip becomes a failed question; synced retries re-create the spike |
| Per-attempt `asyncio.timeout` | `llm_client.py` | one hung call freezes a worker (and its semaphore slot) forever |
| Pooled client lifecycle | `llm_client.py` | a new connection per call leaks sockets until the process dies |
| `to_thread` offload | `retriever.py` | the blocking search freezes **every** worker; concurrency collapses to serial |
| `except Exception` → flagged result | `agent.py` | one bad question crashes the whole batch |
| `except asyncio.CancelledError: raise` | `agent.py` | cancellation is swallowed → zombie tasks, Ctrl-C stops working |
| `TaskGroup` supervision | `pipeline.py` | a crashing worker leaks as an orphaned task and its error vanishes |
| Bounded queue (backpressure) | `pipeline.py` | a large job loads everything into memory until it dies |
| `guard_task` + millisecond logs | `observability.py` | silent background failures; no way to reconstruct a concurrent run |

---

## Using a real LLM

By default the client runs in simulation mode so the project works offline. To point it at a
real provider:

1. In `qagent/config.py`, set `use_fake_llm = False`.
2. In `qagent/llm_client.py`, complete `_real_stream()` — the request shape (a streaming
   `POST` to the messages endpoint) is already there; add your provider's auth header and
   parse its streamed response format.

Nothing else changes. The semaphore, retries, timeouts, and streaming that guarded the
simulator now guard the real provider — because the rest of the system only ever sees the
`stream` / `complete` interface, not the call underneath.

---

## Optional: see the control flow live

`trace_run.py` (if included) instruments the real functions and runs the pipeline, printing a
per-question trace of control passing from file to file and writing an interactive HTML
timeline. Run it the same way:

```bash
python trace_run.py
```

---

## Troubleshooting

- **`ModuleNotFoundError: No module named 'httpx'`** — the virtual environment isn't active or
  the deps aren't installed *in it*. Activate `.venv`, then run
  `python -m pip install httpx pydantic` (using `python -m pip` guarantees they install into
  the same interpreter you run with).
- **`ImportError: attempted relative import with no known parent package`** — you ran the file
  directly. Use `python -m qagent.main` from the project root instead.
- **`FileNotFoundError: ...questionnaire.json`** — the `qagent/data/questionnaire.json` file is
  missing. Make sure it exists (on Windows, confirm it isn't saved as `questionnaire.json.txt`).
- **`SyntaxError` around `except*` or `TaskGroup`** — you're on Python < 3.11. Upgrade to 3.11+.

---

## License

MIT — see `LICENSE`. Free to use, learn from, and build on.
