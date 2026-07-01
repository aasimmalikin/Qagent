"""Retrieval tool.

Wraps the blocking knowledge-base search in async-friendly machinery:

* Concept 8 — `asyncio.to_thread` offloads the CPU-bound `search_sync` so it
  doesn't block the event loop.
* Concept 6 — a retrieval timeout guards against a hung search.
* Concept 2 — `multi_retrieve` fans out several queries with `asyncio.gather`.
* Concept 7 — `gather(return_exceptions=True)` so one failed sub-query yields
  partial results instead of sinking the whole retrieval.
"""

from __future__ import annotations

import asyncio

from .config import Settings
from .knowledge_base import search_sync
from .models import RetrievedDoc
from .observability import log


async def retrieve(query: str, settings: Settings) -> list[RetrievedDoc]:
    # Concept 6 + 8: bounded, off-loop blocking call.
    async with asyncio.timeout(settings.retrieval_timeout_s):
        rows = await asyncio.to_thread(search_sync, query, settings.top_k)
    return [RetrievedDoc(doc_id=d, text=t, score=s) for d, t, s in rows]


async def multi_retrieve(
    queries: list[str], settings: Settings
) -> list[RetrievedDoc]:
    """Run several retrieval queries concurrently and merge the results."""
    # Concept 2: parallel fan-out.
    results = await asyncio.gather(
        *(retrieve(q, settings) for q in queries),
        return_exceptions=True,  # Concept 7: don't let one failure kill the set
    )

    merged: dict[str, RetrievedDoc] = {}
    for res in results:
        if isinstance(res, Exception):
            log.warning("sub-retrieval failed: %r", res)
            continue
        for doc in res:
            # keep the highest score seen for each doc
            cur = merged.get(doc.doc_id)
            if cur is None or doc.score > cur.score:
                merged[doc.doc_id] = doc

    return sorted(merged.values(), key=lambda d: d.score, reverse=True)
