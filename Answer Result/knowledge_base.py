"""Knowledge base: a deliberately SYNCHRONOUS, CPU-bound vector search.

This stands in for a FAISS index or an embedding model running locally. The key
point for the asyncio lesson (Concept 8) is that `search_sync` is a *blocking*
function: it computes embeddings and scans the corpus on the calling thread. If
you call it directly inside the event loop, it stalls EVERY other coroutine
while it runs. The retriever therefore offloads it with `asyncio.to_thread`.

The corpus is a small set of security-policy snippets — the kind of source
material a real vendor-questionnaire responder would retrieve from.
"""

from __future__ import annotations

import math
import re
import time

# --- Corpus: company security knowledge base -------------------------------
CORPUS: list[tuple[str, str]] = [
    ("POL-001", "All customer data is encrypted at rest using AES-256. "
                "Encryption keys are managed in AWS KMS with annual rotation."),
    ("POL-002", "Data in transit is protected with TLS 1.2 or higher. "
                "We enforce HSTS and disable legacy cipher suites."),
    ("POL-003", "We hold a SOC 2 Type II report covering security, "
                "availability, and confidentiality, audited annually by a "
                "third-party CPA firm. The report is available under NDA."),
    ("POL-004", "Customer data is retained for the life of the contract and "
                "deleted within 30 days of termination upon request."),
    ("POL-005", "Access to production systems requires SSO with mandatory "
                "multi-factor authentication. Access follows least-privilege "
                "and is reviewed quarterly."),
    ("POL-006", "Our incident response plan defines severity levels, on-call "
                "rotation, and customer notification within 72 hours of a "
                "confirmed breach."),
    ("POL-007", "Backups are encrypted and tested. Our recovery time "
                "objective (RTO) is 4 hours and recovery point objective "
                "(RPO) is 1 hour."),
    ("POL-008", "We run weekly automated vulnerability scans and engage a "
                "third party for annual penetration testing. Critical findings "
                "are remediated within 7 days."),
    ("POL-009", "All employees complete security awareness training at hire "
                "and annually thereafter, including phishing simulations."),
    ("POL-010", "Production infrastructure runs on AWS in us-east-1 and "
                "eu-west-1. We rely on AWS physical and environmental "
                "data-center controls."),
    ("POL-011", "Passwords must be at least 12 characters; we check against "
                "known-breached password lists and never store plaintext."),
    ("POL-012", "A current list of subprocessors is published on our trust "
                "page and customers are notified 30 days before changes."),
    ("POL-013", "Our business continuity plan is tested annually via tabletop "
                "exercises and covers loss of primary region and key vendors."),
]

# Build a fixed vocabulary from the corpus for a deterministic bag-of-words
# embedding. (A real system would use a learned embedding model; the mechanics
# of "embed -> score -> rank" are identical.)
_TOKEN_RE = re.compile(r"[a-z0-9]+")


def _tokenize(text: str) -> list[str]:
    return _TOKEN_RE.findall(text.lower())


_VOCAB: dict[str, int] = {}
for _doc_id, _text in CORPUS:
    for _tok in _tokenize(_text):
        _VOCAB.setdefault(_tok, len(_VOCAB))


def _embed(text: str) -> list[float]:
    vec = [0.0] * len(_VOCAB)
    for tok in _tokenize(text):
        idx = _VOCAB.get(tok)
        if idx is not None:
            vec[idx] += 1.0
    norm = math.sqrt(sum(v * v for v in vec)) or 1.0
    return [v / norm for v in vec]


_DOC_VECTORS: list[tuple[str, str, list[float]]] = [
    (doc_id, text, _embed(text)) for doc_id, text in CORPUS
]


def _cosine(a: list[float], b: list[float]) -> float:
    return sum(x * y for x, y in zip(a, b))


def search_sync(query: str, k: int = 3) -> list[tuple[str, str, float]]:
    """Blocking, CPU-bound similarity search. NEVER call directly in the loop.

    The tiny sleep simulates index latency so that "blocking the event loop"
    has a visible cost in the demo; the cosine scan itself is real CPU work.
    """
    time.sleep(0.01)  # simulate index I/O / native call latency
    q = _embed(query)
    scored = [(doc_id, text, _cosine(q, vec)) for doc_id, text, vec in _DOC_VECTORS]
    scored.sort(key=lambda r: r[2], reverse=True)
    return scored[:k]
