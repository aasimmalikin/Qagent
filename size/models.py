"""Domain models (Pydantic v2).

These give every stage of the pipeline a typed, validated contract. The agent
produces `AnswerResult` objects; the pipeline streams them to the caller.
"""

from __future__ import annotations

from enum import Enum

from pydantic import BaseModel, Field


class Confidence(str, Enum):
    HIGH = "high"
    MEDIUM = "medium"
    LOW = "low"


class Question(BaseModel):
    id: str
    text: str
    category: str | None = None


class RetrievedDoc(BaseModel):
    doc_id: str
    text: str
    score: float


class AnswerResult(BaseModel):
    """The outcome for a single questionnaire item.

    A result is always produced for every question, even on failure — that is
    what makes partial-failure handling possible (Concept 7). A hard failure
    becomes a LOW-confidence result with `error` set and `needs_human_review`
    True, rather than crashing the whole batch.
    """

    question_id: str
    question_text: str
    answer: str | None = None
    confidence: Confidence = Confidence.LOW
    citations: list[str] = Field(default_factory=list)
    needs_human_review: bool = False
    error: str | None = None
    attempts: int = 1
    elapsed_ms: float | None = None
