"""Rule-based fallback enrichment: signal extraction from free text + heuristic
delay-reason classification when the LLM is unavailable.

Keeps the pipeline landing enriched rows at all times -- the LLM improves
precision, but is never a hard dependency.
"""
from __future__ import annotations

import re

from src.generator import DELAY_REASONS

# keyword -> reason (order matters: first hit wins)
_KEYWORDS: list[tuple[str, str]] = [
    # stem-style prefixes: rained/raining, prepared/preparing, queued/queueing
    (r"\b(rain\w*|storm\w*|fog\w*|weather|snow\w*|heat\w*)\b", "weather"),
    (r"\b(traffic\w*|jam\w*|signal\w*|road ?block\w*|crawl\w*)\b", "traffic"),
    (r"\b(no rider|rider (was )?(not |never )?(assigned|allocated|available)|unassigned)\b", "rider_shortage"),
    (r"\b(address\w*|landmark\w*|locat\w*|find my|wrong location|could ?n[o']t find)\b", "address_issue"),
    (r"\b(kitchen\w*|prep\w*|cooking|cook\w*|restaurant (took|was slow))\b", "restaurant_prep_delay"),
    (r"\b(backlog\w*|rush\w*|swamp\w*|queued|queue\w*|busy place)\b", "restaurant_backlog"),
]

_SENTIMENT_POS = re.compile(r"\b(good|great|quick|fast|on time|hot|well packed|excellent|as always)\b", re.I)
_SENTIMENT_NEG = re.compile(r"\b(cold|late|slow|waited|ages|forever|wrong|could ?n[o']t|delay|no idea)\b", re.I)


def classify_review(text: str) -> dict:
    """Extract {delay_reason, sentiment, mentions} from a review.

    Never returns null for delay_reason -- 'unknown' is a valid class so
    downstream quality gates can distinguish 'classified' from 'missing'.
    """
    if not text or not text.strip():
        return {"delay_reason": "unknown", "sentiment": "neutral", "mentions": []}

    low = text.lower()
    reason = "unknown"
    for pattern, label in _KEYWORDS:
        if re.search(pattern, low):
            reason = label
            break

    pos, neg = bool(_SENTIMENT_POS.search(low)), bool(_SENTIMENT_NEG.search(low))
    sentiment = "positive" if pos and not neg else "negative" if neg else "neutral"

    return {"delay_reason": reason, "sentiment": sentiment, "mentions": [reason] if reason != "unknown" else []}


def heuristic_delay_reason(delay_minutes: int | None, traffic_index: float, weather_bad: int,
                           riders_online: int, prep_minutes: int) -> str:
    """Structured-signal fallback for delivered orders (no review text)."""
    if delay_minutes is None or delay_minutes < 5:
        return "none"
    if weather_bad and delay_minutes >= 10:
        return "weather"
    if traffic_index >= 1.8 and delay_minutes >= 8:
        return "traffic"
    if riders_online < 10 and delay_minutes >= 10:
        return "rider_shortage"
    if prep_minutes >= 22 and delay_minutes >= 8:
        return "restaurant_prep_delay"
    return "unknown"


VALID_REASONS = set(DELAY_REASONS) | {"none"}


def validate_enrichment(enr: dict) -> dict:
    """Schema-validate an LLM/rule enrichment dict; coerces to safe defaults."""
    reason = str(enr.get("delay_reason", "unknown")).strip().lower()
    if reason not in VALID_REASONS:
        reason = "unknown"
    sentiment = str(enr.get("sentiment", "neutral")).strip().lower()
    if sentiment not in {"positive", "negative", "neutral"}:
        sentiment = "neutral"
    mentions = enr.get("mentions") or []
    if not isinstance(mentions, list):
        mentions = []
    return {
        "delay_reason": reason,
        "sentiment": sentiment,
        "mentions": [str(m)[:64] for m in mentions[:5]],
    }
