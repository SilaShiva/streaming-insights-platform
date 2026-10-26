"""Tests for the synthetic event generator."""
from __future__ import annotations

import json

from src.config import load_config
from src.generator import DELAY_REASONS, EventGenerator


def make_gen() -> EventGenerator:
    cfg = load_config()
    return EventGenerator(cfg, seed=123)


def test_batch_is_valid_json_events():
    gen = make_gen()
    lines = gen.stream_batch(20)
    assert len(lines) >= 20  # at least one event per order
    for line in lines:
        ev = json.loads(line)
        assert ev["event_type"] in {"order_placed", "order_delivered", "review_posted"}
        assert ev["event_id"] and ev["order_id"]


def test_delay_reasons_in_expected_space():
    gen = make_gen()
    for _ in range(50):
        out = gen.make_event()
        evs = out if isinstance(out, list) else [out]
        for ev in evs:
            assert ev.delay_reason is None  # ground truth never shipped in events


def test_delivered_events_carry_delay():
    gen = make_gen()
    lines = gen.stream_batch(30)
    delivered = [json.loads(l) for l in lines if json.loads(l)["event_type"] == "order_delivered"]
    assert delivered, "expected delivered events in batch"
    for ev in delivered:
        assert ev["delay_minutes"] is not None
        assert ev["delay_minutes"] >= 0


def test_reviews_correlate_with_delay():
    gen = make_gen()
    lines = gen.stream_batch(200)
    reviews = [json.loads(l) for l in lines if json.loads(l)["event_type"] == "review_posted"]
    on_time = [r for r in reviews if r["rating"] >= 4]
    late = [r for r in reviews if r["rating"] <= 2]
    assert on_time and late, "both happy and unhappy reviews should appear"
    assert all(r["review_text"] for r in reviews)


def test_burst_multiplier_bounds():
    gen = make_gen()
    gen.burst_probability = 1.0
    lines = gen.stream_batch(5)
    reviews = [json.loads(l) for l in lines if json.loads(l)["event_type"] == "review_posted"]
    assert len(reviews) >= 5  # bursts emit 1-3 reviews per order
