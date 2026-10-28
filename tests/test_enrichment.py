"""Tests for enrichment: rules fallback, validation, LLM client wiring."""
from __future__ import annotations

from src.config import load_config
from src.enrichment.enricher import Enricher, EnrichedDuckDBSink
from src.enrichment.llm_client import CostTracker, LLMError, extract_json
from src.enrichment.rules_fallback import classify_review, heuristic_delay_reason, validate_enrichment


def test_rules_classify_known_reasons():
    cases = {
        "Rider never assigned, order sat for 30 min": "rider_shortage",
        "Heavy traffic on the ring road": "traffic",
        "It was raining heavily": "weather",
        "Rider could not find my address": "address_issue",
        "Kitchen took forever to prepare": "restaurant_prep_delay",
        "Restaurant was swamped with orders": "restaurant_backlog",
        "Arrived late": "unknown",
    }
    for text, expected in cases.items():
        assert classify_review(text)["delay_reason"] == expected, text


def test_rules_sentiment():
    assert classify_review("Great experience, delivery was quick")["sentiment"] == "positive"
    assert classify_review("Food arrived cold and late")["sentiment"] == "negative"
    assert classify_review("The order had 4 items")["sentiment"] == "neutral"


def test_classify_empty_text():
    out = classify_review("   ")
    assert out["delay_reason"] == "unknown"
    assert out["sentiment"] == "neutral"


def test_heuristic_reasons_from_structured_signals():
    assert heuristic_delay_reason(3, 1.0, 0, 20, 15) == "none"
    assert heuristic_delay_reason(15, 1.0, 1, 20, 10) == "weather"
    assert heuristic_delay_reason(12, 2.2, 0, 20, 10) == "traffic"
    assert heuristic_delay_reason(12, 1.0, 0, 6, 10) == "rider_shortage"
    assert heuristic_delay_reason(12, 1.2, 0, 20, 25) == "restaurant_prep_delay"


def test_validate_coerces_garbage():
    out = validate_enrichment({"delay_reason": "MARTIANS", "sentiment": 42, "mentions": "not a list"})
    assert out["delay_reason"] == "unknown"
    assert out["sentiment"] == "neutral"
    assert out["mentions"] == []


def test_extract_json_handles_fences():
    text = 'Sure!\n```json\n{"delay_reason": "traffic", "sentiment": "negative", "mentions": ["jam"]}\n```'
    assert extract_json(text)["delay_reason"] == "traffic"


def test_extract_json_raises_without_json():
    try:
        extract_json("no json here")
        raise AssertionError("expected LLMError")
    except LLMError:
        pass


def test_cost_tracker_accounting():
    t = CostTracker(cost_per_1k_input=0.5, cost_per_1k_output=1.5)
    t.record(1000, 200)
    t.record(500, 100)
    t.record_failure()
    s = t.summary()
    assert s["calls"] == 2 and s["failures"] == 1
    assert s["cost_usd"] == round(1.5 * 0.5 + 0.3 * 1.5, 6)


def test_enricher_llm_unavailable_falls_back_to_rules():
    cfg = load_config()
    cfg["llm"].update({"base_url": "", "model": "", "api_key": ""})
    e = Enricher(cfg)
    enr, source = e.enrich_review("Rider never assigned for 20 minutes")
    assert source == "rules"
    assert enr["delay_reason"] == "rider_shortage"
    # cache path: same text -> cache hit
    enr2, source2 = e.enrich_review("Rider never assigned for 20 minutes")
    assert source2 == "cache" or source2 == "rules"


def test_enriched_sink_roundtrip(tmp_path):
    cfg = load_config()
    cfg["warehouse"]["duckdb_path"] = str(tmp_path / "wh2.duckdb")
    sink = EnrichedDuckDBSink(cfg)
    sink.write([(
        "e1", "review_posted", "2026-10-28 10:00:00", "o1", "Bangalore", "ban-r001",
        30, 41, 11, 1, "stuck in traffic", "traffic", "negative", "rules",
    )])
    sink.close()

    import duckdb
    con = duckdb.connect(str(tmp_path / "wh2.duckdb"), read_only=True)
    rows = con.execute(
        f"SELECT delay_reason, sentiment, enrich_source FROM {cfg['warehouse']['enriched_table']}"
    ).fetchall()
    con.close()
    assert rows == [("traffic", "negative", "rules")]
