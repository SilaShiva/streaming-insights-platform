"""Enrichment logic: LLM-first with rules fallback, dedup cache, cost tracking.

The enrichment consumer wraps the plain consumer: review events get an LLM
pass (classify delay reason + sentiment from free text), delivered events
get a structured-signal pass, and every result is schema-validated before
landing in the warehouse.
"""
from __future__ import annotations

import json
import sys
from collections import OrderedDict
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

from src.config import PROJECT_ROOT, load_config
from src.enrichment.llm_client import CostTracker, LLMClient, LLMError, extract_json
from src.enrichment.rules_fallback import classify_review, heuristic_delay_reason, validate_enrichment
from src.streaming.consumer import run_consumer as run_plain_consumer

SYSTEM_PROMPT = """You label food-delivery review texts for an analytics pipeline.
Return ONLY a JSON object with keys:
- "delay_reason": one of restaurant_prep_delay, rider_shortage, traffic, weather, address_issue, restaurant_backlog, unknown
- "sentiment": one of positive, negative, neutral
- "mentions": short list of phrases supporting the label
No other text."""


class Enricher:
    def __init__(self, cfg: dict):
        self.cfg = cfg
        self.llm = LLMClient(cfg)
        self.tracker = self.llm.tracker
        self.fallback_mode = cfg["llm"].get("fallback", "rules")
        self.cache: OrderedDict[str, dict] = OrderedDict()
        self.cache_max = cfg["llm"].get("cache_max_items", 20000)
        self.stats = {"llm": 0, "cache": 0, "rules": 0}

    def _cache_get(self, text: str) -> dict | None:
        hit = self.cache.get(text)
        if hit is not None:
            self.cache.move_to_end(text)
        return hit

    def _cache_put(self, text: str, value: dict) -> None:
        self.cache[text] = value
        if len(self.cache) > self.cache_max:
            self.cache.popitem(last=False)

    def enrich_review(self, text: str) -> tuple[dict, str]:
        """Returns (enrichment_dict, source) where source in {llm, cache, rules}."""
        hit = self._cache_get(text)
        if hit is not None:
            self.stats["cache"] += 1
            return hit, "cache"

        if self.llm.enabled:
            try:
                raw = self.llm.chat(SYSTEM_PROMPT, text)
                enr = validate_enrichment(extract_json(raw))
                self._cache_put(text, enr)
                self.stats["llm"] += 1
                return enr, "llm"
            except (LLMError, ValueError):
                if self.fallback_mode == "passthrough":
                    raise

        enr = validate_enrichment(classify_review(text))
        self.stats["rules"] += 1
        return enr, "rules"

    def enrich_delivered(self, ev: dict) -> dict:
        return {
            "delay_reason": heuristic_delay_reason(
                ev.get("delay_minutes"), ev.get("traffic_index", 1.0),
                ev.get("weather_bad", 0), ev.get("riders_online", 20),
                ev.get("prep_minutes", 0),
            ),
            "sentiment": "neutral",
            "mentions": [],
        }


ENRICHED_COLUMNS = [
    "event_id", "event_type", "ts", "order_id", "city", "restaurant_id",
    "promised_minutes", "actual_minutes", "delay_minutes", "rating",
    "review_text", "delay_reason", "sentiment", "enrich_source",
]


def _enriched_row(ev: dict, enr: dict, source: str) -> tuple:
    return (
        ev.get("event_id"), ev.get("event_type"), ev.get("ts"), ev.get("order_id"),
        ev.get("city"), ev.get("restaurant_id"), ev.get("promised_minutes"),
        ev.get("actual_minutes"), ev.get("delay_minutes"), ev.get("rating"),
        ev.get("review_text"), enr["delay_reason"], enr["sentiment"], source,
    )


class EnrichedDuckDBSink:
    """Writes enriched rows to a second DuckDB table."""

    def __init__(self, cfg: dict, flush_every: int = 200):
        import duckdb

        self.flush_every = flush_every
        self.buffer: list[tuple] = []
        path = cfg["warehouse"]["duckdb_path"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(path)
        table = cfg["warehouse"]["enriched_table"]
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
                event_id VARCHAR,
                event_type VARCHAR,
                ts TIMESTAMP,
                order_id VARCHAR,
                city VARCHAR,
                restaurant_id VARCHAR,
                promised_minutes INTEGER,
                actual_minutes INTEGER,
                delay_minutes INTEGER,
                rating INTEGER,
                review_text VARCHAR,
                delay_reason VARCHAR,
                sentiment VARCHAR,
                enrich_source VARCHAR
            )
        """)
        self.table = table

    def write(self, rows: list[tuple]) -> None:
        self.buffer.extend(rows)
        if len(self.buffer) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        placeholders = ", ".join(["?"] * len(ENRICHED_COLUMNS))
        self.con.executemany(f"INSERT INTO {self.table} VALUES ({placeholders})", self.buffer)
        self.buffer.clear()

    def close(self) -> None:
        self.flush()
        self.con.close()


def run_enrichment_consumer(max_events: int | None = None, sink=None, enricher=None,
                            quiet: bool = False) -> dict:
    """Consume events, enrich, write to enriched table. Returns run stats."""
    cfg = load_config()
    enricher = enricher or Enricher(cfg)
    sink = sink or EnrichedDuckDBSink(cfg)

    seen_ids: set[str] = set()

    def handle(rows: list[dict]) -> list[tuple]:
        out = []
        for ev in rows:
            eid = ev.get("event_id")
            if eid in seen_ids:
                continue
            seen_ids.add(eid)
            if ev.get("event_type") == "review_posted" and ev.get("review_text"):
                enr, source = enricher.enrich_review(ev["review_text"])
            elif ev.get("event_type") == "order_delivered":
                enr, source = enricher.enrich_delivered(ev), "rules"
            else:
                continue  # order_placed carries nothing to enrich
            out.append(_enriched_row(ev, enr, source))
        return out

    # Monkey-patch the sink protocol: plain consumer calls sink.write(rows of dict)
    class _Adapter:
        def __init__(self, inner):
            self.inner = inner

        def write(self, rows: list[dict]) -> None:
            self.inner.write(handle(rows))

        def flush_if_due(self) -> None:
            self.inner.flush()

        def close(self) -> None:
            self.inner.close()

    processed = run_plain_consumer(max_events=max_events, sink=_Adapter(sink))
    stats = {
        "processed": processed,
        "llm": enricher.stats["llm"],
        "cache": enricher.stats["cache"],
        "rules": enricher.stats["rules"],
        "llm_usage": enricher.tracker.summary(),
    }
    if not quiet:
        print("[enricher] " + json.dumps(stats))
    return stats


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser()
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()
    load_dotenv(PROJECT_ROOT / ".env")
    run_enrichment_consumer(max_events=args.max_events)
