"""Dagster assets: batch generation -> consumption -> enrichment -> dbt -> gates.

Defines the daily pipeline as software-defined assets so the Dagster UI
shows lineage, lets each step be re-run independently, and schedules the
whole chain nightly. The live Redis loop (producer/consumer) runs outside
Dagster; these assets handle the batch/shaping side.
"""
from __future__ import annotations

import json
from pathlib import Path

from dagster import AssetExecutionContext, Definitions, asset, asset_check, AssetCheckResult, AssetCheckSeverity, ScheduleDefinition

from src.config import load_config
from src.generator import EventGenerator
from src.sink import DuckDBSink
from src.enrichment.enricher import Enricher, EnrichedDuckDBSink
from src.quality.gates import QualityGateRunner


@asset(description="Generate a day's synthetic events and land them in the raw table")
def raw_events(context: AssetExecutionContext) -> int:
    cfg = load_config()
    gen = EventGenerator(cfg, seed=None)
    sink = DuckDBSink(cfg)
    lines = gen.stream_batch(cfg["generator"]["orders_per_hour"] * 2)
    sink.write([json.loads(l) for l in lines])
    sink.close()
    n = len(lines)
    context.log.info(f"landed {n} raw events")
    return n


@asset(description="Enrich delivered + review events (LLM-first, rules fallback)")
def enriched_events(context: AssetExecutionContext, raw_events: int) -> int:
    cfg = load_config()
    import duckdb
    con = duckdb.connect(cfg["warehouse"]["duckdb_path"])
    rows = con.execute(
        "select event_id, event_type, ts, order_id, city, restaurant_id, promised_minutes, "
        "actual_minutes, delay_minutes, rating, review_text, prep_minutes, riders_online, "
        "traffic_index, weather_bad from raw_events "
        "where event_type in ('order_delivered', 'review_posted') and event_id not in "
        f"(select event_id from {cfg['warehouse']['enriched_table']})"
    ).fetchall()
    con.close()

    enricher = Enricher(cfg)
    sink = EnrichedDuckDBSink(cfg)
    out = []
    for (eid, etype, ts, oid, city, rid, promised, actual, delay, rating,
         text, prep, riders, traffic, weather) in rows:
        ev = {
            "event_id": eid, "event_type": etype, "ts": str(ts), "order_id": oid,
            "city": city, "restaurant_id": rid, "promised_minutes": promised,
            "actual_minutes": actual, "delay_minutes": delay, "rating": rating,
            "review_text": text, "prep_minutes": prep, "riders_online": riders,
            "traffic_index": traffic, "weather_bad": weather,
        }
        if etype == "review_posted" and text:
            e, source = enricher.enrich_review(text)
        else:
            e, source = enricher.enrich_delivered(ev), "rules"
        out.append((eid, etype, str(ts), oid, city, rid, promised, actual, delay,
                    rating, text, e["delay_reason"], e["sentiment"], source))
    sink.write(out)
    sink.close()
    context.log.info(f"enriched {len(out)} events; usage={json.dumps(enricher.tracker.summary())}")
    return len(out)


@asset(description="Run dbt build (staging + marts) over the warehouse")
def dbt_models(context: AssetExecutionContext, enriched_events: int) -> str:
    import subprocess, os
    cfg = load_config()
    env = {**os.environ}
    profiles = Path("dbt/.profiles")
    profiles.mkdir(exist_ok=True)
    (profiles / "profiles.yml").write_text(
        "streaming_insights:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckdb\n"
        f"      path: {Path(cfg['warehouse']['duckdb_path']).resolve().as_posix()}\n"
        "      threads: 4\n",
        encoding="utf-8",
    )
    env["DBT_PROFILES_DIR"] = str(profiles.resolve())
    result = subprocess.run(["dbt", "build"], cwd="dbt", env=env, capture_output=True, text=True, timeout=600)
    context.log.info(result.stdout[-2000:])
    if result.returncode != 0:
        raise RuntimeError(f"dbt build failed: {result.stderr[-1000:]}")
    return "ok"


@asset_check(asset="dbt_models", description="FAIL/WARN quality gates over raw + enriched tables")
def quality_gates(dbt_models: str) -> AssetCheckResult:
    cfg = load_config()
    runner = QualityGateRunner(cfg)
    results = runner.run_all()
    overall = QualityGateRunner.overall(results)
    for r in results:
        print(f"[gate] {r.status:4} {r.name}: {r.detail}")
    return AssetCheckResult(
        passed=overall != "FAIL",
        severity=AssetCheckSeverity.WARN if overall == "WARN" else AssetCheckSeverity.ERROR,
        metadata={r.name: r.detail for r in results},
    )


daily_schedule = ScheduleDefinition(
    name="nightly_pipeline",
    cron_schedule="0 2 * * *",  # 02:00 UTC = 07:30 IST
    target="raw_events",        # kicks off the asset chain
)


defs = Definitions(
    assets=[raw_events, enriched_events, dbt_models],
    asset_checks=[quality_gates],
    schedules=[daily_schedule],
)
