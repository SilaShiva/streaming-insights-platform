"""One-shot batch pipeline: generate -> enrich -> dbt build -> quality gates.

This is the demo entrypoint (no Redis needed): it runs the same asset logic
the Dagster pipeline orchestrates, end to end in one command.
"""
from __future__ import annotations

import json
import os
import shutil
import subprocess
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from dotenv import load_dotenv

from src.config import PROJECT_ROOT, load_config
from src.generator import EventGenerator
from src.sink import DuckDBSink
from src.enrichment.enricher import Enricher, EnrichedDuckDBSink
from src.quality.gates import QualityGateRunner


def main() -> int:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = load_config()

    print("[1/4] generating + landing events")
    gen = EventGenerator(cfg, seed=None)
    sink = DuckDBSink(cfg)
    lines = gen.stream_batch(cfg["generator"]["orders_per_hour"] * 2)
    sink.write([json.loads(l) for l in lines])
    sink.close()
    print(f"      landed {len(lines)} events")

    print("[2/4] enriching")
    import duckdb
    con = duckdb.connect(cfg["warehouse"]["duckdb_path"])
    enriched_exists = con.execute(
        "select count(*) from information_schema.tables where table_name = ?",
        [cfg["warehouse"]["enriched_table"]],
    ).fetchone()[0] > 0
    rows = con.execute(
        "select event_id, event_type, ts, order_id, city, restaurant_id, promised_minutes, "
        "actual_minutes, delay_minutes, rating, review_text, prep_minutes, riders_online, "
        "traffic_index, weather_bad from raw_events "
        f"where event_type in ('order_delivered', 'review_posted') "
        + (f"and event_id not in (select event_id from {cfg['warehouse']['enriched_table']})" if enriched_exists else "")
    ).fetchall()
    con.close()

    enricher = Enricher(cfg)
    enr = EnrichedDuckDBSink(cfg)
    out = []
    for (eid, etype, ts, oid, city, rid, promised, actual, delay, rating,
         text, prep, riders, traffic, weather) in rows:
        ev = {"event_id": eid, "event_type": etype, "ts": str(ts), "order_id": oid,
              "city": city, "restaurant_id": rid, "promised_minutes": promised,
              "actual_minutes": actual, "delay_minutes": delay, "rating": rating,
              "review_text": text, "prep_minutes": prep, "riders_online": riders,
              "traffic_index": traffic, "weather_bad": weather}
        if etype == "review_posted" and text:
            e, source = enricher.enrich_review(text)
        else:
            e, source = enricher.enrich_delivered(ev), "rules"
        out.append((eid, etype, str(ts), oid, city, rid, promised, actual, delay,
                    rating, text, e["delay_reason"], e["sentiment"], source))
    enr.write(out)
    enr.close()
    print(f"      enriched {len(out)} events: {json.dumps(enricher.tracker.summary())}")

    print("[3/4] dbt build")
    import os
    profiles = PROJECT_ROOT / "dbt" / ".profiles"
    profiles.mkdir(exist_ok=True)
    (profiles / "profiles.yml").write_text(
        "streaming_insights:\n  target: dev\n  outputs:\n    dev:\n"
        "      type: duckdb\n"
        f"      path: {(PROJECT_ROOT / cfg['warehouse']['duckdb_path']).resolve().as_posix()}\n"
        "      threads: 4\n",
        encoding="utf-8",
    )
    env = {**os.environ, "DBT_PROFILES_DIR": str(profiles.resolve())}
    dbt_exe = shutil.which("dbt") or str(Path(sys.executable).parent / ("dbt.exe" if os.name == "nt" else "dbt"))
    result = subprocess.run([dbt_exe, "build"], cwd=PROJECT_ROOT / "dbt",
                            env=env, capture_output=True, text=True, timeout=600)
    if result.returncode != 0:
        print(result.stdout[-3000:])
        print(result.stderr[-1000:], file=sys.stderr)
        return 1
    print("      dbt build ok")

    print("[4/4] quality gates")
    runner = QualityGateRunner(cfg)
    results = runner.run_all()
    overall = QualityGateRunner.overall(results)
    for r in results:
        print(f"      [{r.status}] {r.name}: {r.detail}")
    print(f"overall: {overall}")
    return 0 if overall != "FAIL" else 2


if __name__ == "__main__":
    raise SystemExit(main())
