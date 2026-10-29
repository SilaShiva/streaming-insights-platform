"""Runs dbt (DuckDB target) against fixture data and asserts model + test outcomes.

This is the warehouse-layer gate: staging dedup, mart grains, and dbt tests
all exercised without BigQuery.
"""
from __future__ import annotations

import json
import shutil
import subprocess
import sys
from pathlib import Path

import pytest

pytest.importorskip("duckdb")

REPO = Path(__file__).resolve().parents[1]
DBT_DIR = REPO / "dbt"


@pytest.fixture(scope="module")
def warehouse(tmp_path_factory):
    """Build a fixture DuckDB warehouse with raw + enriched events, then run dbt."""
    import duckdb

    tmp = tmp_path_factory.mktemp("wh")
    db_path = tmp / "fixture.duckdb"

    sys.path.insert(0, str(REPO))
    from src.config import load_config
    from src.generator import EventGenerator
    from src.sink import DuckDBSink
    from src.enrichment.enricher import EnrichedDuckDBSink, Enricher

    cfg = load_config()
    cfg["warehouse"]["duckdb_path"] = str(db_path)
    gen = EventGenerator(cfg, seed=11)

    raw = DuckDBSink(cfg)
    enr_sink = EnrichedDuckDBSink(cfg)
    enricher = Enricher(cfg)

    lines = gen.stream_batch(150)
    events = [json.loads(l) for l in lines]
    raw.write(events)
    raw.close()

    rows = []
    for ev in events:
        if ev["event_type"] == "review_posted" and ev.get("review_text"):
            e, source = enricher.enrich_review(ev["review_text"])
        elif ev["event_type"] == "order_delivered":
            e, source = enricher.enrich_delivered(ev), "rules"
        else:
            continue
        rows.append((
            ev["event_id"], ev["event_type"], ev["ts"], ev["order_id"], ev["city"],
            ev["restaurant_id"], ev["promised_minutes"], ev["actual_minutes"],
            ev["delay_minutes"], ev.get("rating"), ev.get("review_text"),
            e["delay_reason"], e["sentiment"], source,
        ))
    enr_sink.write(rows)
    enr_sink.close()

    # dbt profile pointing at the fixture warehouse
    profiles = tmp / "profiles.yml"
    profiles.write_text(
        "streaming_insights:\n"
        "  target: dev\n"
        "  outputs:\n"
        "    dev:\n"
        "      type: duckdb\n"
        f"      path: {db_path.as_posix()}\n"
        "      threads: 2\n",
        encoding="utf-8",
    )

    env = {**dict(__import__("os").environ), "DBT_PROFILES_DIR": str(tmp)}
    run = lambda *args: subprocess.run(
        ["dbt", *args], cwd=DBT_DIR, env=env, capture_output=True, text=True, timeout=300
    )
    deps = run("deps")
    build = run("build")

    return {
        "db_path": db_path,
        "deps": deps,
        "build": build,
        "cfg": cfg,
    }


def test_dbt_build_succeeds(warehouse):
    if warehouse["build"].returncode != 0:
        pytest.fail("dbt build failed:\n" + warehouse["build"].stdout[-3000:] + warehouse["build"].stderr[-1000:])


def test_staging_dedups_event_ids(warehouse):
    import duckdb
    con = duckdb.connect(str(warehouse["db_path"]), read_only=True)
    n_stg = con.execute("select count(*) from stg_events").fetchone()[0]
    n_distinct = con.execute("select count(distinct event_id) from stg_events").fetchone()[0]
    con.close()
    assert n_stg == n_distinct


def test_mart_city_delay_grain(warehouse):
    import duckdb
    con = duckdb.connect(str(warehouse["db_path"]), read_only=True)
    rows = con.execute("select count(*), count(distinct (city, event_date)) from mart_city_delay").fetchone()
    con.close()
    assert rows[0] == rows[1], "mart_city_delay must be unique on (city, event_date)"
    assert rows[0] > 0


def test_mart_review_sentiment_pcts_in_range(warehouse):
    import duckdb
    con = duckdb.connect(str(warehouse["db_path"]), read_only=True)
    bad = con.execute(
        "select count(*) from mart_review_sentiment "
        "where pct_negative < 0 or pct_negative > 100 or pct_llm_enriched < 0 or pct_llm_enriched > 100"
    ).fetchone()[0]
    con.close()
    assert bad == 0
