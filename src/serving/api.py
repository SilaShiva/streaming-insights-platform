"""FastAPI serving layer: aggregates straight from the warehouse marts.

Endpoints:
- GET /health          -> liveness + warehouse reachable
- GET /api/city-delay  -> per-city daily delay aggregates
- GET /api/reasons     -> delay-reason mix across cities
- GET /api/summary     -> top-line KPIs for the dashboard
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

from fastapi import FastAPI, HTTPException, Query

from src.config import PROJECT_ROOT, load_config

app = FastAPI(title="streaming-insights-platform API", version="0.1.0")

_config = None


def get_cfg() -> dict:
    global _config
    if _config is None:
        load_dotenv(PROJECT_ROOT / ".env")
        _config = load_config()
    return _config


def _warehouse_path() -> Path:
    p = Path(get_cfg()["warehouse"]["duckdb_path"])
    if not p.exists():
        raise HTTPException(503, "warehouse not built yet -- run the pipeline first")
    return p


@app.get("/health")
def health() -> dict:
    try:
        _warehouse_path()
        return {"status": "ok"}
    except HTTPException as exc:
        return {"status": "degraded", "detail": exc.detail}


@app.get("/api/city-delay")
def city_delay(days: int = Query(7, ge=1, le=90)) -> dict:
    import duckdb
    cfg = get_cfg()
    con = duckdb.connect(str(_warehouse_path()), read_only=True)
    try:
        rows = con.execute(
            "select city, event_date, deliveries, avg_delay_minutes, pct_late_over_10min "
            "from mart_city_delay order by event_date desc, deliveries desc limit ?", [days * 8]
        ).fetchall()
    finally:
        con.close()
    return {"rows": [
        {"city": r[0], "event_date": str(r[1]), "deliveries": r[2],
         "avg_delay_minutes": r[3], "pct_late_over_10min": r[4]} for r in rows
    ]}


@app.get("/api/reasons")
def reasons() -> dict:
    import duckdb
    con = duckdb.connect(str(_warehouse_path()), read_only=True)
    try:
        rows = con.execute(
            "select delay_reason, sum(reviews), round(avg(avg_rating), 2), "
            "round(sum(pct_negative * reviews) / sum(reviews), 2) "
            "from mart_review_sentiment group by delay_reason order by 2 desc"
        ).fetchall()
    finally:
        con.close()
    return {"rows": [
        {"delay_reason": r[0], "reviews": r[1], "avg_rating": r[2], "pct_negative": r[3]}
        for r in rows
    ]}


@app.get("/api/summary")
def summary() -> dict:
    import duckdb
    con = duckdb.connect(str(_warehouse_path()), read_only=True)
    try:
        deliveries, avg_delay, pct_late = con.execute(
            "select sum(deliveries), round(avg(avg_delay_minutes), 2), "
            "round(sum(pct_late_over_10min * deliveries) / sum(deliveries), 2) "
            "from mart_city_delay"
        ).fetchone()
        top_reason = con.execute(
            "select delay_reason, sum(reviews) from mart_review_sentiment "
            "where delay_reason != 'none' group by 1 order by 2 desc limit 1"
        ).fetchone()
    finally:
        con.close()
    return {
        "total_deliveries": deliveries,
        "avg_delay_minutes": avg_delay,
        "pct_late_over_10min": pct_late,
        "top_delay_reason": top_reason[0] if top_reason else None,
        "top_delay_reason_reviews": top_reason[1] if top_reason else 0,
    }
