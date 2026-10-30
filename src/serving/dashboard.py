"""Streamlit dashboard over the warehouse marts (reads the same local DuckDB).

Run:  streamlit run src/serving/dashboard.py
"""
from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

import pandas as pd
import streamlit as st

from src.config import load_config
from src.quality.gates import QualityGateRunner

st.set_page_config(page_title="Streaming Insights", page_icon=":chart:", layout="wide")
st.title("Streaming Insights — delivery delay intelligence")

cfg = load_config()
db_path = Path(cfg["warehouse"]["duckdb_path"])

if not db_path.exists():
    st.warning("Warehouse not built yet. Run: `python scripts/run_pipeline.py`")
    st.stop()

import duckdb

con = duckdb.connect(str(db_path), read_only=True)


@st.cache_data(ttl=60)
def load_city_delay() -> pd.DataFrame:
    return con.execute("select * from mart_city_delay order by event_date desc").fetchdf()


@st.cache_data(ttl=60)
def load_reasons() -> pd.DataFrame:
    return con.execute("select * from mart_review_sentiment").fetchdf()


@st.cache_data(ttl=60)
def load_enrichment_mix() -> pd.DataFrame:
    return con.execute(
        "select enrich_source, count(*) n from enriched_events group by 1"
    ).fetchdf()


city = load_city_delay()
reasons = load_reasons()
mix = load_enrichment_mix()

# KPI row
c1, c2, c3, c4 = st.columns(4)
total = int(city["deliveries"].sum())
c1.metric("Deliveries", f"{total:,}")
c2.metric("Avg delay (min)", round(city["avg_delay_minutes"].mean(), 1))
c3.metric("Late >10 min", f"{round((city['pct_late_over_10min'] * city['deliveries']).sum() / max(total, 1), 1)}%")
top = reasons.groupby("delay_reason")["reviews"].sum().drop(labels=["none"], errors="index")
c4.metric("Top delay reason", str(top.idxmax() if len(top) else "—"))

left, right = st.columns(2)

with left:
    st.subheader("Avg delay by city")
    by_city = city.groupby("city")["avg_delay_minutes"].mean().sort_values()
    st.bar_chart(by_city)

with right:
    st.subheader("Delay-reason mix (reviews)")
    reason_counts = reasons.groupby("delay_reason")["reviews"].sum()
    st.bar_chart(reason_counts)

st.subheader("Daily late-share trend")
trend = city.groupby("event_date").apply(
    lambda g: (g["pct_late_over_10min"] * g["deliveries"]).sum() / g["deliveries"].sum(),
    include_groups=False,
)
st.line_chart(trend)

with st.expander("Enrichment source mix (LLM vs rules)"):
    st.dataframe(mix, hide_index=True)

with st.expander("Quality gates"):
    runner = QualityGateRunner(cfg, connection=con)
    for r in runner.run_all():
        icon = {"PASS": "✅", "WARN": "⚠️", "FAIL": "⛔"}[r.status]
        st.markdown(f"{icon} **{r.name}** — {r.detail}")
