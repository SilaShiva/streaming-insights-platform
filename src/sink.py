"""Pluggable sinks for consumed events: DuckDB (local) and BigQuery (cloud).

Both sinks buffer rows and flush in batches. Dedup on event_id keeps the
warehouse idempotent under at-least-once redelivery.
"""
from __future__ import annotations

import json
import time
from pathlib import Path

RAW_COLUMNS = [
    "event_id", "event_type", "ts", "order_id", "city", "restaurant_id",
    "customer_id", "promised_minutes", "actual_minutes", "delay_minutes",
    "review_text", "rating", "prep_minutes", "riders_online",
    "traffic_index", "weather_bad",
]


def _row_from_event(ev: dict) -> tuple:
    return (
        ev.get("event_id"), ev.get("event_type"), ev.get("ts"), ev.get("order_id"),
        ev.get("city"), ev.get("restaurant_id"), ev.get("customer_id"),
        ev.get("promised_minutes"), ev.get("actual_minutes"), ev.get("delay_minutes"),
        ev.get("review_text"), ev.get("rating"), ev.get("prep_minutes", 0),
        ev.get("riders_online", 20), ev.get("traffic_index", 1.0), ev.get("weather_bad", 0),
    )


class DuckDBSink:
    """Buffered writer into a DuckDB table (local warehouse mirror)."""

    def __init__(self, cfg: dict, flush_every: int = 500):
        import duckdb

        self.cfg = cfg
        self.flush_every = flush_every
        self.buffer: list[tuple] = []
        path = cfg["warehouse"]["duckdb_path"]
        Path(path).parent.mkdir(parents=True, exist_ok=True)
        self.con = duckdb.connect(path)
        table = cfg["warehouse"]["raw_table"]
        cols_sql = ",\n            ".join(
            f"{c} {'VARCHAR' if c in ('event_id','event_type','ts','order_id','city','restaurant_id','customer_id','review_text') else 'NUMERIC'}"
            for c in RAW_COLUMNS
        )
        self.con.execute(f"""
            CREATE TABLE IF NOT EXISTS {table} (
            {cols_sql}
            )
        """)
        self.table = table

    def write(self, rows: list[dict]) -> None:
        seen: set[str] = set()
        for ev in rows:
            eid = ev.get("event_id")
            if eid in seen:
                continue
            seen.add(eid)
            self.buffer.append(_row_from_event(ev))
        if len(self.buffer) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        placeholders = ", ".join(["?"] * len(RAW_COLUMNS))
        self.con.executemany(
            f"INSERT INTO {self.table} VALUES ({placeholders})", self.buffer
        )
        self.buffer.clear()

    def flush_if_due(self) -> None:
        self.flush()

    def close(self) -> None:
        self.flush()
        self.con.close()


class BigQuerySink:
    """Buffered writer into a BigQuery raw table via the Storage Write API's
    simpler sibling: insertAll (fine for sandbox volumes, free tier)."""

    def __init__(self, cfg: dict, flush_every: int = 500):
        from google.cloud import bigquery

        self.cfg = cfg
        self.flush_every = flush_every
        self.buffer: list[dict] = []
        project = cfg["bigquery"]["project"]
        dataset = cfg["bigquery"]["dataset"]
        self.table_ref = f"{project}.{dataset}.{cfg['warehouse']['raw_table']}"
        self.client = bigquery.Client(project=project)
        # ensure dataset + table exist
        dataset_ref = bigquery.Dataset(f"{project}.{dataset}")
        dataset_ref.location = "US"
        self.client.create_dataset(dataset_ref, exists_ok=True)
        schema = [
            bigquery.SchemaField("event_id", "STRING"),
            bigquery.SchemaField("event_type", "STRING"),
            bigquery.SchemaField("ts", "TIMESTAMP"),
            bigquery.SchemaField("order_id", "STRING"),
            bigquery.SchemaField("city", "STRING"),
            bigquery.SchemaField("restaurant_id", "STRING"),
            bigquery.SchemaField("customer_id", "STRING"),
            bigquery.SchemaField("promised_minutes", "INT64"),
            bigquery.SchemaField("actual_minutes", "INT64"),
            bigquery.SchemaField("delay_minutes", "INT64"),
            bigquery.SchemaField("review_text", "STRING"),
            bigquery.SchemaField("rating", "INT64"),
            bigquery.SchemaField("prep_minutes", "INT64"),
            bigquery.SchemaField("riders_online", "INT64"),
            bigquery.SchemaField("traffic_index", "FLOAT64"),
            bigquery.SchemaField("weather_bad", "INT64"),
        ]
        table = bigquery.Table(self.table_ref, schema=schema)
        table.time_partitioning = bigquery.TimePartitioning(field="ts")
        self.client.create_table(table, exists_ok=True)

    def write(self, rows: list[dict]) -> None:
        seen: set[str] = set()
        for ev in rows:
            eid = ev.get("event_id")
            if eid in seen:
                continue
            seen.add(eid)
            self.buffer.append(dict(zip(RAW_COLUMNS, _row_from_event(ev))))
        if len(self.buffer) >= self.flush_every:
            self.flush()

    def flush(self) -> None:
        if not self.buffer:
            return
        errors = self.client.insert_rows_json(self.table_ref, self.buffer)
        if errors:
            raise RuntimeError(f"BigQuery insert errors: {errors}")
        self.buffer.clear()

    def flush_if_due(self) -> None:
        self.flush()

    def close(self) -> None:
        self.flush()


def build_sink(cfg: dict):
    backend = cfg["warehouse"]["backend"]
    if backend == "bigquery":
        return BigQuerySink(cfg)
    if backend == "duckdb":
        return DuckDBSink(cfg)
    raise ValueError(f"unknown warehouse backend: {backend}")
