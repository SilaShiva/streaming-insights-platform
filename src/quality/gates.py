"""Data-quality gates over the warehouse (FAIL/WARN convention).

Checks row-count drop vs a lookback window, null-enrichment share, and
staleness. Returns structured results so both Dagster assets and the CI
runner share one definition of "healthy".
"""
from __future__ import annotations

from dataclasses import dataclass

import duckdb


@dataclass
class GateResult:
    name: str
    status: str      # PASS | WARN | FAIL
    detail: str


class QualityGateRunner:
    def __init__(self, cfg: dict, connection=None):
        self.cfg = cfg
        self.q = cfg["quality"]
        self.raw_table = cfg["warehouse"]["raw_table"]
        self.enriched_table = cfg["warehouse"]["enriched_table"]
        self.con = connection or duckdb.connect(cfg["warehouse"]["duckdb_path"], read_only=True)

    def _scalar(self, sql: str):
        return self.con.execute(sql).fetchone()[0]

    def gate_row_volume(self) -> GateResult:
        """Compare recent rows against the prior window; catches silent upstream death."""
        total = self._scalar(f"select count(*) from {self.raw_table}")
        if total == 0:
            return GateResult("row_volume", "FAIL", "raw table empty")
        recent = self._scalar(
            f"select count(*) from {self.raw_table} "
            f"where ts > (select max(ts) from {self.raw_table}) - INTERVAL '1' HOUR"
        )
        prior = self._scalar(
            f"select count(*) from {self.raw_table} "
            f"where ts <= (select max(ts) from {self.raw_table}) - INTERVAL '1' HOUR"
        )
        if prior == 0:
            return GateResult("row_volume", "PASS", f"warming up: {total} rows total")
        drop_pct = 100.0 * (1 - recent / prior) if prior else 0.0
        if drop_pct >= self.q["row_count_drop_fail_pct"]:
            return GateResult("row_volume", "FAIL", f"volume dropped {drop_pct:.1f}% vs prior window")
        if drop_pct >= self.q["row_count_drop_warn_pct"]:
            return GateResult("row_volume", "WARN", f"volume dropped {drop_pct:.1f}% vs prior window")
        return GateResult("row_volume", "PASS", f"recent/prior = {recent}/{prior}")

    def gate_enrichment_coverage(self) -> GateResult:
        """Enriched rows must exist for (almost) every delivered + review event."""
        try:
            n_enriched = self._scalar(f"select count(*) from {self.enriched_table}")
        except duckdb.CatalogException:
            return GateResult("enrichment_coverage", "FAIL", "enriched table missing")
        expected = self._scalar(
            f"select count(*) from {self.raw_table} "
            f"where event_type in ('order_delivered', 'review_posted')"
        )
        if expected == 0:
            return GateResult("enrichment_coverage", "WARN", "no enrichable events yet")
        null_pct = 100.0 * (1 - n_enriched / expected)
        if null_pct >= self.q["null_enriched_fail_pct"]:
            return GateResult("enrichment_coverage", "FAIL", f"{null_pct:.1f}% of enrichable events missing enrichment")
        return GateResult("enrichment_coverage", "PASS", f"{n_enriched}/{expected} enriched")

    def gate_staleness(self) -> GateResult:
        latest_minutes = self._scalar(
            f"select datediff('minute', max(ts), now()) from {self.raw_table}"
        )
        if latest_minutes is None:
            return GateResult("staleness", "WARN", "no rows")
        if latest_minutes >= self.q["stale_minutes_warn"]:
            return GateResult("staleness", "WARN", f"latest event {latest_minutes:.0f} min old")
        return GateResult("staleness", "PASS", f"latest event {latest_minutes:.0f} min old")

    def run_all(self) -> list[GateResult]:
        return [self.gate_row_volume(), self.gate_enrichment_coverage(), self.gate_staleness()]

    @staticmethod
    def overall(results: list[GateResult]) -> str:
        statuses = {r.status for r in results}
        if "FAIL" in statuses:
            return "FAIL"
        if "WARN" in statuses:
            return "WARN"
        return "PASS"
