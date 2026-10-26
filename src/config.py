"""Central config for the streaming-insights-platform.

All stage behaviour is config-driven (following the convention used across
this portfolio): a single YAML file controls event generation, streaming,
enrichment and warehouse settings. Secrets never live here -- they come from
environment variables loaded via python-dotenv.
"""
from __future__ import annotations

import os
from pathlib import Path

import yaml

PROJECT_ROOT = Path(__file__).resolve().parents[1]
CONFIG_PATH = PROJECT_ROOT / "configs" / "config.yaml"


def load_config(path: Path | None = None) -> dict:
    """Load the YAML config and overlay environment-driven secrets."""
    path = path or CONFIG_PATH
    with open(path, "r", encoding="utf-8") as fh:
        cfg = yaml.safe_load(fh)

    cfg = _overlay_env(cfg)
    return cfg


def _overlay_env(cfg: dict) -> dict:
    """Inject env-var-backed secrets / overrides into the config tree."""
    llm = cfg.setdefault("llm", {})
    llm["api_key"] = os.getenv("LLM_API_KEY", "")
    llm["base_url"] = os.getenv("LLM_BASE_URL", llm.get("base_url", ""))
    llm["model"] = os.getenv("LLM_MODEL", llm.get("model", ""))

    bq = cfg.setdefault("bigquery", {})
    bq["project"] = os.getenv("GCP_PROJECT", bq.get("project", ""))
    bq["dataset"] = os.getenv("BQ_DATASET", bq.get("dataset", "streaming"))

    stream = cfg.setdefault("stream", {})
    stream["redis_url"] = os.getenv("REDIS_URL", stream.get("redis_url", "redis://localhost:6379/0"))
    return cfg
