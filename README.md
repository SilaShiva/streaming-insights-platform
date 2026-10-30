# streaming-insights-platform

End-to-end streaming data platform: Redis Streams event pipeline with LLM
enrichment (delivery-delay reason classification), dbt + warehouse (DuckDB
locally / BigQuery in the cloud), Dagster orchestration, data-quality gates,
and a FastAPI + Streamlit serving layer — deployed with Terraform and
GitHub Actions CI.

## Architecture

```
EventGenerator (synthetic orders/deliveries/reviews)
        │  XADD
        ▼
Redis Streams ── consumer group ──► Enrichment consumer
                                    ├─ LLM classify (delay reason, sentiment)
                                    └─ rules fallback when LLM down
        ▼
Warehouse (DuckDB local / BigQuery sandbox)
        │
        ▼
dbt: stg_events → enriched_events → marts (city delay, review sentiment)
        │
        ├─ Dagster assets + nightly schedule + quality-gate asset check
        ├─ FastAPI aggregates API
        └─ Streamlit dashboard
```

## Repo map

| Path | What it is |
|---|---|
| `src/generator.py` | Synthetic event generator (latent delay-reason DGP) |
| `src/streaming/` | Redis Streams producer + consumer-group consumer |
| `src/sink.py` | Buffered, dedup-idiopot sinks (DuckDB / BigQuery) |
| `src/enrichment/` | LLM client (cost-tracked), rules fallback, enricher |
| `src/quality/` | FAIL/WARN/PASS quality gates |
| `src/orchestration/` | Dagster assets + schedule + asset checks |
| `src/serving/` | FastAPI aggregates + Streamlit dashboard |
| `dbt/` | dbt project: staging, enriched passthrough, marts, tests |
| `infra/` | Terraform for BigQuery datasets + GCS staging bucket |
| `.github/workflows/` | CI: pytest (incl. dbt build on fixture data) |

## Quickstart (batch demo, no Redis needed)

```bash
python -m venv .venv
.venv/Scripts/pip install -e .          # Windows; use .venv/bin on unix
copy .env.example .env                   # fill LLM_* if you have a gateway
.venv/Scripts/python scripts/run_pipeline.py
.venv/Scripts/streamlit run src/serving/dashboard.py
```

## Streaming mode

```bash
redis-server &                           # or any Redis instance
.venv/Scripts/python -m src.streaming.producer
.venv/Scripts/python -m src.enrichment.enricher --max-events 5000
```

## Orchestration

```bash
export DAGSTER_HOME=./dagster_home       # unix; $env:DAGSTER_HOME on PowerShell
dagster dev -m src.orchestration.jobs
```

## LLM configuration

Any OpenAI-compatible gateway works — point `LLM_BASE_URL` / `LLM_MODEL` /
`LLM_API_KEY` at it (see `.env.example`). When the LLM is unreachable the
enricher falls back to a rule-based classifier, so the pipeline never
stalls; every row records its `enrich_source` (llm / rules / cache) for
lineage, and token/cost accounting is logged per run.

## Tests

```bash
.venv/Scripts/pytest -q
```

Covers: generator invariants, consumer-group semantics (fakeredis, incl.
idempotent redelivery + poison messages), enrichment rules/validation,
cost tracking, and a full dbt build against a seeded DuckDB fixture.
