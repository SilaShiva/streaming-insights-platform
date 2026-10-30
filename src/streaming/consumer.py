"""Redis Streams consumer: reads events via a consumer group and sinks them.

Consumer-group semantics used here:
- XGROUP CREATE with MKSTREAM + `$` start id
- XREADGROUP with `>` (new entries) then XAUTOCLAIM for abandoned pendings
- XACK after a successful sink write (at-least-once semantics)

The sink is pluggable: DuckDB locally, BigQuery when warehouse.backend
is set to bigquery.
"""
from __future__ import annotations

import json
import signal
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

from src.config import PROJECT_ROOT, load_config
from src.sink import build_sink

_redis_module = None


def _get_redis():
    """Import seam: tests patch this to inject fakeredis."""
    global _redis_module
    if _redis_module is None:
        import redis as _redis
        _redis_module = _redis
    return _redis_module.Redis


def ensure_group(client, key: str, group: str) -> None:
    try:
        client.xgroup_create(key, group, id="0", mkstream=True)
    except Exception as exc:  # BUSYGROUP = already exists
        if "BUSYGROUP" not in str(exc):
            raise


def claim_stale(client, key: str, group: str, consumer: str, min_idle_ms: int = 300_000):
    """XAUTOCLAIM entries abandoned by crashed consumers (>5 min idle)."""
    res = client.xautoclaim(key, group, consumer, min_idle_time=min_idle_ms, start_id="0-0", count=50)
    # redis-py >=5 returns [next_cursor, messages, deleted_ids]
    return res[1] if res else []


def run_consumer(max_events: int | None = None, sink=None, cfg: dict | None = None,
                 idle_exits: int = 0) -> int:
    """Consume until Ctrl+C, max_events, or idle_exits empty polls (0 = run forever).

    Returns events processed."""
    cfg = cfg or load_config()
    stream_cfg = cfg["stream"]

    redis_factory = _get_redis()
    client = redis_factory.from_url(
        stream_cfg["redis_url"], decode_responses=True, socket_connect_timeout=5
    )
    client.ping()

    key = stream_cfg["key"]
    group = stream_cfg["consumer_group"]
    consumer = stream_cfg["consumer_name"]
    ensure_group(client, key, group)

    sink = sink or build_sink(cfg)
    processed = 0
    running = True

    def _stop(signum, frame):
        nonlocal running
        running = False

    signal.signal(signal.SIGINT, _stop)
    signal.signal(signal.SIGTERM, _stop)

    print(f"[consumer] group='{group}' consumer='{consumer}' key='{key}'")
    idle_polls = 0
    while running:
        # 1) reclaim abandoned pendings, 2) read new entries
        messages = claim_stale(client, key, group, consumer)
        if not messages:
            res = client.xreadgroup(
                group, consumer, {key: ">"}, count=stream_cfg["batch_count"],
                block=stream_cfg["block_ms"],
            )
            messages = res[0][1] if res else []

        if not messages:
            sink.flush_if_due()
            idle_polls += 1
            if idle_exits and idle_polls >= idle_exits:
                break
            continue
        idle_polls = 0

        rows = []
        ids = []
        for entry_id, fields in messages:
            try:
                rows.append(json.loads(fields["payload"]))
                ids.append(entry_id)
            except (KeyError, json.JSONDecodeError):
                # poison message: ack so it never blocks the stream
                client.xack(key, group, entry_id)
                continue

        if rows:
            sink.write(rows)
            client.xack(key, group, *ids)
            processed += len(rows)
            print(f"[consumer] processed {processed} events", end="\r")

        if max_events and processed >= max_events:
            break

    sink.close()
    print(f"\n[consumer] done. total events: {processed}")
    return processed


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--max-events", type=int, default=None)
    args = parser.parse_args()
    load_dotenv(PROJECT_ROOT / ".env")
    run_consumer(max_events=args.max_events)
