"""Redis Streams producer: publishes synthetic events to a stream.

Redis Streams give real streaming semantics (consumer groups, pending
entries lists, acknowledgments) without Kafka infrastructure -- chosen
deliberately for a laptop-runnable, Docker-free build.
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))

from dotenv import load_dotenv

from src.config import PROJECT_ROOT, load_config
from src.generator import EventGenerator


def publish_batch(redis_client, key: str, lines: list[str]) -> int:
    """XADD each event; returns count published."""
    pipe = redis_client.pipeline(transaction=False)
    for line in lines:
        pipe.xadd(key, {"payload": line})
    pipe.execute()
    return len(lines)


def main() -> None:
    load_dotenv(PROJECT_ROOT / ".env")
    cfg = load_config()
    stream_cfg = cfg["stream"]
    gen_cfg = cfg["generator"]

    import redis

    client = redis.Redis.from_url(
        stream_cfg["redis_url"], decode_responses=True, socket_connect_timeout=5
    )
    client.ping()

    gen = EventGenerator(cfg)
    orders_per_tick = max(1, gen_cfg["orders_per_hour"] // 60)  # ~per-minute rate

    print(f"[producer] publishing to '{stream_cfg['key']}' at ~{orders_per_tick} orders/min (Ctrl+C to stop)")
    published = 0
    try:
        while True:
            lines = gen.stream_batch(orders_per_tick)
            published += publish_batch(client, stream_cfg["key"], lines)
            if published % 500 < orders_per_tick:
                print(f"[producer] total events published: {published}")
            time.sleep(60)
    except KeyboardInterrupt:
        print(f"[producer] stopped. total events published: {published}")


if __name__ == "__main__":
    main()
