"""Tests for the streaming layer: sink semantics + consumer-group loop with a
fakeredis-backed broker (no live Redis needed in CI)."""
from __future__ import annotations

import json

import pytest

from src.config import load_config
from src.generator import EventGenerator

fakeredis = pytest.importorskip("fakeredis")


@pytest.fixture()
def gen(tmp_path, monkeypatch):
    cfg = load_config()
    cfg["warehouse"]["duckdb_path"] = str(tmp_path / "wh.duckdb")
    return EventGenerator(cfg, seed=7), cfg


def test_duckdb_sink_idempotent_on_duplicate_event_ids(gen, tmp_path):
    from src.sink import DuckDBSink

    generator, cfg = gen
    lines = generator.stream_batch(10)
    events = [json.loads(l) for l in lines]

    sink = DuckDBSink(cfg)
    sink.write(events)
    sink.write(events)  # duplicate delivery (at-least-once)
    sink.close()

    import duckdb
    con = duckdb.connect(str(tmp_path / "wh.duckdb"), read_only=True)
    n = con.execute(f"SELECT count(*) FROM {cfg['warehouse']['raw_table']}").fetchone()[0]
    con.close()
    assert n == len({e["event_id"] for e in events})


def test_consumer_group_end_to_end(gen, tmp_path, monkeypatch):
    import fakeredis

    from src.streaming import consumer as consumer_mod

    generator, cfg = gen
    lines = generator.stream_batch(15)

    fake = fakeredis.FakeRedis(decode_responses=True)
    monkeypatch.setattr(consumer_mod.redis, "Redis", None) if False else None

    # Patch build_sink + redis client creation
    from src.sink import DuckDBSink

    created = {}

    class FakeRedisFactory:
        @staticmethod
        def from_url(*a, **kw):
            return fake

    import redis as real_redis
    monkeypatch.setattr(consumer_mod, "redis", type("R", (), {"Redis": FakeRedisFactory}))
    monkeypatch.setattr(consumer_mod, "build_sink", lambda c: created.setdefault("sink", DuckDBSink(c)))

    processed = consumer_mod.run_consumer(max_events=len(lines))
    assert processed == len(lines)

    # second pass: no new events -> consumer idles gracefully (returns same count)
    processed2 = consumer_mod.run_consumer(max_events=len(lines) + 5)
    assert processed2 >= processed


def test_poison_message_does_not_block(gen, tmp_path, monkeypatch):
    import fakeredis

    from src.streaming import consumer as consumer_mod

    generator, cfg = gen
    good = generator.stream_batch(5)

    fake = fakeredis.FakeRedis(decode_responses=True)
    for line in good:
        fake.xadd(cfg["stream"]["key"], {"payload": line})
    fake.xadd(cfg["stream"]["key"], {"payload": "{not json"})

    class FakeRedisFactory:
        @staticmethod
        def from_url(*a, **kw):
            return fake

    from src.sink import DuckDBSink
    monkeypatch.setattr(consumer_mod, "redis", type("R", (), {"Redis": FakeRedisFactory}))
    monkeypatch.setattr(consumer_mod, "build_sink", lambda c: DuckDBSink(c))

    processed = consumer_mod.run_consumer(max_events=len(good))
    assert processed == len(good)
