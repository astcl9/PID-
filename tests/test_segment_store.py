# -*- coding: utf-8 -*-
"""Tests for the SQLite segment store, COPY buffer serialiser, and the
fallback connection pool."""
import json
from datetime import datetime
from unittest import mock

import pandas as pd
import pytest

from app import segment_store


def _item(seg_id, **kw):
    item = {"id": seg_id, "type": "steady", "confidence": 0.8,
            "start": datetime(2026, 1, 1, 8, 0), "end": datetime(2026, 1, 1, 9, 0),
            "reason": "PV波动较小"}
    item.update(kw)
    return item


# ---------- segment_store ----------

def test_put_get_roundtrip(tmp_path):
    store = tmp_path / "segments"
    items = [_item("a"), _item("b")]
    segment_store.put(store, "ds", "ds:2026-01-01T00:00:00:2026-01-02T00:00:", items)
    got = segment_store.get(store, "ds", "ds:2026-01-01T00:00:00:2026-01-02T00:00:")
    assert [g["id"] for g in got] == ["a", "b"]
    # datetimes round-trip through default=str like the old JSON store
    assert got[0]["start"] == "2026-01-01 08:00:00"


def test_get_missing_key_returns_none(tmp_path):
    assert segment_store.get(tmp_path / "segments", "ds", "nope") is None


def test_empty_list_is_cached_not_missing(tmp_path):
    store = tmp_path / "segments"
    segment_store.put(store, "ds", "k", [])
    assert segment_store.get(store, "ds", "k") == []
    assert segment_store.get(store, "ds", "other") is None


def test_put_replaces_previous_items_for_key(tmp_path):
    store = tmp_path / "segments"
    segment_store.put(store, "ds", "k", [_item("a"), _item("b")])
    segment_store.put(store, "ds", "k", [_item("c")])
    assert [g["id"] for g in segment_store.get(store, "ds", "k")] == ["c"]


def test_find_across_datasets_and_save_item(tmp_path):
    store = tmp_path / "segments"
    segment_store.put(store, "ds1", "k1", [_item("x", note="old")])
    segment_store.put(store, "ds2", "k2", [_item("y")])
    found = segment_store.find(store, "y")
    assert found is not None
    dataset_id, key, item = found
    assert (dataset_id, key, item["id"]) == ("ds2", "k2", "y")
    assert segment_store.find(store, "missing") is None

    item["note"] = "人工复核"
    item["manual_override"] = True
    segment_store.save_item(store, dataset_id, key, item)
    cached = segment_store.get(store, dataset_id, key)
    assert cached[0]["note"] == "人工复核"
    # Other key's items untouched
    assert segment_store.get(store, "ds1", "k1")[0]["note"] == "old"


def test_delete_dataset_only_removes_that_dataset(tmp_path):
    store = tmp_path / "segments"
    segment_store.put(store, "ds1", "k", [_item("a")])
    segment_store.put(store, "ds2", "k", [_item("b")])
    removed = segment_store.delete_dataset(store, "ds1")
    assert removed >= 1
    assert segment_store.get(store, "ds1", "k") is None
    assert [g["id"] for g in segment_store.get(store, "ds2", "k")] == ["b"]


def test_legacy_json_store_is_migrated(tmp_path):
    store = tmp_path / "segments"
    store.mkdir(parents=True)
    full_key = "ds:2026-01-01T00:00:00:2026-01-02T00:00:"
    legacy = {full_key: [_item("legacy1")]}
    (store / "ds.json").write_text(json.dumps(legacy, default=str, ensure_ascii=False),
                                   encoding="utf-8")
    got = segment_store.get(store, "ds", full_key)
    assert [g["id"] for g in got] == ["legacy1"]
    # The old file is renamed aside so the migration runs only once.
    assert not (store / "ds.json").exists()
    assert (store / "ds.migrated.json").exists()
    # And a later put writes only to SQLite without resurrecting the JSON.
    segment_store.put(store, "ds", full_key, [_item("new1")])
    assert [g["id"] for g in segment_store.get(store, "ds", full_key)] == ["new1"]
    migrated = json.loads((store / "ds.migrated.json").read_text(encoding="utf-8"))
    assert [g["id"] for g in migrated[full_key]] == ["legacy1"]


# ---------- COPY buffer ----------

def _frame(**kw):
    rows = {
        "dataset_id": "ds",
        "timestamp": pd.to_datetime(["2026-01-01 08:00:00", "2026-01-01 08:01:00"]),
        "tag": ["L1.pv", "L1.sv"],
        "type": ["PV", "SV"],
        "value": [1.5, None],
        "quality": ["good", "missing"],
        "unit": ["%", None],
        "_ingest_seq": [0, 1],
    }
    rows.update(kw)
    return pd.DataFrame(rows)


def test_copy_buffer_nulls_and_offsets():
    from app import postgres_db
    buf = postgres_db._copy_buffer(_frame()).decode("utf-8")
    lines = buf.splitlines()
    assert len(lines) == 2
    fields = lines[0].split("\t")
    assert fields == ["ds", "2026-01-01 08:00:00+0800", "L1.pv", "PV", "1.5", "good", "%", "0"]
    fields2 = lines[1].split("\t")
    assert fields2[4] == "\\N" and fields2[6] == "\\N"


def test_copy_buffer_escapes_special_characters():
    from app import postgres_db
    frame = _frame(tag=["we\tir\\d\nname", "ok"], type=["PV", "SV"])
    buf = postgres_db._copy_buffer(frame).decode("utf-8")
    first = buf.splitlines()[0]
    assert "we\\tir\\\\d\\nname" in first


# ---------- fallback pool ----------

class _FakeInfo:
    def __init__(self, status=0):
        self.transaction_status = status


class _FakeConn:
    def __init__(self):
        self.closed = False
        self.info = _FakeInfo()
        self.rollbacks = 0
        self.closes = 0

    def rollback(self):
        self.rollbacks += 1

    def close(self):
        self.closes += 1


def test_simple_pool_reuses_connections():
    from app import postgres_db
    made = []

    def factory():
        conn = _FakeConn()
        made.append(conn)
        return conn

    pool = postgres_db._SimplePool(max_size=2, factory=factory)
    with pool.connection() as c1:
        c1.info.transaction_status = 2  # simulate open transaction
    assert c1.rollbacks == 1, "returned connection must be rolled back before reuse"
    with pool.connection() as c2:
        assert c2 is c1, "idle connection should be reused"
    assert len(made) == 1


def test_simple_pool_drops_closed_connections():
    from app import postgres_db
    pool = postgres_db._SimplePool(max_size=2, factory=_FakeConn)
    with pool.connection() as c1:
        c1.closed = True
    with pool.connection() as c2:
        assert c2 is not c1


def test_connection_context_uses_pool(monkeypatch):
    from app import postgres_db
    pool = postgres_db._SimplePool(max_size=1, factory=_FakeConn)
    monkeypatch.setattr(postgres_db, "_POOL", pool)
    with postgres_db._connection() as conn:
        assert conn is not None
