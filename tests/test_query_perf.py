# -*- coding: utf-8 -*-
"""Regression tests for the query-path optimisations: meta caching, day-summary
write-back, vectorised ISO timestamps, and vectorised /api/data grouping."""
import json
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main, storage
from app.main import app
from app.query import _iso_milliseconds

from test_boundaries import _write_partition


def _write_days(root, dataset_id, days):
    """Write small multi-tag partitions used by query performance tests."""
    for day, timestamps in days:
        target = root / "parquet" / dataset_id / f"date={day}"
        target.mkdir(parents=True, exist_ok=True)
        rows = []
        for timestamp in timestamps:
            rows.extend({
                "dataset_id": dataset_id,
                "timestamp": pd.Timestamp(timestamp),
                "tag": "A.PV",
                "type": "PV",
                "value": 1.0,
                "quality": "good",
                "unit": None,
            } for _ in range(50))
        pd.DataFrame(rows).to_parquet(target / "data.parquet", index=False)


@pytest.fixture()
def client(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    monkeypatch.setattr(main, "DATA_DIR", root)
    return TestClient(app)


# ---------- list_meta caching ----------

def test_list_meta_picks_up_external_writes(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    (root / "meta").mkdir(parents=True)
    payload = {"id": "d1", "signals": [], "storage": "parquet"}
    (root / "meta" / "d1.json").write_text(json.dumps(payload), encoding="utf-8")
    assert storage.list_meta() == [payload]
    # Simulate an importer finishing in another thread/process: same path,
    # new content. The mtime signature must invalidate the cached parse.
    payload2 = {"id": "d2", "signals": [{"tag": "L1.pv", "type": "pv"}], "storage": "parquet"}
    (root / "meta" / "d2.json").write_text(json.dumps(payload2), encoding="utf-8")
    assert any(item.get("id") == "d2" for item in storage.list_meta())
    # Deleted metadata must also disappear from subsequent reads.
    (root / "meta" / "d2.json").unlink()
    assert storage.list_meta() == [payload]


# ---------- days endpoint write-back ----------

def _two_day_dataset(root):
    _write_partition(root, "d", "2026-01-01", ["2026-01-01 00:00:00", "2026-01-01 00:01:00"])
    _write_partition(root, "d", "2026-01-02", ["2026-01-02 00:00:00"])
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "d.json").write_text(json.dumps(
        {"id": "d", "name": "d.csv", "signals": [{"tag": "L1.pv", "type": "PV"}],
         "storage": "parquet"}), encoding="utf-8")


def test_days_fallback_persists_summaries(tmp_path, monkeypatch, client):
    root = tmp_path / "data"
    _two_day_dataset(root)
    first = client.get("/api/datasets/d/days")
    assert first.status_code == 200
    days = first.json()
    assert [d["date"] for d in days] == ["2026-01-01", "2026-01-02"]
    # The computed summaries must be written back to the metadata file so the
    # next request takes the cached branch instead of re-segmenting.
    meta = json.loads((root / "meta" / "d.json").read_text(encoding="utf-8"))
    assert meta["day_summaries"] == days
    second = client.get("/api/datasets/d/days")
    assert second.status_code == 200
    assert second.json() == days


def test_days_cached_branch_after_import(tmp_path, monkeypatch, client):
    root = tmp_path / "data"
    _two_day_dataset(root)
    meta = json.loads((root / "meta" / "d.json").read_text(encoding="utf-8"))
    meta["days"] = ["2026-01-01"]
    (root / "meta" / "d.json").write_text(json.dumps(meta), encoding="utf-8")
    response = client.get("/api/datasets/d/days")
    assert response.status_code == 200
    days = response.json()
    assert [item["date"] for item in days] == ["2026-01-01", "2026-01-02"]
    assert all(item["segmentCount"] > 0 for item in days)
    upgraded = json.loads((root / "meta" / "d.json").read_text(encoding="utf-8"))
    assert upgraded["segment_summaries_ready"] is True


# ---------- vectorised ISO formatting ----------

def test_iso_milliseconds_matches_legacy_lambda():
    naive = pd.Series(pd.to_datetime(["2026-01-01 08:00:00.123456", "2026-06-30 23:59:59.9999"]))
    legacy_naive = naive.map(lambda x: x.isoformat(timespec="milliseconds"))
    assert _iso_milliseconds(naive).tolist() == legacy_naive.tolist()

    utc = pd.Series(pd.to_datetime(["2026-01-01 08:00:00.123456"]).tz_localize("UTC"))
    legacy_utc = utc.map(lambda x: x.isoformat(timespec="milliseconds").replace("+00:00", "Z"))
    assert _iso_milliseconds(utc).tolist() == legacy_utc.tolist()

    shanghai = pd.Series(pd.to_datetime(["2026-01-01 08:00:00.123456"]).tz_localize("Asia/Shanghai"))
    legacy_sh = shanghai.map(lambda x: x.isoformat(timespec="milliseconds").replace("+00:00", "Z"))
    assert _iso_milliseconds(shanghai).tolist() == legacy_sh.tolist()


# ---------- /api/data grouping ----------

def test_cap_bucket_coarsens_only_for_many_tags():
    from app.query import _cap_bucket
    day = (pd.Timestamp("2026-01-02") - pd.Timestamp("2026-01-01"))
    start, end = pd.Timestamp("2026-01-01"), pd.Timestamp("2026-01-02")
    # few tags: keep 1m for a full day
    assert _cap_bucket("1m", start, end, 1) == "1m"
    assert _cap_bucket("1m", start, end, 10) == "1m"
    # many tags: coarsen until the per-tag budget fits
    assert _cap_bucket("1m", start, end, 50) == "10m"
    # no tag info: unchanged
    assert _cap_bucket("1m", start, end, 0) == "1m"
    # already coarsest: stays
    assert _cap_bucket("1h", start, end, 500) == "1h"
    # raw is not a bucket: untouched
    assert _cap_bucket("raw", start, end, 500) == "raw"


def test_query_frame_returns_dataframe_with_many_tags(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _write_days(root, "d", [("2026-01-01", ["2026-01-01 00:00:00", "2026-01-01 00:01:00"])])
    from app import query
    tags = [f"L{i}.pv" for i in range(50)]
    resolution, frame = query.query_frame("d", "2026-01-01T00:00:00", "2026-01-02T00:00:00",
                                          tags=tags, resolution="1m")
    assert resolution == "10m", "50 tags over a day must coarsen to fit the point budget"
    assert isinstance(frame, pd.DataFrame)
    assert {"timestamp", "tag", "type", "value"} <= set(frame.columns)


def test_query_data_many_tags_reports_coarsened_resolution(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _write_days(root, "d", [("2026-01-01", ["2026-01-01 00:00:00"])])
    from app import query
    tags = [f"L{i}.pv" for i in range(50)]
    payload = query.query_data("d", "2026-01-01T00:00:00", "2026-01-02T00:00:00",
                               tags=tags, resolution="1m")
    assert payload["resolution"] == "10m"


def test_api_data_groups_series_with_none_values(tmp_path, monkeypatch, client):
    root = tmp_path / "data"
    _write_partition(root, "d", "2026-01-01", ["2026-01-01 00:00:00", "2026-01-01 00:01:00"])
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "d.json").write_text(json.dumps(
        {"id": "d", "name": "d.csv", "signals": [{"tag": "A.PV", "type": "pv"}],
         "storage": "parquet"}), encoding="utf-8")
    response = client.get("/api/data", params={
        "dataset_id": "d", "start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00",
        "resolution": "raw"})
    assert response.status_code == 200
    series = response.json()["series"]
    assert len(series) == 1
    assert series[0]["name"] == "A.PV"
    assert series[0]["type"] == "PV"  # mapping applied + upper-cased
    assert series[0]["x"] == ["2026-01-01T00:00:00.000", "2026-01-01T00:01:00.000"]
    assert series[0]["y"] == [0.0, 1.0]


def test_api_data_loop_filter_keeps_only_matching_tags(tmp_path, monkeypatch, client):
    root = tmp_path / "data"
    _write_partition(root, "d", "2026-01-01", ["2026-01-01 00:00:00"])
    _write_partition(root, "d2", "2026-01-01", ["2026-01-01 00:00:00"])
    (root / "meta").mkdir(parents=True, exist_ok=True)
    (root / "meta" / "d.json").write_text(json.dumps(
        {"id": "d", "name": "d.csv", "signals": [{"tag": "L1.pv", "type": "pv"}],
         "storage": "parquet"}), encoding="utf-8")
    # Re-tag second partition under a different loop by writing it as its own dataset.
    (root / "meta" / "d2.json").write_text(json.dumps(
        {"id": "d2", "name": "d2.csv", "signals": [{"tag": "L2.pv", "type": "pv"}],
         "storage": "parquet"}), encoding="utf-8")
    # Merge both tags into one dataset's partition for the filter test.
    import pyarrow.parquet as pq
    import pyarrow as pa
    frame = pd.DataFrame({
        "dataset_id": ["d", "d"],
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 00:00:00"]),
        "tag": ["L1.pv", "L2.pv"],
        "type": ["pv", "pv"],
        "value": [1.0, 2.0],
        "quality": ["good", "good"],
        "unit": [None, None],
    })
    table = pa.Table.from_pandas(frame, preserve_index=False)
    import shutil
    shutil.rmtree(root / "parquet" / "d", ignore_errors=True)
    dest = root / "parquet" / "d" / "date=2026-01-01"
    dest.mkdir(parents=True)
    pq.write_table(table, dest / "data.parquet")
    response = client.get("/api/data", params={
        "dataset_id": "d", "start": "2026-01-01T00:00:00", "end": "2026-01-02T00:00:00",
        "resolution": "raw", "loop": "L2"})
    assert response.status_code == 200
    series = response.json()["series"]
    assert [s["name"] for s in series] == ["L2.pv"]
    assert series[0]["y"] == [2.0]
