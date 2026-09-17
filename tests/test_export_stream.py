"""Regression tests for bounded-memory segment and export paths."""

import pandas as pd


def _two_hour_dataset(tmp_path):
    source = tmp_path / "hist.csv"
    ts = pd.date_range("2026-01-01", periods=120, freq="min")
    rows = []
    for i, t in enumerate(ts):
        sp = 10 if i < 60 else 20
        rows.append((t, "A.PV", sp + 0.1 * (i % 5)))
        rows.append((t, "A.SV", sp))
        rows.append((t, "A.MV", 40.0 + i % 10))
    frame = pd.DataFrame(rows, columns=["timestamp", "tag", "value"])
    wide = frame.pivot(index="timestamp", columns="tag", values="value").reset_index()
    wide.to_csv(source, index=False)
    return source


def _client(tmp_path, monkeypatch):
    from app.importer import import_dataset
    from fastapi.testclient import TestClient
    import app.config as config
    import app.storage as storage
    from app import main

    data_dir = tmp_path / "data"
    metadata = import_dataset(_two_hour_dataset(tmp_path), "ds", "hist.csv", data_dir=data_dir)
    monkeypatch.setattr(config, "DATA_DIR", data_dir)
    monkeypatch.setattr(storage, "DATA_DIR", data_dir)
    monkeypatch.setattr(main, "DATA_DIR", data_dir)
    monkeypatch.setattr(main, "SEGMENTS", {})
    return TestClient(main.app), metadata


def test_export_streams_parquet_csv_without_full_load(tmp_path, monkeypatch):
    client, metadata = _client(tmp_path, monkeypatch)
    resp = client.get("/api/export", params={"dataset_id": "ds", "date": "2026-01-01"})

    assert resp.status_code == 200
    assert resp.content.startswith(b"\xef\xbb\xbf"), "CSV should carry a UTF-8 BOM"
    lines = resp.content.decode("utf-8-sig").splitlines()
    assert lines[0] == "dataset_id,timestamp,tag,type,value,quality,unit"
    assert len(lines) - 1 == metadata["rows"]


def test_segments_endpoint_uses_bucketed_query(tmp_path, monkeypatch):
    client, _ = _client(tmp_path, monkeypatch)
    resp = client.get("/api/segments", params={"dataset_id": "ds", "date": "2026-01-01"})

    assert resp.status_code == 200
    segments = resp.json()
    assert isinstance(segments, list) and segments
    assert any(s["type"] == "setpoint_change" for s in segments)


def test_segment_patch_persists_without_in_memory_cache(tmp_path, monkeypatch):
    from app import main

    client, _ = _client(tmp_path, monkeypatch)
    resp = client.get("/api/segments", params={"dataset_id": "ds", "date": "2026-01-01"})
    assert resp.status_code == 200
    segments = resp.json()
    assert segments
    seg_id = segments[0]["id"]

    # Simulate a service restart by dropping the in-memory cache; the PATCH
    # must then locate and persist the correction via the on-disk store.
    monkeypatch.setattr(main, "SEGMENTS", {})
    resp = client.patch(f"/api/segments/{seg_id}", params={"note": "人工复核", "type": "steady"})
    assert resp.status_code == 200
    assert resp.json()["manual_override"] is True
    assert resp.json()["note"] == "人工复核"

    # A fresh read (again without cache) must reflect the persisted correction.
    monkeypatch.setattr(main, "SEGMENTS", {})
    resp2 = client.get("/api/segments", params={"dataset_id": "ds", "date": "2026-01-01"})
    updated = {s["id"]: s for s in resp2.json()}
    assert updated[seg_id]["note"] == "人工复核"
    assert updated[seg_id]["manual_override"] is True


def test_segment_cache_is_bounded(tmp_path, monkeypatch):
    from app import main

    client, _ = _client(tmp_path, monkeypatch)
    monkeypatch.setattr(main, "SEGMENT_CACHE_MAX", 2)
    monkeypatch.setattr(main, "SEGMENTS", {})

    windows = [
        ("2026-01-01T00:00:00", "2026-01-01T00:30:00"),
        ("2026-01-01T00:30:00", "2026-01-01T01:00:00"),
        ("2026-01-01T01:00:00", "2026-01-01T01:30:00"),
        ("2026-01-01T01:30:00", "2026-01-01T02:00:00"),
    ]
    for start, end in windows:
        resp = client.get("/api/segments", params={"dataset_id": "ds", "start": start, "end": end})
        assert resp.status_code == 200

    assert len(main.SEGMENTS) <= 2
