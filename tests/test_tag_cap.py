import json

import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import jobs, main, storage
from app.main import app


@pytest.fixture()
def workspace(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    monkeypatch.setattr(main, "DATA_DIR", root)
    monkeypatch.setattr(jobs, "DATA_DIR", root)
    jobs._jobs.clear()
    target = root / "parquet" / "d" / "date=2026-01-01"
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "dataset_id": "d",
        "timestamp": pd.to_datetime(["2026-01-01 00:00:00", "2026-01-01 00:01:00"] * 2),
        "tag": ["A.PV", "A.PV", "B.PV", "B.PV"],
        "type": ["PV", "PV", "PV", "PV"],
        "value": [1.0, 2.0, 3.0, 4.0],
        "quality": ["good"] * 4,
        "unit": [None] * 4,
    }).to_parquet(target / "data.parquet", index=False)
    meta_dir = root / "meta"
    meta_dir.mkdir(parents=True, exist_ok=True)
    (meta_dir / "d.json").write_text(json.dumps({
        "id": "d", "name": "d.csv", "rows": 4,
        "signals": [{"tag": "A.PV", "type": "PV"}, {"tag": "B.PV", "type": "PV"}],
        "start_time": "2026-01-01 00:00:00", "end_time": "2026-01-01 00:01:00",
        "storage": "parquet",
    }), encoding="utf-8")
    return root


def _data(params):
    return TestClient(app).get("/api/data", params=params)


def test_dataset_list_does_not_embed_all_signal_metadata(workspace):
    response = TestClient(app).get("/api/datasets")

    assert response.status_code == 200
    item = response.json()[0]
    assert item["signal_count"] == 2
    assert item["signals"] == []


def test_unknown_tag_returns_empty_rows(workspace):
    response = _data({"dataset_id": "d", "start": "2026-01-01", "end": "2026-01-02",
                      "tags": "__none__", "resolution": "1m"})

    assert response.status_code == 200
    assert response.json()["series"] == []


def test_known_tag_filters_to_matching_series(workspace):
    response = _data({"dataset_id": "d", "start": "2026-01-01", "end": "2026-01-02",
                      "tags": "A.PV", "resolution": "1m"})

    assert response.status_code == 200
    rows = response.json()["series"]
    assert rows
    assert {s["name"] for s in rows} == {"A.PV"}


def test_omitted_tags_keeps_all_series(workspace):
    response = _data({"dataset_id": "d", "start": "2026-01-01", "end": "2026-01-02",
                      "resolution": "1m"})

    assert response.status_code == 200
    assert {s["name"] for s in response.json()["series"]} == {"A.PV", "B.PV"}
