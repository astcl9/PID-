# -*- coding: utf-8 -*-
import pandas as pd
import pytest
from fastapi.testclient import TestClient

from app import main, storage
from app.main import app

from test_boundaries import _write_partition


@pytest.fixture()
def client(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    monkeypatch.setattr(main, "DATA_DIR", root)
    return TestClient(app)


def _write_days(root, dataset_id, days_and_ts):
    for day, ts_list in days_and_ts:
        _write_partition(root, dataset_id, day, ts_list)


# ---------- HTTP 入口对非法日期/窗口统一返回 400 ----------

@pytest.mark.parametrize("bad_start", ["2026-13-45", "", "12345", "not-a-date"])
def test_api_data_invalid_start_is_400_not_500(client, bad_start):
    response = client.get("/api/data", params={"dataset_id": "d", "start": bad_start, "end": "2026-01-02"})
    assert response.status_code == 400


@pytest.mark.parametrize("bad_end", ["2026-13-45", "", "12345"])
def test_api_data_invalid_end_is_400_not_500(client, bad_end):
    response = client.get("/api/data", params={"dataset_id": "d", "start": "2026-01-01", "end": bad_end})
    assert response.status_code == 400


def test_api_data_equal_and_reversed_window_are_400(client):
    equal = client.get("/api/data", params={"dataset_id": "d", "start": "2026-01-01", "end": "2026-01-01"})
    reversed_ = client.get("/api/data", params={"dataset_id": "d", "start": "2026-01-02", "end": "2026-01-01"})
    assert equal.status_code == 400
    assert reversed_.status_code == 400


def test_api_data_nonexistent_dataset_returns_empty_series(client):
    response = client.get("/api/data", params={"dataset_id": "missing", "start": "2026-01-01", "end": "2026-01-02"})
    assert response.status_code == 200
    assert response.json()["series"] == []


def test_api_segments_invalid_date_is_400(client):
    response = client.get("/api/segments", params={"dataset_id": "d", "date": "2026-13-45"})
    assert response.status_code == 400


def test_api_segments_empty_date_is_400(client):
    # Regression: empty date used to pass _validate_window because
    # pd.Timestamp("") is NaT and NaT >= NaT is False, yielding a 200.
    response = client.get("/api/segments", params={"dataset_id": "d", "date": ""})
    assert response.status_code == 400


def test_api_segments_start_equal_end_is_400(client):
    response = client.get("/api/segments", params={"dataset_id": "d", "start": "2026-01-01T00:00", "end": "2026-01-01T00:00"})
    assert response.status_code == 400


def test_api_segments_nonexistent_dataset_returns_empty_list(client):
    response = client.get("/api/segments", params={"dataset_id": "missing", "start": "2026-01-01", "end": "2026-01-02"})
    assert response.status_code == 200
    assert response.json() == []


def test_api_export_invalid_date_is_400(client):
    response = client.get("/api/export", params={"dataset_id": "d", "date": "2026-13-45"})
    assert response.status_code == 400


def test_api_export_nonexistent_dataset_returns_empty_csv(client):
    response = client.get("/api/export", params={"dataset_id": "d", "date": "2026-01-01"})
    assert response.status_code == 200
    body = response.content.decode("utf-8-sig")
    assert "timestamp" in body.splitlines()[0] and len(body.strip().splitlines()) == 1


def test_api_days_nonexistent_dataset_returns_empty_list(client):
    response = client.get("/api/datasets/missing/days")
    assert response.status_code == 200
    assert response.json() == []


# ---------- 月末/年末/闰年跨日：半开区间不重复、不漏点 ----------

def _span_days(root):
    _write_days(root, "d", [
        ("2026-02-28", ["2026-02-28 23:59:00"]),
        ("2026-03-01", ["2026-03-01 00:00:00"]),
        ("2026-12-31", ["2026-12-31 23:59:00"]),
        ("2027-01-01", ["2027-01-01 00:00:00"]),
        ("2028-02-28", ["2028-02-28 23:59:00"]),
        ("2028-02-29", ["2028-02-29 00:00:00"]),
    ])


def test_month_boundary_feb_to_mar_half_open(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _span_days(root)
    frame = storage.load_frame("d", "2026-02-28T00:00:00", "2026-03-01T00:00:00")
    assert frame["timestamp"].tolist() == [pd.Timestamp("2026-02-28 23:59:00")]


def test_year_boundary_dec_to_jan_half_open(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _span_days(root)
    frame = storage.load_frame("d", "2026-12-31T00:00:00", "2027-01-01T00:00:00")
    assert frame["timestamp"].tolist() == [pd.Timestamp("2026-12-31 23:59:00")]


def test_leap_year_feb29_is_included(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _span_days(root)
    frame = storage.load_frame("d", "2028-02-28T00:00:00", "2028-03-01T00:00:00")
    assert frame["timestamp"].tolist() == [
        pd.Timestamp("2028-02-28 23:59:00"),
        pd.Timestamp("2028-02-29 00:00:00"),
    ]


def test_cross_midnight_window_no_duplicate_no_gap(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _write_days(root, "d", [
        ("2026-01-01", ["2026-01-01 22:30:00", "2026-01-01 23:59:00"]),
        ("2026-01-02", ["2026-01-02 00:00:00", "2026-01-02 01:30:00"]),
    ])
    frame = storage.load_frame("d", "2026-01-01T22:30:00", "2026-01-02T01:30:00")
    got = frame["timestamp"].tolist()
    assert got == [
        pd.Timestamp("2026-01-01 22:30:00"),
        pd.Timestamp("2026-01-01 23:59:00"),
        pd.Timestamp("2026-01-02 00:00:00"),
        ]
    # Half-open [start, end): end exclusive, so querying up to midnight must
    # not duplicate the 00:00 sample into the previous day window.
    before = storage.load_frame("d", "2026-01-01T00:00:00", "2026-01-02T00:00:00")
    after = storage.load_frame("d", "2026-01-02T00:00:00", "2026-01-03T00:00:00")
    assert pd.Timestamp("2026-01-02 00:00:00") not in before["timestamp"].tolist()
    assert pd.Timestamp("2026-01-02 00:00:00") in after["timestamp"].tolist()


# ---------- query_data 降级 / 边界行为 ----------

def test_query_data_max_points_below_one_returns_empty(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _write_days(root, "d", [("2026-01-01", ["2026-01-01 00:00:00"])])
    from app import query
    payload = query.query_data("d", "2026-01-01T00:00:00", "2026-01-02T00:00:00", max_points=0)
    assert payload["rows"] == []


def test_query_data_raw_large_span_downgrades_resolution(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    from app import query
    payload = query.query_data("d", "2026-01-01T00:00:00", "2026-01-11T00:00:00", resolution="raw")
    assert payload["resolution"] == "1m"
    payload2 = query.query_data("d", "2026-01-01T00:00:00", "2026-01-03T00:00:00", resolution="raw")
    assert payload2["resolution"] == "10s"


def test_query_data_invalid_window_returns_empty_rows(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    from app import query
    payload = query.query_data("d", "2026-01-02T00:00:00", "2026-01-01T00:00:00")
    assert payload["rows"] == []


def test_query_data_cross_midnight_bucket_no_duplicate(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _write_days(root, "d", [
        ("2026-01-01", ["2026-01-01 23:59:00"]),
        ("2026-01-02", ["2026-01-02 00:00:00"]),
    ])
    from app import query
    payload = query.query_data("d", "2026-01-01T22:00:00", "2026-01-02T02:00:00", resolution="1m")
    stamps = [row["timestamp"] for row in payload["rows"]]
    assert stamps == sorted(stamps)
    values = [row["value"] for row in payload["rows"]]
    assert 1 not in [v for v in values if values.count(v) > 1]  # no exact duplicate rows


