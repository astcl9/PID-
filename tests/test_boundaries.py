import pandas as pd

from app import storage


def _write_partition(root, dataset_id, day, timestamps):
    target = root / "parquet" / dataset_id / f"date={day}"
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "dataset_id": dataset_id,
        "timestamp": pd.to_datetime(timestamps),
        "tag": "A.PV",
        "type": "PV",
        "value": range(len(timestamps)),
        "quality": "good",
        "unit": None,
    }).to_parquet(target / "data.parquet", index=False)


def test_midnight_window_is_half_open(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    _write_partition(root, "d", "2026-01-01", ["2026-01-01 23:59:00"])
    _write_partition(root, "d", "2026-01-02", ["2026-01-02 00:00:00"])

    frame = storage.load_frame("d", "2026-01-01T00:00:00", "2026-01-02T00:00:00")

    assert frame["timestamp"].tolist() == [pd.Timestamp("2026-01-01 23:59:00")]


def test_empty_dataset_and_invalid_range_return_empty_frames(tmp_path, monkeypatch):
    monkeypatch.setattr(storage, "DATA_DIR", tmp_path / "data")

    empty = storage.load_frame("missing", "2026-01-01", "2026-01-02")
    reversed_window = storage.load_frame("missing", "2026-01-02", "2026-01-01")

    assert empty.empty
    assert reversed_window.empty


def test_duckdb_bucket_query_preserves_spike_and_excludes_end(tmp_path, monkeypatch):
    root = tmp_path / "data"
    monkeypatch.setattr(storage, "DATA_DIR", root)
    target = root / "parquet" / "d" / "date=2026-01-01"
    target.mkdir(parents=True, exist_ok=True)
    pd.DataFrame({
        "dataset_id": "d",
        "timestamp": pd.to_datetime([
            "2026-01-01 00:00:00",
            "2026-01-01 00:00:20",
            "2026-01-01 00:01:00",
            "2026-01-02 00:00:00",
        ]),
        "tag": "A.PV",
        "type": "PV",
        "value": [1.0, 99.0, 2.0, 1000.0],
        "quality": "good",
        "unit": None,
    }).to_parquet(target / "data.parquet", index=False)

    frame = storage.query_buckets("d", "2026-01-01T00:00:00", "2026-01-02T00:00:00", seconds=60)

    assert 99.0 in frame["value"].tolist()
    assert 1000.0 not in frame["value"].tolist()
