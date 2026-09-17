"""按大小路由导入后端：CSV < 100MB 走 Parquet，CSV >= 100MB 走 PostgreSQL，XLSX 永远走 Parquet。"""
from pathlib import Path
import json
import os
import pytest

os.environ.pop("PID_STORAGE_BACKEND", None)
os.environ.pop("PID_STORAGE", None)

from app.jobs import pick_backend
from app.importer import import_parquet_only
from app import db


def _sized_file(tmp_path: Path, name: str, size: int) -> Path:
    p = tmp_path / name
    with p.open("wb") as fh:
        fh.truncate(size)
    return p


def _small_csv(tmp_path: Path, name: str = "sample.csv") -> Path:
    path = tmp_path / name
    path.write_text(
        "timestamp,FIC101.PV,FIC101.MV\n"
        "2026-01-01 00:00:00,1.0,10.0\n"
        "2026-01-01 00:00:10,2.0,20.0\n",
        encoding="utf-8",
    )
    return path


def test_small_csv_routes_to_parquet(tmp_path):
    assert pick_backend(_sized_file(tmp_path, "a.csv", 1024), "a.csv") == "parquet"


def test_large_csv_routes_to_postgres(tmp_path):
    big = _sized_file(tmp_path, "b.csv", 100 * 1024 * 1024 + 1)
    assert pick_backend(big, "b.csv") == "postgres"


def test_xlsx_always_routes_to_parquet(tmp_path):
    big_xlsx = _sized_file(tmp_path, "c.xlsx", 200 * 1024 * 1024)
    assert pick_backend(big_xlsx, "c.xlsx") == "parquet"


def test_global_backend_overrides_size_router(tmp_path, monkeypatch):
    """An explicit global backend wins, matching the documented Docker mode."""
    small = _sized_file(tmp_path, "small.csv", 1024)
    monkeypatch.setenv("PID_STORAGE_BACKEND", "postgres")
    assert pick_backend(small, "small.csv") == "postgres"
    monkeypatch.delenv("PID_STORAGE_BACKEND")
    assert pick_backend(small, "small.csv") == "parquet"


def test_import_parquet_only_ignores_global_env(tmp_path, monkeypatch):
    csv_path = _small_csv(tmp_path)
    data_dir = tmp_path / "data"
    monkeypatch.setenv("PID_STORAGE_BACKEND", "postgres")
    try:
        meta = import_parquet_only(csv_path, "ds-test", "sample.csv", data_dir=data_dir)
    finally:
        monkeypatch.delenv("PID_STORAGE_BACKEND")
    assert meta["id"] == "ds-test"
    assert (data_dir / "parquet" / "ds-test").is_dir()
    assert not db.dataset_uses_db("ds-test")


def test_per_dataset_backend_override(tmp_path):
    db.mark_backend("ds-large", "postgres")
    try:
        assert db.dataset_backend("ds-large") == "postgres"
        assert db.dataset_uses_db("ds-large") is True
        assert db.dataset_uses_db("ds-small") is False
    finally:
        db._DATASET_BACKEND.clear()


def test_parquet_import_records_storage_in_metadata(tmp_path):
    """The importer must record its backend so routing survives a restart."""
    csv_path = _small_csv(tmp_path)
    data_dir = tmp_path / "data"
    import_parquet_only(csv_path, "ds-meta", "sample.csv", data_dir=data_dir)

    metadata = json.loads((data_dir / "meta" / "ds-meta.json").read_text(encoding="utf-8"))
    assert metadata["storage"] == "parquet"


def test_backend_resolution_survives_process_restart(tmp_path, monkeypatch):
    """Regression: routing used to live only in an in-memory dict.

    After a restart the dataset must still resolve to its recorded backend, and
    the read paths must send it to the database instead of looking for Parquet
    partitions that were never written.
    """
    data_dir = tmp_path / "data"
    (data_dir / "meta").mkdir(parents=True)
    (data_dir / "meta" / "ds-big.json").write_text(
        json.dumps({"id": "ds-big", "name": "big.csv", "storage": "postgres"}),
        encoding="utf-8",
    )
    db._DATASET_BACKEND.clear()
    monkeypatch.delenv("PID_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("PID_STORAGE", raising=False)

    assert db.dataset_backend("ds-big", data_dir=data_dir) == "postgres"
    assert db.uses_database("ds-big", data_dir=data_dir) is True
    assert db.dataset_uses_postgres("ds-big", data_dir=data_dir) is True


def test_large_dataset_reads_go_to_postgres_not_parquet(tmp_path, monkeypatch):
    """Regression: read paths tested only the global env and returned no rows.

    A dataset routed to PostgreSQL while the process default is Parquet must
    still dispatch its reads to the PostgreSQL adapter.
    """
    from app import query, storage

    data_dir = tmp_path / "data"
    (data_dir / "meta").mkdir(parents=True)
    (data_dir / "meta" / "ds-big.json").write_text(
        json.dumps({"id": "ds-big", "name": "big.csv", "storage": "postgres"}),
        encoding="utf-8",
    )
    db._DATASET_BACKEND.clear()
    monkeypatch.delenv("PID_STORAGE_BACKEND", raising=False)
    monkeypatch.delenv("PID_STORAGE", raising=False)
    monkeypatch.setattr(db, "_meta_root", lambda data_dir=None: tmp_path / "data")

    calls = {}

    def fake_query_frame(dataset_id, start=None, end=None, tags=None):
        calls["query_frame"] = dataset_id
        import pandas as pd
        return pd.DataFrame(columns=["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit"])

    def fake_buckets(dataset_id, start, end, tags=None, seconds=60):
        calls["buckets"] = dataset_id
        import pandas as pd
        return pd.DataFrame(columns=["timestamp", "tag", "type", "value"])

    monkeypatch.setattr(db, "query_frame", fake_query_frame)
    monkeypatch.setattr(storage.db, "query_frame", fake_query_frame)
    monkeypatch.setattr(query.db, "query_buckets", fake_buckets)

    storage.load_frame("ds-big")
    context = query.query_data("ds-big", "2026-01-01T00:00:00", "2026-01-01T02:00:00", resolution="1m")

    assert calls["query_frame"] == "ds-big", "load_frame must route a PostgreSQL dataset to the database"
    assert calls["buckets"] == "ds-big", "query_data must aggregate a PostgreSQL dataset in the database"
    assert context["rows"] == []


def test_small_parquet_dataset_stays_on_parquet_when_global_is_postgres(tmp_path, monkeypatch):
    """A small dataset recorded as Parquet must not be sent to the database."""
    from app import storage

    (tmp_path / "meta").mkdir(parents=True)
    (tmp_path / "meta" / "ds-small.json").write_text(
        json.dumps({"id": "ds-small", "name": "small.csv", "storage": "parquet"}),
        encoding="utf-8",
    )
    db._DATASET_BACKEND.clear()
    monkeypatch.setenv("PID_STORAGE_BACKEND", "postgres")
    monkeypatch.setattr(db, "_meta_root", lambda data_dir=None: tmp_path)

    assert db.uses_database("ds-small") is False

    def explode(*args, **kwargs):  # pragma: no cover - must not be called
        raise AssertionError("Parquet dataset must not be read from PostgreSQL")

    monkeypatch.setattr(storage.db, "query_frame", explode)
    assert db.dataset_uses_postgres("ds-small") is False


def test_import_backend_override_is_thread_scoped(tmp_path, monkeypatch):
    """Concurrent imports must not see each other's backend override."""
    import threading

    monkeypatch.setenv("PID_STORAGE_BACKEND", "postgres")
    observed = {}

    def worker():
        with db.import_backend_override("parquet"):
            observed["inside"] = db.enabled()
        observed["after"] = db.enabled()

    thread = threading.Thread(target=worker)
    thread.start()
    # The main thread keeps the global PostgreSQL backend while the worker
    # forces Parquet only for itself.
    observed["main"] = db.enabled()
    thread.join()

    assert observed["main"] is True
    assert observed["inside"] is False
    assert observed["after"] is True


def test_large_csv_without_database_fails_with_actionable_message(tmp_path, monkeypatch):
    """Routing a large CSV to PostgreSQL with no configuration must explain why."""
    for name in ("PID_POSTGRES_DSN", "PID_POSTGRES_PASSWORD", "PID_POSTGRES_HOST", "PID_POSTGRES_USER"):
        monkeypatch.delenv(name, raising=False)

    with pytest.raises(ValueError) as excinfo:
        db.route_import(
            _sized_file(tmp_path, "big.csv", 1024),
            "ds-big",
            "big.csv",
            tmp_path / "data",
            "postgres",
        )

    assert "PostgreSQL" in str(excinfo.value)
    assert "ds-big" not in db._DATASET_BACKEND
