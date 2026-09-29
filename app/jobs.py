"""Serialized import queue with lightweight SQLite job persistence."""
from __future__ import annotations

from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
import json
import os
from pathlib import Path
import sqlite3
from threading import Lock
from uuid import uuid4

from .config import DATA_DIR, IMPORT_WORKERS
from .importer import import_dataset, import_parquet_only

# Concurrent imports share bounded per-chunk buffers, but the cross-chunk
# dedupe compaction can use PID_DUCKDB_MEMORY_LIMIT per worker for smaller
# files, so the pool size is a deliberate trade-off exposed via
# PID_IMPORT_WORKERS (default 2).
_executor = ThreadPoolExecutor(max_workers=IMPORT_WORKERS, thread_name_prefix="pid-import")
_jobs: dict[str, dict] = {}
_lock = Lock()

def _now():
    return datetime.now(timezone.utc).isoformat()


def _db_path() -> Path:
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    return DATA_DIR / "jobs.sqlite3"


def _connect():
    conn = sqlite3.connect(_db_path(), timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.execute("CREATE TABLE IF NOT EXISTS jobs (job_id TEXT PRIMARY KEY, payload TEXT NOT NULL, updated_at TEXT NOT NULL)")
    return conn


def _persist_job(item: dict) -> None:
    payload = json.dumps(item, default=str, ensure_ascii=False)
    with _connect() as conn:
        conn.execute(
            "INSERT INTO jobs(job_id,payload,updated_at) VALUES(?,?,?) "
            "ON CONFLICT(job_id) DO UPDATE SET payload=excluded.payload,updated_at=excluded.updated_at",
            (item["job_id"], payload, item.get("updated_at", item.get("created_at", _now()))),
        )


def _load_job(job_id: str) -> dict | None:
    with _connect() as conn:
        row = conn.execute("SELECT payload FROM jobs WHERE job_id=?", (str(job_id),)).fetchone()
    return json.loads(row[0]) if row else None


def recover_interrupted_jobs() -> int:
    """Mark unfinished jobs failed after a process restart."""
    recovered = 0
    with _connect() as conn:
        rows = conn.execute("SELECT job_id,payload FROM jobs").fetchall()
        for job_id, payload in rows:
            item = json.loads(payload)
            if item.get("status") not in {"queued", "processing"}:
                continue
            item.update({"status": "failed", "error": "服务重启导致导入中断，请重新上传文件", "updated_at": _now()})
            conn.execute(
                "UPDATE jobs SET payload=?,updated_at=? WHERE job_id=?",
                (json.dumps(item, ensure_ascii=False), item["updated_at"], job_id),
            )
            recovered += 1
    return recovered


def job_summary() -> dict[str, int]:
    counts = {"queued": 0, "processing": 0, "completed": 0, "failed": 0}
    with _connect() as conn:
        for (payload,) in conn.execute("SELECT payload FROM jobs"):
            status = str(json.loads(payload).get("status", ""))
            if status in counts:
                counts[status] += 1
    return counts

def pick_backend(path: Path, filename: str) -> str:
    """Resolve the import backend.

    Parquet is the only production backend: the 2026-09-18 1 GB benchmark
    (docs/benchmark-1gb-*.json) showed PostgreSQL imports 8x slower, uses 32x
    more disk, and its all-day query takes 80 s vs 0.13 s. PostgreSQL remains
    available as an explicit opt-in experiment via PID_STORAGE_BACKEND=postgres
    (or PID_STORAGE); there is no automatic size-based routing.
    """
    configured = os.getenv("PID_STORAGE_BACKEND", os.getenv("PID_STORAGE", "parquet")).strip().lower()
    if configured in {"postgres", "postgresql", "clickhouse"}:
        return configured
    return "parquet"


def create_job(path: Path, dataset_id: str, filename: str, backend: str | None = None,
               fingerprint: dict | None = None) -> str:
    job_id = uuid4().hex
    with _lock:
        _jobs[job_id] = {"job_id": job_id, "status": "queued", "progress": 0, "backend": backend,
                         "fingerprint": fingerprint,
                         "rows_processed": 0, "bytes_processed": 0,
                         "total_bytes": path.stat().st_size, "filename": filename,
                         "created_at": _now()}
        _persist_job(_jobs[job_id])
    _executor.submit(_run, job_id, path, dataset_id, filename, backend)
    return job_id

def _run(job_id, path, dataset_id, filename, backend=None):
    def progress(**values):
        with _lock:
            item = _jobs.get(job_id)
            if item:
                item.update(values)
                item["status"] = "processing" if values.get("stage") != "completed" else "completed"
                item["updated_at"] = _now()
                _persist_job(item)
    progress(stage="processing")
    try:
        if backend == "postgres":
            from . import db
            metadata = db.route_import(path, dataset_id, filename, DATA_DIR, "postgres", progress=progress)
        elif backend == "parquet":
            metadata = import_parquet_only(path, dataset_id, filename, data_dir=DATA_DIR, progress=progress)
        else:
            metadata = import_dataset(path, dataset_id, filename, data_dir=DATA_DIR, progress=progress)
        # Attach the upload-time content fingerprint to the persisted metadata
        # so future uploads of the same file can be flagged as duplicates.
        fingerprint = _jobs.get(job_id, {}).get("fingerprint")
        if fingerprint and isinstance(metadata, dict):
            metadata = dict(metadata)
            metadata["fingerprint"] = fingerprint
            metadata.setdefault("source_name", filename)
            metadata.setdefault("source_size", fingerprint.get("size"))
            meta_path = DATA_DIR / "meta" / f"{dataset_id}.json"
            try:
                meta_path.write_text(json.dumps(metadata, default=str, ensure_ascii=False), encoding="utf-8")
            except OSError:
                pass
        with _lock:
            _jobs[job_id].update({"status": "completed", "progress": 1, "metadata": metadata, "updated_at": _now()})
            _persist_job(_jobs[job_id])
    except Exception as exc:
        with _lock:
            _jobs[job_id].update({"status": "failed", "error": str(exc), "updated_at": _now()})
            _persist_job(_jobs[job_id])
    finally:
        Path(path).unlink(missing_ok=True)

def get_job(job_id: str):
    with _lock:
        item = _jobs.get(job_id)
        if item:
            return dict(item)
    return _load_job(job_id)


recover_interrupted_jobs()

