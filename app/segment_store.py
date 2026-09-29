"""SQLite-backed store for computed segmentations.

Replaces the previous per-dataset JSON files, which rewrote the whole store on
every new window (write amplification growing with the number of keys) and
forced every PATCH to scan and parse every dataset's store. Row-level inserts
and an index on segment id fix both. Legacy ``<dataset_id>.json`` files are
migrated once on first access and renamed aside.
"""
from __future__ import annotations

import json
import sqlite3
import threading
from pathlib import Path

_LOCK = threading.Lock()
_MIGRATED: set[str] = set()

# Sentinel row marking that a key was stored with an empty segment list, so
# get() can distinguish "cached empty result" from "never computed". Real
# segment ids are uuid4 hex and can never collide.
_EMPTY_KEY_ID = "__key__"

_SCHEMA = """
CREATE TABLE IF NOT EXISTS segments (
    dataset_id TEXT NOT NULL,
    key TEXT NOT NULL,
    id TEXT NOT NULL,
    payload TEXT NOT NULL,
    PRIMARY KEY (dataset_id, key, id)
);
CREATE INDEX IF NOT EXISTS segments_id_idx ON segments(id);
"""


def _open(store_dir: Path) -> sqlite3.Connection:
    store_dir.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(store_dir / "segments.sqlite3", timeout=30)
    conn.execute("PRAGMA journal_mode=WAL")
    conn.executescript(_SCHEMA)
    return conn


def _migrate_legacy(store_dir: Path) -> None:
    """Import pre-SQLite per-dataset JSON stores once, then rename them aside."""
    with _LOCK:
        if str(store_dir) in _MIGRATED:
            return
        try:
            legacy = sorted(store_dir.glob("*.json"))
        except OSError:
            legacy = []
        for path in legacy:
            try:
                store = json.loads(path.read_text(encoding="utf-8"))
            except (ValueError, OSError, TypeError):
                continue
            if not isinstance(store, dict):
                continue
            rows = []
            for key, items in store.items():
                if not isinstance(items, list):
                    continue
                rows.extend(
                    (path.stem, str(key), str(item.get("id")),
                     json.dumps(item, ensure_ascii=False, default=str))
                    for item in items if isinstance(item, dict)
                )
            if rows:
                with _open(store_dir) as conn:
                    conn.executemany(
                        "INSERT OR REPLACE INTO segments(dataset_id,key,id,payload) VALUES(?,?,?,?)",
                        rows,
                    )
            try:
                path.rename(path.with_suffix(".migrated.json"))
            except OSError:
                pass
        _MIGRATED.add(str(store_dir))


def get(store_dir: Path, dataset_id: str, key: str) -> list[dict] | None:
    """Return the cached segmentation for one window, or None if never stored."""
    _migrate_legacy(store_dir)
    with _open(store_dir) as conn:
        rows = conn.execute(
            "SELECT id, payload FROM segments WHERE dataset_id=? AND key=? ORDER BY rowid",
            (str(dataset_id), str(key)),
        ).fetchall()
    if not rows:
        return None
    return [json.loads(payload) for rid, payload in rows if rid != _EMPTY_KEY_ID]


def put(store_dir: Path, dataset_id: str, key: str, items: list[dict]) -> None:
    _migrate_legacy(store_dir)
    rows = [(_EMPTY_KEY_ID, "[]")]
    rows.extend(
        (str(item.get("id")), json.dumps(item, ensure_ascii=False, default=str))
        for item in items
    )
    with _open(store_dir) as conn:
        conn.execute("DELETE FROM segments WHERE dataset_id=? AND key=?", (str(dataset_id), str(key)))
        conn.executemany(
            "INSERT OR REPLACE INTO segments(dataset_id,key,id,payload) VALUES(?,?,?,?)",
            [(str(dataset_id), str(key), rid, payload) for rid, payload in rows],
        )


def find(store_dir: Path, segment_id: str) -> tuple[str, str, dict] | None:
    """Locate one segment by id: returns (dataset_id, key, item) or None."""
    _migrate_legacy(store_dir)
    with _open(store_dir) as conn:
        row = conn.execute(
            "SELECT dataset_id, key, payload FROM segments WHERE id=? ORDER BY rowid LIMIT 1",
            (str(segment_id),),
        ).fetchone()
    if row is None or row[2] is None:
        return None
    return row[0], row[1], json.loads(row[2])


def items_for_key(store_dir: Path, dataset_id: str, key: str) -> list[dict]:
    return get(store_dir, dataset_id, key) or []


def save_item(store_dir: Path, dataset_id: str, key: str, item: dict) -> None:
    with _open(store_dir) as conn:
        conn.execute(
            "INSERT OR REPLACE INTO segments(dataset_id,key,id,payload) VALUES(?,?,?,?)",
            (str(dataset_id), str(key), str(item.get("id")),
             json.dumps(item, ensure_ascii=False, default=str)),
        )


def delete_dataset(store_dir: Path, dataset_id: str) -> int:
    """Drop every stored segmentation for one dataset; returns rows removed."""
    _migrate_legacy(store_dir)
    with _open(store_dir) as conn:
        cur = conn.execute("DELETE FROM segments WHERE dataset_id=?", (str(dataset_id),))
        return int(cur.rowcount or 0)
