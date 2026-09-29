from pathlib import Path
import json
import os
import pandas as pd
from .config import DATA_DIR
from . import db


def _duckdb_threads() -> int:
    """DuckDB worker threads for query aggregation (default: all CPU cores).

    Override with PID_DUCKDB_THREADS; PID_DUCKDB_MEMORY_LIMIT controls the
    in-memory budget for the same aggregation connection.
    """
    raw = os.getenv("PID_DUCKDB_THREADS", "").strip()
    if raw.isdigit():
        return max(1, int(raw))
    return os.cpu_count() or 4


def load_frame(dataset_id: str, start=None, end=None, tags=None) -> pd.DataFrame:
    """Load a bounded frame, pruning day partitions before they are scanned."""
    # Check the per-dataset backend, not just the global env: a large CSV
    # imported through the size router lives in PostgreSQL even when the
    # process default is Parquet.
    if db.uses_database(dataset_id):
        return db.query_frame(dataset_id, start=start, end=end, tags=tags)
    base = DATA_DIR / "parquet" / dataset_id
    start_ts = pd.Timestamp(start) if start is not None else None
    end_ts = pd.Timestamp(end) if end is not None else None
    if start_ts is not None and end_ts is not None and start_ts >= end_ts:
        return pd.DataFrame(columns=["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit"])
    files = []
    for path in base.glob("date=*/*.parquet"):
        try:
            day = pd.Timestamp(path.parent.name.split("=", 1)[1]).date()
        except (ValueError, IndexError):
            continue
        if start_ts is not None and day < start_ts.date():
            continue
        # A midnight end excludes that entire partition. Otherwise the end day
        # can contain samples before the half-open endpoint.
        if end_ts is not None and day >= end_ts.date() and end_ts == end_ts.normalize():
            continue
        files.append(path)
    if not files:
        return pd.DataFrame(columns=["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit"])
    # DuckDB scans only matching Parquet partitions when installed; pandas is a
    # portable fallback for development environments without the optional engine.
    try:
        import duckdb
        clauses = []
        params = []
        if start_ts is not None: clauses.append("timestamp >= ?"); params.append(start_ts.to_pydatetime())
        if end_ts is not None: clauses.append("timestamp < ?"); params.append(end_ts.to_pydatetime())
        if tags:
            clauses.append("tag IN (SELECT * FROM UNNEST(?))"); params.append(list(tags))
        where = (" WHERE " + " AND ".join(clauses)) if clauses else ""
        return duckdb.connect().execute(
            f"SELECT * FROM read_parquet(?) {where}", [ [str(f) for f in files], *params ]
        ).df()
    except Exception:
        pass
    df = pd.concat([pd.read_parquet(f) for f in files], ignore_index=True)
    if start_ts is not None: df = df[df.timestamp >= start_ts]
    if end_ts is not None: df = df[df.timestamp < end_ts]
    if tags: df = df[df.tag.isin(tags)]
    return df.sort_values("timestamp")


def query_buckets(dataset_id: str, start, end, tags=None, seconds: int = 60) -> pd.DataFrame:
    """Aggregate Parquet extrema in DuckDB without materialising raw samples."""
    seconds = int(seconds)
    if seconds not in {10, 60, 600, 3600}:
        raise ValueError("不支持的聚合间隔")
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    if start_ts >= end_ts:
        return pd.DataFrame(columns=["timestamp", "tag", "type", "value"])
    base = DATA_DIR / "parquet" / dataset_id
    files = []
    for path in base.glob("date=*/*.parquet"):
        try:
            day = pd.Timestamp(path.parent.name.split("=", 1)[1]).date()
        except (ValueError, IndexError):
            continue
        if day < start_ts.date():
            continue
        if day >= end_ts.date() and end_ts == end_ts.normalize():
            continue
        files.append(str(path))
    if not files:
        return pd.DataFrame(columns=["timestamp", "tag", "type", "value"])

    import duckdb
    clauses = ["timestamp >= ?", "timestamp < ?", "value IS NOT NULL"]
    params: list[object] = [files, start_ts.to_pydatetime(), end_ts.to_pydatetime()]
    if tags:
        clauses.append("tag IN (SELECT * FROM UNNEST(?))")
        params.append([str(tag) for tag in tags])
    sql = f"""
        SELECT tag, type,
               arg_min(timestamp, value) AS min_timestamp, min(value) AS min_value,
               arg_max(timestamp, value) AS max_timestamp, max(value) AS max_value
        FROM read_parquet(?)
        WHERE {' AND '.join(clauses)}
        GROUP BY tag, type, time_bucket(INTERVAL {seconds} SECOND, timestamp)
        ORDER BY tag, time_bucket(INTERVAL {seconds} SECOND, timestamp)
    """
    con = duckdb.connect(config={
        "memory_limit": os.getenv("PID_DUCKDB_MEMORY_LIMIT", "512MB"),
        "threads": _duckdb_threads(),
    })
    try:
        result = con.execute(sql, params).df()
    finally:
        con.close()
    rows = []
    for row in result.to_dict("records"):
        rows.append({"timestamp": row["min_timestamp"], "tag": row["tag"], "type": row["type"], "value": row["min_value"]})
        if row["max_timestamp"] != row["min_timestamp"]:
            rows.append({"timestamp": row["max_timestamp"], "tag": row["tag"], "type": row["type"], "value": row["max_value"]})
    return pd.DataFrame(rows, columns=["timestamp", "tag", "type", "value"]).sort_values(["tag", "timestamp"])


def csv_stream(dataset_id: str, start, end, tags=None, type_map=None):
    """Stream CSV rows from day-partitioned Parquet without loading a whole day.

    Mirrors ``db.csv_stream`` for the default local backend so exporting a
    large single-day dataset does not require materialising every row at once.
    """
    import csv
    import io
    import pyarrow.parquet as pq

    base = DATA_DIR / "parquet" / dataset_id
    start_ts, end_ts = pd.Timestamp(start), pd.Timestamp(end)
    columns = ["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit"]
    files = []
    for path in base.glob("date=*/*.parquet"):
        try:
            day = pd.Timestamp(path.parent.name.split("=", 1)[1]).date()
        except (ValueError, IndexError):
            continue
        if day < start_ts.date():
            continue
        if end_ts == end_ts.normalize() and day >= end_ts.date():
            continue
        files.append(path)
    yield "\ufeff" + ",".join(columns) + "\n"
    tag_set = set(tags) if tags is not None else None
    type_map = type_map or {}
    for path in sorted(files):
        parquet = pq.ParquetFile(path)
        for batch in parquet.iter_batches(batch_size=50_000, columns=columns):
            frame = batch.to_pandas()
            frame = frame[(frame["timestamp"] >= start_ts) & (frame["timestamp"] < end_ts)]
            if tag_set is not None:
                frame = frame[frame["tag"].isin(tag_set)]
            if type_map:
                frame = frame.copy()
                frame["type"] = frame["tag"].astype(str).map(type_map).fillna(frame["type"])
            if frame.empty:
                continue
            output = io.StringIO()
            frame.to_csv(output, index=False, header=False)
            yield output.getvalue()


_META_CACHE: dict[str, tuple[tuple, list[dict]]] = {}
_META_CACHE_MAX_DIRS = 8


def _meta_signature(meta_dir: Path) -> tuple | None:
    """Cheap staleness probe: names + mtimes + sizes, no content reads."""
    try:
        entries = []
        for p in meta_dir.glob("*.json"):
            st = p.stat()
            entries.append((p.name, st.st_mtime_ns, st.st_size))
        return tuple(sorted(entries))
    except OSError:
        return None


def list_meta() -> list[dict]:
    """List dataset metadata, caching parses behind an mtime signature.

    Every request used to re-read and re-parse every meta JSON; with many
    datasets that dominated lightweight endpoints. A directory listing plus
    one stat per file is enough to detect importer/PATCH writes, so stale
    entries are impossible while reads stay cheap. The cache is keyed by the
    resolved meta directory so tests (and multi-root deployments) that swap
    DATA_DIR never see each other's entries.
    """
    meta_dir = Path(DATA_DIR) / "meta"
    key = str(meta_dir)
    sig = _meta_signature(meta_dir)
    cached = _META_CACHE.get(key)
    if cached is not None and cached[0] == sig:
        return list(cached[1])
    out = []
    for p in meta_dir.glob("*.json"):
        out.append(json.loads(p.read_text(encoding="utf-8")))
    if sig is not None:
        _META_CACHE[key] = (sig, out)
        while len(_META_CACHE) > _META_CACHE_MAX_DIRS:
            dead = [k for k in _META_CACHE if not Path(k).parent.exists()]
            if not dead:
                break
            for k in dead:
                del _META_CACHE[k]
        while len(_META_CACHE) > _META_CACHE_MAX_DIRS:
            _META_CACHE.pop(next(iter(_META_CACHE)))
    return out


def update_signal_metadata(dataset_id: str, mappings: list[dict]) -> list[dict] | None:
    """Persist user signal classifications without rewriting raw samples."""
    path = DATA_DIR / "meta" / f"{dataset_id}.json"
    if not path.exists():
        return None
    metadata = json.loads(path.read_text(encoding="utf-8"))
    requested = {str(item["tag"]): item for item in mappings}
    signals = metadata.get("signals", [])
    known = {str(item.get("tag")) for item in signals}
    unknown = sorted(set(requested) - known)
    if unknown:
        raise ValueError(f"未知信号: {', '.join(unknown)}")
    for signal in signals:
        update = requested.get(str(signal.get("tag")))
        if not update:
            continue
        signal["type"] = update["type"]
        signal["unit"] = update.get("unit")
        signal["note"] = update.get("note")
    _atomic_write_meta(path, metadata)
    return signals


def _atomic_write_meta(path: Path, metadata: dict) -> None:
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(metadata, default=str, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)


def cache_day_summaries(dataset_id: str, summaries: list[dict]) -> None:
    """Persist computed day summaries so the next request skips the full scan.

    Legacy datasets imported before importers published ``day_summaries`` used
    to re-run per-day segmentation (a full-dataset pivot + resample) on every
    calendar request. Writing the result back to the metadata file upgrades
    them to the cached path; failures are swallowed because the computation is
    still correct, just slow.
    """
    path = DATA_DIR / "meta" / f"{dataset_id}.json"
    try:
        metadata = json.loads(path.read_text(encoding="utf-8"))
    except (OSError, ValueError, TypeError):
        return
    metadata["day_summaries"] = summaries
    metadata["segment_summaries_ready"] = True
    try:
        _atomic_write_meta(path, metadata)
    except OSError:
        pass
