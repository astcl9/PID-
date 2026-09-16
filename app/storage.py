from pathlib import Path
import json
import pandas as pd
from .config import DATA_DIR
from . import db

def save_dataset(dataset_id: str, frame: pd.DataFrame, source: Path, report: dict) -> None:
    root = DATA_DIR
    (root / "raw" / dataset_id).mkdir(parents=True, exist_ok=True)
    (root / "parquet" / dataset_id).mkdir(parents=True, exist_ok=True)
    (root / "raw" / dataset_id / source.name).write_bytes(source.read_bytes())
    for day, part in frame.groupby(frame["timestamp"].dt.date):
        dest = root / "parquet" / dataset_id / f"date={day.isoformat()}"
        dest.mkdir(parents=True, exist_ok=True)
        partition = dest / "data.parquet"
        part.to_parquet(partition, index=False)
        if db.enabled():
            db.insert_parquet(dataset_id, partition)
    (root / "meta").mkdir(exist_ok=True)
    (root / "meta" / f"{dataset_id}.json").write_text(json.dumps(report, default=str, ensure_ascii=False), encoding="utf-8")

def load_frame(dataset_id: str, start=None, end=None, tags=None) -> pd.DataFrame:
    """Load a bounded frame, pruning day partitions before they are scanned."""
    if db.enabled():
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
    con = duckdb.connect(config={"memory_limit": "512MB", "threads": 2})
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

def list_meta() -> list[dict]:
    out = []
    for p in (DATA_DIR / "meta").glob("*.json"):
        out.append(json.loads(p.read_text(encoding="utf-8")))
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
    temp = path.with_suffix(".json.tmp")
    temp.write_text(json.dumps(metadata, default=str, ensure_ascii=False), encoding="utf-8")
    temp.replace(path)
    return signals
