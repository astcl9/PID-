from __future__ import annotations

"""Database storage dispatch for large PID datasets.

The application keeps Parquet as the portable source format.  Set
``PID_STORAGE_BACKEND=postgres`` routes imports and bounded reads to PostgreSQL.
The legacy ClickHouse implementation remains lazy for old deployments; the
default single-process mode needs no database installation.
"""

import os
import json
import shutil
import itertools
from pathlib import Path
from urllib.parse import urlparse

import pandas as pd

TABLE = "pid_samples"


def enabled() -> bool:
    return os.getenv("PID_STORAGE_BACKEND", os.getenv("PID_STORAGE", "parquet")).strip().lower() in {"clickhouse", "postgres", "postgresql"}


def _postgres() -> bool:
    return os.getenv("PID_STORAGE_BACKEND", os.getenv("PID_STORAGE", "parquet")).strip().lower() in {"postgres", "postgresql"}


def _client():
    try:
        import clickhouse_connect
    except ImportError as exc:  # pragma: no cover - exercised in deployment
        raise RuntimeError(
            "ClickHouse backend requires clickhouse-connect; install project dependencies"
        ) from exc
    raw = os.getenv("PID_CLICKHOUSE_URL", "http://clickhouse:8123")
    parsed = urlparse(raw if "://" in raw else f"http://{raw}")
    kwargs = {
        "host": parsed.hostname or "localhost",
        "port": parsed.port or (8443 if parsed.scheme == "https" else 8123),
        "username": os.getenv("PID_CLICKHOUSE_USER", os.getenv("CLICKHOUSE_USER", "default")),
        "password": os.getenv("PID_CLICKHOUSE_PASSWORD", os.getenv("CLICKHOUSE_PASSWORD", "")),
        "database": os.getenv("PID_CLICKHOUSE_DATABASE", os.getenv("CLICKHOUSE_DATABASE", "default")),
    }
    if parsed.scheme == "https":
        kwargs["secure"] = True
    return clickhouse_connect.get_client(**kwargs)


def ensure_schema() -> None:
    if _postgres():
        from .postgres_db import ensure_schema as ensure_postgres
        return ensure_postgres()
    client = _client()
    try:
        client.command(f"""
            CREATE TABLE IF NOT EXISTS {TABLE} (
                dataset_id String,
                timestamp DateTime64(3, 'Asia/Shanghai'),
                tag LowCardinality(String),
                type LowCardinality(String),
                value Nullable(Float64),
                quality LowCardinality(String),
                unit Nullable(String),
                ingest_seq UInt64,
                day Date MATERIALIZED toDate(timestamp)
            ) ENGINE = ReplacingMergeTree(ingest_seq)
            PARTITION BY toYYYYMM(timestamp)
            ORDER BY (dataset_id, tag, timestamp)
            SETTINGS index_granularity = 8192
        """)
    finally:
        client.close()


def insert_parquet(dataset_id: str, parquet_path: Path, batch_rows: int = 50_000) -> int:
    if _postgres():
        raise RuntimeError("PostgreSQL 模式不需要 Parquet 中转")
    """Insert one partition without materialising the partition in memory."""
    import pyarrow.parquet as pq

    ensure_schema()
    client = _client()
    count = 0
    columns = ["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit"]
    try:
        for batch in pq.ParquetFile(parquet_path).iter_batches(batch_size=batch_rows, columns=columns):
            frame = batch.to_pandas()
            if frame.empty:
                continue
            frame["timestamp"] = pd.to_datetime(frame["timestamp"]).dt.tz_localize(
                "Asia/Shanghai", ambiguous="NaT", nonexistent="NaT"
            )
            frame["ingest_seq"] = range(count, count + len(frame))
            # clickhouse-connect accepts pandas batches and keeps memory bounded.
            client.insert_df(TABLE, frame[columns + ["ingest_seq"]])
            count += len(frame)
    finally:
        client.close()
    return count


def query_frame(dataset_id: str, start=None, end=None, tags=None) -> pd.DataFrame:
    if _postgres():
        from .postgres_db import query_frame as query_postgres
        return query_postgres(dataset_id, start, end, tags)
    client = _client()
    clauses = ["dataset_id = {dataset_id:String}"]
    params: dict[str, object] = {"dataset_id": str(dataset_id)}
    if start is not None:
        clauses.append("timestamp >= {start:DateTime64(3, 'Asia/Shanghai')}")
        value = pd.Timestamp(start)
        params["start"] = (value.tz_localize("Asia/Shanghai") if value.tzinfo is None else value.tz_convert("Asia/Shanghai")).to_pydatetime()
    if end is not None:
        clauses.append("timestamp < {end:DateTime64(3, 'Asia/Shanghai')}")
        value = pd.Timestamp(end)
        params["end"] = (value.tz_localize("Asia/Shanghai") if value.tzinfo is None else value.tz_convert("Asia/Shanghai")).to_pydatetime()
    if tags:
        clauses.append("tag IN {tags:Array(String)}")
        params["tags"] = [str(v) for v in tags]
    where = " AND ".join(clauses)
    try:
        return client.query_df(
            f"SELECT dataset_id,timestamp,tag,type,value,quality,unit FROM {TABLE} FINAL WHERE {where} ORDER BY timestamp,tag",
            parameters=params,
        )
    finally:
        client.close()


def query_buckets(dataset_id: str, start, end, tags=None, seconds: int = 60) -> pd.DataFrame:
    if _postgres():
        from .postgres_db import query_buckets as buckets_postgres
        return buckets_postgres(dataset_id, start, end, tags, seconds)
    """Return two extrema per bucket directly from ClickHouse."""
    client = _client()
    seconds = int(seconds)
    if seconds not in {10, 60, 600, 3600}:
        raise ValueError("不支持的聚合间隔")
    clauses = ["dataset_id = {dataset_id:String}", "timestamp >= {start:DateTime64(3, 'Asia/Shanghai')}", "timestamp < {end:DateTime64(3, 'Asia/Shanghai')}"]
    params = {"dataset_id": str(dataset_id)}
    for name, value in (("start", start), ("end", end)):
        stamp = pd.Timestamp(value)
        params[name] = (stamp.tz_localize("Asia/Shanghai") if stamp.tzinfo is None else stamp.tz_convert("Asia/Shanghai")).to_pydatetime()
    if tags:
        clauses.append("tag IN {tags:Array(String)}")
        params["tags"] = [str(value) for value in tags]
    bucket = f"toStartOfInterval(timestamp, INTERVAL {seconds} SECOND)"
    sql = f"""SELECT tag, type,
        argMin(timestamp, value) AS min_timestamp, min(value) AS min_value,
        argMax(timestamp, value) AS max_timestamp, max(value) AS max_value
        FROM {TABLE} FINAL WHERE {' AND '.join(clauses)}
        GROUP BY tag,type,{bucket} ORDER BY tag,{bucket}"""
    try:
        result = client.query_df(sql, parameters=params)
        rows = []
        for row in result.to_dict("records"):
            if row.get("min_value") is not None:
                rows.append({"timestamp": row.get("min_timestamp"), "tag": row["tag"], "type": row["type"], "value": row["min_value"]})
            if row.get("max_value") is not None and row.get("max_timestamp") != row.get("min_timestamp"):
                rows.append({"timestamp": row.get("max_timestamp"), "tag": row["tag"], "type": row["type"], "value": row["max_value"]})
        return pd.DataFrame(rows, columns=["timestamp", "tag", "type", "value"])
    finally:
        client.close()


def csv_stream(dataset_id: str, start, end, tags=None):
    if _postgres():
        from .postgres_db import csv_stream as stream_postgres
        yield from stream_postgres(dataset_id, start, end, tags)
        return
    """Yield CSV rows from ClickHouse without building a giant response buffer."""
    import csv
    import io
    client = _client()
    clauses = ["dataset_id = {dataset_id:String}", "timestamp >= {start:DateTime64(3, 'Asia/Shanghai')}", "timestamp < {end:DateTime64(3, 'Asia/Shanghai')}"]
    params = {"dataset_id": str(dataset_id)}
    for name, value in (("start", start), ("end", end)):
        stamp = pd.Timestamp(value)
        params[name] = (stamp.tz_localize("Asia/Shanghai") if stamp.tzinfo is None else stamp.tz_convert("Asia/Shanghai")).to_pydatetime()
    if tags:
        clauses.append("tag IN {tags:Array(String)}")
        params["tags"] = [str(value) for value in tags]
    sql = f"SELECT dataset_id,timestamp,tag,type,value,quality,unit FROM {TABLE} FINAL WHERE {' AND '.join(clauses)} ORDER BY tag,timestamp"
    try:
        yield "dataset_id,timestamp,tag,type,value,quality,unit\n"
        with client.query_rows_stream(sql, parameters=params) as stream:
            for batch in stream:
                output = io.StringIO()
                writer = csv.writer(output, lineterminator="\n")
                writer.writerows(batch)
                yield output.getvalue()
    finally:
        client.close()


def daily_summary(dataset_id: str) -> list[dict]:
    if _postgres():
        from .postgres_db import daily_summary as summary_postgres
        return summary_postgres(dataset_id)
    client = _client()
    try:
        result = client.query_df(f"""
            SELECT toString(day) AS day, tag, type,
                   count(value) AS count, avg(value) AS mean,
                   min(value) AS min, max(value) AS max
            FROM {TABLE}
            WHERE dataset_id = {{dataset_id:String}}
            GROUP BY day, tag, type ORDER BY day, tag
        """, parameters={"dataset_id": str(dataset_id)})
        return result.where(pd.notna(result), None).to_dict("records")
    finally:
        client.close()


def ping() -> bool:
    if _postgres():
        from .postgres_db import ping as ping_postgres
        return ping_postgres()
    try:
        client = _client()
        client.command("SELECT 1")
        client.close()
        return True
    except Exception:
        return False


def import_file(path: Path, dataset_id: str, filename: str, data_dir: Path, progress=None, chunk_rows: int = 25000) -> dict:
    if _postgres():
        from .postgres_db import import_file as import_postgres
        return import_postgres(path, dataset_id, filename, data_dir, progress, chunk_rows)
    """Import directly into ClickHouse without creating a full Parquet staging copy."""
    from .importer import _normalise_chunk, _xlsx_batches, detect_timestamp, _report_progress, read_csv_chunks
    import pandas as pd

    path = Path(path)
    total_bytes = path.stat().st_size
    client = _client()
    ensure_schema()
    rows_processed = db_rows = seq = 0
    warnings, signals, days = [], set(), set()
    min_time = max_time = None
    try:
        if path.suffix.lower() == ".csv":
            reader = read_csv_chunks(path, chunksize=chunk_rows)
            try:
                first = next(reader)
            except StopIteration as exc:
                raise ValueError("输入文件没有数据行") from exc
            timestamp_col = detect_timestamp(first)
            chunks = itertools.chain((first,), reader)
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            chunks = _xlsx_batches(path, chunk_rows)
            timestamp_col = None
        else:
            raise ValueError("ClickHouse 导入仅支持 CSV/XLSX")
        for chunk in chunks:
            if timestamp_col is None:
                timestamp_col = detect_timestamp(chunk)
            normalized, seq, chunk_warnings = _normalise_chunk(chunk, dataset_id, timestamp_col, seq)
            warnings.extend(chunk_warnings)
            rows_processed += len(chunk)
            if not normalized.empty:
                data = normalized.copy()
                data["timestamp"] = pd.to_datetime(data["timestamp"]).dt.tz_localize("Asia/Shanghai", ambiguous="NaT", nonexistent="NaT")
                data["quality"] = data["quality"].fillna("missing").astype(str)
                data["unit"] = data["unit"].fillna("").astype(str)
                data = data.rename(columns={"_ingest_seq": "ingest_seq"})
                client.insert_df(TABLE, data[["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit", "ingest_seq"]])
                db_rows += len(data)
                signals.update((str(tag), str(kind)) for tag, kind in data[["tag", "type"]].drop_duplicates().itertuples(index=False, name=None))
                days.update(pd.to_datetime(data["timestamp"]).dt.strftime("%Y-%m-%d").unique())
                lo, hi = data["timestamp"].min(), data["timestamp"].max()
                min_time = lo if min_time is None or lo < min_time else min_time
                max_time = hi if max_time is None or hi > max_time else max_time
            _report_progress(progress, stage="processing", rows_processed=rows_processed,
                             bytes_processed=min(total_bytes, int(total_bytes * rows_processed / max(rows_processed + chunk_rows, 1))),
                             total_bytes=total_bytes, progress=None if path.suffix.lower() != ".csv" else min(0.99, rows_processed / max(rows_processed + chunk_rows, 1)))
        if not db_rows:
            raise ValueError("输入文件没有可用的数值数据")
        root = Path(data_dir); (root / "raw" / str(dataset_id)).mkdir(parents=True, exist_ok=True); (root / "meta").mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, root / "raw" / str(dataset_id) / Path(filename).name)
        metadata = {"id": str(dataset_id), "name": filename, "rows": db_rows, "start_time": min_time,
                    "end_time": max_time, "warnings": warnings,
                    "signals": [{"tag": tag, "type": kind} for tag, kind in sorted(signals)],
                    "loops": sorted({tag.rsplit('.', 1)[0] for tag, _ in signals if '.' in tag}),
                    "days": sorted(days), "storage": "clickhouse"}
        (root / "meta" / f"{dataset_id}.json").write_text(json.dumps(metadata, default=str, ensure_ascii=False), encoding="utf-8")
        _report_progress(progress, stage="completed", rows_processed=rows_processed, bytes_processed=total_bytes, total_bytes=total_bytes, progress=1.0)
        return metadata
    finally:
        client.close()
