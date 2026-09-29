"""PostgreSQL adapter used when PID_STORAGE_BACKEND=postgres."""
from __future__ import annotations

import csv
import io
import itertools
import json
import os
import shutil
import threading
from contextlib import contextmanager
from pathlib import Path

import pandas as pd

TABLE = "pid_samples"


def _connect_kwargs():
    dsn = os.getenv("PID_POSTGRES_DSN")
    if dsn:
        return {"conninfo": dsn}
    return {"host": os.getenv("PID_POSTGRES_HOST", "127.0.0.1"),
            "port": int(os.getenv("PID_POSTGRES_PORT", "5432")),
            "dbname": os.getenv("PID_POSTGRES_DATABASE", "pid"),
            "user": os.getenv("PID_POSTGRES_USER", "postgres"),
            "password": os.getenv("PID_POSTGRES_PASSWORD", "")}


def connect():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("PostgreSQL 模式需要安装 psycopg[binary]：python -m pip install 'psycopg[binary]>=3.2'") from exc
    return psycopg.connect(**_connect_kwargs())


class _SimplePool:
    """Minimal LIFO connection pool used when psycopg_pool is not installed.

    psycopg connections must not be shared by threads concurrently, so checkouts
    are exclusive. Returned connections are rolled back (clearing any aborted
    transaction) before reuse; broken connections are dropped.
    """

    def __init__(self, max_size: int = 4, factory=None):
        self._idle: list = []
        self._lock = threading.Lock()
        self._max = max(1, int(max_size))
        self._factory = factory or connect

    @contextmanager
    def connection(self):
        conn = None
        with self._lock:
            if self._idle:
                conn = self._idle.pop()
        if conn is None:
            conn = self._factory()
        try:
            yield conn
        finally:
            try:
                if conn.closed:
                    return
                if conn.info.transaction_status != 0:  # idle (no open transaction)
                    conn.rollback()
            except Exception:
                try: conn.close()
                except Exception: pass
                return
            with self._lock:
                if len(self._idle) < self._max:
                    self._idle.append(conn)
                    conn = None
            if conn is not None:
                conn.close()


_POOL = None
_POOL_LOCK = threading.Lock()


def _pool():
    """Process-wide pool; prefers psycopg_pool when installed, else the
    built-in simple pool. Every read path re-used a fresh TCP+auth handshake
    per query, which dominated bucketed chart queries on high-latency links."""
    global _POOL
    if _POOL is None:
        with _POOL_LOCK:
            if _POOL is None:
                max_size = int(os.getenv("PID_POSTGRES_POOL_SIZE", "4"))
                try:
                    from psycopg_pool import ConnectionPool
                    kwargs = _connect_kwargs()
                    conninfo = kwargs.pop("conninfo", "")
                    _POOL = ConnectionPool(conninfo=conninfo, kwargs=kwargs or None,
                                           min_size=1, max_size=max_size, open=True)
                except ImportError:
                    _POOL = _SimplePool(max_size)
    return _POOL


@contextmanager
def _connection():
    pool = _pool()
    with pool.connection() as conn:
        yield conn


def ensure_schema(conn=None):
    own = conn is None
    conn = conn or connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"""CREATE TABLE IF NOT EXISTS {TABLE} (
                dataset_id text NOT NULL, timestamp timestamptz NOT NULL,
                tag text NOT NULL, type text NOT NULL, value double precision,
                quality text NOT NULL, unit text, ingest_seq bigint NOT NULL,
                PRIMARY KEY (dataset_id, tag, timestamp))""")
            cur.execute(f"CREATE INDEX IF NOT EXISTS {TABLE}_time_idx ON {TABLE}(dataset_id,timestamp)")
        conn.commit()
    finally:
        if own: conn.close()


def local_ts(value):
    stamp = pd.Timestamp(value)
    stamp = stamp.tz_localize("Asia/Shanghai") if stamp.tzinfo is None else stamp.tz_convert("Asia/Shanghai")
    return stamp.to_pydatetime()


def _where(dataset_id, start=None, end=None, tags=None):
    clauses, params = ["dataset_id=%s"], [str(dataset_id)]
    if start is not None: clauses += ["timestamp >= %s"]; params += [local_ts(start)]
    if end is not None: clauses += ["timestamp < %s"]; params += [local_ts(end)]
    if tags: clauses += ["tag = ANY(%s)"]; params += [[str(t) for t in tags]]
    return " AND ".join(clauses), params


_COPY_SPECIAL = ("\t", "\n", "\r", "\\")


def _copy_escape(value) -> str:
    """Escape one field for the COPY text format (tab-separated, \\N = NULL)."""
    text = str(value)
    if not any(c in text for c in _COPY_SPECIAL):
        return text
    return (text.replace("\\", "\\\\").replace("\t", "\\t")
                .replace("\n", "\\n").replace("\r", "\\r"))


def _copy_buffer(frame) -> bytes:
    """Serialise a normalised batch to the COPY text format in one buffer."""
    dataset = str(frame["dataset_id"].iat[0])
    stamps = pd.DatetimeIndex(frame["timestamp"])
    stamps = stamps.tz_localize("Asia/Shanghai") if stamps.tz is None else stamps.tz_convert("Asia/Shanghai")
    ts_text = stamps.strftime("%Y-%m-%d %H:%M:%S%z")  # +0800 offset, PG-valid
    tags = frame["tag"].tolist()
    types = frame["type"].tolist()
    vals = frame["value"].to_numpy(dtype="float64")
    value_missing = frame["value"].isna().to_numpy()
    quals = frame["quality"].tolist()
    units = frame["unit"].tolist()
    unit_missing = frame["unit"].isna().to_numpy()
    seqs = frame["_ingest_seq"].to_numpy(dtype="int64")
    lines = [
        "\t".join((
            dataset,
            ts_text[i],
            _copy_escape(tags[i]),
            _copy_escape(types[i]),
            "\\N" if value_missing[i] else repr(float(vals[i])),
            _copy_escape(quals[i]),
            "\\N" if unit_missing[i] else _copy_escape(units[i]),
            str(int(seqs[i])),
        ))
        for i in range(len(frame))
    ]
    return ("\n".join(lines) + "\n").encode("utf-8")


def insert_batch(conn, frame):
    if frame.empty: return 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS pid_import_stage (dataset_id text,timestamp timestamptz,tag text,type text,value double precision,quality text,unit text,ingest_seq bigint) ON COMMIT PRESERVE ROWS")
        cur.execute("TRUNCATE pid_import_stage")
        # One bulk write() of a pre-serialised buffer instead of per-row
        # write_row: every write_row pays psycopg's per-row adaptation
        # overhead, which dominated on wide imports (~300k rows).
        with cur.copy("COPY pid_import_stage (dataset_id,timestamp,tag,type,value,quality,unit,ingest_seq) FROM STDIN") as copy:
            copy.write(_copy_buffer(frame))
        cur.execute(f"""INSERT INTO {TABLE}(dataset_id,timestamp,tag,type,value,quality,unit,ingest_seq)
          SELECT DISTINCT ON(dataset_id,tag,timestamp) dataset_id,timestamp,tag,type,value,quality,unit,ingest_seq
          FROM pid_import_stage ORDER BY dataset_id,tag,timestamp,ingest_seq DESC
          ON CONFLICT(dataset_id,tag,timestamp) DO UPDATE SET type=EXCLUDED.type,value=EXCLUDED.value,quality=EXCLUDED.quality,unit=EXCLUDED.unit,ingest_seq=EXCLUDED.ingest_seq""")
    return len(frame)


def import_file(path, dataset_id, filename, data_dir, progress=None, chunk_rows=25000):
    from .importer import _normalise_chunk, _xlsx_batches, detect_timestamp, _report_progress, read_csv_chunks
    path, total = Path(path), Path(path).stat().st_size
    conn = connect(); seq = rows_processed = db_rows = 0
    warnings, signals, days = [], set(), set(); min_time = max_time = None
    try:
        ensure_schema(conn)
        if path.suffix.lower() == ".csv":
            reader = read_csv_chunks(path, chunksize=chunk_rows)
            try: first = next(reader)
            except StopIteration as exc: raise ValueError("输入文件没有数据行") from exc
            tcol, chunks = detect_timestamp(first), itertools.chain((first,), reader)
        elif path.suffix.lower() in {".xlsx", ".xls"}:
            tcol, chunks = None, _xlsx_batches(path, chunk_rows)
        else: raise ValueError("PostgreSQL 导入仅支持 CSV/XLSX")
        for chunk in chunks:
            tcol = tcol or detect_timestamp(chunk)
            normalized, seq, ws = _normalise_chunk(chunk, str(dataset_id), tcol, seq)
            warnings.extend(ws); rows_processed += len(chunk)
            if not normalized.empty:
                insert_batch(conn, normalized); conn.commit(); db_rows += len(normalized)
                signals.update((str(t), str(k)) for t,k in normalized[["tag","type"]].drop_duplicates().itertuples(index=False, name=None))
                stamp = pd.to_datetime(normalized["timestamp"]); days.update(stamp.dt.strftime("%Y-%m-%d").unique())
                lo, hi = stamp.min(), stamp.max(); min_time = lo if min_time is None or lo < min_time else min_time; max_time = hi if max_time is None or hi > max_time else max_time
            _report_progress(progress, stage="processing", rows_processed=rows_processed, bytes_processed=min(total, int(total*rows_processed/max(rows_processed+chunk_rows,1))), total_bytes=total, progress=min(.99, rows_processed/max(rows_processed+chunk_rows,1)))
        if not db_rows: raise ValueError("输入文件没有可用的数值数据")
        root = Path(data_dir); (root/"raw"/str(dataset_id)).mkdir(parents=True, exist_ok=True); (root/"meta").mkdir(parents=True, exist_ok=True)
        shutil.copy2(path, root/"raw"/str(dataset_id)/Path(filename).name)
        metadata = {"id":str(dataset_id),"name":filename,"rows":db_rows,"start_time":min_time,"end_time":max_time,"warnings":warnings,"signals":[{"tag":t,"type":k} for t,k in sorted(signals)],"loops":sorted({t.rsplit('.',1)[0] for t,_ in signals if '.' in t}),"days":sorted(days),"storage":"postgres"}
        (root/"meta"/f"{dataset_id}.json").write_text(json.dumps(metadata,default=str,ensure_ascii=False),encoding="utf-8")
        _report_progress(progress,stage="completed",rows_processed=rows_processed,bytes_processed=total,total_bytes=total,progress=1.0)
        return metadata
    except Exception:
        conn.rollback()
        # Batches are committed independently to keep transactions bounded;
        # remove any rows already copied when a later batch fails.
        try:
            with conn.cursor() as cur:
                cur.execute(f"DELETE FROM {TABLE} WHERE dataset_id=%s", [str(dataset_id)])
            conn.commit()
        except Exception:
            conn.rollback()
        raise
    finally: conn.close()


def query_frame(dataset_id, start=None, end=None, tags=None):
    with _connection() as conn:
        where,params=_where(dataset_id,start,end,tags)
        with conn.cursor() as cur: cur.execute(f"SELECT dataset_id,timestamp,tag,type,value,quality,unit FROM {TABLE} WHERE {where} ORDER BY timestamp,tag",params); rows=cur.fetchall()
        return pd.DataFrame(rows,columns=["dataset_id","timestamp","tag","type","value","quality","unit"])


def query_buckets(dataset_id,start,end,tags=None,seconds=60):
    if int(seconds) not in {10,60,600,3600}: raise ValueError("不支持的聚合间隔")
    with _connection() as conn:
        where,params=_where(dataset_id,start,end,tags); origin=local_ts(start); sec=int(seconds)
        sql=f"""WITH b AS (SELECT *,date_bin(make_interval(secs=>%s),timestamp,%s) bucket FROM {TABLE} WHERE {where} AND value IS NOT NULL)
          SELECT tag,type,bucket,(array_agg(timestamp ORDER BY value))[1],min(value),(array_agg(timestamp ORDER BY value DESC))[1],max(value) FROM b GROUP BY tag,type,bucket ORDER BY tag,bucket"""
        with conn.cursor() as cur: cur.execute(sql,[sec,origin,*params]); rows=cur.fetchall()
        out=[]
        for tag,typ,bucket,mint,minv,maxt,maxv in rows:
            out.append({"timestamp":mint,"tag":tag,"type":typ,"value":minv})
            if maxt!=mint: out.append({"timestamp":maxt,"tag":tag,"type":typ,"value":maxv})
        return pd.DataFrame(out,columns=["timestamp","tag","type","value"])


def csv_stream(dataset_id,start,end,tags=None):
    with _connection() as conn:
        where,params=_where(dataset_id,start,end,tags)
        with conn.cursor() as cur:
            cur.execute(f"SELECT dataset_id,timestamp,tag,type,value,quality,unit FROM {TABLE} WHERE {where} ORDER BY tag,timestamp",params)
            yield "dataset_id,timestamp,tag,type,value,quality,unit\n"
            while rows:=cur.fetchmany(5000):
                out=io.StringIO(); csv.writer(out,lineterminator="\n").writerows(rows); yield out.getvalue()


def daily_summary(dataset_id):
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT to_char(timestamp AT TIME ZONE 'Asia/Shanghai','YYYY-MM-DD'),tag,type,count(value),avg(value),min(value),max(value) FROM {TABLE} WHERE dataset_id=%s GROUP BY 1,tag,type ORDER BY 1,tag",[str(dataset_id)])
            return [dict(zip(["day","tag","type","count","mean","min","max"],r)) for r in cur.fetchall()]


def delete_dataset(dataset_id: str) -> int:
    """Remove every sample row for one dataset; returns rows deleted."""
    with _connection() as conn:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE dataset_id=%s", [str(dataset_id)])
            return int(cur.rowcount or 0)


def ping():
    try:
        with _connection() as conn:
            conn.execute("SELECT 1")
        return True
    except Exception: return False
