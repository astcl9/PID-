"""PostgreSQL adapter used when PID_STORAGE_BACKEND=postgres."""
from __future__ import annotations

import csv
import io
import itertools
import json
import os
import shutil
from pathlib import Path

import pandas as pd

TABLE = "pid_samples"


def connect():
    try:
        import psycopg
    except ImportError as exc:
        raise RuntimeError("PostgreSQL 模式需要安装 psycopg[binary]：python -m pip install 'psycopg[binary]>=3.2'") from exc
    dsn = os.getenv("PID_POSTGRES_DSN")
    if dsn:
        return psycopg.connect(dsn)
    return psycopg.connect(host=os.getenv("PID_POSTGRES_HOST", "127.0.0.1"),
                           port=int(os.getenv("PID_POSTGRES_PORT", "5432")),
                           dbname=os.getenv("PID_POSTGRES_DATABASE", "pid"),
                           user=os.getenv("PID_POSTGRES_USER", "postgres"),
                           password=os.getenv("PID_POSTGRES_PASSWORD", ""))


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


def insert_batch(conn, frame):
    if frame.empty: return 0
    with conn.cursor() as cur:
        cur.execute("CREATE TEMP TABLE IF NOT EXISTS pid_import_stage (dataset_id text,timestamp timestamptz,tag text,type text,value double precision,quality text,unit text,ingest_seq bigint) ON COMMIT PRESERVE ROWS")
        cur.execute("TRUNCATE pid_import_stage")
        with cur.copy("COPY pid_import_stage (dataset_id,timestamp,tag,type,value,quality,unit,ingest_seq) FROM STDIN") as copy:
            for row in frame.itertuples(index=False, name=None):
                dataset, timestamp, tag, typ, value, quality, unit, seq = row
                copy.write_row((str(dataset), local_ts(timestamp), str(tag), str(typ), None if pd.isna(value) else float(value), str(quality), None if pd.isna(unit) else str(unit), int(seq)))
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
    conn=connect()
    try:
        where,params=_where(dataset_id,start,end,tags)
        with conn.cursor() as cur: cur.execute(f"SELECT dataset_id,timestamp,tag,type,value,quality,unit FROM {TABLE} WHERE {where} ORDER BY timestamp,tag",params); rows=cur.fetchall()
        return pd.DataFrame(rows,columns=["dataset_id","timestamp","tag","type","value","quality","unit"])
    finally: conn.close()


def query_buckets(dataset_id,start,end,tags=None,seconds=60):
    if int(seconds) not in {10,60,600,3600}: raise ValueError("不支持的聚合间隔")
    conn=connect()
    try:
        where,params=_where(dataset_id,start,end,tags); origin=local_ts(start); sec=int(seconds)
        sql=f"""WITH b AS (SELECT *,date_bin(make_interval(secs=>%s),timestamp,%s) bucket FROM {TABLE} WHERE {where} AND value IS NOT NULL)
          SELECT tag,type,bucket,(array_agg(timestamp ORDER BY value))[1],min(value),(array_agg(timestamp ORDER BY value DESC))[1],max(value) FROM b GROUP BY tag,type,bucket ORDER BY tag,bucket"""
        with conn.cursor() as cur: cur.execute(sql,[sec,origin,*params]); rows=cur.fetchall()
        out=[]
        for tag,typ,bucket,mint,minv,maxt,maxv in rows:
            out.append({"timestamp":mint,"tag":tag,"type":typ,"value":minv})
            if maxt!=mint: out.append({"timestamp":maxt,"tag":tag,"type":typ,"value":maxv})
        return pd.DataFrame(out,columns=["timestamp","tag","type","value"])
    finally: conn.close()


def csv_stream(dataset_id,start,end,tags=None):
    conn=connect()
    try:
        where,params=_where(dataset_id,start,end,tags)
        with conn.cursor() as cur:
            cur.execute(f"SELECT dataset_id,timestamp,tag,type,value,quality,unit FROM {TABLE} WHERE {where} ORDER BY tag,timestamp",params)
            yield "dataset_id,timestamp,tag,type,value,quality,unit\n"
            while rows:=cur.fetchmany(5000):
                out=io.StringIO(); csv.writer(out,lineterminator="\n").writerows(rows); yield out.getvalue()
    finally: conn.close()


def daily_summary(dataset_id):
    conn=connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT to_char(timestamp AT TIME ZONE 'Asia/Shanghai','YYYY-MM-DD'),tag,type,count(value),avg(value),min(value),max(value) FROM {TABLE} WHERE dataset_id=%s GROUP BY 1,tag,type ORDER BY 1,tag",[str(dataset_id)])
            return [dict(zip(["day","tag","type","count","mean","min","max"],r)) for r in cur.fetchall()]
    finally: conn.close()


def ping():
    try:
        conn=connect(); conn.close(); return True
    except Exception: return False
