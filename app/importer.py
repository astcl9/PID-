from pathlib import Path
import re
import uuid
import json
import shutil
import tempfile
import itertools
import codecs
import os
from datetime import datetime, timezone
from typing import Callable
import pandas as pd

TYPE_RE = {
    "pv": re.compile(r"(?:^|[._\- ])pv(?:$|[._\- ])", re.I),
    "sp": re.compile(r"(?:^|[._\- ])(?:sp|sv)(?:$|[._\- ])", re.I),
    "mv": re.compile(r"(?:^|[._\- ])mv(?:$|[._\- ])", re.I),
}


def detect_csv_encoding(path: Path, sample_size: int = 64 * 1024) -> str:
    """Detect common CSV encodings from a bounded byte prefix.

    BOMs take precedence.  For files without a BOM, strict UTF-8 is tried
    first, followed by GB18030 (which also covers GBK).  UTF-16 files without
    a BOM are recognised from their characteristic NUL-byte pattern.
    """
    path = Path(path)
    with path.open("rb") as fh:
        prefix = fh.read(max(4, int(sample_size)))
    if prefix.startswith(codecs.BOM_UTF32_LE) or prefix.startswith(codecs.BOM_UTF32_BE):
        return "utf-32"
    if prefix.startswith(codecs.BOM_UTF8):
        return "utf-8-sig"
    if prefix.startswith(codecs.BOM_UTF16_LE) or prefix.startswith(codecs.BOM_UTF16_BE):
        return "utf-16"

    # UTF-16 without a BOM: ASCII-compatible CSV has NULs in alternating
    # positions.  Check both endian layouts before trying legacy encodings.
    if len(prefix) >= 4:
        even_nuls = sum(b == 0 for b in prefix[0::2]) / max(1, len(prefix[0::2]))
        odd_nuls = sum(b == 0 for b in prefix[1::2]) / max(1, len(prefix[1::2]))
        if odd_nuls > 0.30 and odd_nuls > even_nuls * 2:
            return "utf-16-le"
        if even_nuls > 0.30 and even_nuls > odd_nuls * 2:
            return "utf-16-be"

    # Decode incrementally so a multibyte character cut at the sample boundary
    # does not cause a false negative.  Full-file decoding remains strict in
    # pandas; this probe only selects the codec.
    for encoding in ("utf-8", "gb18030"):
        decoder = codecs.getincrementaldecoder(encoding)(errors="strict")
        try:
            decoder.decode(prefix, final=False)
            return encoding
        except UnicodeDecodeError:
            continue
    raise ValueError("无法识别 CSV 编码，请另存为 UTF-8、GB18030 或 UTF-16")


def read_csv_chunks(path: Path, chunksize: int | None = None, max_values: int = 100_000):
    """Read CSV using bounded encoding detection and optional pandas chunks."""
    encoding = detect_csv_encoding(path)
    # Guard against malformed exports (for example a missing newline or a
    # wrong delimiter) that make pandas infer millions of columns and allocate
    # a giant rectangular array before any useful row is returned.
    with Path(path).open("rb") as fh:
        prefix = fh.read(8 * 1024 * 1024)
    newline = prefix.find(b"\n")
    if newline < 0 and len(prefix) == 8 * 1024 * 1024 and Path(path).stat().st_size > len(prefix):
        raise ValueError("CSV 首行超过 8 MB，疑似分隔符或换行格式错误；请确认文件是标准 CSV（每行一条记录）")
    if newline > 2 * 1024 * 1024:
        raise ValueError("CSV 首行超过 2 MB，疑似分隔符或换行格式错误；请检查导出格式")
    kwargs = {"encoding": encoding}
    if chunksize is not None:
        # Normalisation expands a wide row-oriented table into one row per
        # signal.  Bound that expanded cell count, not just input row count
        # (25,000 rows × 779 signals is already ~19.5 million values).
        try:
            header = pd.read_csv(path, encoding=encoding, nrows=0)
            signal_columns = max(1, len(header.columns) - 1)
            chunksize = min(int(chunksize), max(1, int(max_values) // signal_columns))
        except MemoryError as exc:
            raise ValueError("CSV 表头过宽，无法安全导入；请减少信号列或改用 PostgreSQL 模式") from exc
        kwargs["chunksize"] = max(1, int(chunksize))
    try:
        reader = pd.read_csv(path, **kwargs)
    except MemoryError as exc:
        raise ValueError("CSV 结构异常：解析器推断出过多列。请检查分隔符、换行符，或另存为标准 UTF-8 CSV") from exc
    if chunksize is None:
        return reader
    return _guard_csv_reader(reader)


def _guard_csv_reader(reader):
    try:
        for chunk in reader:
            yield chunk
    except MemoryError as exc:
        raise ValueError("CSV 结构异常：解析器推断出过多列。请检查分隔符、换行符，或另存为标准 UTF-8 CSV") from exc

def classify_signal(name: str) -> str:
    n = str(name)
    for kind, rx in TYPE_RE.items():
        if rx.search(n) or n.lower().endswith(kind):
            return "SV" if kind == "sp" else kind.upper()
    return "other"

def detect_timestamp(df: pd.DataFrame) -> str:
    preferred = [c for c in df.columns if str(c).lower() in {"timestamp", "time", "datetime", "date"}]
    if preferred:
        return preferred[0]
    for c in df.columns:
        parsed = pd.to_datetime(df[c], errors="coerce")
        if parsed.notna().mean() > 0.8:
            return c
    raise ValueError("未找到可识别的时间列")

def load_table(path: Path) -> pd.DataFrame:
    suffix = path.suffix.lower()
    if suffix == ".csv":
        return read_csv_chunks(path)
    if suffix in {".xlsx", ".xls"}:
        return pd.read_excel(path)
    raise ValueError("仅支持 CSV/XLSX 文件")

def normalize_file(path: Path, dataset_id: str | None = None) -> tuple[str, pd.DataFrame, dict]:
    dataset_id = dataset_id or uuid.uuid4().hex
    source = load_table(path)
    tcol = detect_timestamp(source)
    ts = pd.to_datetime(source[tcol], errors="coerce", utc=True)
    warnings = []
    bad = int(ts.isna().sum())
    if bad:
        warnings.append(f"忽略 {bad} 行无效时间")
    source = source.loc[ts.notna()].copy()
    source["timestamp"] = ts.loc[ts.notna()].dt.tz_convert(None).values
    source = source.sort_values("timestamp")
    dup = int(source.duplicated("timestamp").sum())
    if dup:
        warnings.append(f"发现 {dup} 个重复时间戳")
        source = source.drop_duplicates("timestamp", keep="last")
    rows = []
    for col in source.columns:
        if col in {tcol, "timestamp"}:
            continue
        val = pd.to_numeric(source[col], errors="coerce")
        if val.notna().sum() == 0:
            continue
        kind = classify_signal(col)
        for t, v in zip(source["timestamp"], val):
            rows.append((dataset_id, t, str(col), kind, None if pd.isna(v) else float(v), "good" if pd.notna(v) else "missing", None))
    out = pd.DataFrame(rows, columns=["dataset_id", "timestamp", "tag", "type", "value", "quality", "unit"])
    signals = [{"tag": t, "type": k} for t, k in out[["tag", "type"]].drop_duplicates().itertuples(index=False, name=None)]
    loops = sorted({str(t).rsplit(".", 1)[0] for t in out["tag"].dropna().unique() if "." in str(t)})
    report = {"id": dataset_id, "rows": len(out), "start_time": source["timestamp"].min(), "end_time": source["timestamp"].max(), "warnings": warnings,
              "signals": signals, "loops": loops}
    return dataset_id, out, report


def _report_progress(callback: Callable[..., object] | None, **fields) -> None:
    """Report progress without making an import fail because a UI callback failed."""
    if callback is None:
        return
    try:
        callback(**fields)
    except TypeError:
        # A few callers use a positional callback; retain compatibility.
        try:
            callback(fields)
        except Exception:
            pass
    except Exception:
        pass


def _timestamp_column(columns, sample: pd.DataFrame | None = None) -> str:
    if sample is None:
        sample = pd.DataFrame(columns=columns)
    return detect_timestamp(sample)


def _normalise_chunk(chunk: pd.DataFrame, dataset_id: str, timestamp_col: str,
                     seq_start: int) -> tuple[pd.DataFrame, int, list[str]]:
    if chunk.empty:
        return pd.DataFrame(), seq_start, []
    raw_values = chunk[timestamp_col]
    # Naive timestamps are plant-local (Asia/Shanghai); offset-bearing values
    # are converted from their stated zone into that local wall clock.
    raw_ts = pd.to_datetime(raw_values, errors="coerce", utc=False)
    try:
        if getattr(raw_ts.dt, "tz", None) is None:
            raw_ts = raw_ts.dt.tz_localize("Asia/Shanghai")
        else:
            raw_ts = raw_ts.dt.tz_convert("Asia/Shanghai")
    except (AttributeError, TypeError, ValueError):
        raw_ts = pd.to_datetime(raw_values, errors="coerce", utc=True).dt.tz_convert("Asia/Shanghai")
    valid = raw_ts.notna()
    warnings = []
    bad = int((~valid).sum())
    if bad:
        warnings.append(f"忽略 {bad} 行无效时间")
    if not valid.any():
        return pd.DataFrame(), seq_start + len(chunk), warnings
    # Convert offset-aware timestamps to the configured local wall clock.
    timestamps = raw_ts.dt.tz_localize(None)
    work = chunk.loc[valid].copy()
    work["timestamp"] = timestamps.loc[valid].to_numpy()
    signal_cols = [c for c in work.columns if c not in {timestamp_col, "timestamp"}]
    records = []
    for col in signal_cols:
        values = pd.to_numeric(work[col], errors="coerce")
        if values.notna().sum() == 0:
            continue
        records.append(pd.DataFrame({
            "dataset_id": dataset_id,
            "timestamp": work["timestamp"].to_numpy(),
            "tag": str(col),
            "type": classify_signal(col),
            "value": values.astype("float64").to_numpy(),
            "quality": values.notna().map({True: "good", False: "missing"}).to_numpy(),
            "unit": None,
            "_ingest_seq": range(seq_start, seq_start + len(work)),
        }))
    out = pd.concat(records, ignore_index=True) if records else pd.DataFrame()
    return out, seq_start + len(chunk), warnings


def _xlsx_batches(path: Path, chunk_rows: int):
    try:
        from openpyxl import load_workbook
    except ImportError as exc:
        raise ValueError("读取 XLSX 需要安装 openpyxl") from exc
    wb = load_workbook(path, read_only=True, data_only=True)
    try:
        ws = wb.active
        rows = ws.iter_rows(values_only=True)
        try:
            header = list(next(rows))
        except StopIteration as exc:
            raise ValueError("输入文件为空") from exc
        if not any(v is not None and str(v).strip() for v in header):
            raise ValueError("输入文件缺少表头")
        header = [str(v).strip() if v is not None else f"column_{i}" for i, v in enumerate(header)]
        chunk_rows = min(int(chunk_rows), max(1, 100_000 // max(1, len(header) - 1)))
        batch = []
        for row in rows:
            batch.append(row)
            if len(batch) >= chunk_rows:
                yield pd.DataFrame(batch, columns=header)
                batch = []
        if batch:
            yield pd.DataFrame(batch, columns=header)
    finally:
        wb.close()


def _append_parquet_by_day(out: pd.DataFrame, root: Path, dataset_id: str, writers: dict, day_stats: dict) -> None:
    """Append a normalized chunk directly to day Parquet writers.

    This path deliberately does not invoke DuckDB or build a year-sized
    intermediate relation.  Each chunk is reduced to its last duplicate row,
    then written to the corresponding daily file with bounded memory.
    """
    import pyarrow as pa
    import pyarrow.parquet as pq

    # Keep the monotonically increasing ingest sequence in the staging file.
    # It is removed only after all chunks have arrived, allowing duplicate
    # samples split across chunk boundaries to retain the final source row.
    work = out.copy()
    work["timestamp"] = pd.to_datetime(work["timestamp"])
    work = work.drop_duplicates(["timestamp", "tag"], keep="last")
    work["_day"] = work["timestamp"].dt.strftime("%Y-%m-%d")
    for dstr, part in work.groupby("_day", sort=False):
        part = part.drop(columns=["_day"])
        dest = root / "parquet" / str(dataset_id) / f"date={dstr}"
        dest.mkdir(parents=True, exist_ok=True)
        target = dest / "data.parquet"
        table = pa.Table.from_pandas(part, preserve_index=False)
        writer = writers.get(dstr)
        if writer is None:
            writer = pq.ParquetWriter(target, table.schema, compression="zstd")
            writers[dstr] = writer
        writer.write_table(table)
        stamp = pd.to_datetime(part["timestamp"])
        current = day_stats.get(dstr)
        count, lo, hi = len(part), stamp.min(), stamp.max()
        if current:
            day_stats[dstr] = (current[0] + count, min(current[1], lo), max(current[2], hi))
        else:
            day_stats[dstr] = (count, lo, hi)


def _compact_day_partitions(root: Path, dataset_id: str, days) -> dict[str, tuple[int, pd.Timestamp, pd.Timestamp]]:
    """Deduplicate each day partition on disk and return exact final stats."""
    try:
        import duckdb
    except ImportError as exc:
        raise RuntimeError("大文件导入去重需要安装 duckdb") from exc

    final_stats: dict[str, tuple[int, pd.Timestamp, pd.Timestamp]] = {}
    temp_dir = root / "tmp" / f"compact-{dataset_id}"
    temp_dir.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(config={
        "memory_limit": os.getenv("PID_DUCKDB_MEMORY_LIMIT", "512MB"),
        "threads": 1,
        "preserve_insertion_order": "false",
        "temp_directory": str(temp_dir),
    })
    try:
        for dstr in days:
            target = root / "parquet" / str(dataset_id) / f"date={dstr}" / "data.parquet"
            compacted = target.with_name("data.compacted.parquet")
            source_sql = str(target).replace("'", "''")
            compacted_sql = str(compacted).replace("'", "''")
            con.execute(f"""
                COPY (
                    SELECT * EXCLUDE (_ingest_seq)
                    FROM read_parquet('{source_sql}')
                    QUALIFY row_number() OVER (
                        PARTITION BY timestamp, tag
                        ORDER BY _ingest_seq DESC
                    ) = 1
                    ORDER BY timestamp, tag
                ) TO '{compacted_sql}' (FORMAT PARQUET, COMPRESSION ZSTD)
            """)
            compacted.replace(target)
            count, lo, hi = con.execute(
                f"SELECT count(*), min(timestamp), max(timestamp) FROM read_parquet('{source_sql}')"
            ).fetchone()
            final_stats[str(dstr)] = (int(count), pd.Timestamp(lo), pd.Timestamp(hi))
    finally:
        con.close()
    return final_stats


def import_dataset(path, dataset_id, filename, data_dir=None, progress=None, chunk_rows=25000):
    """Stream a CSV/XLSX into date-partitioned Parquet files in bounded chunks."""
    path = Path(path)
    if not path.exists() or path.stat().st_size == 0:
        raise ValueError("输入文件不存在或为空")
    if chunk_rows <= 0:
        raise ValueError("chunk_rows 必须为正数")
    from . import db
    if db.enabled():
        return db.import_file(path, dataset_id, filename, Path(data_dir) if data_dir is not None else Path("data"), progress=progress, chunk_rows=chunk_rows)
    root = Path(data_dir) if data_dir is not None else Path("data")
    dataset_id = str(dataset_id)
    total_bytes = path.stat().st_size
    suffix = path.suffix.lower()
    if suffix not in {".csv", ".xlsx", ".xls"}:
        raise ValueError("仅支持 CSV/XLSX 文件")
    warnings, signal_names = [], set()
    rows_processed = bytes_processed = seq = 0
    writers = {}
    day_stats = {}
    success = False
    try:
        if suffix == ".csv":
            try:
                reader = read_csv_chunks(path, chunksize=chunk_rows)
                first = next(reader)
            except StopIteration as exc:
                raise ValueError("输入文件没有数据行") from exc
            except pd.errors.EmptyDataError as exc:
                raise ValueError("输入文件为空") from exc
            timestamp_col = detect_timestamp(first)
            for chunk in itertools.chain((first,), reader):
                if timestamp_col is None:
                    timestamp_col = detect_timestamp(chunk)
                out, seq, ws = _normalise_chunk(chunk, dataset_id, timestamp_col, seq)
                warnings.extend(ws)
                if not out.empty:
                    signal_names.update(out["tag"].unique())
                    _append_parquet_by_day(out, root, dataset_id, writers, day_stats)
                rows_processed += len(chunk)
                # pandas does not expose a portable byte offset for TextFileReader;
                # report a monotonic estimate until the final completed callback.
                bytes_processed = min(total_bytes, int(total_bytes * rows_processed / max(rows_processed + chunk_rows, 1)))
                _report_progress(progress, stage="processing", rows_processed=rows_processed,
                                 bytes_processed=bytes_processed, total_bytes=total_bytes,
                                 progress=(bytes_processed / total_bytes if total_bytes else None))
        else:
            timestamp_col = None
            for chunk in _xlsx_batches(path, chunk_rows):
                if timestamp_col is None:
                    timestamp_col = detect_timestamp(chunk)
                out, seq, ws = _normalise_chunk(chunk, dataset_id, timestamp_col, seq)
                warnings.extend(ws)
                if not out.empty:
                    signal_names.update(out["tag"].unique())
                    _append_parquet_by_day(out, root, dataset_id, writers, day_stats)
                rows_processed += len(chunk)
                _report_progress(progress, stage="processing", rows_processed=rows_processed,
                                 bytes_processed=bytes_processed, total_bytes=total_bytes, progress=None)
        if not day_stats:
            raise ValueError("输入文件没有可用的数值数据")
        for writer in writers.values(): writer.close()
        writers.clear()
        days = sorted(day_stats)
        # A full windowed deduplication requires an external sort over every
        # normalized row. For very wide files this can be tens of millions of
        # rows even when the source has only a few tens of thousands of lines.
        # Keep the bounded per-chunk dedupe in that case and publish a warning;
        # PostgreSQL mode remains available for exact upserts at that scale.
        max_dedupe_rows = int(os.getenv("PID_MAX_DEDUPE_ROWS", "5000000"))
        estimated_rows = max(1, rows_processed) * max(1, len(signal_names))
        if estimated_rows <= max_dedupe_rows:
            day_stats = _compact_day_partitions(root, dataset_id, days)
        else:
            warnings.append(
                f"标准化记录预计约 {estimated_rows:,} 行，超过全量去重阈值 {max_dedupe_rows:,}；"
                "已执行分块去重，超大文件建议使用 PostgreSQL 模式"
            )
        stats = (sum(x[0] for x in day_stats.values()), min(x[1] for x in day_stats.values()), max(x[2] for x in day_stats.values()))
        sigs = [(str(tag), classify_signal(tag)) for tag in sorted(signal_names)]
        signal_count = max(1, len(sigs))
        day_summaries = []
        for day in days:
            count, lo, hi = day_stats[str(day)]
            expected = max(1, int((hi - lo).total_seconds() / 60) + 1)
            samples_per_signal = count / signal_count
            coverage = min(1.0, samples_per_signal / expected)
            day_summaries.append({
                "date": str(day), "coverage": round(float(coverage), 6),
                "segmentCount": 0, "alarmCount": 0,
                "score": round(float(coverage) * 100, 2),
            })
        metadata = {"id": dataset_id, "name": filename, "rows": int(stats[0]),
                    "start_time": stats[1], "end_time": stats[2], "warnings": warnings,
                    "signals": [{"tag": str(t), "type": str(k)} for t, k in sigs],
                    "loops": sorted({str(t).rsplit('.', 1)[0] for t, _ in sigs if '.' in str(t)}),
                    "days": [str(d) for d in days], "day_summaries": day_summaries}
        (root / "meta").mkdir(parents=True, exist_ok=True)
        (root / "meta" / f"{dataset_id}.json").write_text(json.dumps(metadata, default=str, ensure_ascii=False), encoding="utf-8")
        raw = root / "raw" / dataset_id
        raw.mkdir(parents=True, exist_ok=True)
        target_raw = raw / Path(filename).name
        if target_raw.resolve() != path.resolve():
            shutil.copy2(path, target_raw)
        _report_progress(progress, stage="completed", rows_processed=rows_processed,
                         bytes_processed=total_bytes, total_bytes=total_bytes, progress=1.0)
        success = True
        return metadata
    finally:
        for writer in writers.values():
            try: writer.close()
            except Exception: pass
        if not success:
            shutil.rmtree(root / "parquet" / dataset_id, ignore_errors=True)
