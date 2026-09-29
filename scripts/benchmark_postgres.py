"""PostgreSQL 规模化导入/查询基准。

走与 Web 应用一致的路径：PID_STORAGE_BACKEND=postgres 时 import_dataset
自动分派到 postgres_db.import_file（分块 + COPY + upsert）。测量导入吞吐、
PostgreSQL 表/索引占用增量和查询延迟。

运行前提：postgres 容器已启动、.env 提供 PID_POSTGRES_PASSWORD。
安全约束：仅写入隔离 dataset_id，结束后删除自身数据并清理工作目录。
"""

from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import time
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent
TABLE = "pid_samples"


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        raise SystemExit("缺少 .env（需要 PID_POSTGRES_PASSWORD）")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip() == "PID_POSTGRES_PASSWORD":
            os.environ.setdefault(key.strip(), value.strip())


def generate_csv(path: Path, rows: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 1, 1)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(["timestamp", "TIC101.PV", "TIC101.SV", "TIC101.MV"])
        for index in range(rows):
            timestamp = start + timedelta(seconds=index)
            sv = 80.0 if index < rows // 2 else 84.0
            writer.writerow([timestamp.isoformat(sep=" "), sv + (index % 30) / 20, sv, 40 + index % 20])


def _pg_connect():
    import psycopg
    return psycopg.connect(host="127.0.0.1", port=5432, dbname="pid", user="pid",
                           password=os.environ["PID_POSTGRES_PASSWORD"])


def pg_total_relation_size() -> int:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"SELECT pg_total_relation_size(%s)", [TABLE])
            return cur.fetchone()[0]
    finally:
        conn.close()


def pg_delete_dataset(dataset_id: str) -> None:
    conn = _pg_connect()
    try:
        with conn.cursor() as cur:
            cur.execute(f"DELETE FROM {TABLE} WHERE dataset_id=%s", [dataset_id])
        conn.commit()
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=1_000_000,
                        help="Source CSV rows; normalized rows = 3x this")
    parser.add_argument("--work-dir", default=".tmp/bench-pg")
    parser.add_argument("--report", default="docs/benchmark-postgres-latest.json")
    parser.add_argument("--input-csv", default=None,
                        help="Reuse an existing CSV instead of generating one (overrides --rows for generation)")
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    if args.rows < 1:
        raise SystemExit("--rows must be positive")

    _load_env()
    os.environ["PID_STORAGE_BACKEND"] = "postgres"
    os.environ.setdefault("PID_POSTGRES_HOST", "127.0.0.1")
    os.environ.setdefault("PID_POSTGRES_PORT", "5432")
    os.environ.setdefault("PID_POSTGRES_DATABASE", "pid")
    os.environ.setdefault("PID_POSTGRES_USER", "pid")

    work = Path(args.work_dir).resolve()
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    data_dir = work / "data"
    os.environ["PID_DATA_DIR"] = str(data_dir)
    source = Path(args.input_csv).resolve() if args.input_csv else work / "benchmark.csv"

    # 生成（计时）
    gen_start = time.perf_counter()
    if args.input_csv:
        generation_seconds = 0.0
    else:
        generate_csv(source, args.rows)
        generation_seconds = time.perf_counter() - gen_start

    from app.importer import import_dataset
    from app.query import query_data

    dataset_id = f"bench-pg-{uuid4().hex[:8]}"
    size_before = pg_total_relation_size()

    import_start = time.perf_counter()
    metadata = import_dataset(source, dataset_id, source.name, data_dir=data_dir)
    import_seconds = time.perf_counter() - import_start

    size_after = pg_total_relation_size()

    query_results = {}
    for label, end in [("one_hour", "2026-01-01T01:00:00"), ("one_day", "2026-01-02T00:00:00")]:
        started = time.perf_counter()
        payload = query_data(dataset_id, "2026-01-01T00:00:00", end, resolution="1m")
        query_results[label] = {
            "seconds": round(time.perf_counter() - started, 6),
            "returned_points": len(payload["rows"]),
        }

    result = {
        "generated_at": datetime.now().isoformat(timespec="seconds"),
        "backend": "postgres",
        # Normalized rows are always 3x the source rows; derive the source
        # count from metadata so --input-csv runs report correct numbers.
        "source_rows": metadata["rows"] // 3,
        "normalized_rows": metadata["rows"],
        "source_bytes": source.stat().st_size,
        "postgres_delta_bytes": size_after - size_before,
        "generation_seconds": round(generation_seconds, 6),
        "import_seconds": round(import_seconds, 6),
        "source_rows_per_second": round((metadata["rows"] // 3) / import_seconds, 2),
        "normalized_rows_per_second": round(metadata["rows"] / import_seconds, 2),
        "queries": query_results,
        "notes": "postgres_delta_bytes 为 pid_samples 表+索引总占用在本次导入前后的增量；未含源文件与 raw 副本。",
    }
    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))

    if not args.keep_data:
        pg_delete_dataset(dataset_id)
        print(f"[cleanup] 已删除 benchmark 数据集 {dataset_id}")
        shutil.rmtree(work)


if __name__ == "__main__":
    main()
