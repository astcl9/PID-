"""Generate a deterministic PID CSV and measure import/query performance."""
from __future__ import annotations

import argparse
import csv
from datetime import datetime, timedelta
import json
import os
from pathlib import Path
import shutil
import time
import tracemalloc
from uuid import uuid4


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


def directory_size(path: Path) -> int:
    return sum(item.stat().st_size for item in path.rglob("*") if item.is_file()) if path.exists() else 0


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--rows", type=int, default=100_000, help="Source CSV rows; normalized storage has three times this count")
    parser.add_argument("--work-dir", default=".tmp/benchmark")
    parser.add_argument("--report", default="docs/benchmark-latest.json")
    parser.add_argument("--input-csv", default=None,
                        help="Reuse an existing CSV instead of generating one (overrides --rows for generation)")
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()
    if args.rows < 1:
        raise SystemExit("--rows must be positive")

    work = Path(args.work_dir).resolve()
    data_dir = work / "data"
    source = Path(args.input_csv).resolve() if args.input_csv else work / "benchmark.csv"
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    os.environ["PID_DATA_DIR"] = str(data_dir)
    os.environ["PID_STORAGE_BACKEND"] = "parquet"

    from app.importer import import_dataset
    from app import storage
    from app.query import query_data

    storage.DATA_DIR = data_dir
    generate_start = time.perf_counter()
    if args.input_csv:
        generation_seconds = 0.0
    else:
        generate_csv(source, args.rows)
        generation_seconds = time.perf_counter() - generate_start
    dataset_id = f"benchmark-{uuid4().hex[:8]}"

    tracemalloc.start()
    import_start = time.perf_counter()
    metadata = import_dataset(source, dataset_id, source.name, data_dir=data_dir)
    import_seconds = time.perf_counter() - import_start
    _, peak_memory = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    # Normalized rows are always 3x the source rows (PV/SV/MV); derive the
    # source count from metadata so --input-csv runs report correct numbers.
    source_rows = metadata["rows"] // 3

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
        "source_rows": source_rows,
        "normalized_rows": metadata["rows"],
        "source_bytes": source.stat().st_size,
        "parquet_bytes": directory_size(data_dir / "parquet" / dataset_id),
        "generation_seconds": round(generation_seconds, 6),
        "import_seconds": round(import_seconds, 6),
        "source_rows_per_second": round(source_rows / import_seconds, 2),
        "normalized_rows_per_second": round(metadata["rows"] / import_seconds, 2),
        "python_peak_memory_bytes": peak_memory,
        "queries": query_results,
        "notes": "Peak memory is measured with tracemalloc and excludes some native-library allocations.",
    }
    report = Path(args.report)
    report.parent.mkdir(parents=True, exist_ok=True)
    report.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
    print(json.dumps(result, indent=2, ensure_ascii=False))
    if not args.keep_data:
        shutil.rmtree(work)


if __name__ == "__main__":
    main()
