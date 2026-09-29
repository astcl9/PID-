"""Parquet 后端多回路并发查询基准。

补齐"只用 Parquet"决策前的最后一块验证：合成多回路数据集（默认 50 回路 x
PV/SV/MV、全天 1 分钟采样），拉起独立 uvicorn 实例（独立数据目录与端口，
不影响 8080 上的常驻服务），用 N 个并发线程模拟局域网用户随机查窗口，
记录延迟分布（p50/p95/max）与吞吐。

结果写入 docs/benchmark-concurrency.json，结束后清理子进程与工作目录。
"""
from __future__ import annotations

import argparse
import csv
import json
import os
import shutil
import socket
import statistics
import subprocess
import sys
import threading
import time
import urllib.request
from datetime import datetime, timedelta
from pathlib import Path
from uuid import uuid4

ROOT = Path(__file__).resolve().parent.parent


def _free_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def generate_csv(path: Path, loops: int, minutes: int) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    start = datetime(2026, 1, 1)
    header = ["timestamp"] + [f"L{i}.{sig}" for i in range(1, loops + 1) for sig in ("PV", "SV", "MV")]
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.writer(handle)
        writer.writerow(header)
        for index in range(minutes):
            ts = start + timedelta(minutes=index)
            row = [ts.isoformat(sep=" ")]
            for i in range(1, loops + 1):
                sv = 80.0 + i
                row.extend([sv + (index % 30) / 20, sv, 40 + (index + i) % 20])
            writer.writerow(row)


def _get_json(base: str, path: str, timeout: float = 120.0):
    started = time.perf_counter()
    with urllib.request.urlopen(base + path, timeout=timeout) as resp:
        payload = json.loads(resp.read().decode("utf-8"))
    return payload, time.perf_counter() - started


def percentile(sorted_values: list[float], pct: float) -> float:
    if not sorted_values:
        return 0.0
    index = min(len(sorted_values) - 1, max(0, int(round((pct / 100) * (len(sorted_values) - 1)))))
    return sorted_values[index]


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--loops", type=int, default=50)
    parser.add_argument("--users", type=int, default=20)
    parser.add_argument("--requests-per-user", type=int, default=10)
    parser.add_argument("--work-dir", default=".tmp/bench-concurrency")
    parser.add_argument("--report", default="docs/benchmark-concurrency.json")
    parser.add_argument("--keep-data", action="store_true")
    args = parser.parse_args()

    work = (ROOT / args.work_dir).resolve()
    if work.exists():
        shutil.rmtree(work)
    work.mkdir(parents=True)
    data_dir = work / "data"
    source = work / "loops.csv"

    generate_csv(source, args.loops, minutes=24 * 60)
    os.environ["PID_DATA_DIR"] = str(data_dir)

    from app.importer import import_parquet_only
    from app import storage

    storage.DATA_DIR = data_dir
    dataset_id = f"bench-conc-{uuid4().hex[:8]}"
    import_start = time.perf_counter()
    metadata = import_parquet_only(source, dataset_id, source.name, data_dir=data_dir)
    import_seconds = time.perf_counter() - import_start

    port = _free_port()
    env = dict(os.environ, PID_DATA_DIR=str(data_dir))
    server = subprocess.Popen(
        [sys.executable, "-m", "uvicorn", "app.main:app", "--host", "127.0.0.1", "--port", str(port)],
        cwd=str(ROOT), env=env,
        stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
    )
    base = f"http://127.0.0.1:{port}"
    try:
        # 等待服务就绪
        for _ in range(120):
            try:
                with urllib.request.urlopen(base + "/health", timeout=2):
                    break
            except Exception:
                time.sleep(0.5)
        else:
            raise SystemExit("uvicorn 未在预期时间内就绪")

        # 预热：一次全天 + 一次 1 小时查询（触发 DuckDB/元数据缓存）
        _get_json(base, f"/api/data?dataset_id={dataset_id}&start=2026-01-01T00:00:00&end=2026-01-02T00:00:00&resolution=1m")
        _get_json(base, f"/api/data?dataset_id={dataset_id}&start=2026-01-01T00:00:00&end=2026-01-01T01:00:00&resolution=1m")

        latencies: list[float] = []
        errors: list[str] = []
        lock = threading.Lock()
        day_start = datetime(2026, 1, 1)

        def user_worker(seed: int) -> None:
            for step in range(args.requests_per_user):
                loop_id = 1 + ((seed * 31 + step * 17) % args.loops)
                window_hours = 1 + ((seed + step) % 6)
                start = day_start + timedelta(hours=(seed * 7 + step * 3) % (24 - window_hours))
                end = start + timedelta(hours=window_hours)
                tags = f"L{loop_id}.PV,L{loop_id}.SV,L{loop_id}.MV"
                path = (f"/api/data?dataset_id={dataset_id}&start={start.isoformat()}"
                        f"&end={end.isoformat()}&tags={tags}&resolution=1m")
                try:
                    _, elapsed = _get_json(base, path)
                    with lock:
                        latencies.append(elapsed)
                except Exception as exc:  # noqa: BLE001 - 记录并发下的真实失败
                    with lock:
                        errors.append(f"{type(exc).__name__}: {exc}")

        run_start = time.perf_counter()
        threads = [threading.Thread(target=user_worker, args=(i,)) for i in range(args.users)]
        for thread in threads:
            thread.start()
        for thread in threads:
            thread.join()
        wall_seconds = time.perf_counter() - run_start

        ordered = sorted(latencies)
        result = {
            "generated_at": datetime.now().isoformat(timespec="seconds"),
            "backend": "parquet",
            "dataset": {"loops": args.loops, "signals": args.loops * 3,
                        "minutes": 24 * 60, "normalized_rows": metadata["rows"]},
            "load": {"concurrent_users": args.users, "requests_per_user": args.requests_per_user,
                     "total_requests": args.users * args.requests_per_user,
                     "window": "随机回路 + 1~6 小时窗口 @1m 分辨率（3 信号/请求）"},
            "import_seconds": round(import_seconds, 3),
            "wall_seconds": round(wall_seconds, 3),
            "completed_requests": len(latencies),
            "failed_requests": len(errors),
            "requests_per_second": round(len(latencies) / wall_seconds, 2) if wall_seconds else 0,
            "latency_seconds": {
                "p50": round(percentile(ordered, 50), 4),
                "p95": round(percentile(ordered, 95), 4),
                "max": round(ordered[-1], 4) if ordered else 0,
            },
            "errors": errors[:10],
        }
        report = ROOT / args.report
        report.parent.mkdir(parents=True, exist_ok=True)
        report.write_text(json.dumps(result, indent=2, ensure_ascii=False), encoding="utf-8")
        print(json.dumps(result, indent=2, ensure_ascii=False))
    finally:
        server.terminate()
        try:
            server.wait(timeout=10)
        except subprocess.TimeoutExpired:
            server.kill()
        if not args.keep_data:
            shutil.rmtree(work, ignore_errors=True)


if __name__ == "__main__":
    main()
