"""PostgreSQL 容器级重启持久性验收。

验证流程：写入一条标记数据 -> 确认已提交 -> docker restart postgres ->
等待健康 -> 重新连接读取标记数据 -> 清理标记数据。

安全约束：只写/删自己的标记数据，不动其它任何数据；不重建卷。
"""

from __future__ import annotations

import os
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
CONTAINER = "pid-postgres-1"
MARKER_DS = "pg-restart-marker"


def _load_env() -> None:
    env_path = ROOT / ".env"
    if not env_path.exists():
        sys.exit("缺少 .env（需要 PID_POSTGRES_PASSWORD）")
    for line in env_path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        if key.strip() == "PID_POSTGRES_PASSWORD":
            os.environ.setdefault(key.strip(), value.strip())


def _connect():
    import psycopg
    return psycopg.connect(
        host="127.0.0.1", port=5432, dbname="pid", user="pid",
        password=os.environ["PID_POSTGRES_PASSWORD"],
    )


def _count(ds: str) -> int:
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT count(*) FROM pid_samples WHERE dataset_id=%s", [ds])
            return cur.fetchone()[0]
    finally:
        conn.close()


def _wait_healthy(timeout: float = 90.0) -> float:
    """Wait until the database accepts connections; return seconds elapsed."""
    started = time.perf_counter()
    deadline = started + timeout
    last_err = ""
    while time.perf_counter() < deadline:
        try:
            conn = _connect()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
            conn.close()
            return time.perf_counter() - started
        except Exception as exc:  # noqa: BLE001
            last_err = str(exc)
            time.sleep(1.0)
    raise RuntimeError(f"等待 PostgreSQL 恢复超时（{timeout}s）：{last_err}")


def main() -> int:
    _load_env()
    import psycopg

    # 1. 写入标记数据并提交
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pid_samples WHERE dataset_id=%s", [MARKER_DS])  # 清理历史残留
            cur.execute(
                "INSERT INTO pid_samples"
                " (dataset_id,timestamp,tag,type,value,quality,unit,ingest_seq)"
                " VALUES (%s,%s,%s,%s,%s,%s,%s,%s)",
                [MARKER_DS, "2026-01-01 00:00:00+08:00", "A.PV", "PV", 42.0, "good", None, 1],
            )
        conn.commit()
        before = _count(MARKER_DS)
        assert before == 1, f"写入标记失败，计数 {before} != 1"
        print(f"[1] 写入标记数据成功，count={before}")
    finally:
        conn.close()

    # 2. 容器级重启
    print(f"[2] docker restart {CONTAINER} ...")
    subprocess.run(["docker", "restart", CONTAINER], check=True, capture_output=True)

    # 3. 等待恢复并计时
    recovery = _wait_healthy()
    print(f"[3] PostgreSQL 恢复可用，耗时 {recovery:.1f}s")

    # 4. 重启后读取标记数据
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute(
                "SELECT value, quality FROM pid_samples WHERE dataset_id=%s",
                [MARKER_DS],
            )
            rows = cur.fetchall()
        assert rows == [(42.0, "good")], f"重启后标记数据异常：{rows}"
        print(f"[4] 重启后读取成功，value={rows[0][0]} quality={rows[0][1]}")
    finally:
        conn.close()

    # 5. 清理标记数据
    conn = _connect()
    try:
        with conn.cursor() as cur:
            cur.execute("DELETE FROM pid_samples WHERE dataset_id=%s", [MARKER_DS])
        conn.commit()
    finally:
        conn.close()
    print(f"[5] 已清理标记数据")

    print("\nPASS: 容器级重启持久性验证通过")
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except Exception as exc:  # noqa: BLE001
        print(f"\nFAIL: {exc}")
        sys.exit(1)
