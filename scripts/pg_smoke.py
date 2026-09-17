"""PostgreSQL 集成冒烟验收脚本。

针对 app/postgres_db.py 的真实数据库路径做端到端验收：
    连接 / 建表与索引 / COPY 导入 / 明细查询 / 分桶查询 /
    CSV 流式导出 / 重复键 upsert / 失败回滚清理 / 重连持久性。

运行前提：
    - 本机 127.0.0.1:5432 存在 PostgreSQL（docker compose up -d postgres）
    - 项目根目录 .env 提供 PID_POSTGRES_PASSWORD
    - .venv 安装 psycopg[binary]

安全约束：
    - 仅使用隔离的 dataset_id（pg-smoke-<ts>）与 .tmp 下隔离 data 目录；
    - 不 DROP 表/库，不重建或清空 postgres_data；
    - 结束仅删除本脚本创建的冒烟数据行，退出码非 0 表示存在失败项。
"""

from __future__ import annotations

import os
import sys
import time
import traceback
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
TMP = ROOT / ".tmp" / "pg-smoke"


def _load_env() -> None:
    """从 .env 读取 PID_POSTGRES_PASSWORD，不打印其值。"""
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


def _configure_postgres() -> None:
    _load_env()
    os.environ["PID_STORAGE_BACKEND"] = "postgres"
    os.environ.setdefault("PID_POSTGRES_HOST", "127.0.0.1")
    os.environ.setdefault("PID_POSTGRES_PORT", "5432")
    os.environ.setdefault("PID_POSTGRES_DATABASE", "pid")
    os.environ.setdefault("PID_POSTGRES_USER", "pid")


RESULTS: list[tuple[str, bool, str]] = []


def record(name: str, ok: bool, detail: str = "") -> None:
    RESULTS.append((name, ok, detail))
    mark = "PASS" if ok else "FAIL"
    print(f"[{mark}] {name}" + (f"  -> {detail}" if detail else ""))


def main() -> int:
    _configure_postgres()
    import pandas as pd
    from app import postgres_db as pg

    tag = time.strftime("%Y%m%d-%H%M%S")
    ds = f"pg-smoke-{tag}"
    ds_rollback = f"pg-smoke-rb-{tag}"
    data_dir = TMP / tag
    data_dir.mkdir(parents=True, exist_ok=True)

    conn = None
    try:
        # 1. 真实连接
        try:
            conn = pg.connect()
            with conn.cursor() as cur:
                cur.execute("SELECT 1")
                assert cur.fetchone() == (1,)
            record("连接 (connect/SELECT 1)", True)
        except Exception as exc:  # noqa: BLE001
            record("连接 (connect/SELECT 1)", False, str(exc))
            return 1

        # 2. 建表与索引
        try:
            pg.ensure_schema(conn)
            with conn.cursor() as cur:
                cur.execute(
                    "SELECT indexname FROM pg_indexes WHERE tablename=%s",
                    [pg.TABLE],
                )
                indexes = [r[0] for r in cur.fetchall()]
            assert f"{pg.TABLE}_time_idx" in indexes, f"缺少索引，现有: {indexes}"
            record("建表与索引 (ensure_schema)", True, f"indexes={indexes}")
        except Exception as exc:  # noqa: BLE001
            record("建表与索引 (ensure_schema)", False, str(exc))
            return 1

        # 准备两份同时间戳、不同取值的 CSV（用于导入 + upsert）
        n = 120
        ts = pd.date_range("2026-01-01 00:00:00", periods=n, freq="1s")
        v1 = pd.DataFrame({"timestamp": ts, "A.PV": [float(i) for i in range(n)],
                           "A.SV": [10.0] * n})
        v2 = pd.DataFrame({"timestamp": ts, "A.PV": [float(1000 + i) for i in range(n)],
                           "A.SV": [10.0] * n})
        csv1 = data_dir / "v1.csv"
        csv2 = data_dir / "v2.csv"
        v1.to_csv(csv1, index=False)
        v2.to_csv(csv2, index=False)

        # 3. COPY 导入（宽表按信号纵向展开，源 120 行 × 2 信号 = 240 条记录）
        expect_rows = n * 2
        try:
            meta = pg.import_file(csv1, ds, csv1.name, data_dir)
            assert meta["rows"] == expect_rows, f"行数不符 {meta['rows']} != {expect_rows}"
            assert {s["tag"] for s in meta["signals"]} == {"A.PV", "A.SV"}
            record("导入 (COPY/upsert)", True,
                   f"rows={meta['rows']} signals={sorted(s['tag'] for s in meta['signals'])}")
        except Exception as exc:  # noqa: BLE001
            record("导入 (COPY/upsert)", False, str(exc))
            return 1

        # 4. 明细查询
        try:
            frame = pg.query_frame(ds, "2026-01-01 00:00:00", "2026-01-01 00:02:00")
            assert len(frame) == expect_rows, f"明细行数 {len(frame)} != {expect_rows}"
            pv = frame[frame["tag"] == "A.PV"].sort_values("timestamp")
            assert pv.iloc[0]["value"] == 0.0
            record("明细查询 (query_frame)", True, f"rows={len(frame)}")
        except Exception as exc:  # noqa: BLE001
            record("明细查询 (query_frame)", False, str(exc))
            return 1

        # 5. 分桶查询（60s）
        try:
            buckets = pg.query_buckets(ds, "2026-01-01 00:00:00", "2026-01-01 00:02:00", seconds=60)
            assert not buckets.empty, "分桶结果为空"
            record("分桶查询 (query_buckets)", True, f"points={len(buckets)}")
        except Exception as exc:  # noqa: BLE001
            record("分桶查询 (query_buckets)", False, str(exc))
            return 1

        # 6. CSV 流式导出
        try:
            chunks = list(pg.csv_stream(ds, "2026-01-01 00:00:00", "2026-01-01 00:00:10"))
            body = "".join(chunks)
            assert body.startswith("dataset_id,timestamp,tag,type,value,quality,unit"), "缺少表头"
            record("CSV 流式导出 (csv_stream)", True, f"chunks={len(chunks)} bytes={len(body)}")
        except Exception as exc:  # noqa: BLE001
            record("CSV 流式导出 (csv_stream)", False, str(exc))
            return 1

        # 7. 重复键 upsert：同时间戳新取值覆盖旧值，不新增行
        try:
            pg.import_file(csv2, ds, csv2.name, data_dir)
            frame = pg.query_frame(ds, "2026-01-01 00:00:00", "2026-01-01 00:02:00")
            assert len(frame) == expect_rows, f"upsert 后行数 {len(frame)} != {expect_rows}"
            pv = frame[frame["tag"] == "A.PV"].sort_values("timestamp")
            assert pv.iloc[0]["value"] == 1000.0, f"upsert 未覆盖值: {pv.iloc[0]['value']}"
            record("重复键 upsert", True, f"rows={len(frame)} first_pv={pv.iloc[0]['value']}")
        except Exception as exc:  # noqa: BLE001
            record("重复键 upsert", False, str(exc))
            return 1

        # 8. 重连持久性：断开后新连接仍读到数据
        try:
            conn.close()
            conn = pg.connect()
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM pid_samples WHERE dataset_id=%s", [ds])
                cnt = cur.fetchone()[0]
            assert cnt == expect_rows, f"重连后行数 {cnt} != {expect_rows}"
            record("重连持久性", True, f"rows={cnt}")
        except Exception as exc:  # noqa: BLE001
            record("重连持久性", False, str(exc))
            return 1

        # 9. 失败回滚清理：后续批失败时，已提交的数据被清理（真实 DB，注入失败触发）
        try:
            import unittest.mock as mock

            pg.import_file(csv1, ds_rollback, csv1.name, data_dir)
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM pid_samples WHERE dataset_id=%s", [ds_rollback])
                before = cur.fetchone()[0]
            assert before == expect_rows

            real_insert = pg.insert_batch
            calls = {"n": 0}

            def flaky_insert(c, frame):
                calls["n"] += 1
                if calls["n"] == 2:
                    raise RuntimeError("注入的批处理失败")
                return real_insert(c, frame)

            raised = False
            with mock.patch.object(pg, "insert_batch", flaky_insert):
                try:
                    pg.import_file(csv2, ds_rollback, csv2.name, data_dir, chunk_rows=1)
                except Exception:  # noqa: BLE001
                    raised = True
            with conn.cursor() as cur:
                cur.execute("SELECT count(*) FROM pid_samples WHERE dataset_id=%s", [ds_rollback])
                leftover = cur.fetchone()[0]
            assert raised, "未触发注入异常"
            assert leftover == 0, f"失败后仍残留 {leftover} 行"
            record("失败回滚清理", True, f"raised={raised} before={before} leftover={leftover}")
        except Exception as exc:  # noqa: BLE001
            record("失败回滚清理", False, str(exc))
            return 1

        return 0
    finally:
        # 仅清理本脚本创建的数据行，保留其它一切数据
        try:
            if conn is None or conn.closed:
                conn = pg.connect()
            with conn.cursor() as cur:
                for name in (ds, ds_rollback):
                    cur.execute("DELETE FROM pid_samples WHERE dataset_id=%s", [name])
                cur.execute("DELETE FROM pid_samples WHERE dataset_id LIKE %s", ["pg-smoke-%"])
            conn.commit()
            print(f"[cleanup] 已删除本脚本冒烟数据 (datasets: {ds}, {ds_rollback})")
        except Exception as exc:  # noqa: BLE001
            print(f"[cleanup] 清理失败: {exc}")
        finally:
            if conn is not None and not conn.closed:
                conn.close()


if __name__ == "__main__":
    code = main()
    print("\n===== 冒烟验收汇总 =====")
    passed = sum(1 for _, ok, _ in RESULTS if ok)
    for name, ok, detail in RESULTS:
        print(f"{'PASS' if ok else 'FAIL'}  {name}" + (f"  ({detail})" if detail else ""))
    print(f"结果: {passed}/{len(RESULTS)} 通过")
    sys.exit(0 if code == 0 else 1)
