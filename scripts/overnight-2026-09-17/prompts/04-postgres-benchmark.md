# 任务：大文件导入与 PostgreSQL 性能基准

## 背景（已实测的基线）

`docs/benchmark-postgres-latest.json`：100 万源行 / 300 万规范化行，导入 218.8 秒（约 13710 规范化行/秒），源 CSV 34.5 MB，表+索引增量 711 MB，查询一天 3.89 秒。PostgreSQL 容器 `pid-postgres-1` 正在运行（`127.0.0.1:5432`，健康）。磁盘剩余约 56 GB。`scripts/benchmark_postgres.py` 已能跑该基准，`scripts/benchmark.py` 负责 Parquet 模式。

## 要做的事

1. 先看空间：确认 C 盘剩余空间与 `postgres_data` 当前占用，再决定压测规模；任何单次导入后的表增量不要超过剩余空间的 25%。
2. 扩展 `scripts/benchmark_postgres.py`，记录更多可复核指标：生成/导入/查询各阶段耗时、规范化行每秒、`pid_samples` 表大小、索引大小、总增量（分列）、upsert 冲突行数、查询延迟（one_hour / one_day / one_month 三档）。
3. 跑一次比现有基线更大的基准（建议 200 万源行，量力而行），报告写到 `docs/benchmark-postgres-2026-09-18.json`，**不要覆盖** `docs/benchmark-postgres-latest.json`。若时间或空间不允许放大，就跑 100 万行并把新增指标补齐，并说明原因。
4. 验证重启恢复：导入一个隔离 dataset_id → `docker compose restart postgres` → 查询结果与重启前一致 → 清理该数据集。重启会短暂断开 5432，先在重启前后记录容器状态，确认 8080 的 `/health` 不受影响。
5. 结束后必须清理自己创建的临时数据（隔离 dataset_id 与 `.tmp` 工作目录）；不要删 `postgres_data`，不要动既有 dataset。

## 验收

- 新报告文件存在且字段完整；回复里贴出关键数字（吞吐、磁盘增量、三档查询延迟）。
- 说明本次实测规模、耗时与「还不足以证明什么」（例如未覆盖 10GB 级）。

## 全局约束（必须遵守）

- 工作区：`D:\codex\PID数据可视化`；Python 一律用 `.venv\Scripts\python.exe`。
- **禁止**执行 `git commit`、`git push`、`git reset`、`git checkout`、`git clean`、`git stash`；只允许读 git 状态与 diff。
- **禁止删除** `data/`、`postgres_data/`、`.tmp/` 下任何已有内容；不要读取或打印 `.env` 的密码明文。
- 保留任务开始前已存在的未提交修改，不要回滚别人的改动。
- 用户自己的 uvicorn 正在监听 `0.0.0.0:8080`，**不要停掉它**。需要临时服务时用别的端口（如 8099），并且只结束你自己启动的进程。
- 不要向正式 `data/` 导入大文件；大文件只在明确要求时使用 `.tmp/` 下的临时副本。
- 改动要小、可审查；每处行为改动都配对应测试。收尾必须运行 `.venv\Scripts\python.exe -m pytest -q`，失败就修到通过；确实修不了的如实说明。
- 不确定是否真 bug 时，先写清证据，不要凭猜测大改。

## 最终回复格式（中文，简洁）

1. 修改/新增的文件清单
2. 关键改动说明（每条一句话讲清为什么）
3. 实际运行的命令与结果（pytest 结尾那行数字照抄）
4. 仍未验证或未覆盖的事项
