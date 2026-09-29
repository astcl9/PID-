# 任务：增加 API、导入、查询与边界回归测试

## 背景

现有测试：`test_api.py`(5) `test_boundaries.py`(3) `test_export_stream.py`(4) `test_frontend_smoke.py`(2) `test_importer.py`(7) `test_jobs.py`(4) `test_large_import.py`(5) `test_segmentation.py`(4) `test_signal_mapping.py`(2) `test_size_routing.py`(12)。共 48 项。

## 要做的事

1. 找出尚未被测试覆盖的真实行为，优先覆盖这些高风险路径：
   - 导入失败路径：空文件、只有表头、时间列缺失、全非数值、编码异常，以及失败后任务状态与 `data/` 是否被正确清理（`/api/imports/{job_id}` 返回 `failed` 且带 error）。
   - 并发导入：`PID_IMPORT_WORKERS>1` 时任务互不干扰、各自 dataset_id 独立。
   - 查询：`tags` 过滤、`loop` 过滤、`resolution` 非法值、跨多天分桶、抽稀后仍保留桶内极值。
   - 导出：流式 CSV 的表头与行数、`loop` 过滤、空窗口导出（只有表头、不报错）。
   - 任务队列：`app/jobs.py` 的持久化与重启后恢复（SQLite `data/jobs.sqlite3`）。
2. 测试必须隔离：用 `tmp_path` + `monkeypatch` 改 `DATA_DIR`，不要污染真实 `data/`，不要依赖 8080 上的服务。
3. 只补测试；只有在测试暴露出可复现的真实缺陷时才顺手修，并在回复中单独列出这个缺陷。

## 验收

- 测试总数显著增加（目标增加 25 个以上），全部通过。
- 回复里列出新增测试文件/用例名与该用例锁定的行为。

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
