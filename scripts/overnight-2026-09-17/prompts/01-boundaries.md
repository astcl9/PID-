# 任务：修正时间边界、跨午夜查询与空数据处理

## 背景

后端 `app/main.py` 有 `_validate_window` 与 `datePlus`；`app/query.py`、`app/storage.py`、`app/postgres_db.py` 各自处理半开区间 `[start, end)`。现有 `tests/test_boundaries.py` 只有 3 个用例。

## 要做的事

1. 审计这些入口的边界与空数据行为：`/api/data`、`/api/segments`、`/api/datasets/{id}/days`、`/api/export`、`/api/segments/export`，以及 `app/query.py::query_data`、`app/storage.py::load_frame`、`app/postgres_db.py::_where`。
2. 重点检查并补测试：
   - 月末/年末跨日：`2026-02-28 → 2026-03-01`、`2026-12-31 → 2027-01-01`，以及闰年 `2028-02-28 → 2028-02-29`。
   - 跨午夜窗口（如 `2026-01-01T22:30 → 2026-01-02T01:30`）在半开区间下不重复、不漏点。
   - 空数据集 / 不存在的数据集在每个 HTTP 入口的返回（是空列表还是 400，要一致且明确）。
   - `start == end`、`start > end`、超长区间、`max_points < 1`、`resolution="raw"` 且跨度很大时的降级行为。
   - 非法日期（`2026-13-45`、空串、纯数字）始终是 400 而不是 500。
3. 发现不一致就小步修正（例如某入口对非法输入返回 500、某处用了闭区间导致边界重复），并让所有入口对同类输入给出相同语义。

## 验收

- `tests/test_boundaries.py` 显著扩充（建议 12 个用例以上），另可新增 `tests/test_time_windows.py`。
- 全部 pytest 通过。
- 回复里明确列出「原本会出错、现在修好了」的具体入口和证据。

## 全局约束（必须遵守）

- 工作区：`C:\Users\llin\Documents\ChatGPT\PID数据可视化`；Python 一律用 `.venv\Scripts\python.exe`。
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
