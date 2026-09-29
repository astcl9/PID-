# 任务：按回路拆分智能分段，并补充分段统计

## 背景

`app/main.py::_segments_for_frame` 已按 `tag` 前缀（`rsplit('.', 1)[0]`）分回路调用 `app/segmentation.py::segment_dataframe`。`_segment_stats` 已产出 pv/sv/mv 的 mean/min/max/std/first/last/count/missing_rate、`pv_sv_mean_error`、`pv_sv_mean_absolute_error`、`mv_saturation_rate`、`steady_minutes`。`docs/项目规划书-2026-09-17.md` 第 63 行要求补「首末值、偏度」并做图表化展示。

## 要做的事

1. 审计多回路分段的隔离性：一个回路的 PV 缺失/跳变不应影响另一个回路的分段结果；无点号（`TIC101PV` 这类）标签要有明确、一致的处理；`loop` 参数过滤要能命中。
2. 补充分段统计：加入偏度（skew）与值域（range），必要时加 PV-SV 误差分位数。保持 `dict[str, Any]` 结构，旧字段不删。
3. 确认统计在「合并小碎片」之后重新计算（`segment_dataframe` 末尾已有重算逻辑），并补测试覆盖「合并后统计等于对合并区间重算」，防止出现统计区间与 start/end 不一致。
4. 前端或导出至少有一处能消费新统计（例如分段 CSV 导出已含 `stats` 列，确认新字段随之输出；若前端有分段统计展示区，补上偏度/值域）。
5. 补测试：多回路隔离、无点号标签、新统计字段数值正确、合并后统计自洽。

## 验收

- 新增/扩充测试全绿，总 pytest 通过。
- 回复里给出一个真实算例：某段数据的 skew/range 数值与手算或 pandas 直算一致。

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
