# 任务：手动信号映射与人工分段修订能力

## 背景

`PATCH /api/datasets/{id}/signals` 与 `PATCH /api/segments/{id}` 已存在，人工修订会写 `data/segments/{dataset}.json` 并标 `manual_override`。**已发现一个真实缺陷**：`app/main.py::segments` 在 `recompute=true` 时会重新计算并执行 `stored[key] = result`，把人工修订的 `type/start/end/note/manual_override` 整体覆盖掉。请先复现，再修。

## 要做的事

1. 先写一个失败测试复现：构造带人工修订的分段 → 以 `recompute=true` 再取一次 → 人工修订丢失。修好后再保留该测试。
2. 修复逻辑：重新计算时保留已有的人工修订（按 segment 稳定标识匹配；现有 id 是随机 uuid，需设计可复现的匹配键，例如「同回路 + 时间重叠」或引入稳定 fingerprint），并保证 `manual_override` 不被清除。
3. 增强人工修订能力：至少支持一步新能力——例如把一段标记为「已确认正常/已确认异常」，或允许把一段拆分为两段。前后端都要能用（`app/static/index.html` + `app.js` + `app/main.py` + `app/models.py` 按需改）。
4. 保证修订持久化能跨进程重启：写盘后再读回一致。
5. 补测试：覆盖修订保留、越界起止时间被拒绝、修订后导出 CSV 反映最新值。

## 验收

- 复现测试先失败后通过的证据（回复里贴前后结果）。
- pytest 全绿。
- 回复里说明人工修订的匹配键设计，以及什么情况下仍会丢失。

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
