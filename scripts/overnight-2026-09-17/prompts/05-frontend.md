# 任务：优化前端窗口交互、过期请求与异常提示

## 背景

`app/static/app.js`（65 行压缩风格）里 `state.token` 已用于 `/api/data` 的过期丢弃，但 `/api/segments` 请求没有做同样保护；全局没有 `AbortController`；错误只靠 `toast` 与少量 `innerHTML` 文案。`tests/test_frontend_smoke.py` 只做静态冒烟。

## 要做的事

1. 让 `/api/data` 与 `/api/segments` 都受同一个请求代次保护：切换日期/回路/分辨率后，旧响应不得覆盖新图表，也不得刷新分段表。
2. 用 `AbortController` 中止被新请求取代的在途请求，避免慢查询堆积；中止导致的 `AbortError` 不要弹错误提示。
3. 统一异常提示：网络失败、HTTP 4xx/5xx、后端 `detail`、空数据（无点）要给出可区分的文案；空数据不要显示成「失败」。
4. 日内时间窗口交互要稳：拖动时不重复发请求（已有 220ms 防抖），窗口右端接近 1440 分钟时不要越过当天边界，跨午夜时标签显示要正确。
5. 补测试（可在 `tests/test_frontend_smoke.py` 扩展，检查源码中的关键保护逻辑；若引入可测的纯函数请拆出来单测）。

## 验收

- `node --check app/static/app.js` 通过（node 在 `C:\Program Files\nodejs\node.exe`）。
- pytest 全绿。
- 回复里说明：并发点了两次不同日期时，最终渲染的是哪一次，以及你如何证明。

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
