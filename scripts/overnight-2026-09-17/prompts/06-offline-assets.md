# 任务：本地化 Plotly.js 与离线局域网部署闭环

## 背景

`app/static/vendor/plotly-2.35.2.min.js`（约 4.56 MB）已本地化，`index.html` 不引用 CDN，`tests/test_frontend_smoke.py` 已断言这两点。但没有启动自检，也没有离线部署清单；另外项目**缺少 `.dockerignore`**，`docker build` 会把 `postgres_data`（约 7.4 GB）和 `data`（约 1.3 GB）当作构建上下文上传，这是实打实的部署缺陷。

## 要做的事

1. 增加启动自检：在健康检查里暴露静态资源状态（扩展 `/health/storage` 或新增 `/health/assets`），校验 Plotly 文件存在、可读、体积合理、版本号与 `index.html` 引用一致；缺失时状态为 degraded 而不是直接 500。
2. 新增 `.dockerignore`：排除 `postgres_data/`、`data/`、`.venv/`、`.tmp/`、`.uv-cache/`、`.uv-python/`、`__pycache__/`、`.git/`、`.pytest_cache/`、`*.pyc`、`.env`。写完后用 `docker build --no-cache --progress=plain` 的上下文大小对比证明生效（对比前后 transferring context 大小；日志存 `.tmp`）。构建产生的新镜像可保留，不要删用户旧镜像与卷。
3. 写 `docs/离线部署清单.md`：断外网 → 启动 → 首页加载 → 数据集列表 → 日历 → 单日曲线 → 时间窗口 → 分段 → 导出，每步写明预期结果与失败时的排查方法；明确说明「无需访问任何 CDN」。
4. 补测试：自检端点在文件存在时的返回、文件缺失时返回 degraded（用 `monkeypatch` 改路径，不要真的删文件）。
5. 顺带确认 `app/static/index.html` 中没有残留的外链（字体、图标、地图等）。

## 验收

- pytest 全绿；`docker build` 上下文大小有前后对比数字。
- 回复里给出自检端点的实际返回与 `.dockerignore` 生效的证据。

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
