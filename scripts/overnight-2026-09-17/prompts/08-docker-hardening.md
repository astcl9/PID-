# 任务：加固 Docker、密码、端口与存储健康检查

## 背景

`docker-compose.yml`：PostgreSQL 17 端口只绑 `127.0.0.1:5432`，密码走 `${PID_POSTGRES_PASSWORD:?...}` 强制要求，`pid-web` 依赖 postgres 健康，`restart: unless-stopped`；但 `pid-web` 没有 healthcheck，`Dockerfile` 没有非 root 用户，项目缺少 `.dockerignore`（另一任务在建），`.env` 里有 `PID_POSTGRES_PASSWORD/USER/DATABASE`。`/health/storage` 目前只返回 `backend` 与 `ok`，`/health/system` 返回磁盘与任务计数。

## 要做的事

1. 审计并加固 compose：密码必须来自环境变量（不得有默认弱口令）、数据库端口保持只绑本机、给 `pid-web` 加 healthcheck（复用 `/health`）、显式声明数据卷。**不要把 5432 暴露到公网**。
2. `Dockerfile`：切换到非 root 用户运行、固定基础镜像 tag（`python:3.11-slim` → 带小版本或 digest 并说明取舍）、确认 `.dockerignore` 生效（若另一任务已创建就复用，不要重复定义冲突内容）。
3. 存储健康检查升级：`/health/storage` 或 `/health/system` 增加可执行的信息——后端类型、连通性、`data/` 与 `postgres_data/` 占用、剩余空间百分比、低于阈值时 degraded。阈值走环境变量。
4. 端口与凭据自检脚本：写 `scripts/preflight_deploy.py`（或 .ps1），检查 8080 是否被占用、5432 是否只绑本机、`.env` 是否被 `.gitignore` 忽略、密码长度是否达标，输出可读结论；**不要打印密码明文**。
5. 补测试：健康检查阈值逻辑、自检脚本的关键判定（拆成纯函数测试）。`docker compose config` 必须在设置环境变量后通过。

## 验收

- `docker compose config` 通过（附命令与结果）。
- pytest 全绿。
- 回复里列出「已加固项」与「仍未做、需要你决定」的清单。

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
