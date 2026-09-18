# PID 数据可视化 Web MVP

一个面向局域网的历史 PID 数据分析工具。支持大文件 CSV/XLSX 分块导入、按天浏览、PV/SV/MV 曲线、日内时间窗口拖动、服务端抽稀、智能分段和 CSV 导出。

## 本地运行

建议使用 Python 3.10–3.12（Windows 上优先使用已安装的稳定 Python 3 版本）：

```powershell
python -m venv .venv
.venv\Scripts\Activate.ps1
python -m pip install -e ".[test]"
python scripts/sample_data.py
uvicorn app.main:app --host 127.0.0.1 --port 8080
```

浏览器打开 <http://127.0.0.1:8080>，上传 `data/sample_pid.csv` 即可体验。上传阶段进度条显示网络传输进度，随后显示后台解析行数和进度；导入完成后才会出现在数据集列表。

图表依赖已保存到 `app/static/vendor/`，运行时不需要访问外部 CDN，适合完全离线的局域网环境。

## 局域网运行

```powershell
uvicorn app.main:app --host 0.0.0.0 --port 8080
```

在服务器 Windows 防火墙放行 TCP 8080，其他设备访问 `http://服务器IP:8080`。数据会保存在项目下的 `data/` 目录；原始文件保存在 `data/raw/`，按天的 Parquet 数据保存在 `data/parquet/`。

## 改代码后如何让网页端生效

改完代码**不会**自动变成网页上的最新效果，分三层处理：

**后端改动（`app/` 下的 `.py` 文件）**：必须重启 uvicorn。在运行服务的 PowerShell 窗口按 `Ctrl+C` 停掉，再重新执行启动命令。开发阶段建议加 `--reload`，保存代码后约 1 秒自动重载，不用手动重启：

```powershell
.venv\Scripts\python.exe -m uvicorn app.main:app --host 127.0.0.1 --port 8080 --reload
```

**前端改动（`app/static/` 下的 `index.html`、`app.js`、CSS）**：不用重启服务，但浏览器可能用旧缓存。强刷一次即可：Edge / Chrome / Firefox 按 `Ctrl+F5`（或 `Ctrl+Shift+R`）；还不对就按 `F12` 打开开发者工具，右键刷新按钮选「清空缓存并硬性重新加载」。

**分段结果缓存**：改了分段算法后页面看起来"没变化"，不是缓存问题——`/api/segments` 默认返回已持久化的旧分段（`data/segments/<数据集id>.json` 和内存缓存）。需要重新计算时，请求带 `recompute=true`，或停止服务后删掉对应数据集的分段缓存文件，再强刷页面。

## Docker

```powershell
$env:PID_POSTGRES_PASSWORD = "set-a-long-random-password"
docker compose up --build -d
```

### 200GB+ 数据库模式

超大文件不应作为一个 DataFrame 常驻内存。部署 `docker compose` 时会启动 PostgreSQL；Web 服务在 `PID_STORAGE_BACKEND=postgres` 时直接把 CSV/XLSX 分块通过 COPY 写入 `pid_samples`，不会执行 DuckDB 全量整理。查询接口也直接走 PostgreSQL，在数据库中完成时间范围过滤和分桶聚合。Parquet 模式保留给未配置数据库的本地回退。

```powershell
$env:PID_STORAGE_BACKEND = "postgres"
docker compose up --build -d
```

可用环境变量：`PID_POSTGRES_DSN`，或 `PID_POSTGRES_HOST`、`PID_POSTGRES_PORT`、
`PID_POSTGRES_USER`、`PID_POSTGRES_PASSWORD`、`PID_POSTGRES_DATABASE`。PostgreSQL 模式仍会保留原始上传文件，但不会为同一份 200GB 文件额外生成完整 Parquet 副本；数据库目录映射到 `postgres_data`。

PostgreSQL 密码必须通过 `PID_POSTGRES_PASSWORD` 环境变量提供；Compose 默认仅将 5432 绑定到服务器本机，正式部署不要改为对外暴露。

## 数据格式

文件必须包含时间列（如 `timestamp`、`time` 或 `datetime`）。CSV 会自动识别 UTF-8（含 BOM）、GB18030/GBK 和 UTF-16 编码；其他数值列会被识别为信号；名称包含 `PV`、`SV`/`SP`、`MV` 的列会自动归类，导入后仍可在页面按信号筛选。默认单文件上限为 2048 MiB，可通过 `PID_MAX_UPLOAD_MB` 调整。多个上传会并发导入，并发数默认 2，可通过 `PID_IMPORT_WORKERS` 调整（注意并发数越高，去重整理阶段的峰值内存越大）。Parquet 写入压缩默认用 `snappy`（写盘更快、文件略大），可通过 `PID_PARQUET_COMPRESSION` 改为 `zstd`、`gzip` 或 `none`。

## 大小路由

导入后端按文件大小自动选择（详见 docs/操作指南.md）：

| 文件 | 后端 |
|------|------|
| CSV < 100 MB | Parquet + DuckDB |
| CSV >= 100 MB | PostgreSQL（需配置连接） |
| XLSX 任何大小 | Parquet + DuckDB |

阈值通过 PID_LARGE_FILE_THRESHOLD_MB 调整。

## 日内时间窗口

进入“单日”页面后，使用“日内时间窗口”滑块移动起始时间，选择窗口长度后会重新请求该段数据。支持全天、6 小时、3 小时、1 小时、30 分钟和 15 分钟；数据查询使用半开区间 `[start, end)`，不会重复跨日边界。

## 验证

```powershell
python -m compileall app tests scripts
node --check app/static/app.js
pytest -q
```

如果当前 Python 版本没有对应的第三方轮子，使用 Python 3.10–3.12 后再安装依赖。

## 性能基准

使用可重复的合成数据记录导入吞吐、Python 峰值内存、Parquet 占用和查询耗时：

```powershell
.venv\Scripts\python.exe scripts\benchmark.py --rows 100000
```

报告写入 `docs/benchmark-latest.json`。进行 GB 级测试时应逐级增加 `--rows`，并先确认磁盘空间；`tracemalloc` 结果不包含部分原生库内存，因此生产验收还需结合系统级监控。

## 备份与恢复

停止 Web 服务后备份项目 `data/` 目录；其中 `raw/` 是原始上传文件，`meta/` 是数据集及信号映射，`segments/` 是分段和人工修订，`jobs.sqlite3*` 是导入任务状态。Parquet 模式还需备份 `parquet/`。

PostgreSQL 模式应另外执行数据库备份，例如：

```powershell
docker compose exec postgres pg_dump -U pid -d pid -Fc -f /tmp/pid.backup
docker compose cp postgres:/tmp/pid.backup .\pid.backup
```

恢复前先保留当前数据副本。文件模式可在服务停止时还原完整 `data/`；PostgreSQL 模式使用 `pg_restore --clean --if-exists` 恢复到空闲维护窗口，并确认 `data/meta/` 与数据库中的数据集编号一致。

