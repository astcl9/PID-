from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent

try:
    from dotenv import load_dotenv
    load_dotenv(BASE_DIR / ".env")
except ImportError:
    pass

DATA_DIR = Path(os.getenv("PID_DATA_DIR", BASE_DIR / "data"))
MAX_UPLOAD_MB = int(os.getenv("PID_MAX_UPLOAD_MB", "2048"))
IMPORT_WORKERS = max(1, int(os.getenv("PID_IMPORT_WORKERS", "2")))
# 大小路由阈值：小于该值走 Parquet，大于等于走 PostgreSQL（XLSX 无论大小一律走 Parquet）
LARGE_FILE_THRESHOLD_MB = int(os.getenv("PID_LARGE_FILE_THRESHOLD_MB", "100"))
HOST = os.getenv("PID_HOST", "0.0.0.0")
PORT = int(os.getenv("PID_PORT", "8080"))
STORAGE_BACKEND = os.getenv("PID_STORAGE_BACKEND", os.getenv("PID_STORAGE", "parquet")).lower()
POSTGRES_DSN = os.getenv("PID_POSTGRES_DSN", "")
POSTGRES_HOST = os.getenv("PID_POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("PID_POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("PID_POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("PID_POSTGRES_PASSWORD", "")
POSTGRES_DATABASE = os.getenv("PID_POSTGRES_DATABASE", "pid")
