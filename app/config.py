from pathlib import Path
import os

BASE_DIR = Path(__file__).resolve().parent.parent
DATA_DIR = Path(os.getenv("PID_DATA_DIR", BASE_DIR / "data"))
MAX_UPLOAD_MB = int(os.getenv("PID_MAX_UPLOAD_MB", "2048"))
HOST = os.getenv("PID_HOST", "0.0.0.0")
PORT = int(os.getenv("PID_PORT", "8080"))
STORAGE_BACKEND = os.getenv("PID_STORAGE_BACKEND", os.getenv("PID_STORAGE", "parquet")).lower()
POSTGRES_DSN = os.getenv("PID_POSTGRES_DSN", "")
POSTGRES_HOST = os.getenv("PID_POSTGRES_HOST", "127.0.0.1")
POSTGRES_PORT = int(os.getenv("PID_POSTGRES_PORT", "5432"))
POSTGRES_USER = os.getenv("PID_POSTGRES_USER", "postgres")
POSTGRES_PASSWORD = os.getenv("PID_POSTGRES_PASSWORD", "")
POSTGRES_DATABASE = os.getenv("PID_POSTGRES_DATABASE", "pid")
