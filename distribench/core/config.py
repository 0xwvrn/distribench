import os
from pathlib import Path


REDIS_HOST = os.getenv("REDIS_HOST", "localhost")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD", None)

SHARED_DIR = Path(os.getenv("SHARED_DIR", os.path.abspath("./data/shared")))
LOGS_DIR = SHARED_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

REDIS_QUEUE_KEY = "distribench:queue:priority"
REDIS_TASKS_PREFIX = "distribench:task:"
REDIS_JOBS_PREFIX = "distribench:job:"
REDIS_WORKERS_PREFIX = "distribench:worker:"

HEARTBEAT_TIMEOUT_SECONDS = int(os.getenv("HEARTBEAT_TIMEOUT_SECONDS", "15"))