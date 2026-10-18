import os 
import sys
import time
import socket
import threading
import traceback
from datetime import datetime, timezone
from pathlib import Path

from distribench.core import config
from distribench.core.models import (
    Task, TaskStatus, WorkerHeartbeat, WorkerStatus
)
from distribench.core.scheduler import ClusterScheduler
from distribench.worker.tasks import TASK_REGISTRY

class ClusterWorker:
    def __init__(self, worker_id: str | None = None): 
        self.worker_id = worker_id or f"worker-{socket.gethostname()[:8]}-{os.getpid()}"
        self.hostname = socket.gethostname()
        self.scheduler = ClusterScheduler()
        self.running = False
        self.current_task_id: str | None = None
    
    def start_heartbeat_loop(self):
        def _loop():
            while self.running:
                status = WorkerStatus.BUSY if self.current_task_id else WorkerStatus.IDLE
                hb = WorkerHeartbeat(
                    worker_id=self.worker_id,
                    hostname=self.hostname,
                    status=status,
                    current_task_id=self.current_task_id,
                    cpu_percent=0.0,
                    memory_percent=0.0,
                    last_seen=datetime.now(timezone.utc)
                )
                self.scheduler.emit_heartbeat(hb)
                time.sleep(config.HEARTBEAT_TIMEOUT_SECONDS // 2)
        
        thread = threading.Thread(target=_loop, daemon = True)
        thread.start()

    def execute_task(self, task: Task):
        self.current_task_id = task.id
        log_path = config.LOGS_DIR / f"{task.id}.log"
        task.log_file = str(log_path)

        handler = TASK_REGISTRY.get(task.task_type)
        if not handler:
            err = f"Unknown task type '{task.task_type}'. Available:{list(TASK_REGISTRY.keys())}"
            task.status = TaskStatus.FAILED
            task.error_message = err
            task.completed_at = datetime.now(timezone.utc)
            with open(log_path, "w") as f: f.write(err + "\n")
            self.scheduler.update_task(task)
            self.current_task_id = None
            return

        with open(log_path, "w", buffering=1) as log_file:
            log_file.write(f"=== Task {task.id} started on {self.worker_id} at {datetime.now(timezone.utc)} ===\n")
            log_file.write(f"Payload: {task.payload}\n\n")

            old_stdout, old_stderr = sys.stdout, sys.stderr
            sys.stdout = log_file
            sys.stderr = log_file

            try:
                result = handler(task.payload)
                print(f"\n=== Task Succeeded: {result} ===")
                task.status = TaskStatus.SUCCESS
                task.completed_at = datetime.now(timezone.utc)
            except Exception as e:
                print(f"\nTask failed")
                traceback.print_exc()

                task.retry_count += 1
                if task.retry_count <= task.max_retries:
                    print(f"\n[Worker] Re-queueing task for retry ({task.retry_count}/{task.max_retries})...")
                    task.status = TaskStatus.PENDING
                    self.scheduler.submit_task(task)
                else:
                    print(f"\n[Worker] Max retries reached ({task.max_retries}). Marking as FAILED.")
                    task.status = TaskStatus.FAILED
                    task.error_message = f"{type(e).__name__}: {str(e)}"
                    task.completed_at = datetime.now(timezone.utc)
            finally:
                sys.stdout = old_stdout
                sys.stderr = old_stderr
                self.current_task_id = None
                self.scheduler.update_task(task)

    def run(self, poll_interval: float = 1.0):
        self.running = True
        self.start_heartbeat_loop()
        print(f"[*] Worker {self.worker_id} online and waiting for tasks...")

        try: 
            while self.running:
                task = self.scheduler.pop_next_task(self.worker_id)
                if task:
                    print(f"[+] Worker {self.worker_id} claimed task {task.id} ({task.task_type}, priority={task.priority})")
                    self.execute_task(task)
                    print(f"[✓] Task {task.id} finished with status: {task.status.value}")

                else:
                    time.sleep(poll_interval)
        except KeyboardInterrupt:
            print(f"\n[!] Worker {self.worker_id} shutting down cleanly...")
        finally:
            self.running = False

if __name__ == "__main__":
    worker = ClusterWorker()
    worker.run()

