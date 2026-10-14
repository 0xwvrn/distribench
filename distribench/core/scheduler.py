import time
import json
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import redis

from distribench.core.models import (
    Task, Job, TaskStatus, WorkerHeartbeat, WorkerStatus
)
from distribench.core import config

class ClusterScheduler:
    def __init__(self, client: Optional[redis.Redis] = None):
        self.redis = client or redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db = config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True
        )

    def submit_task(self, task: Task) -> Task:
        task_key = f"{config.REDIS_TASKS_PREFIX}{task.id}"
        self.redis.set(task_key, task.model_dump_json())

        score = -(task.priority * 1_000_000_000) + time.time()
        self.redis.zadd(config.REDIS_QUEUE_KEY, {task.id: score})
        return task

    def submit_batch(self, name:str, tasks: List[Task]) -> Job:
        job = Job(name=name, task_ids=[t.id for t in tasks])
        job_key = f"{config.REDIS_JOBS_PREFIX}{job.id}"
        self.redis.set(job_key, job.model_dump_json())

        for task in tasks:
            task.job_id = job.id
            self.submit_task(task)
        return job
    
    def get_task(self, task_id: str) -> Optional[Task]:
        data = self.redis.get(f"{config.REDIS_TASKS_PREFIX}{task_id}")
        if not data: return None
        return Task.model_validate_json(data)

    def update_task(self, task: Task): 
        self.redis.set(f"{config.REDIS_TASKS_PREFIX}{task.id}", task.model_dump_json())
    
    def pop_next_task(self, worker_id: str) -> Optional[Task]:
        items = self.redis.zpopmin(config.REDIS_QUEUE_KEY, count=1)
        if not items: return None

        task_id, _score = items[0]
        task = self.get_task(task_id)
        if not task: return None

        task.status = TaskStatus.RUNNING
        task.assigned_worker = worker_id
        task.started_at = datetime.now(timezone.utc)
        self.update_task(task)
        return task

    def emit_heartbeat(self, heartbeat: WorkerHeartbeat):
        key = f"{config.REDIS_WORKERS_PREFIX}{heartbeat.worker_id}"
        self.redis.set(key, heartbeat.model_dump_json(), ex=config.HEARTBEAT_TIMEOUT_SECONDS * 2)

    def get_active_worker(self) -> List[WorkerHeartbeat]:
        worker_keys = self,redis.keys(f"{config.REDIS_WORKERS_PREFIX}*")
        workers = []

        for k in worker_keys: 
            raw = self.redis.get(k)
            if raw: workers.append(WorkerHeartbeat.model_validate_json(raw))
            return workers
        
    def get_cluster_stats(self) -> Dict[str, Any]:
        queued_count = self.redis.zcard(config.REDIS_QUEUE_KEY)
        workers = self.get_active_worker()

        task_keys = self.redis.keys(f"{config.REDIS_TASKS_PREFIX}*")
        tasks_by_status = {s.value: 0 for s in TaskStatus}

        for k in task_keys:
            raw = self.redis.get(k)
            if raw: 
                try: 
                    t = Task.model_validate_json(raw)
                    tasks_by_status[t.status.value] = tasks_by_status.get(t.status.value, 0) + 1
                except Exception:
                    pass
        
        return {
            "queued_tasks": queued_count,
            "active_workers": len(workers),
            "workers": [w.model_dump() for w in workers],
            "task_counts": tasks_by_status
        }
        