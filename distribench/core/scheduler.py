import time
import json
from typing import List, Optional, Dict, Any
from datetime import datetime, timezone
import redis

from distribench.core.models import (
    Task, Job, TaskStatus, WorkerHeartbeat, WorkerStatus
)
from distribench.core import config

CLAIM_TASK_LUA = """
local items = redis.call('ZPOPMIN', KEYS[1], 1)
if #items == 0 then return nil end
local id = items[1]
local task_key = KEYS[2] .. id 
local raw_task = redis.call('GET', task_key)
return {id, raw_task}
"""

class ClusterScheduler:
    def __init__(self, client: Optional[redis.Redis] = None):
        self.redis = client or redis.Redis(
            host=config.REDIS_HOST,
            port=config.REDIS_PORT,
            db = config.REDIS_DB,
            password=config.REDIS_PASSWORD,
            decode_responses=True
        )
        self._claim_script = self.redis.register_script(CLAIM_TASK_LUA)

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
        result = self._claim_script(
            keys=[config.REDIS_QUEUE_KEY, config.REDIS_TASKS_PREFIX]
        )
        if not result or not result[0]: return None

        task_id = result[0]
        raw_json = result[1]
        task = Task.model_validate_json(raw_json)

        task.status = TaskStatus.RUNNING
        task.assigned_worker = worker_id
        task.started_at = datetime.now(timezone.utc)
        self.update_task(task)
        return task

    def emit_heartbeat(self, heartbeat: WorkerHeartbeat):
        key = f"{config.REDIS_WORKERS_PREFIX}{heartbeat.worker_id}"
        self.redis.set(key, heartbeat.model_dump_json(), ex=config.HEARTBEAT_TIMEOUT_SECONDS * 2)

    def get_active_workers(self) -> List[WorkerHeartbeat]:
        worker_keys = self.redis.keys(f"{config.REDIS_WORKERS_PREFIX}*")
        workers = []

        for k in worker_keys: 
            raw = self.redis.get(k)
            if raw: workers.append(WorkerHeartbeat.model_validate_json(raw))
        return workers
        
    def get_cluster_stats(self) -> Dict[str, Any]:
        queued_count = self.redis.zcard(config.REDIS_QUEUE_KEY)
        workers = self.get_active_workers()

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
            "workers": [w.model_dump(mode="json") for w in workers],
            "task_counts": tasks_by_status
        }

    def schedule_retry(self, task: Task, base_backoff: float = 2.0):
        task.retry_count += 1
        backoff_delay = base_backoff * (2 ** (task.retry_count - 1))
        task.status = TaskStatus.PENDING
        task.started_at = None
        task.assigned_worker = None

        delayed_timestamp = time.time() + backoff_delay
        score = -(task.priority * 1_000_000_000) + delayed_timestamp

        self.update_task(task)

        self.redis.zadd(config.REDIS_QUEUE_KEY, {task.id: score})
        return backoff_delay

    def cancel_task(self, task_id: str) -> bool:
        """
        Cancels a task. If pending in the priority queue, removes it atomically.
        Transitions task status to CANCELLED.
        """
        task = self.get_task(task_id)
        if not task:
            return False

        if task.status in [TaskStatus.SUCCESS, TaskStatus.FAILED, TaskStatus.CANCELLED]:
            return False

        # Remove from priority queue if pending
        self.redis.zrem(config.REDIS_QUEUE_KEY, task_id)
        task.status = TaskStatus.CANCELLED
        task.completed_at = datetime.now(timezone.utc)
        self.update_task(task)
        return True

    def reap_orphaned_tasks(self) -> List[str]:
        """
        Scans all running tasks and reaps tasks whose assigned worker
        heartbeat lease has expired. Re-enqueues recoverable tasks with
        exponential backoff or marks them as FAILED if max retries exceeded.
        """
        active_worker_ids = {w.worker_id for w in self.get_active_workers()}
        task_keys = self.redis.keys(f"{config.REDIS_TASKS_PREFIX}*")
        reaped_task_ids = []

        for k in task_keys:
            raw = self.redis.get(k)
            if not raw:
                continue
            try:
                task = Task.model_validate_json(raw)
                if task.status == TaskStatus.RUNNING and task.assigned_worker:
                    if task.assigned_worker not in active_worker_ids:
                        # Worker is dead / heartbeat lease expired
                        if task.retry_count < task.max_retries:
                            self.schedule_retry(task, base_backoff=2.0)
                        else:
                            task.status = TaskStatus.FAILED
                            task.error_message = "WorkerNodeLost: Worker node died or heartbeat lease expired."
                            task.completed_at = datetime.now(timezone.utc)
                            self.update_task(task)
                        reaped_task_ids.append(task.id)
            except Exception:
                continue

        return reaped_task_ids
        