from typing import List, Dict, Any, Optional
from mcp.server.mcpserver import MCPServer

from distribench.core.models import Task, TaskPriority, TaskStatus
from distribench.core.scheduler import ClusterScheduler
from distribench.triage.analyzer import ErrorTriageEngine
from distribench.core import config

mcp = MCPServer(
    name="Distribench-Cluster",
    description="Distributed batch execution orchestrator and cluster management tools."
)

scheduler = ClusterScheduler()
triage_engine = ErrorTriageEngine(scheduler)

@mcp.tool()
def query_cluster_capacity() -> dict:
    stats = scheduler.get_cluster_stats()
    return {
        "status": "Healthy" if stats["active_workers"] > 0 else "Degraded No Workers",
        "active_workers_count": stats["active_workers"],
        "queued_backlog_count": stats["queued_tasks"],
        "task_breakdown": stats["task_counts"],
        "workers": stats["workers"]
    }

@mcp.tool()
def dispatch_workload(
    task_type: str,
    payload: dict,
    priority: int = 5,
    timeout_s: int = 60,
) -> dict:
    priority = max(1, min(10, priority))
    task = Task(
        task_type=task_type,
        payload=payload,
        priority=priority,
        timeout_seconds=timeout_s
    )
    scheduler.submit_task(task)
    return {
        "status": "QUEUED",
        "task_id": task.id,
        "priority": task.priority,
        "task_type": task.task_type
    }

@mcp.tool()
def dispatch_batch(
    name: str,
    tasks: list,
    priority: int = 5
) -> dict:
    task_objs = [
        Task(
            task_type=t["task_type"],
            payload=t.get("payload", {}),
            priority=priority
        )
        for t in tasks
    ]
    job = scheduler.submit_batch(name, task_objs)
    return {
        "status": "BATCH_SUBMITTED",
        "job_id": job.id,
        "task_count": len(task_objs),
        "task_ids": job.task_ids
    }

@mcp.tool()
def tail_execution_logs(task_id: str, lines: int = 50) -> dict:
    task = scheduler.get_task(task_id)
    if not task: return {"error": f"{task_id} not found"}

    log_path = config.LOGS_DIR / f"{task_id}.log"
    if not log_path.exists(): 
        return {
            "task_id": task_id, 
            "status": task.status.value, 
            "logs": "No execution logs recorded yet"
        }
    
    with open(log_path, "r", errors="replace") as f:
        content = "".join(f.readlines()[-lines:])
    
    return {
        "task_id": task_id,
        "status": task.status.value,
        "assigned_worker": task.assigned_worker,
        "log_stream": content
    }

@mcp.tool()
def triage_failed_task(task_id: str) -> dict:
    task = scheduler.get_task(task_id)
    if not task: return {"error": f"Task '{task_id}' not found."}
    report = triage_engine.triage_task(task_id)
    return report.model_dump(mode="json")

if __name__ == "__main__":
    mcp.run()

