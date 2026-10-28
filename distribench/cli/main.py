import sys
import time
import json
import click
from rich.console import Console
from rich.table import Table
from rich.panel import Panel
from rich.progress import Progress, SpinnerColumn, TextColumn

from distribench.core.models import Task, TaskPriority, TaskStatus
from distribench.core.scheduler import ClusterScheduler
from distribench.triage.analyzer import ErrorTriageEngine
from distribench.core import config

console = Console()
scheduler = ClusterScheduler()
triage_engine = ErrorTriageEngine()

@click.group()
def cli():
    pass

@cli.command()
def status():
    stats = scheduler.get_cluster_stats()
    status_color = "green" if stats["active_workers"] > 0 else "red"
    console.print(Panel(
        f"[bold {status_color}]Cluster Status: {'HEALTHY' if stats['active_workers'] > 0 else 'NO ACTIVE WORKERS'}[/]\n"
        f"Active Worker Nodes: [cyan]{stats['active_workers']}[/] | Queued Backlog: [yellow]{stats['queued_tasks']}[/]",
        title="[bold blue]DistriBench Cluster Overview[/]",
        border_style="blue"
    ))

    worker_table = Table(title="Live Worker Pool", border_style="dim")
    worker_table.add_column("Worker ID", style="bold cyan")
    worker_table.add_column("Hostname", style="dim")
    worker_table.add_column("Status", style="bold")
    worker_table.add_column("Current Task", style="yellow")
    worker_table.add_column("Last Heartbeat", style="magenta")

    workers = stats.get("workers", [])

    if not workers:
        worker_table.add_row("-", "-", "[red]No workers connected[/]", "-", "-")
    else:
        for w in workers:
            w_status = w.get("status", "IDLE")
            status_style = "green" if w_status == "IDLE" else "yellow"
            worker_table.add_row(
                w.get("worker_id", "unknown"),
                w.get("hostname", "unknown"),
                f"[{status_style}]{w_status}[/]",
                w.get("current_task_id") or "[dim]None[/]",
                str(w.get("last_seen", ""))[:19]
            )
    console.print(worker_table)

    breakdown_table = Table(title="Task Lifecycle Breakdown", border_style="dim")
    breakdown_table.add_column("Pending", style="yellow")
    breakdown_table.add_column("Running", style="cyan")
    breakdown_table.add_column("Success", style="green")
    breakdown_table.add_column("Failed", style="red")
    breakdown_table.add_column("Timeout", style="magenta")
    counts = stats.get("task_counts", {})
    breakdown_table.add_row(
        str(counts.get("PENDING", 0)),
        str(counts.get("RUNNING", 0)),
        str(counts.get("SUCCESS", 0)),
        str(counts.get("FAILED", 0)),
        str(counts.get("TIMEOUT", 0))
    )
    console.print(breakdown_table)


@cli.command()
@click.option("--type", "task_type", default="compute_matrix", help="Task type (e.g. compute_matrix, eval_dataset, sim_oom, sim_assertion).")
@click.option("--priority", default=5, type=click.IntRange(1, 10), help="Priority level (1-10, 10 highest).")
@click.option("--payload", default="{}", help="JSON string of task payload arguments.")

def submit(task_type: str, priority: int, payload: str):
    try:
        payload_dict = json.loads(payload)
    except json.JSONDecodeError:
        console.print("[bold red]Error: Payload must be a valid JSON string.[/]")
        sys.exit(1)
    task = Task(task_type=task_type, priority=priority, payload=payload_dict)
    scheduler.submit_task(task)
    console.print(f"[bold green]✓ Task submitted successfully![/] ID: [cyan]{task.id}[/] | Priority: [yellow]{priority}[/] | Type: [bold]{task_type}[/]")

@cli.command()
@click.argument("task_id")
@click.option("--lines", default=50, help="Number of tail lines to read.")
def logs(task_id: str, lines: int):
    task = scheduler.get_task(task_id)
    if not task:
        console.print(f"[bold red]Task '{task_id}' not found in cluster state.[/]")
        return
    log_path = config.LOGS_DIR / f"{task_id}.log"
    if not log_path.exists():
        console.print(f"[yellow]No log file found for task {task_id} (Status: {task.status.value}).[/]")
        return
    with open(log_path, "r", errors="replace") as f:
        content = "".join(f.readlines()[-lines:])
    console.print(Panel(
        content or "[dim]Log file is empty[/]",
        title=f"[bold cyan]Task {task_id} Execution Stream (Status: {task.status.value})[/]",
        border_style="cyan"
    ))

@cli.command()
@click.argument("task_id")
def triage(task_id: str):
    """Execute AI error triage on a failed task."""
    task = scheduler.get_task(task_id)
    if not task:
        console.print(f"[bold red]Task '{task_id}' not found.[/]")
        return
    with console.status(f"[bold magenta]Analyzing failure logs for task {task_id}..."):
        report = triage_engine.triage_task(task_id)
    color = "red" if report.category != "UNKNOWN" else "yellow"
    console.print(Panel(
        f"[bold]Category:[/] [{color}]{report.category.value}[/]\n"
        f"[bold]Summary:[/] {report.summary}\n"
        f"[bold]Root Cause:[/] {report.root_cause}\n"
        f"[bold green]Suggested Fix:[/] {report.suggested_fix}\n"
        f"[dim]Triaged by: {report.triaged_by} | Confidence: {report.confidence * 100:.0f}%[/]",
        title=f"[bold {color}]AI Diagnostic Report — Task {task_id}[/]",
        border_style=color
    ))

@cli.command()
def demo():
    """Run an automated end-to-end cluster demonstration."""
    console.print(Panel(
        "[bold cyan]Starting DistriBench-MCP End-to-End Cluster Showcase[/]\n"
        "1. Dispatch high and normal priority compute tasks\n"
        "2. Dispatch a failure task to trigger crash handler\n"
        "3. Stream worker execution\n"
        "4. Trigger Gemini AI automated diagnostic triage",
        title="DistriBench Showcase",
        border_style="blue"
    ))
    # Submit tasks
    t1 = Task(task_type="compute_matrix", priority=TaskPriority.HIGH, payload={"size": 120})
    t2 = Task(task_type="sim_oom", priority=TaskPriority.NORMAL, max_retries=0)
    t3 = Task(task_type="eval_dataset", priority=TaskPriority.LOW, payload={"batch_size": 30})
    scheduler.submit_task(t1)
    scheduler.submit_task(t2)
    scheduler.submit_task(t3)
    console.print(f"[green]✓ Submitted Priority 8 Task:[/] [cyan]{t1.id}[/] (compute_matrix)")
    console.print(f"[green]✓ Submitted Priority 5 Task:[/] [cyan]{t2.id}[/] (sim_oom failure simulation)")
    console.print(f"[green]✓ Submitted Priority 1 Task:[/] [cyan]{t3.id}[/] (eval_dataset)")
    console.print("\n[yellow]Waiting 4 seconds for cluster workers to process queue...[/]")
    time.sleep(4)
    # Check status
    for t_id, label in [(t1.id, "Matrix Compute"), (t2.id, "OOM Simulator"), (t3.id, "Dataset Eval")]:
        task = scheduler.get_task(t_id)
        status_color = "green" if task.status == TaskStatus.SUCCESS else ("red" if task.status == TaskStatus.FAILED else "yellow")
        console.print(f"[{status_color}]• {label} ({t_id}): {task.status.value}[/]")
    # Run AI triage on the failed task
    console.print("\n[bold magenta]Running AI Diagnostic Triage on failed task:[/] [cyan]" + t2.id + "[/]")
    report = triage_engine.triage_task(t2.id)
    console.print(Panel(
        f"[bold]Category:[/] [red]{report.category.value}[/]\n"
        f"[bold]Summary:[/] {report.summary}\n"
        f"[bold]Root Cause:[/] {report.root_cause}\n"
        f"[bold green]Suggested Fix:[/] {report.suggested_fix}\n"
        f"[dim]Triaged by: {report.triaged_by} | Confidence: {report.confidence * 100:.0f}%[/]",
        title=f"[bold red]AI Diagnostic Triage Card — Task {t2.id}[/]",
        border_style="red"
    ))
if __name__ == "__main__":
    cli()