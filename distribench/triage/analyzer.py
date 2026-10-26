from pydantic import errors
import os
import re
from pathlib import Path
from typing import Optional

from distribench.core import config
from distribench.core.models import (
    FailureCategory, TriageReport, Task, TaskStatus
)
from distribench.core.scheduler import ClusterScheduler

FAILURE_PATTERNS = [
    (
        r"(OOMKilled|MemoryError|out of memory|cgroup limit|exceeds node memory)",
        FailureCategory.RESOURCE_EXHAUSTION,
        "Worker node ran out of memory (Out of Memory killed or MemoryError)."
    ),
    (
        r"(Segmentation fault|SIGSEGV|core dumped|segmentation violation)",
        FailureCategory.RESOURCE_EXHAUSTION,
        "Low-level memory corruption or segmentation fault in native process."
    ),
    (
        r"(AssertionError|ValidationAssertionError|assertion failed|panic:)",
        FailureCategory.LOGIC_ASSERTION,
        "Assertion failure or validation panic in business/model logic."
    ),
    (
        r"(TimeoutError|timed out|exceeded execution deadline)",
        FailureCategory.TIMEOUT,
        "Task exceeded execution timeout deadline."
    ),
    (
        r"(ModuleNotFoundError|ImportError|FileNotFoundError|No such file)",
        FailureCategory.DEPENDENCY_ERROR,
        "Missing dependency, uninstalled package, or missing filesystem artifact."
    ),
]

class ErrorTriageEngine:
    def __init__(self, scheduler: Optional[ClusterScheduler] = None):
        self.scheduler = scheduler or ClusterScheduler()
        self.llm_api_key = os.getenv("GEMINI_API_KEY")

    def _read_task_logs(self, task_id:str, max_lines: int = 150) -> str :
        """Read raw stdout/stderr stream from disk."""
        log_path = config.LOGS_DIR / f"{task_id}.log"
        if not log_path.exists(): return f"No log file found at {log_path}"
        with open(log_path, "r", errors="replace") as f:
            lines = f.readlines()
            return "".join(lines[-max_lines:])

    def _classify_heuristic(self, task_id: str, log_content: str) -> TriageReport:
        for pattern, category, summary in FAILURE_PATTERNS:
            match = re.search(pattern, log_content, re.IGNORECASE)
            if match:
                matched_snippet = match.group(0)
                if category == FailureCategory.RESOURCE_EXHAUSTION:
                    fix = "Increase worker container memory limits (e.g. mem_limit: 4G) or reduce batch size."
                elif category == FailureCategory.LOGIC_ASSERTION:
                    fix = "Inspect assertion line in trace. Verify expected vs actual data shapes and assertions."
                elif category == FailureCategory.TIMEOUT:
                    fix = "Increase task.timeout_seconds or optimize computational hot-loop."
                elif category == FailureCategory.DEPENDENCY_ERROR:
                    fix = "Ensure all required packages are specified in requirements.txt or installed in Dockerfile."
                else:
                    fix = "Review stack trace and logs for unhandled exceptions."
                
                return TriageReport(
                    task_id=task_id,
                    category=category,
                    summary=summary,
                    root_cause=f"Signature detected: '{matched_snippet}' in worker execution stream.",
                    suggested_fix=fix,
                    confidence=0.92,
                    triaged_by="heuristic-signature-analyzer"
                )
        return TriageReport(
            task_id=task_id,
            category=FailureCategory.UNKNOWN,
            summary="Unclassified execution error.",
            root_cause="No known failure signature matched the log output.",
            suggested_fix="Check full log stream in data/shared/logs for unhandled exceptions.",
            confidence=0.50,
            triaged_by="heuristic-signature-analyzer"
        )
    
    def _classify_gemini(self, task_id: str, log_content: str) -> Optional[TriageReport]:
        try:
            from google import genai
            from google.genai import types

            # Pass http_options with v1beta and model="gemini-3.1-flash-lite"
            client = genai.Client(
                api_key=self.llm_api_key,
                http_options={"api_version": "v1beta"}
            )
            
            prompt = (
                f"You are an expert distributed systems engineer. Analyze this worker failure log:\n\n"
                f"```\n{log_content}\n```\n\n"
                f"Provide a root-cause diagnosis for task {task_id}."
            )

            response = client.models.generate_content(
                model="gemini-3.5-flash-lite",
                contents=prompt,
                config=types.GenerateContentConfig(
                    response_mime_type="application/json",
                    response_schema=TriageReport
                )
            )
            return TriageReport.model_validate_json(response.text)
        except Exception as e:
            print(f"[Triage warning]: Gemini API unavailable!")
            return None

    def triage_task(self, task_id: str) -> TriageReport:
        task = self.scheduler.get_task(task_id)
        logs = self._read_task_logs(task_id)

        if self.llm_api_key:
            report = self._classify_gemini(task_id, logs)
            if report: return report
        return self._classify_heuristic(task_id, logs)
        