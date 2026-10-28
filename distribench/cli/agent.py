import os
import logging
from dotenv import load_dotenv

# Suppress internal Google GenAI SDK verbose logs & warnings
logging.disable(logging.WARNING)

from rich.console import Console
from rich.panel import Panel
from rich.prompt import Prompt
from rich.markdown import Markdown

from google import genai
from google.genai import types

from distribench.mcp.server import (
    query_cluster_capacity,
    dispatch_workload,
    dispatch_batch,
    tail_execution_logs,
    triage_failed_task,
)

load_dotenv()
console = Console()

TOOLS = [
    query_cluster_capacity,
    dispatch_workload,
    dispatch_batch,
    tail_execution_logs,
    triage_failed_task,
]


def run_tui():
    api_key = os.getenv("GEMINI_API_KEY")
    if not api_key:
        console.print("[bold red]Error: GEMINI_API_KEY is not set in your .env file.[/]")
        return

    client = genai.Client(api_key=api_key)

    system_instruction = (
        "You are DistriBench Agent, an autonomous distributed systems cluster operator. "
        "You have direct access to cluster tools to query capacity, dispatch single or batch workloads, "
        "tail logs, and triage failed tasks. "
        "Always call the appropriate tools to take action. "
        "CRITICAL: After calling tools, always respond with a clean, formatted markdown summary detailing "
        "all actions taken, including all task IDs, task types, priority levels, and execution statuses. "
        "Never return an empty response."
    )

    chat = client.chats.create(
        model="gemini-3.5-flash-lite",
        config=types.GenerateContentConfig(
            system_instruction=system_instruction,
            tools=TOOLS,
            temperature=0.2,
        ),
    )

    console.print(Panel(
        "[bold cyan]DistriBench MCP Autonomous Agent Console[/]\n"
        "Talk to Google Gemini in natural language to manage cluster workloads.\n"
        "Examples:\n"
        " • [italic]'Check cluster health and active workers'[/]\n"
        " • [italic]'Dispatch a matrix computation with size 150 at priority 9'[/]\n"
        " • [italic]'Why did task d8c1c312 fail? Diagnose it.'[/]\n"
        "Type [bold red]'exit'[/] or [bold red]'quit'[/] to stop.",
        title="[bold blue]DistriBench AI TUI[/]",
        border_style="blue",
    ))

    while True:
        try:
            user_input = Prompt.ask("\n[bold cyan][You][/]")
            if not user_input.strip():
                continue
            if user_input.lower() in ("exit", "quit", "q"):
                console.print("[yellow]Exiting agent console. Goodbye![/]")
                break

            with console.status("[bold magenta]Gemini Agent reasoning & orchestrating..."):
                response = chat.send_message(user_input)

            agent_text = response.text or "Operation completed successfully."
            console.print(Panel(
                Markdown(agent_text),
                title="[bold green]Gemini Cluster Operator[/]",
                border_style="green",
            ))

        except (KeyboardInterrupt, EOFError):
            console.print("\n[yellow]Exiting...[/]")
            break
        except Exception as e:
            console.print(f"[bold red]Error:[/] {e}")


if __name__ == "__main__":
    run_tui()
