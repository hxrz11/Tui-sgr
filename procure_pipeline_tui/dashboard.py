from __future__ import annotations

"""Rich layout-based dashboard for console pipeline."""

from typing import Any, Dict, List

import json
import select
import sys
import threading
import termios
import time
import tty
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree


class Dashboard:
    """Simple dashboard with pipeline plan, plan preview and center area."""

    def __init__(self) -> None:
        self.stage: str = "Init"
        self.statuses: Dict[str, List[str]] = {}
        self.meta: Dict[str, str] = {}
        self.pipeline_steps: Dict[str, Dict[str, int | None]] = {}

        # plan preview state
        self.plan_steps: List[Dict[str, Any]] = []  # {id, json}
        self.current_step: int = 0
        self.active_tab: str = "meta"
        self._key_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        # Build layout: top area split into pipeline plan (left) and plan (right),
        # bottom area named "center" for future use.
        self._layout = Layout()
        self._layout.split_column(
            Layout(name="top", ratio=2),
            Layout(name="center", ratio=1),
        )
        self._layout["top"].split_row(
            Layout(name="pipeline_plan"),
            Layout(name="plan", size=40),
        )

        self._layout["pipeline_plan"].update(self._render_pipeline_plan())
        self._layout["plan"].update(self._render_plan_panel())
        self._layout["center"].update(self._render_center())

        self._live = Live(self._layout, refresh_per_second=4)
        self._live.__enter__()

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------
    def _render_statuses(self) -> Tree:
        tree = Tree("Steps")
        for step, times in self.pipeline_steps.items():
            start = times.get("start")
            end = times.get("end")
            if start and end:
                duration = times.get("duration") or end - start
                label = f"[green]{step} ({self._format_duration(duration)})[/]"
            else:
                label = step
            branch = tree.add(label)
            for msg in self.statuses.get(step, []):
                branch.add(msg)
        for block, messages in self.statuses.items():
            if block in self.pipeline_steps:
                continue
            branch = tree.add(block)
            for msg in messages:
                branch.add(msg)
        return tree

    def _render_pipeline_plan(self) -> Panel:
        tree = self._render_statuses()
        return Panel(tree, title=f"Stage: {self.stage}")

    def _render_meta(self) -> Table:
        table = Table(show_header=False, box=None)
        for k, v in self.meta.items():
            table.add_row(str(k), str(v))
        return table

    def _render_json_preview(self) -> Panel:
        if not self.plan_steps:
            content = "No plan"
            title = "JSON Preview"
        else:
            step = self.plan_steps[self.current_step]
            content = step.get("json", "{}")
            step_id = step.get("id", str(self.current_step + 1))
            title = f"Step {step_id}"
        syntax = Syntax(content, "json", theme="monokai", word_wrap=True)
        return Panel(syntax, title=title, border_style="cyan")

    def _render_plan_panel(self) -> Panel:
        if self.active_tab == "meta":
            body = self._render_meta()
        else:
            body = self._render_json_preview()
        tabs = (
            "[bold]Metrics[/bold] | JSON Preview"
            if self.active_tab == "meta"
            else "Metrics | [bold]JSON Preview[/bold]"
        )
        return Panel(body, title=tabs, border_style="magenta")

    def _render_center(self) -> Panel:
        return Panel("", border_style="green")

    @staticmethod
    def _format_duration(ns: int) -> str:
        seconds = ns / 1_000_000_000
        if seconds >= 60:
            minutes = seconds / 60
            return f"{minutes:.2f} min"
        return f"{seconds:.2f} s"

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def set_stage(self, stage: str) -> None:
        self.stage = stage
        self._layout["pipeline_plan"].update(self._render_pipeline_plan())

    def update_status(self, block: str, message: str) -> None:
        self.statuses.setdefault(block, []).append(message)
        self._layout["pipeline_plan"].update(self._render_pipeline_plan())

    def start_pipeline_step(self, step: str) -> None:
        """Mark the start time of a pipeline step."""
        self.pipeline_steps[step] = {"start": time.monotonic_ns(), "end": None, "duration": None}
        self._layout["pipeline_plan"].update(self._render_pipeline_plan())

    def finish_pipeline_step(self, step: str) -> None:
        """Mark the end of a pipeline step and compute its duration."""
        info = self.pipeline_steps.get(step)
        if not info:
            return
        end = time.monotonic_ns()
        info["end"] = end
        start = info.get("start")
        if start:
            info["duration"] = end - start
        self._layout["pipeline_plan"].update(self._render_pipeline_plan())

    def update_meta(self, meta: Dict[str, int | float | str | None]) -> None:
        for k, v in meta.items():
            if v is None:
                continue
            if "duration" in k and isinstance(v, int):
                self.meta[k] = self._format_duration(v)
            else:
                self.meta[k] = str(v)
        self._layout["plan"].update(self._render_plan_panel())

    def ask(self, prompt: str) -> str:
        """Request input from the user via the dashboard console."""
        return self._live.console.input(prompt)

    # ------------------------------------------------------------------
    # Plan preview controls
    # ------------------------------------------------------------------
    def set_plan_preview(self, steps: List[Dict[str, Any]]) -> None:
        """Populate steps for JSON preview and start key listener."""
        self.plan_steps = steps
        self.current_step = 0
        self.active_tab = "json"
        self._layout["plan"].update(self._render_plan_panel())
        if self._key_thread is None:
            self._key_thread = threading.Thread(target=self._key_listener, daemon=True)
            self._key_thread.start()

    def _key_listener(self) -> None:
        fd = sys.stdin.fileno()
        old = termios.tcgetattr(fd)
        try:
            tty.setcbreak(fd)
            while not self._stop_event.is_set():
                r, _, _ = select.select([sys.stdin], [], [], 0.1)
                if not r:
                    continue
                ch = sys.stdin.read(1)
                if ch == "\t":
                    self.active_tab = "json" if self.active_tab == "meta" else "meta"
                    self._layout["plan"].update(self._render_plan_panel())
                elif ch == "\x1b":
                    seq = sys.stdin.read(2)
                    if seq == "[C":
                        self.next_step()
                    elif seq == "[D":
                        self.prev_step()
        finally:
            termios.tcsetattr(fd, termios.TCSADRAIN, old)

    def next_step(self) -> None:
        if not self.plan_steps:
            return
        self.current_step = (self.current_step + 1) % len(self.plan_steps)
        self._layout["plan"].update(self._render_plan_panel())

    def prev_step(self) -> None:
        if not self.plan_steps:
            return
        self.current_step = (self.current_step - 1) % len(self.plan_steps)
        self._layout["plan"].update(self._render_plan_panel())

    def close(self) -> None:
        self._stop_event.set()
        if self._key_thread and self._key_thread.is_alive():
            self._key_thread.join(timeout=0.2)
        self._live.__exit__(None, None, None)
