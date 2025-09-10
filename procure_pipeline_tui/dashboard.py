from __future__ import annotations

"""Rich layout-based dashboard for console pipeline."""

from typing import Any, Dict, List

import json
import select
import sys
import threading
import termios
import tty
from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.syntax import Syntax
from rich.table import Table
from rich.tree import Tree


class Dashboard:
    """Simple dashboard with header, step tree and metrics panel."""

    def __init__(self) -> None:
        self.stage: str = "Init"
        self.statuses: Dict[str, List[str]] = {}
        self.meta: Dict[str, str] = {}

        # plan preview state
        self.plan_steps: List[Dict[str, Any]] = []  # {id, json}
        self.current_step: int = 0
        self.active_tab: str = "meta"
        self._key_thread: threading.Thread | None = None
        self._stop_event = threading.Event()

        self._layout = Layout()
        self._layout.split(Layout(name="header", size=3), Layout(name="body"))
        self._layout["body"].split_row(Layout(name="left"), Layout(name="right", size=40))

        self._layout["header"].update(self._render_header())
        self._layout["left"].update(self._render_statuses())
        self._layout["right"].update(self._render_right_panel())

        self._live = Live(self._layout, refresh_per_second=4)
        self._live.__enter__()

    # ------------------------------------------------------------------
    # Rendering helpers
    # ------------------------------------------------------------------
    def _render_header(self) -> Panel:
        text = f"Procurement Pipeline\nStage: {self.stage}"
        return Panel(text, style="bold white on blue", expand=True)

    def _render_statuses(self) -> Tree:
        tree = Tree("Steps")
        for block, messages in self.statuses.items():
            branch = tree.add(f"[bold]{block}[/]")
            for msg in messages:
                branch.add(msg)
        return tree

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

    def _render_right_panel(self) -> Panel:
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
        self._layout["header"].update(self._render_header())

    def update_status(self, block: str, message: str) -> None:
        self.statuses.setdefault(block, []).append(message)
        self._layout["left"].update(self._render_statuses())

    def update_meta(self, meta: Dict[str, int | float | str | None]) -> None:
        for k, v in meta.items():
            if v is None:
                continue
            if "duration" in k and isinstance(v, int):
                self.meta[k] = self._format_duration(v)
            else:
                self.meta[k] = str(v)
        self._layout["right"].update(self._render_right_panel())

    # ------------------------------------------------------------------
    # Plan preview controls
    # ------------------------------------------------------------------
    def set_plan_preview(self, steps: List[Dict[str, Any]]) -> None:
        """Populate steps for JSON preview and start key listener."""
        self.plan_steps = steps
        self.current_step = 0
        self.active_tab = "json"
        self._layout["right"].update(self._render_right_panel())
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
                    self._layout["right"].update(self._render_right_panel())
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
        self._layout["right"].update(self._render_right_panel())

    def prev_step(self) -> None:
        if not self.plan_steps:
            return
        self.current_step = (self.current_step - 1) % len(self.plan_steps)
        self._layout["right"].update(self._render_right_panel())

    def close(self) -> None:
        self._stop_event.set()
        if self._key_thread and self._key_thread.is_alive():
            self._key_thread.join(timeout=0.2)
        self._live.__exit__(None, None, None)
