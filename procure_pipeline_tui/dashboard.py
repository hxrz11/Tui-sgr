from __future__ import annotations

"""Rich layout-based dashboard for console pipeline."""

from typing import Dict, List

from rich.layout import Layout
from rich.live import Live
from rich.panel import Panel
from rich.table import Table
from rich.tree import Tree


class Dashboard:
    """Simple dashboard with header, step tree and metrics panel."""

    def __init__(self) -> None:
        self.stage: str = "Init"
        self.statuses: Dict[str, List[str]] = {}
        self.meta: Dict[str, str] = {}

        self._layout = Layout()
        self._layout.split(Layout(name="header", size=3), Layout(name="body"))
        self._layout["body"].split_row(Layout(name="left"), Layout(name="right", size=40))

        self._layout["header"].update(self._render_header())
        self._layout["left"].update(self._render_statuses())
        self._layout["right"].update(self._render_meta())

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

    def _render_meta(self) -> Panel:
        table = Table(show_header=False, box=None)
        for k, v in self.meta.items():
            table.add_row(str(k), str(v))
        return Panel(table, title="Metrics", border_style="magenta")

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
        self._layout["right"].update(self._render_meta())

    def close(self) -> None:
        self._live.__exit__(None, None, None)
