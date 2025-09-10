from __future__ import annotations

"""Rich-based progress display for plan steps."""

from typing import Dict, List, Optional, Set

from rich.console import Group
from rich.live import Live
from rich.progress import BarColumn, Progress, SpinnerColumn, TextColumn
from rich.table import Table


class PlanProgress:
    """Render plan steps with a progress bar using Rich."""

    def __init__(self, steps: List[Dict[str, str]]) -> None:
        self.steps: List[Dict[str, str]] = steps
        self.current_id: Optional[str] = None
        self.done_ids: Set[str] = set()

        self._progress = Progress(
            SpinnerColumn(),
            BarColumn(bar_width=None),
            TextColumn("{task.completed}/{task.total}"),
            transient=True,
        )
        self._task_id = self._progress.add_task("steps", total=len(steps))

        # Live needs explicit start/stop to ensure clean shutdown
        self._live = Live(self._render(), refresh_per_second=4)
        self._live.__enter__()

    def _table(self) -> Table:
        table = Table(show_header=True, header_style="bold")
        table.add_column("ID", style="cyan", no_wrap=True)
        table.add_column("Title")
        table.add_column("Status", style="magenta")

        for step in self.steps:
            sid = step.get("id", "")
            title = step.get("title", "")
            if sid in self.done_ids:
                status = "✅ done"
            elif sid == self.current_id:
                status = "… running"
            else:
                status = "pending"
            table.add_row(sid, title, status)
        return table

    def _render(self) -> Group:
        return Group(self._table(), self._progress)

    def refresh(self) -> None:
        """Refresh the live display."""
        self._live.update(self._render())

    def set_current(self, step_id: str) -> None:
        self.current_id = step_id
        self.refresh()

    def mark_done(self, step_id: str) -> None:
        if step_id not in self.done_ids:
            self.done_ids.add(step_id)
            self._progress.update(self._task_id, completed=len(self.done_ids))
        self.refresh()

    def close(self) -> None:
        """Stop the live context."""
        self._live.__exit__(None, None, None)

