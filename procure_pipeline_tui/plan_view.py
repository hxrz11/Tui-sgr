from __future__ import annotations

"""Simple curses-based view for plan steps."""

from typing import Dict, List, Optional, Set
import curses


class PlanView:
    """Display plan steps and track current/completed state."""

    def __init__(self, steps: List[Dict[str, str]]) -> None:
        self.steps: List[Dict[str, str]] = steps
        self.current_id: Optional[str] = None
        self.done_ids: Set[str] = set()
        self.screen = curses.initscr()
        curses.noecho()
        curses.cbreak()
        self.screen.keypad(True)
        self.refresh()

    def _line(self, step: Dict[str, str]) -> str:
        sid = step.get("id")
        title = step.get("title", "")
        prefix = "✅ " if sid in self.done_ids else "  "
        return f"{prefix}{sid}. {title}"

    def set_current(self, step_id: str) -> None:
        self.current_id = step_id

    def mark_done(self, step_id: str) -> None:
        self.done_ids.add(step_id)

    def refresh(self) -> None:
        self.screen.clear()
        for idx, step in enumerate(self.steps):
            line = self._line(step)
            if step.get("id") == self.current_id:
                self.screen.addstr(idx, 0, line, curses.A_REVERSE)
            else:
                self.screen.addstr(idx, 0, line)
        self.screen.refresh()

    def close(self) -> None:
        curses.nocbreak()
        self.screen.keypad(False)
        curses.echo()
        curses.endwin()
