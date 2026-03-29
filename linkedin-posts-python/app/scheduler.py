from __future__ import annotations

from datetime import datetime, timedelta, timezone
from threading import Event, Thread

from . import db
from .config import Settings, get_settings
from .services.search_runner import SearchRunner


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    try:
        return datetime.fromisoformat(value.replace("Z", "+00:00"))
    except ValueError:
        return None


class SearchScheduler:
    def __init__(self, settings: Settings | None = None) -> None:
        self.settings = settings or get_settings()
        self._stop_event = Event()
        self._thread: Thread | None = None

    def start(self) -> None:
        if self._thread and self._thread.is_alive():
            return
        self._thread = Thread(target=self._run_loop, name="search-scheduler", daemon=True)
        self._thread.start()

    def stop(self) -> None:
        self._stop_event.set()
        if self._thread and self._thread.is_alive():
            self._thread.join(timeout=2)

    def _run_loop(self) -> None:
        while not self._stop_event.wait(self.settings.scheduler_poll_seconds):
            self._run_due_searches()

    def _run_due_searches(self) -> None:
        now = datetime.now(timezone.utc)
        runner = SearchRunner(self.settings)
        for search in db.list_active_scheduled_searches(self.settings):
            last_run = _parse_iso(search["last_run_at"])
            schedule_minutes = int(search["schedule_minutes"] or 0)
            if schedule_minutes <= 0:
                continue
            if last_run and now - last_run < timedelta(minutes=schedule_minutes):
                continue
            try:
                runner.run_search(search["id"])
            except Exception:
                # Background scheduler errors are already captured per run in the database.
                continue
