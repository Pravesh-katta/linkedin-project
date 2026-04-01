from __future__ import annotations

import logging
from functools import lru_cache
from logging.handlers import RotatingFileHandler
from pathlib import Path
from typing import Iterable


LOG_FORMAT = "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
LOG_MAX_BYTES = 2_000_000
LOG_BACKUP_COUNT = 5
APP_LOG_FILENAMES = (
    "search_runner.log",
    "linkedin_scraper.log",
)


def _detach_log_handlers(log_dir: Path) -> None:
    resolved_log_dir = log_dir.resolve()
    logger_objects = [logging.getLogger()]
    logger_objects.extend(
        logger
        for logger in logging.root.manager.loggerDict.values()
        if isinstance(logger, logging.Logger)
    )

    for logger in logger_objects:
        removable_handlers = [
            handler
            for handler in logger.handlers
            if isinstance(handler, RotatingFileHandler)
            and Path(handler.baseFilename).resolve().parent == resolved_log_dir
        ]
        for handler in removable_handlers:
            logger.removeHandler(handler)
            handler.close()


def reset_app_logs(log_dir: Path, *, log_filenames: Iterable[str] = APP_LOG_FILENAMES) -> None:
    log_dir.mkdir(parents=True, exist_ok=True)
    _detach_log_handlers(log_dir)
    get_rotating_file_logger.cache_clear()

    for existing_log in log_dir.glob("*.log*"):
        if existing_log.is_file():
            existing_log.unlink()

    for log_filename in log_filenames:
        (log_dir / log_filename).touch()


@lru_cache(maxsize=None)
def get_rotating_file_logger(name: str, log_path: str) -> logging.Logger:
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)
    logger.propagate = False

    resolved_path = Path(log_path)
    resolved_path.parent.mkdir(parents=True, exist_ok=True)

    if not any(
        isinstance(handler, RotatingFileHandler) and Path(handler.baseFilename) == resolved_path
        for handler in logger.handlers
    ):
        handler = RotatingFileHandler(
            resolved_path,
            maxBytes=LOG_MAX_BYTES,
            backupCount=LOG_BACKUP_COUNT,
            encoding="utf-8",
            mode="a",
        )
        handler.setLevel(logging.DEBUG)
        handler.setFormatter(logging.Formatter(LOG_FORMAT))
        logger.addHandler(handler)

    return logger
