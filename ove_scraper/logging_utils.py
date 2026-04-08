from __future__ import annotations

import json
import logging
from pathlib import Path

from ove_scraper.schemas import SyncExecutionLog


def configure_logging(log_level: str, log_file_path: Path) -> logging.Logger:
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    logger = logging.getLogger("ove_scraper")
    logger.setLevel(getattr(logging, log_level.upper(), logging.INFO))
    logger.handlers.clear()

    formatter = logging.Formatter("%(asctime)s %(levelname)s %(message)s")

    stream_handler = logging.StreamHandler()
    stream_handler.setFormatter(formatter)
    logger.addHandler(stream_handler)

    file_handler = logging.FileHandler(log_file_path, encoding="utf-8")
    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

    return logger


def append_sync_log(log_file_path: Path, payload: SyncExecutionLog) -> None:
    log_file_path.parent.mkdir(parents=True, exist_ok=True)
    with log_file_path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(payload.model_dump(mode="json"), sort_keys=True))
        handle.write("\n")
