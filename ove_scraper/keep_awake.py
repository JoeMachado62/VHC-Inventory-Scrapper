from __future__ import annotations

import ctypes
import logging
from contextlib import AbstractContextManager


ES_AWAYMODE_REQUIRED = 0x00000040
ES_CONTINUOUS = 0x80000000
ES_SYSTEM_REQUIRED = 0x00000001


class KeepAwake(AbstractContextManager["KeepAwake"]):
    """Prevent Windows from sleeping while the scraper is active."""

    def __init__(self, logger: logging.Logger | None = None) -> None:
        self.logger = logger
        self._enabled = False

    def __enter__(self) -> "KeepAwake":
        if hasattr(ctypes, "windll"):
            result = ctypes.windll.kernel32.SetThreadExecutionState(
                ES_CONTINUOUS | ES_SYSTEM_REQUIRED | ES_AWAYMODE_REQUIRED
            )
            self._enabled = bool(result)
            if self.logger:
                if self._enabled:
                    self.logger.info("Windows sleep prevention enabled for scraper process")
                else:
                    self.logger.warning("Failed to enable Windows sleep prevention")
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        if hasattr(ctypes, "windll") and self._enabled:
            ctypes.windll.kernel32.SetThreadExecutionState(ES_CONTINUOUS)
            if self.logger:
                self.logger.info("Windows sleep prevention released")
        return None
