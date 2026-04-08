from __future__ import annotations

import ctypes
import os
from contextlib import AbstractContextManager


class AutomationLockBusyError(RuntimeError):
    """Raised when another local OVE automation process already owns the browser lock."""


_WAIT_OBJECT_0 = 0x00000000
_WAIT_ABANDONED = 0x00000080
_WAIT_TIMEOUT = 0x00000102
_INFINITE = 0xFFFFFFFF


class OveAutomationLock(AbstractContextManager["OveAutomationLock"]):
    def __init__(self, name: str = r"Local\OVE_Browser_Automation", timeout_seconds: int = 900) -> None:
        self.name = name
        self.timeout_seconds = timeout_seconds
        self._handle = None
        self._acquired = False

    def __enter__(self) -> "OveAutomationLock":
        self.acquire()
        return self

    def __exit__(self, exc_type, exc, exc_tb) -> None:
        self.close()

    def acquire(self) -> None:
        if os.name != "nt":
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.CreateMutexW.argtypes = [ctypes.c_void_p, ctypes.c_bool, ctypes.c_wchar_p]
        kernel32.CreateMutexW.restype = ctypes.c_void_p
        kernel32.WaitForSingleObject.argtypes = [ctypes.c_void_p, ctypes.c_uint32]
        kernel32.WaitForSingleObject.restype = ctypes.c_uint32

        handle = kernel32.CreateMutexW(None, False, self.name)
        if not handle:
            raise OSError(ctypes.get_last_error(), f"Unable to create automation mutex {self.name}")

        timeout_ms = _INFINITE if self.timeout_seconds <= 0 else int(self.timeout_seconds * 1000)
        result = kernel32.WaitForSingleObject(handle, timeout_ms)
        if result in (_WAIT_OBJECT_0, _WAIT_ABANDONED):
            self._handle = handle
            self._acquired = True
            return

        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool
        kernel32.CloseHandle(handle)

        if result == _WAIT_TIMEOUT:
            raise AutomationLockBusyError(
                f"Timed out waiting for another OVE automation process to release mutex {self.name}"
            )
        raise OSError(ctypes.get_last_error(), f"Unexpected mutex wait result {result} for {self.name}")

    def close(self) -> None:
        if os.name != "nt":
            return
        if self._handle is None:
            return
        kernel32 = ctypes.WinDLL("kernel32", use_last_error=True)
        kernel32.ReleaseMutex.argtypes = [ctypes.c_void_p]
        kernel32.ReleaseMutex.restype = ctypes.c_bool
        kernel32.CloseHandle.argtypes = [ctypes.c_void_p]
        kernel32.CloseHandle.restype = ctypes.c_bool
        try:
            if self._acquired:
                kernel32.ReleaseMutex(self._handle)
        finally:
            kernel32.CloseHandle(self._handle)
            self._handle = None
            self._acquired = False
