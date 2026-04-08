from __future__ import annotations

import ctypes
import math
import os
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class SystemResources:
    logical_processors: int
    total_memory_bytes: int | None


class _MemoryStatusEx(ctypes.Structure):
    _fields_ = [
        ("dwLength", ctypes.c_ulong),
        ("dwMemoryLoad", ctypes.c_ulong),
        ("ullTotalPhys", ctypes.c_ulonglong),
        ("ullAvailPhys", ctypes.c_ulonglong),
        ("ullTotalPageFile", ctypes.c_ulonglong),
        ("ullAvailPageFile", ctypes.c_ulonglong),
        ("ullTotalVirtual", ctypes.c_ulonglong),
        ("ullAvailVirtual", ctypes.c_ulonglong),
        ("ullAvailExtendedVirtual", ctypes.c_ulonglong),
    ]


def detect_system_resources() -> SystemResources:
    logical_processors = max(1, os.cpu_count() or 1)
    total_memory_bytes = _detect_total_memory_bytes()
    return SystemResources(
        logical_processors=logical_processors,
        total_memory_bytes=total_memory_bytes,
    )


def recommend_deep_scrape_workers(
    resources: SystemResources | None = None,
    *,
    max_resource_utilization: float = 0.90,
    reserved_cpu_threads: int = 2,
    reserved_memory_gb: float = 5.0,
    per_worker_memory_gb: float = 1.6,
    hard_cap: int = 8,
) -> int:
    resources = resources or detect_system_resources()

    cpu_budget = max(1, math.floor(resources.logical_processors * max_resource_utilization) - reserved_cpu_threads)

    if resources.total_memory_bytes:
        total_memory_gb = resources.total_memory_bytes / (1024**3)
        memory_budget_gb = max(1.0, total_memory_gb * max_resource_utilization - reserved_memory_gb)
        memory_budget = max(1, math.floor(memory_budget_gb / per_worker_memory_gb))
    else:
        memory_budget = hard_cap

    return max(1, min(cpu_budget, memory_budget, hard_cap))


def _detect_total_memory_bytes() -> int | None:
    try:
        status = _MemoryStatusEx()
        status.dwLength = ctypes.sizeof(_MemoryStatusEx)
        if not ctypes.windll.kernel32.GlobalMemoryStatusEx(ctypes.byref(status)):
            return None
        return int(status.ullTotalPhys)
    except Exception:
        return None
