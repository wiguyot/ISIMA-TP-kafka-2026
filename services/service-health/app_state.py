from __future__ import annotations

import threading
from typing import Any


SNAPSHOT_CACHE: dict[str, Any] = {
    "captured_at": None,
    "sample": None,
}

HISTORY_MAX_POINTS = 18
HISTORY_CACHE: list[dict[str, Any]] = []

CONTROL_LOCK = threading.Lock()
CONTROL_STATE: dict[str, Any] = {
    "status": "idle",
    "last_action": None,
    "last_started_at": None,
    "last_finished_at": None,
    "last_result": None,
    "busy_message": None,
    "last_output": [],
}
