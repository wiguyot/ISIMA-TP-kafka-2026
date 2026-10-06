from __future__ import annotations

from datetime import datetime
from html import escape
from typing import Any


def display_value(value: Any, suffix: str = "") -> str:
    if value in (None, "", "None"):
        return "n/a"
    return f"{value}{suffix}"


def format_history_time(timestamp: Any) -> str:
    raw = str(timestamp or "")
    if not raw:
        return "n/a"
    try:
        normalized = raw.replace("Z", "+00:00")
        parsed = datetime.fromisoformat(normalized)
        return parsed.astimezone().strftime("%H:%M:%S")
    except ValueError:
        return raw


def format_status_label(status: Any) -> str:
    value = str(status or "unknown").strip().lower()
    if value == "ok":
        return "OK"
    if value in ("warning", "degraded"):
        return "DEGRADE"
    if value in ("error", "critical", "down"):
        return "CASSE"
    return value.upper()


def status_badge_class(status: Any) -> str:
    value = str(status or "unknown").strip().lower()
    if value == "ok":
        return "badge-ok"
    if value in ("warning", "degraded"):
        return "badge-degraded"
    return "badge-critical"


def render_network_warning_banner(network_state: dict[str, Any]) -> str:
    if not int(network_state.get("active") or 0):
        return ""
    profile = escape(str(network_state.get("profile") or "unknown"))
    target = escape(str(network_state.get("target_service") or "unknown"))
    return (
        "<div class='banner banner-warning' style='display:flex; align-items:center; justify-content:space-between; gap:12px; flex-wrap:wrap;'>"
        f"<span>Perturbation réseau active : profil <code>{profile}</code> sur <code>{target}</code>.</span>"
        "<form method='post' action='/control' style='margin:0; flex-shrink:0;'>"
        "<input type='hidden' name='action' value='reset_network_profile'>"
        "<input type='hidden' name='return_view' value='scenario'>"
        "<button type='submit' style='font-weight:600; white-space:nowrap; background:#fff4d7; border-color:#e1bf57;'>Supprimer toutes les perturbations</button>"
        "</form>"
        "</div>"
    )
