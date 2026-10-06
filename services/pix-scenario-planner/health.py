import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from planning import build_plan
from state import STATE


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health", "/plan"):
            self.send_response(404)
            self.end_headers()
            return
        if self.path == "/plan":
            payload_obj = build_plan()
        else:
            payload_obj = {
                "status": STATE["status"],
                "service": os.getenv("SIMULPIX_SERVICE_NAME", "pix-scenario-planner"),
                "ready_to_plan": STATE["ready_to_plan"],
                "scenario_type": STATE["scenario_type"],
                "plan_name": STATE["plan_name"],
                "phase_count": STATE["phase_count"],
                "total_messages": STATE["total_messages"],
                "default_rate_per_second": STATE["default_rate_per_second"],
                "last_error": STATE["last_error"],
            }
        payload = json.dumps(payload_obj).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return


def serve_http() -> None:
    port = int(os.getenv("SIMULPIX_PORT", "8090"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
