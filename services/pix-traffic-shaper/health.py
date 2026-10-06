import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from state import STATE


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(
            {
                "status": STATE["status"],
                "service": os.getenv("SIMULPIX_SERVICE_NAME", "pix-traffic-shaper"),
                "ready_to_shape": STATE["ready_to_shape"],
                "scenario_type": STATE["scenario_type"],
                "current_phase": STATE["current_phase"],
                "phase_progress": STATE["phase_progress"],
                "phase_total": STATE["phase_total"],
                "traffic_model": STATE["traffic_model"],
                "target_rate_per_second": STATE["target_rate_per_second"],
                "bucket_seconds": STATE["bucket_seconds"],
                "last_bucket_count": STATE["last_bucket_count"],
                "batches_sent": STATE["batches_sent"],
                "messages_requested": STATE["messages_requested"],
                "last_error": STATE["last_error"],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        if self.path != "/update":
            self.send_response(404)
            self.end_headers()
            return
        try:
            length = int(self.headers.get("Content-Length", "0"))
            data = json.loads(self.rfile.read(length).decode("utf-8") or "{}")
            if "rate_per_second" in data:
                rate = float(data["rate_per_second"])
                STATE["live_rate_override"] = max(rate, 0.0)
            if "traffic_model" in data:
                STATE["live_model_override"] = str(data["traffic_model"])
            response = json.dumps({"status": "ok"}).encode("utf-8")
            self.send_response(200)
        except Exception as exc:
            response = json.dumps({"status": "error", "error": repr(exc)}).encode("utf-8")
            self.send_response(500)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(response)))
        self.end_headers()
        self.wfile.write(response)

    def log_message(self, format: str, *args: Any) -> None:
        return


def serve_http() -> None:
    port = int(os.getenv("SIMULPIX_PORT", "8089"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
