import json
import os
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from typing import Any

from state import STATE


EMIT_BATCH_HANDLER = None
READINESS_HANDLER = None


def register_control(emit_batch_handler, readiness_handler) -> None:
    global EMIT_BATCH_HANDLER, READINESS_HANDLER
    EMIT_BATCH_HANDLER = emit_batch_handler
    READINESS_HANDLER = readiness_handler


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(
            {
                "status": STATE["status"],
                "service": os.getenv("SIMULPIX_SERVICE_NAME", "generator"),
                "ready_for_commands": bool(READINESS_HANDLER()) if READINESS_HANDLER else False,
                "messages_sent": STATE["messages_sent"],
                "messages_enqueued": STATE["messages_enqueued"],
                "messages_delivered": STATE["messages_delivered"],
                "messages_delivery_failed": STATE["messages_delivery_failed"],
                "last_message_id": STATE["last_message_id"],
                "last_delivery_error": STATE["last_delivery_error"],
                "scenario_type": STATE["scenario_type"],
                "current_phase": STATE["current_phase"],
                "phase_progress": STATE["phase_progress"],
                "phase_total": STATE["phase_total"],
                "last_requested_batch_count": STATE["last_requested_batch_count"],
                "traffic_model": STATE["traffic_model"],
                "producer_semantics": STATE["producer_semantics"],
                "producer_acks": STATE["producer_acks"],
                "producer_retries": STATE["producer_retries"],
                "last_error": STATE["last_error"],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def do_POST(self) -> None:
        if self.path != "/emit-batch":
            self.send_response(404)
            self.end_headers()
            return
        if EMIT_BATCH_HANDLER is None:
            self.send_response(503)
            self.end_headers()
            return
        try:
            content_length = int(self.headers.get("Content-Length", "0"))
            raw_body = self.rfile.read(content_length)
            payload = json.loads(raw_body.decode("utf-8") or "{}")
            result = EMIT_BATCH_HANDLER(payload)
            response = json.dumps({"status": "ok", **result}).encode("utf-8")
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
    port = int(os.getenv("SIMULPIX_PORT", "8081"))
    server = ThreadingHTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
