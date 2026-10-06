import json
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any

from state import STATE


def get_consumer_group() -> str:
    return os.getenv("SIMULPIX_CONSUMER_GROUP") or os.getenv("SIMULPIX_PIX_VALIDATOR_GROUP", "simulpix-validator-v1")


class Handler(BaseHTTPRequestHandler):
    def do_GET(self) -> None:
        if self.path not in ("/", "/health"):
            self.send_response(404)
            self.end_headers()
            return
        payload = json.dumps(
            {
                "status": STATE["status"],
                "service": os.getenv("SIMULPIX_SERVICE_NAME", "pix-validator"),
                "arch": STATE["arch"],
                "consumer_group": get_consumer_group(),
                "workers_configured": STATE["workers_configured"],
                "workers_ready": STATE["workers_ready"],
                "received": STATE["received"],
                "checked": STATE["checked"],
                "validation_failed": STATE["validation_failed"],
                "ready_for_messages": STATE["ready_for_messages"],
                "assigned_partitions": STATE["assigned_partitions"],
                "last_transaction_id": STATE["last_transaction_id"],
                "last_error": STATE["last_error"],
                "producer_semantics": STATE["producer_semantics"],
                "consumer_semantics": STATE["consumer_semantics"],
                "transactional_mode": STATE["transactional_mode"],
                "transactional_id": STATE["transactional_id"],
                "consumer_auto_commit": STATE["consumer_auto_commit"],
                "consumer_commit_strategy": STATE["consumer_commit_strategy"],
                "consumer_max_poll_interval_ms": STATE["consumer_max_poll_interval_ms"],
                "consumer_commit_batch_size": STATE["consumer_commit_batch_size"],
            }
        ).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(payload)))
        self.end_headers()
        self.wfile.write(payload)

    def log_message(self, format: str, *args: Any) -> None:
        return


def serve_http() -> None:
    port = int(os.getenv("SIMULPIX_PORT", "8086"))
    server = HTTPServer(("0.0.0.0", port), Handler)
    server.serve_forever()
