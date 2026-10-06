from typing import Any


STATE: dict[str, Any] = {
    "status": "starting",
    "arch": "split",
    "workers_configured": 1,
    "workers_ready": 0,
    "received": 0,
    "checked": 0,
    "validation_failed": 0,
    "ready_for_messages": False,
    "assigned_partitions": 0,
    "last_error": None,
    "last_transaction_id": None,
    "producer_semantics": None,
    "consumer_semantics": None,
    "transactional_mode": None,
    "transactional_id": None,
    "consumer_auto_commit": None,
    "consumer_commit_strategy": None,
    "consumer_max_poll_interval_ms": None,
    "consumer_commit_batch_size": None,
}
