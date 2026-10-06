from typing import Any


STATE: dict[str, Any] = {
    "status": "starting",
    "arch": "split",
    "processed": 0,
    "validated": 0,
    "rejected": 0,
    "decisions_published": 0,
    "ready_for_messages": False,
    "assigned_partitions": 0,
    "last_error": None,
    "last_transaction_id": None,
    "producer_semantics": None,
    "consumer_semantics": None,
    "consumer_auto_commit": None,
    "consumer_commit_strategy": None,
    "consumer_max_poll_interval_ms": None,
    "consumer_commit_batch_size": None,
    "transactional_mode": None,
    "transactional_id": None,
    "workers_configured": 1,
    "workers_ready": 0,
}
