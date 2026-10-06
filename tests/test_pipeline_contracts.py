import importlib.util
import sys
import types
import unittest
from pathlib import Path


ROOT = Path(__file__).resolve().parents[1]


def load_module(module_name: str, relative_path: str):
    if "confluent_kafka" not in sys.modules:
        fake_kafka = types.ModuleType("confluent_kafka")
        fake_kafka.Consumer = object
        fake_kafka.Producer = object
        fake_kafka.TopicPartition = object
        sys.modules["confluent_kafka"] = fake_kafka
    if "psycopg" not in sys.modules:
        fake_psycopg = types.ModuleType("psycopg")
        fake_psycopg.Connection = object
        sys.modules["psycopg"] = fake_psycopg

    module_path = ROOT / relative_path
    module_dir = str(module_path.parent)
    if module_dir not in sys.path:
        sys.path.insert(0, module_dir)
    spec = importlib.util.spec_from_file_location(module_name, module_path)
    module = importlib.util.module_from_spec(spec)
    assert spec is not None and spec.loader is not None
    spec.loader.exec_module(module)
    return module


PIX_VALIDATOR = load_module("pix_validator_validation", "services/pix-validator/validation.py")
PIX_DECISION_EVENTS = load_module("pix_decision_event_builder", "services/pix-decision-engine/event_builder.py")
PIX_DECISION_LOGIC = load_module("pix_decision_logic", "services/pix-decision-engine/decision.py")
PIX_DECISION_KAFKA = load_module("pix_decision_kafka", "services/pix-decision-engine/kafka_io.py")
PERSISTER_VALID = load_module("persister_valid_main", "services/persister-valid/main.py")
PERSISTER_REJECTED = load_module("persister_rejected_main", "services/persister-rejected/main.py")


class PipelineContractsTest(unittest.TestCase):
    def test_decision_engine_consumer_uses_manual_commit(self):
        config = PIX_DECISION_KAFKA.build_kafka_client_config(group_id="group-1")
        self.assertFalse(config["enable.auto.commit"])

    def test_persisters_use_manual_commit(self):
        valid_config = PERSISTER_VALID.build_kafka_client_config("valid-group")
        rejected_config = PERSISTER_REJECTED.build_kafka_client_config("rejected-group")
        self.assertFalse(valid_config["enable.auto.commit"])
        self.assertFalse(rejected_config["enable.auto.commit"])

    def test_validate_message_detects_expected_business_errors(self):
        message = {
            "transaction_id": "tx-1",
            "event_time": "2026-03-23T10:00:00Z",
            "emitter_account_id": "acc-1",
            "beneficiary_account_id": "acc-1",
            "emitter_pix_client_id": "pix-1",
            "beneficiary_pix_client_id": "pix-9",
            "emitter_tax_id": "tax-1",
            "beneficiary_tax_id": "tax-2",
            "amount": -1,
            "currency": "BRL",
            "status": "UNKNOWN",
            "scenario_type": "errors_simple",
            "producer_id": "generator",
        }
        tax_to_pix = {"tax-1": "pix-1", "tax-2": "pix-2"}

        errors = PIX_VALIDATOR.validate_message(message, tax_to_pix)

        self.assertIn("amount_must_be_positive", errors)
        self.assertIn("accounts_must_differ", errors)
        self.assertIn("invalid_initial_status", errors)
        self.assertIn("beneficiary_tax_pix_mismatch", errors)

    def test_rejected_message_carries_replay_identity(self):
        message = {
            "transaction_id": "tx-1-retry-001",
            "source_transaction_id": "tx-1",
            "retry_attempt": 1,
            "event_time": "2026-03-23T10:00:00Z",
            "decision_deadline": "2126-03-23T10:00:05Z",
            "decision_sla_seconds": 5,
            "scenario_type": "errors_simple_replayed",
            "producer_id": "generator",
            "trace_id": "trace-1",
        }

        rejected = PIX_DECISION_EVENTS.build_rejected_message(message, ["amount_must_be_positive"])

        self.assertEqual(rejected["source_transaction_id"], "tx-1")
        self.assertEqual(rejected["retry_attempt"], 1)
        self.assertEqual(rejected["rejection_fingerprint"], "tx-1:1:amount_must_be_positive")
        self.assertEqual(rejected["decision_status"], "REJECTED")
        self.assertEqual(rejected["decision_reason_code"], "BANK_REJECTED")
        self.assertTrue(rejected["decision_within_sla"])

    def test_validated_message_carries_client_decision(self):
        message = {
            "transaction_id": "tx-1",
            "event_time": "2026-03-23T10:00:00Z",
            "decision_deadline": "2126-03-23T10:00:05Z",
            "decision_sla_seconds": 5,
            "emitter_account_id": "acc-1",
            "beneficiary_account_id": "acc-2",
            "emitter_pix_client_id": "pix-1",
            "beneficiary_pix_client_id": "pix-2",
            "emitter_tax_id": "tax-1",
            "beneficiary_tax_id": "tax-2",
            "amount": 10,
            "currency": "BRL",
            "status": "RECEIVED",
            "scenario_type": "nominal",
            "producer_id": "generator",
            "trace_id": "trace-1",
        }

        validated = PIX_DECISION_EVENTS.build_validated_message(message, "tax-1")

        self.assertEqual(validated["decision_status"], "ACCEPTED")
        self.assertEqual(validated["decision_reason_code"], "ACCEPTED")
        self.assertEqual(validated["client_message"], "Paiement Pix accepté")

    def test_timeout_error_is_detected_when_deadline_is_passed(self):
        message = {
            "transaction_id": "tx-timeout",
            "event_time": "2026-03-23T10:00:00Z",
            "decision_deadline": "2026-03-23T10:00:01Z",
            "decision_sla_seconds": 1,
        }

        self.assertEqual(PIX_DECISION_LOGIC.build_timeout_error(message), ["processing_timeout"])


if __name__ == "__main__":
    unittest.main()
