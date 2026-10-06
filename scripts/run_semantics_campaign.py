#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import urllib.error
import urllib.request
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


ROOT = Path(__file__).resolve().parents[1]
OUTPUT_DIR = ROOT / "docs" / "analyses-techniques"
HEALTH_URL = "http://127.0.0.1:8082/health"
HEALTH_DETAILS_URL = "http://127.0.0.1:8082/health/details"

DEFAULT_TOTAL_MESSAGES = 5000
DEFAULT_RATE_PER_SECOND = 200
DEFAULT_TTL_SECONDS = 10
DEFAULT_STARTUP_STABILIZATION_SECONDS = 5.0
DEFAULT_POLL_SECONDS = 5.0
DEFAULT_CASE_TIMEOUT_SECONDS = 600.0
DEFAULT_POST_PUBLISH_OBSERVATION_SECONDS = 30.0
DEFAULT_HEALTH_TIMEOUT_SECONDS = 180.0
DEFAULT_PREPARE_MODE = "full_restart"

TRAFFIC_MODELS = ["linear", "poisson", "bursty"]
PRODUCER_SEMANTICS = ["at_most_once", "at_least_once", "exactly_once"]
CONSUMER_SEMANTICS = ["at_most_once", "at_least_once", "exactly_once_kafka"]


@dataclass(frozen=True)
class CampaignCase:
    traffic_model: str
    producer_semantics: str
    consumer_semantics: str

    @property
    def run_id(self) -> str:
        return f"{self.traffic_model}__pub_{self.producer_semantics}__sub_{self.consumer_semantics}"


@dataclass(frozen=True)
class CampaignConfig:
    total_messages: int
    rate_per_second: int
    ttl_seconds: int
    startup_stabilization_seconds: float
    poll_seconds: float
    case_timeout_seconds: float
    post_publish_observation_seconds: float
    health_timeout_seconds: float
    prepare_mode: str
    output_prefix: str
    traffic_models: list[str]
    producer_semantics: list[str]
    consumer_semantics: list[str]
    estimate_only: bool

    @property
    def case_count(self) -> int:
        return len(self.traffic_models) * len(self.producer_semantics) * len(self.consumer_semantics)

    @property
    def injection_seconds(self) -> float:
        if self.total_messages <= 0 or self.rate_per_second <= 0:
            return 0.0
        return self.total_messages / self.rate_per_second

    @property
    def per_case_estimated_seconds(self) -> float:
        if self.prepare_mode == "full_restart":
            prepare_seconds = 50.0
        elif self.prepare_mode == "reset_only":
            prepare_seconds = 20.0
        else:
            prepare_seconds = 5.0
        return (
            prepare_seconds
            + self.startup_stabilization_seconds
            + self.injection_seconds
            + self.post_publish_observation_seconds
        )

    @property
    def campaign_estimated_seconds(self) -> float:
        return self.case_count * self.per_case_estimated_seconds


def parse_csv_list(raw_value: str | None, allowed: list[str]) -> list[str]:
    if not raw_value:
        return list(allowed)
    values = [item.strip() for item in raw_value.split(",") if item.strip()]
    invalid = [item for item in values if item not in allowed]
    if invalid:
        raise ValueError(f"unsupported values: {', '.join(invalid)}; allowed: {', '.join(allowed)}")
    return values


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Lance une campagne combinatoire sur les modeles de trafic et les semantiques Kafka."
    )
    parser.add_argument("--total-messages", type=int, default=DEFAULT_TOTAL_MESSAGES)
    parser.add_argument("--rate-per-second", type=int, default=DEFAULT_RATE_PER_SECOND)
    parser.add_argument("--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS)
    parser.add_argument("--startup-stabilization-seconds", type=float, default=DEFAULT_STARTUP_STABILIZATION_SECONDS)
    parser.add_argument("--poll-seconds", type=float, default=DEFAULT_POLL_SECONDS)
    parser.add_argument("--case-timeout-seconds", type=float, default=DEFAULT_CASE_TIMEOUT_SECONDS)
    parser.add_argument(
        "--post-publish-observation-seconds",
        type=float,
        default=DEFAULT_POST_PUBLISH_OBSERVATION_SECONDS,
    )
    parser.add_argument("--health-timeout-seconds", type=float, default=DEFAULT_HEALTH_TIMEOUT_SECONDS)
    parser.add_argument(
        "--prepare-mode",
        choices=["full_restart", "reset_only", "none"],
        default=DEFAULT_PREPARE_MODE,
        help="Preparation avant chaque cas : stop/clean/start, reset-scenario seul, ou rien.",
    )
    parser.add_argument(
        "--traffic-models",
        default=",".join(TRAFFIC_MODELS),
        help="Sous-ensemble comma-separated de linear,poisson,bursty",
    )
    parser.add_argument(
        "--producer-semantics",
        default=",".join(PRODUCER_SEMANTICS),
        help="Sous-ensemble comma-separated de at_most_once,at_least_once,exactly_once",
    )
    parser.add_argument(
        "--consumer-semantics",
        default=",".join(CONSUMER_SEMANTICS),
        help="Sous-ensemble comma-separated de at_most_once,at_least_once,exactly_once_kafka",
    )
    parser.add_argument(
        "--output-prefix",
        default="semantics_campaign_results",
        help="Prefixe des fichiers JSON/XLSX dans docs/analyses-techniques/.",
    )
    parser.add_argument(
        "--estimate-only",
        action="store_true",
        help="Affiche uniquement l'estimation de duree et la matrice de cas sans lancer la campagne.",
    )
    return parser


def parse_args(argv: list[str]) -> CampaignConfig:
    args = build_parser().parse_args(argv)
    traffic_models = parse_csv_list(args.traffic_models, TRAFFIC_MODELS)
    producer_semantics = parse_csv_list(args.producer_semantics, PRODUCER_SEMANTICS)
    consumer_semantics = parse_csv_list(args.consumer_semantics, CONSUMER_SEMANTICS)
    return CampaignConfig(
        total_messages=max(args.total_messages, 1),
        rate_per_second=max(args.rate_per_second, 1),
        ttl_seconds=max(args.ttl_seconds, 3),
        startup_stabilization_seconds=max(args.startup_stabilization_seconds, 0.0),
        poll_seconds=max(args.poll_seconds, 1.0),
        case_timeout_seconds=max(args.case_timeout_seconds, 30.0),
        post_publish_observation_seconds=max(args.post_publish_observation_seconds, 0.0),
        health_timeout_seconds=max(args.health_timeout_seconds, 30.0),
        prepare_mode=args.prepare_mode,
        output_prefix=args.output_prefix,
        traffic_models=traffic_models,
        producer_semantics=producer_semantics,
        consumer_semantics=consumer_semantics,
        estimate_only=args.estimate_only,
    )


def run_command(
    command: list[str],
    env: dict[str, str] | None = None,
    timeout: float | None = None,
) -> subprocess.CompletedProcess[str]:
    completed = subprocess.run(
        command,
        cwd=ROOT,
        env=env,
        text=True,
        capture_output=True,
        timeout=timeout,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(command)}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    return completed


def wait_for_health(timeout_seconds: float) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=5) as response:
                if response.status == 200:
                    return
        except urllib.error.URLError:
            pass
        time.sleep(1.0)
    raise RuntimeError("service-health did not become ready in time")


def fetch_details() -> dict[str, Any]:
    with urllib.request.urlopen(HEALTH_DETAILS_URL, timeout=10) as response:
        return json.loads(response.read().decode("utf-8"))


def safe_int(value: Any) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return 0


def safe_float(value: Any) -> float:
    try:
        return float(value)
    except (TypeError, ValueError):
        return 0.0


def col_name(index: int) -> str:
    result = []
    current = index
    while current > 0:
        current, rem = divmod(current - 1, 26)
        result.append(chr(65 + rem))
    return "".join(reversed(result))


def worksheet_xml(rows: list[list[Any]]) -> str:
    xml_rows: list[str] = []
    for row_index, row in enumerate(rows, start=1):
        cells: list[str] = []
        for col_index, value in enumerate(row, start=1):
            ref = f"{col_name(col_index)}{row_index}"
            if isinstance(value, (int, float)) and not isinstance(value, bool):
                cells.append(f'<c r="{ref}"><v>{value}</v></c>')
            else:
                text = escape("" if value is None else str(value))
                cells.append(f'<c r="{ref}" t="inlineStr"><is><t>{text}</t></is></c>')
        xml_rows.append(f"<row r=\"{row_index}\">{''.join(cells)}</row>")
    return (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<worksheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        "<sheetData>"
        + "".join(xml_rows)
        + "</sheetData></worksheet>"
    )


def build_xlsx(path: Path, sheets: list[tuple[str, list[list[Any]]]]) -> None:
    workbook_xml = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<workbook xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main" '
        'xmlns:r="http://schemas.openxmlformats.org/officeDocument/2006/relationships">'
        "<sheets>"
        + "".join(
            f'<sheet name="{escape(name)}" sheetId="{index}" r:id="rId{index}"/>'
            for index, (name, _) in enumerate(sheets, start=1)
        )
        + "</sheets></workbook>"
    )
    workbook_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        + "".join(
            f'<Relationship Id="rId{index}" '
            'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet" '
            f'Target="worksheets/sheet{index}.xml"/>'
            for index, _sheet in enumerate(sheets, start=1)
        )
        + '<Relationship Id="rIdStyles" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/styles" '
        'Target="styles.xml"/>'
        "</Relationships>"
    )
    root_rels = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Relationships xmlns="http://schemas.openxmlformats.org/package/2006/relationships">'
        '<Relationship Id="rId1" '
        'Type="http://schemas.openxmlformats.org/officeDocument/2006/relationships/officeDocument" '
        'Target="xl/workbook.xml"/>'
        "</Relationships>"
    )
    content_types = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<Types xmlns="http://schemas.openxmlformats.org/package/2006/content-types">'
        '<Default Extension="rels" ContentType="application/vnd.openxmlformats-package.relationships+xml"/>'
        '<Default Extension="xml" ContentType="application/xml"/>'
        '<Override PartName="/xl/workbook.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml"/>'
        '<Override PartName="/xl/styles.xml" '
        'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.styles+xml"/>'
        + "".join(
            f'<Override PartName="/xl/worksheets/sheet{index}.xml" '
            'ContentType="application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml"/>'
            for index, _sheet in enumerate(sheets, start=1)
        )
        + "</Types>"
    )
    styles = (
        '<?xml version="1.0" encoding="UTF-8" standalone="yes"?>'
        '<styleSheet xmlns="http://schemas.openxmlformats.org/spreadsheetml/2006/main">'
        '<fonts count="1"><font><sz val="11"/><name val="Calibri"/></font></fonts>'
        '<fills count="1"><fill><patternFill patternType="none"/></fill></fills>'
        '<borders count="1"><border/></borders>'
        '<cellStyleXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0"/></cellStyleXfs>'
        '<cellXfs count="1"><xf numFmtId="0" fontId="0" fillId="0" borderId="0" xfId="0"/></cellXfs>'
        '<cellStyles count="1"><cellStyle name="Normal" xfId="0" builtinId="0"/></cellStyles>'
        "</styleSheet>"
    )
    with zipfile.ZipFile(path, "w", compression=zipfile.ZIP_DEFLATED) as workbook:
        workbook.writestr("[Content_Types].xml", content_types)
        workbook.writestr("_rels/.rels", root_rels)
        workbook.writestr("xl/workbook.xml", workbook_xml)
        workbook.writestr("xl/_rels/workbook.xml.rels", workbook_rels)
        workbook.writestr("xl/styles.xml", styles)
        for index, (_name, rows) in enumerate(sheets, start=1):
            workbook.writestr(f"xl/worksheets/sheet{index}.xml", worksheet_xml(rows))


def prepare_platform_for_case(config: CampaignConfig) -> None:
    if config.prepare_mode == "full_restart":
        run_command(["./stop.sh"])
        run_command(["./start.sh"])
    elif config.prepare_mode == "reset_only":
        run_command(["./scripts/reset-scenario.sh"])
    wait_for_health(config.health_timeout_seconds)
    time.sleep(config.startup_stabilization_seconds)


def current_queue_length(payload: dict[str, Any]) -> int:
    counts = ((payload.get("metrics") or {}).get("counts") or {})
    return sum(
        safe_int(counts.get(name))
        for name in (
            "pix_validator_lag_total",
            "pix_decision_engine_lag_total",
            "pix_outcome_publisher_lag_total",
            "persister_valid_lag_total",
            "persister_rejected_lag_total",
        )
    )


def collect_sample(start_time: float, payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload.get("metrics") or {}
    counts = metrics.get("counts") or {}
    timings = metrics.get("timings") or {}
    details = payload.get("details") or {}
    return {
        "elapsed_s": round(time.time() - start_time, 2),
        "published": safe_int(counts.get("raw_topic_end_offsets")),
        "accepted": safe_int(counts.get("validated")),
        "rejected": safe_int(counts.get("rejected")),
        "rejected_ttl": safe_int(counts.get("rejected_sla_breaches")),
        "acceptance_rate_recent": safe_float((metrics.get("rates") or {}).get("validated_per_second")),
        "rejection_rate_recent": safe_float((metrics.get("rates") or {}).get("rejected_per_second")),
        "avg_validation_latency_ms": safe_float(timings.get("avg_validation_latency_ms")),
        "queue_length": current_queue_length(payload),
        "generator_phase": str((details.get("pix-traffic-shaper") or {}).get("current_phase") or ""),
    }


def run_case(case: CampaignCase, config: CampaignConfig) -> dict[str, Any]:
    print(f"[simulpix] case start: {case.run_id}", flush=True)
    prepare_platform_for_case(config)
    env = {
        **{
            "SIMULPIX_KEEP_HEALTH_RUNNING": "1",
            "SIMULPIX_KAFKA_PRODUCER_SEMANTICS": case.producer_semantics,
            "SIMULPIX_KAFKA_CONSUMER_SEMANTICS": case.consumer_semantics,
            "SIMULPIX_TRAFFIC_MODEL": case.traffic_model,
            "SIMULPIX_DECISION_SLA_SECONDS": str(config.ttl_seconds),
        }
    }
    run_command(
        ["./scripts/run-scenario.sh", "nominal", str(config.total_messages), str(config.rate_per_second)],
        env=env,
        timeout=config.case_timeout_seconds,
    )
    wait_for_health(config.health_timeout_seconds)

    start_time = time.time()
    deadline = start_time + config.case_timeout_seconds
    samples: list[dict[str, Any]] = []
    payload: dict[str, Any] | None = None
    publish_reached_at: float | None = None
    expected_total = config.total_messages
    while time.time() < deadline:
        payload = fetch_details()
        sample = collect_sample(start_time, payload)
        samples.append(sample)
        if len(samples) == 1 or len(samples) % 6 == 0:
            print(
                f"[simulpix] {case.run_id} t={sample['elapsed_s']}s "
                f"published={sample['published']} accepted={sample['accepted']} rejected={sample['rejected']} "
                f"rejected_ttl={sample['rejected_ttl']} queue={sample['queue_length']}",
                flush=True,
            )
        published = sample["published"]
        if published >= expected_total and publish_reached_at is None:
            publish_reached_at = time.time()
        if (
            publish_reached_at is not None
            and (time.time() - publish_reached_at) >= config.post_publish_observation_seconds
        ):
            break
        time.sleep(config.poll_seconds)

    if payload is None:
        raise RuntimeError(f"no payload collected for case {case.run_id}")

    metrics = payload.get("metrics") or {}
    counts = metrics.get("counts") or {}
    timings = metrics.get("timings") or {}
    total_duration = max(time.time() - start_time, 0.001)
    published = safe_int(counts.get("raw_topic_end_offsets"))
    accepted = safe_int(counts.get("validated"))
    rejected = safe_int(counts.get("rejected"))
    rejected_ttl = safe_int(counts.get("rejected_sla_breaches"))
    acceptance_rate_avg = round(accepted / total_duration, 3)
    rejection_rate_avg = round(rejected / total_duration, 3)
    avg_validation_latency_ms = safe_float(timings.get("avg_validation_latency_ms"))
    max_queue_length = max((sample["queue_length"] for sample in samples), default=0)
    result = {
        "run_id": case.run_id,
        "traffic_model": case.traffic_model,
        "producer_semantics": case.producer_semantics,
        "consumer_semantics": case.consumer_semantics,
        "ttl_seconds": config.ttl_seconds,
        "target_total_messages": config.total_messages,
        "target_rate_per_second": config.rate_per_second,
        "published_pix": published,
        "accepted_pix": accepted,
        "rejected_pix": rejected,
        "rejected_ttl_pix": rejected_ttl,
        "acceptance_rate_per_second": acceptance_rate_avg,
        "rejection_rate_per_second": rejection_rate_avg,
        "avg_validation_delay_ms": round(avg_validation_latency_ms, 3) if avg_validation_latency_ms else "",
        "max_queue_length": max_queue_length,
        "elapsed_seconds": round(total_duration, 2),
        "status": str(payload.get("status") or "unknown"),
    }
    print(
        f"[simulpix] case done: {case.run_id} published={published} accepted={accepted} rejected={rejected} ttl={rejected_ttl}",
        flush=True,
    )
    return {"result": result, "samples": samples}


def build_cases(config: CampaignConfig) -> list[CampaignCase]:
    return [
        CampaignCase(traffic_model=traffic, producer_semantics=producer, consumer_semantics=consumer)
        for traffic in config.traffic_models
        for producer in config.producer_semantics
        for consumer in config.consumer_semantics
    ]


def format_duration(seconds: float) -> str:
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def print_estimate(config: CampaignConfig, cases: list[CampaignCase]) -> None:
    estimate = {
        "cases": len(cases),
        "prepare_mode": config.prepare_mode,
        "traffic_models": config.traffic_models,
        "producer_semantics": config.producer_semantics,
        "consumer_semantics": config.consumer_semantics,
        "total_messages": config.total_messages,
        "rate_per_second": config.rate_per_second,
        "ttl_seconds": config.ttl_seconds,
        "estimated_injection_seconds_per_case": round(config.injection_seconds, 2),
        "estimated_total_seconds_per_case": round(config.per_case_estimated_seconds, 2),
        "estimated_campaign_seconds": round(config.campaign_estimated_seconds, 2),
        "estimated_campaign_human": format_duration(config.campaign_estimated_seconds),
    }
    print(json.dumps(estimate, indent=2))


def main(argv: list[str]) -> int:
    config = parse_args(argv)
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    cases = build_cases(config)
    print_estimate(config, cases)
    if config.estimate_only:
        return 0

    results: list[dict[str, Any]] = []
    samples_rows: list[dict[str, Any]] = []
    started_at = time.time()
    for index, case in enumerate(cases, start=1):
        print(f"[simulpix] progress {index}/{len(cases)}", flush=True)
        try:
            outcome = run_case(case, config)
            results.append(outcome["result"])
            for sample in outcome["samples"]:
                samples_rows.append({"run_id": case.run_id, **sample})
        except Exception as exc:
            print(f"[simulpix] case failed: {case.run_id}: {exc}", flush=True)
            results.append(
                {
                    "run_id": case.run_id,
                    "traffic_model": case.traffic_model,
                    "producer_semantics": case.producer_semantics,
                    "consumer_semantics": case.consumer_semantics,
                    "ttl_seconds": config.ttl_seconds,
                    "target_total_messages": config.total_messages,
                    "target_rate_per_second": config.rate_per_second,
                    "published_pix": "",
                    "accepted_pix": "",
                    "rejected_pix": "",
                    "rejected_ttl_pix": "",
                    "acceptance_rate_per_second": "",
                    "rejection_rate_per_second": "",
                    "avg_validation_delay_ms": "",
                    "max_queue_length": "",
                    "elapsed_seconds": "",
                    "status": f"error: {exc}",
                }
            )

    raw_json_path = OUTPUT_DIR / f"{config.output_prefix}.json"
    raw_json_path.write_text(json.dumps({"results": results, "samples": samples_rows}, indent=2), encoding="utf-8")

    protocol_rows: list[list[Any]] = [
        [
            "run_id",
            "traffic_model",
            "producer_semantics",
            "consumer_semantics",
            "target_total_messages",
            "target_rate_per_second",
            "ttl_seconds",
            "prepare_mode",
        ]
    ]
    for case in cases:
        protocol_rows.append(
            [
                case.run_id,
                case.traffic_model,
                case.producer_semantics,
                case.consumer_semantics,
                config.total_messages,
                config.rate_per_second,
                config.ttl_seconds,
                config.prepare_mode,
            ]
        )

    result_headers = [
        "run_id",
        "traffic_model",
        "producer_semantics",
        "consumer_semantics",
        "ttl_seconds",
        "target_total_messages",
        "target_rate_per_second",
        "published_pix",
        "accepted_pix",
        "rejected_pix",
        "rejected_ttl_pix",
        "acceptance_rate_per_second",
        "rejection_rate_per_second",
        "avg_validation_delay_ms",
        "max_queue_length",
        "elapsed_seconds",
        "status",
    ]
    result_rows: list[list[Any]] = [result_headers]
    for result in results:
        result_rows.append([result.get(key, "") for key in result_headers])

    sample_headers = [
        "run_id",
        "elapsed_s",
        "published",
        "accepted",
        "rejected",
        "rejected_ttl",
        "acceptance_rate_recent",
        "rejection_rate_recent",
        "avg_validation_latency_ms",
        "queue_length",
        "generator_phase",
    ]
    sample_sheet_rows: list[list[Any]] = [sample_headers]
    for row in samples_rows:
        sample_sheet_rows.append([row.get(key, "") for key in sample_headers])

    summary_rows: list[list[Any]] = [
        ["metric", "value"],
        ["cases_total", len(cases)],
        ["cases_success", sum(1 for result in results if safe_int(result.get("published_pix")) > 0)],
        ["prepare_mode", config.prepare_mode],
        ["estimated_campaign_human", format_duration(config.campaign_estimated_seconds)],
        ["real_campaign_human", format_duration(time.time() - started_at)],
        ["raw_json", str(raw_json_path)],
    ]

    xlsx_path = OUTPUT_DIR / f"{config.output_prefix}.xlsx"
    build_xlsx(
        xlsx_path,
        [
            ("Protocole", protocol_rows),
            ("Resultats", result_rows),
            ("Samples", sample_sheet_rows),
            ("Synthese", summary_rows),
        ],
    )
    print(json.dumps({"xlsx": str(xlsx_path), "json": str(raw_json_path)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
