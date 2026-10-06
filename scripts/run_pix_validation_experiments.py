#!/usr/bin/env python3
from __future__ import annotations

import json
import os
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
OUTPUT_DIR = ROOT / "analysis"
HEALTH_DETAILS_URL = "http://127.0.0.1:8082/health/details"
HEALTH_URL = "http://127.0.0.1:8082/health"


@dataclass
class Experiment:
    run_id: str
    scenario: str
    total_messages: str
    rate_per_second: str
    decision_sla_seconds: str
    processing_delay_ms_override: str = ""
    network_profile: str = ""
    network_target_service: str = ""
    network_delay_ms_override: str = ""
    network_jitter_ms_override: str = ""
    network_loss_percent_override: str = ""
    network_rate_kbit_override: str = ""
    producer_acks: str = "all"
    producer_retries: str = "0"
    notes: str = ""
    startup_grace_seconds: float = 3.0
    post_network_wait_seconds: float = 4.0
    timeout_seconds: float = 180.0


EXPERIMENTS: list[Experiment] = [
    Experiment("R1_baseline", "nominal", "400", "40", "10", notes="Reference nominale."),
    Experiment("R2_rate_only", "nominal", "2000", "250", "10", notes="Debit eleve sans changer le TTL."),
    Experiment("R3_low_ttl", "nominal", "2000", "250", "3", notes="Debit eleve avec TTL court."),
    Experiment("R4_processing_delay", "nominal", "1200", "120", "10", processing_delay_ms_override="60", notes="Ralentissement du pix-decision-engine."),
    Experiment(
        "R5_processing_delay_plus_low_ttl",
        "nominal",
        "1200",
        "120",
        "3",
        processing_delay_ms_override="60",
        notes="Combinaison backlog + TTL court.",
    ),
    Experiment(
        "R6_network_latency_mild",
        "nominal",
        "1200",
        "120",
        "10",
        network_profile="kafka_latency",
        network_target_service="pix-decision-engine",
        network_delay_ms_override="100",
        network_jitter_ms_override="20",
        notes="Latence reseau moderee appliquee au pix-decision-engine.",
    ),
    Experiment(
        "R7_network_latency_medium",
        "nominal",
        "1200",
        "120",
        "10",
        network_profile="kafka_latency",
        network_target_service="pix-decision-engine",
        network_delay_ms_override="250",
        network_jitter_ms_override="40",
        notes="Latence reseau moyenne appliquee au pix-decision-engine.",
    ),
    Experiment(
        "R8_network_latency_high",
        "nominal",
        "1200",
        "120",
        "10",
        network_profile="kafka_latency",
        network_target_service="pix-decision-engine",
        network_delay_ms_override="400",
        network_jitter_ms_override="60",
        notes="Latence reseau forte appliquee au pix-decision-engine.",
    ),
    Experiment(
        "R9_network_latency_medium_low_ttl",
        "nominal",
        "1200",
        "120",
        "3",
        network_profile="kafka_latency",
        network_target_service="pix-decision-engine",
        network_delay_ms_override="250",
        network_jitter_ms_override="40",
        notes="Latence reseau moyenne combinee a un TTL court.",
    ),
]


def run_command(command: list[str], env: dict[str, str] | None = None) -> None:
    completed = subprocess.run(command, cwd=ROOT, env=env, text=True, capture_output=True)
    if completed.returncode != 0:
        raise RuntimeError(
            f"command failed: {' '.join(command)}\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )


def parse_expected_total(raw_value: str) -> int:
    try:
        value = int(raw_value)
    except (TypeError, ValueError):
        return 0
    return max(value, 0)


def wait_for_health(timeout_seconds: float = 120.0) -> None:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            with urllib.request.urlopen(HEALTH_URL, timeout=3) as response:
                if response.status == 200:
                    return
        except urllib.error.URLError:
            pass
        time.sleep(1.0)
    raise RuntimeError("service-health did not become ready in time")


def fetch_details() -> dict[str, Any]:
    with urllib.request.urlopen(HEALTH_DETAILS_URL, timeout=5) as response:
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


def classify_stop(samples: list[dict[str, Any]]) -> tuple[bool, float | None, str]:
    stagnant = 0
    previous_validated = None
    for sample in samples:
        validated = sample["validated"]
        generated = sample["generated"]
        processed = sample["processed"]
        rejected = sample["rejected"]
        lag = sample["pix_decision_engine_lag_total"]
        oldest = sample["estimated_oldest_lag_seconds"]
        rejected_sla = sample["rejected_sla_breaches"]
        if previous_validated is not None and validated <= previous_validated:
            if generated > validated or processed > validated:
                stagnant += 1
            else:
                stagnant = 0
        else:
            stagnant = 0
        previous_validated = validated
        if stagnant >= 3:
            if rejected_sla > 0:
                return True, sample["elapsed_s"], "TTL/SLA"
            if rejected > 0:
                return True, sample["elapsed_s"], "rejects_without_sla"
            if lag > 0 or oldest > 0:
                return True, sample["elapsed_s"], "consumer_backlog"
    return False, None, "none"


def summarize_run(experiment: Experiment, samples: list[dict[str, Any]], final_payload: dict[str, Any]) -> dict[str, Any]:
    counts = ((final_payload.get("metrics") or {}).get("counts") or {})
    gaps = ((final_payload.get("metrics") or {}).get("gaps") or {})
    timings = ((final_payload.get("metrics") or {}).get("timings") or {})
    generator = (final_payload.get("details") or {}).get("generator") or {}
    stop_detected, stop_time, stop_mode = classify_stop(samples)
    max_pix_decision_engine_lag = max((sample["pix_decision_engine_lag_total"] for sample in samples), default=0)
    max_oldest_backlog_s = max((sample["estimated_oldest_lag_seconds"] for sample in samples), default=0.0)
    max_gap_valid = max((sample["persistence_gap_valid"] for sample in samples), default=0)
    max_gap_rejected = max((sample["persistence_gap_rejected"] for sample in samples), default=0)
    generated = safe_int(counts.get("generated"))
    processed = safe_int(counts.get("processed"))
    validated = safe_int(counts.get("validated"))
    rejected = safe_int(counts.get("rejected"))
    validated_sla_breaches = safe_int(counts.get("validated_sla_breaches"))
    rejected_sla_breaches = safe_int(counts.get("rejected_sla_breaches"))

    if stop_mode == "TTL/SLA":
        dominant_factor = "TTL trop court par rapport au temps de traitement observe"
    elif stop_mode == "consumer_backlog":
        dominant_factor = "backlog consommateur / saturation du pix-decision-engine"
    elif stop_mode == "rejects_without_sla":
        dominant_factor = "rejets applicatifs non SLA"
    elif rejected == 0 and validated == generated:
        dominant_factor = "aucune rupture detectee"
    else:
        dominant_factor = "a confirmer"

    return {
        "run_id": experiment.run_id,
        "scenario": experiment.scenario,
        "total_messages": experiment.total_messages,
        "rate_per_second": experiment.rate_per_second,
        "decision_sla_seconds": experiment.decision_sla_seconds,
        "processing_delay_ms_override": experiment.processing_delay_ms_override,
        "network_profile": experiment.network_profile,
        "network_target_service": experiment.network_target_service,
        "producer_acks": experiment.producer_acks,
        "producer_retries": experiment.producer_retries,
        "generated": generated,
        "processed": processed,
        "validated": validated,
        "rejected": rejected,
        "validated_sla_breaches": validated_sla_breaches,
        "rejected_sla_breaches": rejected_sla_breaches,
        "max_pix_decision_engine_lag": max_pix_decision_engine_lag,
        "max_estimated_oldest_lag_seconds": round(max_oldest_backlog_s, 2),
        "max_persistence_gap_valid": max_gap_valid,
        "max_persistence_gap_rejected": max_gap_rejected,
        "avg_validation_latency_ms_recent": safe_float(timings.get("avg_validation_latency_ms")),
        "avg_rejection_latency_ms_recent": safe_float(timings.get("avg_rejection_latency_ms")),
        "validation_stop_detected": "yes" if stop_detected else "no",
        "validation_stop_time_s": "" if stop_time is None else round(stop_time, 2),
        "stop_mode": stop_mode,
        "generator_phase_finale": str(generator.get("current_phase", "")),
        "dominant_factor_hypothesis": dominant_factor,
        "notes": experiment.notes,
    }


def collect_sample(start_time: float, payload: dict[str, Any]) -> dict[str, Any]:
    metrics = payload.get("metrics") or {}
    counts = metrics.get("counts") or {}
    gaps = metrics.get("gaps") or {}
    timings = metrics.get("timings") or {}
    kafka = (payload.get("details") or {}).get("kafka") or {}
    consumer_groups = kafka.get("consumer_groups") or {}
    pix_decision_engine_group = consumer_groups.get("pix-decision-engine") or {}
    generator = (payload.get("details") or {}).get("generator") or {}
    return {
        "elapsed_s": round(time.time() - start_time, 2),
        "generated": safe_int(counts.get("generated")),
        "processed": safe_int(counts.get("processed")),
        "validated": safe_int(counts.get("validated")),
        "rejected": safe_int(counts.get("rejected")),
        "validated_sla_breaches": safe_int(counts.get("validated_sla_breaches")),
        "rejected_sla_breaches": safe_int(counts.get("rejected_sla_breaches")),
        "pix_decision_engine_lag_total": safe_int(pix_decision_engine_group.get("lag_total")),
        "estimated_oldest_lag_seconds": safe_float(timings.get("estimated_oldest_lag_seconds")),
        "persistence_gap_valid": safe_int(gaps.get("persistence_gap_valid")),
        "persistence_gap_rejected": safe_int(gaps.get("persistence_gap_rejected")),
        "generator_messages_sent": safe_int(generator.get("messages_sent")),
        "generator_status": str(generator.get("status", "")),
        "generator_phase": str(generator.get("current_phase", "")),
    }


def run_experiment(experiment: Experiment) -> dict[str, Any]:
    print(f"[simulpix] starting {experiment.run_id}: {experiment.notes}", flush=True)
    env = os.environ.copy()
    env["SIMULPIX_KEEP_HEALTH_RUNNING"] = "1"
    env["SIMULPIX_DECISION_SLA_SECONDS"] = experiment.decision_sla_seconds
    env["SIMULPIX_KAFKA_ACKS"] = experiment.producer_acks
    env["SIMULPIX_KAFKA_RETRIES"] = experiment.producer_retries
    if experiment.processing_delay_ms_override:
        env["SIMULPIX_PROCESSING_DELAY_MS_OVERRIDE"] = experiment.processing_delay_ms_override
    if experiment.network_delay_ms_override:
        env["SIMULPIX_NETWORK_DELAY_MS_OVERRIDE"] = experiment.network_delay_ms_override
    if experiment.network_jitter_ms_override:
        env["SIMULPIX_NETWORK_JITTER_MS_OVERRIDE"] = experiment.network_jitter_ms_override
    if experiment.network_loss_percent_override:
        env["SIMULPIX_NETWORK_LOSS_PERCENT_OVERRIDE"] = experiment.network_loss_percent_override
    if experiment.network_rate_kbit_override:
        env["SIMULPIX_NETWORK_RATE_KBIT_OVERRIDE"] = experiment.network_rate_kbit_override

    run_command(["./scripts/reset-scenario.sh"], env=env)
    wait_for_health()
    run_command(
        ["./scripts/run-scenario.sh", experiment.scenario, experiment.total_messages, experiment.rate_per_second],
        env=env,
    )

    time.sleep(experiment.startup_grace_seconds)
    if experiment.network_profile and experiment.network_target_service:
        run_command(["/bin/sh", "./scripts/network-perturb.sh", experiment.network_profile, experiment.network_target_service], env=env)
        time.sleep(experiment.post_network_wait_seconds)

    start_time = time.time()
    expected_total = parse_expected_total(experiment.total_messages)
    samples: list[dict[str, Any]] = []
    last_payload: dict[str, Any] | None = None
    deadline = start_time + experiment.timeout_seconds
    stable_completion_hits = 0
    while time.time() < deadline:
        payload = fetch_details()
        last_payload = payload
        sample = collect_sample(start_time, payload)
        samples.append(sample)

        generated = sample["generated"]
        processed = sample["processed"]
        messages_sent = sample["generator_messages_sent"]
        status = sample["generator_status"]
        phase = sample["generator_phase"]

        if len(samples) == 1 or len(samples) % 5 == 0:
            print(
                f"[simulpix] {experiment.run_id} t={sample['elapsed_s']}s "
                f"gen={generated} proc={processed} val={sample['validated']} rej={sample['rejected']} "
                f"lag={sample['pix_decision_engine_lag_total']} phase={phase or 'n/a'}",
                flush=True,
            )

        completed_by_volume = expected_total > 0 and generated >= expected_total and processed >= generated
        completed_by_generator_state = (
            expected_total > 0
            and messages_sent >= expected_total
            and status in {"completed", "idle"}
            and processed >= generated
        )
        if completed_by_volume or completed_by_generator_state:
            stable_completion_hits += 1
        else:
            stable_completion_hits = 0
        if stable_completion_hits >= 2:
            break
        time.sleep(2.0)

    if last_payload is None:
        raise RuntimeError(f"no payload collected for {experiment.run_id}")

    if experiment.network_profile and experiment.network_target_service:
        run_command(["/bin/sh", "./scripts/network-reset.sh"], env=env)

    summary = summarize_run(experiment, samples, last_payload)
    print(
        f"[simulpix] completed {experiment.run_id}: "
        f"generated={summary['generated']} processed={summary['processed']} "
        f"validated={summary['validated']} rejected={summary['rejected']} "
        f"stop={summary['validation_stop_detected']} mode={summary['stop_mode']}",
        flush=True,
    )
    return {"experiment": summary, "samples": samples}


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


def main() -> int:
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    run_command(["./start.sh"])
    wait_for_health()

    results: list[dict[str, Any]] = []
    all_samples: list[dict[str, Any]] = []
    for experiment in EXPERIMENTS:
        result = run_experiment(experiment)
        results.append(result["experiment"])
        for sample in result["samples"]:
            all_samples.append({"run_id": experiment.run_id, **sample})

    raw_json_path = OUTPUT_DIR / "pix_validation_stop_experiments.json"
    raw_json_path.write_text(json.dumps({"results": results, "samples": all_samples}, indent=2), encoding="utf-8")

    protocol_rows: list[list[Any]] = [
        ["run_id", "objectif", "scenario", "total_messages", "rate_per_second", "decision_sla_seconds", "processing_delay_ms", "network_profile", "notes"],
    ]
    protocol_map = {
        "R1_baseline": "reference nominale",
        "R2_rate_only": "isoler le debit",
        "R3_low_ttl": "isoler le TTL court",
        "R4_processing_delay": "isoler le ralentissement du pix-decision-engine",
        "R5_processing_delay_plus_low_ttl": "tester backlog + TTL court",
        "R6_network_latency_mild": "latence reseau moderee",
        "R7_network_latency_medium": "latence reseau moyenne",
        "R8_network_latency_high": "latence reseau forte",
        "R9_network_latency_medium_low_ttl": "latence reseau moyenne + TTL court",
    }
    for experiment in EXPERIMENTS:
        protocol_rows.append(
            [
                experiment.run_id,
                protocol_map.get(experiment.run_id, ""),
                experiment.scenario,
                experiment.total_messages,
                experiment.rate_per_second,
                experiment.decision_sla_seconds,
                experiment.processing_delay_ms_override,
                experiment.network_profile,
                experiment.notes,
            ]
        )

    results_rows = [
        [
            "run_id",
            "scenario",
            "total_messages",
            "rate_per_second",
            "decision_sla_seconds",
            "processing_delay_ms_override",
            "network_profile",
            "network_target_service",
            "producer_acks",
            "producer_retries",
            "generated",
            "processed",
            "validated",
            "rejected",
            "validated_sla_breaches",
            "rejected_sla_breaches",
            "max_pix_decision_engine_lag",
            "max_estimated_oldest_lag_seconds",
            "max_persistence_gap_valid",
            "max_persistence_gap_rejected",
            "avg_validation_latency_ms_recent",
            "avg_rejection_latency_ms_recent",
            "validation_stop_detected",
            "validation_stop_time_s",
            "stop_mode",
            "generator_phase_finale",
            "dominant_factor_hypothesis",
            "notes",
        ]
    ]
    for result in results:
        results_rows.append([result[key] for key in results_rows[0]])

    samples_rows = [
        [
            "run_id",
            "elapsed_s",
            "generated",
            "processed",
            "validated",
            "rejected",
            "validated_sla_breaches",
            "rejected_sla_breaches",
            "pix_decision_engine_lag_total",
            "estimated_oldest_lag_seconds",
            "persistence_gap_valid",
            "persistence_gap_rejected",
        ]
    ]
    for sample in all_samples:
        samples_rows.append([sample[key] for key in samples_rows[0]])

    factor_counts: dict[str, int] = {}
    for result in results:
        key = str(result["dominant_factor_hypothesis"])
        factor_counts[key] = factor_counts.get(key, 0) + 1
    leading_factor = max(factor_counts.items(), key=lambda item: item[1])[0] if factor_counts else "indetermine"
    conclusions_rows = [
        ["Synthese", "Valeur"],
        ["runs_executes", len(results)],
        ["ruptures_detectees", sum(1 for result in results if result["validation_stop_detected"] == "yes")],
        ["facteur_dominant_initial", leading_factor],
        [
            "lecture",
            "Comparer prioritairement les runs R2, R3, R4 et R5 pour separer debit, TTL et backlog du pix-decision-engine.",
        ],
        ["json_brut", str(raw_json_path)],
    ]

    xlsx_path = OUTPUT_DIR / "pix_validation_stop_experiments.xlsx"
    build_xlsx(
        xlsx_path,
        [
            ("Protocole", protocol_rows),
            ("Resultats", results_rows),
            ("Samples", samples_rows),
            ("Conclusions", conclusions_rows),
        ],
    )
    print(json.dumps({"xlsx": str(xlsx_path), "json": str(raw_json_path)}, indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
