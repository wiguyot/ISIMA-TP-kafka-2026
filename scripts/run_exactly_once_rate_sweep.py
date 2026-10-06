#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import subprocess
import sys
import time
import zipfile
from dataclasses import dataclass
from pathlib import Path
from typing import Any
from xml.sax.saxutils import escape


ROOT = Path(__file__).resolve().parents[1]
ANALYSIS_DIR = ROOT / "docs" / "analyses-techniques"
DEFAULT_TOTAL_MESSAGES = 5000
DEFAULT_TTL_SECONDS = 60
DEFAULT_POST_PUBLISH_OBSERVATION_SECONDS = 60
DEFAULT_PREPARE_MODE = "reset_only"
DEFAULT_OUTPUT_PREFIX = "exactly_once_rate_sweep_ttl60_obs60"


@dataclass(frozen=True)
class SweepConfig:
    total_messages: int
    ttl_seconds: int
    post_publish_observation_seconds: int
    prepare_mode: str
    output_prefix: str
    rates: list[int]
    estimate_only: bool


def build_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Balaye les debits pour le cas fixe linear / PUB exactly_once / SUB exactly_once_kafka."
    )
    parser.add_argument("--total-messages", type=int, default=DEFAULT_TOTAL_MESSAGES)
    parser.add_argument("--ttl-seconds", type=int, default=DEFAULT_TTL_SECONDS)
    parser.add_argument(
        "--post-publish-observation-seconds",
        type=int,
        default=DEFAULT_POST_PUBLISH_OBSERVATION_SECONDS,
    )
    parser.add_argument(
        "--prepare-mode",
        choices=["full_restart", "reset_only", "none"],
        default=DEFAULT_PREPARE_MODE,
    )
    parser.add_argument("--output-prefix", default=DEFAULT_OUTPUT_PREFIX)
    parser.add_argument(
        "--rates",
        default=",".join(str(value) for value in range(200, 0, -10)),
        help="Liste comma-separated des debits a tester.",
    )
    parser.add_argument("--estimate-only", action="store_true")
    return parser


def parse_rates(raw: str) -> list[int]:
    values = []
    for item in raw.split(","):
        item = item.strip()
        if not item:
            continue
        values.append(max(int(item), 1))
    if not values:
        raise ValueError("at least one rate is required")
    return values


def parse_args(argv: list[str]) -> SweepConfig:
    args = build_parser().parse_args(argv)
    return SweepConfig(
        total_messages=max(args.total_messages, 1),
        ttl_seconds=max(args.ttl_seconds, 3),
        post_publish_observation_seconds=max(args.post_publish_observation_seconds, 0),
        prepare_mode=args.prepare_mode,
        output_prefix=args.output_prefix,
        rates=parse_rates(args.rates),
        estimate_only=args.estimate_only,
    )


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


def estimate_case_seconds(rate: int, config: SweepConfig) -> float:
    prepare_seconds = 20.0 if config.prepare_mode == "reset_only" else 50.0 if config.prepare_mode == "full_restart" else 5.0
    injection_seconds = config.total_messages / max(rate, 1)
    return prepare_seconds + 5.0 + injection_seconds + config.post_publish_observation_seconds


def format_duration(seconds: float) -> str:
    total_seconds = int(round(seconds))
    hours, remainder = divmod(total_seconds, 3600)
    minutes, secs = divmod(remainder, 60)
    if hours:
        return f"{hours}h {minutes:02d}m {secs:02d}s"
    if minutes:
        return f"{minutes}m {secs:02d}s"
    return f"{secs}s"


def run_single_rate(rate: int, config: SweepConfig) -> dict[str, Any]:
    rate_prefix = f"{config.output_prefix}__rate_{rate:03d}"
    command = [
        "python3",
        "scripts/run_semantics_campaign.py",
        "--total-messages",
        str(config.total_messages),
        "--rate-per-second",
        str(rate),
        "--ttl-seconds",
        str(config.ttl_seconds),
        "--post-publish-observation-seconds",
        str(config.post_publish_observation_seconds),
        "--prepare-mode",
        config.prepare_mode,
        "--traffic-models",
        "linear",
        "--producer-semantics",
        "exactly_once",
        "--consumer-semantics",
        "exactly_once_kafka",
        "--output-prefix",
        rate_prefix,
    ]
    completed = subprocess.run(
        command,
        cwd=ROOT,
        text=True,
        capture_output=True,
        check=False,
    )
    if completed.returncode != 0:
        raise RuntimeError(
            f"rate {rate} failed\nstdout:\n{completed.stdout}\nstderr:\n{completed.stderr}"
        )
    rate_json = ANALYSIS_DIR / f"{rate_prefix}.json"
    payload = json.loads(rate_json.read_text())
    result = payload["results"][0]
    result["rate_sweep_target"] = rate
    return result


def main(argv: list[str]) -> int:
    config = parse_args(argv)
    ANALYSIS_DIR.mkdir(parents=True, exist_ok=True)

    estimate = {
        "rates": config.rates,
        "cases": len(config.rates),
        "total_messages": config.total_messages,
        "ttl_seconds": config.ttl_seconds,
        "post_publish_observation_seconds": config.post_publish_observation_seconds,
        "prepare_mode": config.prepare_mode,
        "estimated_total_seconds": round(sum(estimate_case_seconds(rate, config) for rate in config.rates), 2),
    }
    estimate["estimated_total_human"] = format_duration(estimate["estimated_total_seconds"])
    print(json.dumps(estimate, indent=2))
    if config.estimate_only:
        return 0

    started_at = time.time()
    results: list[dict[str, Any]] = []
    for index, rate in enumerate(config.rates, start=1):
        print(f"[simulpix] progress {index}/{len(config.rates)} rate={rate}", flush=True)
        result = run_single_rate(rate, config)
        results.append(result)
        print(
            f"[simulpix] rate={rate} status={result['status']} "
            f"published={result['published_pix']} accepted={result['accepted_pix']} "
            f"rejected={result['rejected_pix']} ttl={result['rejected_ttl_pix']} "
            f"queue={result['max_queue_length']}",
            flush=True,
        )

    payload = {
        "generated_at": time.strftime("%Y-%m-%dT%H:%M:%SZ", time.gmtime()),
        "elapsed_seconds": round(time.time() - started_at, 2),
        "config": {
            "total_messages": config.total_messages,
            "ttl_seconds": config.ttl_seconds,
            "post_publish_observation_seconds": config.post_publish_observation_seconds,
            "prepare_mode": config.prepare_mode,
            "rates": config.rates,
            "traffic_model": "linear",
            "producer_semantics": "exactly_once",
            "consumer_semantics": "exactly_once_kafka",
        },
        "results": results,
    }

    json_path = ANALYSIS_DIR / f"{config.output_prefix}.json"
    xlsx_path = ANALYSIS_DIR / f"{config.output_prefix}.xlsx"
    json_path.write_text(json.dumps(payload, indent=2), encoding="utf-8")

    rows = [[
        "rate_per_second",
        "status",
        "published_pix",
        "accepted_pix",
        "rejected_pix",
        "rejected_ttl_pix",
        "acceptance_rate_per_second",
        "rejection_rate_per_second",
        "avg_validation_delay_ms",
        "max_queue_length",
        "elapsed_seconds",
    ]]
    for result in results:
        rows.append([
            result["target_rate_per_second"],
            result["status"],
            result["published_pix"],
            result["accepted_pix"],
            result["rejected_pix"],
            result["rejected_ttl_pix"],
            result["acceptance_rate_per_second"],
            result["rejection_rate_per_second"],
            result["avg_validation_delay_ms"],
            result["max_queue_length"],
            result["elapsed_seconds"],
        ])
    build_xlsx(xlsx_path, [("rate_sweep", rows)])
    print(json.dumps({"json": str(json_path), "xlsx": str(xlsx_path)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main(sys.argv[1:]))
