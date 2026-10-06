from html import escape
from typing import Any, Callable

from formatting import display_value, format_status_label, status_badge_class


def summarize_services(services: dict[str, str]) -> tuple[int, int]:
    ok_count = sum(1 for value in services.values() if value in ("ok", "not_probed"))
    issue_count = sum(1 for value in services.values() if value not in ("ok", "not_probed"))
    return ok_count, issue_count


def render_scenario_entry_card() -> str:
    return (
        "<section class='card'>"
        "<h2>Service de pilotage</h2>"
        "<p>La météo sur <code>8082</code> reste concentrée sur l'observation. Le parcours TP, le lancement du flux Pix et les perturbations réseau se pilotent sur <code>8083</code>.</p>"
        "<form method='get' action='http://localhost:8083/scenario'>"
        "<p><button type='submit'>Ouvrir le parcours Kafka Simul-Pix</button></p>"
        "</form>"
        "</section>"
    )


def render_dashboard_html(
    payload: dict[str, Any],
    *,
    safe_int: Callable[[Any], int],
    render_sparkline: Callable[[list[dict[str, Any]], str, str], str],
    render_network_warning_banner: Callable[[dict[str, Any]], str],
    format_control_message: Callable[[str, str, dict[str, Any]], tuple[str, str]],
) -> str:
    services = payload.get("services", {})
    details = payload.get("details", {})
    metrics = payload.get("metrics", {})
    alerts = payload.get("alerts", [])
    control = payload.get("control", {})
    ui_state = payload.get("ui", {})
    kafka_detail = details.get("kafka") or {}
    kafka_groups = kafka_detail.get("consumer_groups", {})
    run_state = payload.get("run") or {}
    counts = metrics.get("counts", {})
    ratios = metrics.get("ratios", {})
    timings = metrics.get("timings", {})
    rates = metrics.get("rates") or {}
    history = payload.get("history", [])
    processing_arch = "split"
    active_processing_detail = (details.get("pix-decision-engine") or {})
    ok_services, issue_services = summarize_services(services)
    current_lag_total = sum(safe_int(group.get("lag_total")) for group in kafka_groups.values())
    alert_count = len(alerts)
    scenario_planner_detail = details.get("pix-scenario-planner") or {}
    traffic_shaper_detail = details.get("pix-traffic-shaper") or {}
    generator_phase = escape(str(traffic_shaper_detail.get("current_phase") or "n/a"))
    alert_rows = "\n".join(
        f"<tr><td>{escape(str(item.get('level', '')))}</td><td>{escape(str(item.get('kind', '')))}</td><td>{escape(str(item.get('message', '')))}</td></tr>"
        for item in alerts
    )
    if not alert_rows:
        alert_rows = "<tr><td>ok</td><td>none</td><td>aucune alerte active</td></tr>"
    control_disabled = "disabled" if control.get("status") in ("running", "busy") else ""
    control_banner = ""
    ui_message, ui_level = format_control_message(
        str(ui_state.get("action", "")),
        str(ui_state.get("message", "")),
        control,
    )
    if ui_message:
        control_banner = f"<div class='banner banner-{escape(ui_level)}'>{escape(ui_message)}</div>"
    network_state = details.get("network") or {}
    network_banner = render_network_warning_banner(network_state)
    architecture_note = (
        "L'amont pédagogique sépare désormais la planification dans <code>pix-scenario-planner</code>, "
        "la forme du trafic dans <code>pix-traffic-shaper</code> et la publication Kafka dans <code>generator</code>, "
        "avant la chaîne <code>pix-validator</code>, <code>pix-decision-engine</code> et "
        "<code>pix-outcome-publisher</code>."
    )
    metric_cards = [
        ("Pix générés", counts.get("generated", 0)),
        ("Pix en attente de résultat", counts.get("pending_result_count", 0)),
        ("Pix traités", counts.get("processed", 0)),
        ("Pix valides", counts.get("validated", 0)),
        ("Pix rejetés", counts.get("rejected", 0)),
        ("Taux de rejet", f"{ratios.get('reject_rate_percent', 0)}%"),
        ("Débit raw", f"{rates.get('topic_raw_offsets_per_second', 0)} msg/s"),
    ]
    pipeline_step_cards_html = "\n".join(
        [
            (
                "<div class='metric'>"
                "<strong>1. pix-scenario-planner</strong>"
                f"<span>plan={escape(str(scenario_planner_detail.get('plan_name', 'n/a')))}</span>"
                f"<small>scenario={escape(str(scenario_planner_detail.get('scenario_type', 'n/a')))} "
                f"phases={escape(str(scenario_planner_detail.get('phase_count', 0)))}</small>"
                "</div>"
            ),
            (
                "<div class='metric'>"
                "<strong>2. pix-traffic-shaper</strong>"
                f"<span>phase={generator_phase}</span>"
                f"<small>batches={escape(str(traffic_shaper_detail.get('batches_sent', 0)))} "
                f"demandes={escape(str(traffic_shaper_detail.get('messages_requested', 0)))}</small>"
                "</div>"
            ),
            (
                "<div class='metric'>"
                "<strong>3. generator</strong>"
                f"<span>Pix émis={escape(str(counts.get('raw_topic_end_offsets', 0)))}</span>"
                f"<small>mis en file={escape(str((details.get('generator') or {}).get('messages_enqueued', 0)))} "
                f"livrés={escape(str((details.get('generator') or {}).get('messages_delivered', 0)))}</small>"
                "</div>"
            ),
            (
                "<div class='metric'>"
                "<strong>4. pix-validator</strong>"
                f"<span>Pix contrôlés={escape(str(counts.get('checked_topic_end_offsets', 0)))}</span>"
                f"<small>reçus={escape(str((details.get('pix-validator') or {}).get('received', 0)))} "
                f"contrôlés={escape(str((details.get('pix-validator') or {}).get('checked', 0)))}</small>"
                "</div>"
            ),
        ]
    )
    pipeline_flow_html = (
        "<div class='flowline'>"
        "<div class='flow-node'>"
        "<strong>pix-scenario-planner</strong>"
        f"<span>plan={escape(str(scenario_planner_detail.get('plan_name', 'n/a')))}</span>"
        f"<small>phases={escape(str(scenario_planner_detail.get('phase_count', 0)))}</small>"
        "</div>"
        "<div class='flow-arrow'>&rarr;</div>"
        "<div class='flow-node'>"
        "<strong>pix-traffic-shaper</strong>"
        f"<span>phase={generator_phase}</span>"
        f"<small>batches={escape(str(traffic_shaper_detail.get('batches_sent', 0)))} demandés={escape(str(traffic_shaper_detail.get('messages_requested', 0)))}</small>"
        "</div>"
        "<div class='flow-arrow'>&rarr;</div>"
        "<div class='flow-node'>"
        "<strong>generator</strong>"
        f"<span>Pix émis={escape(str(counts.get('raw_topic_end_offsets', 0)))}</span>"
        f"<small>livrés={escape(str((details.get('generator') or {}).get('messages_delivered', 0)))}</small>"
        "</div>"
        "<div class='flow-arrow'>&rarr;</div>"
        "<div class='flow-node'>"
        "<strong>pix-validator</strong>"
        f"<span>Pix contrôlés={escape(str(counts.get('checked_topic_end_offsets', 0)))}</span>"
        f"<small>reçus={escape(str((details.get('pix-validator') or {}).get('received', 0)))}</small>"
        "</div>"
        "<div class='flow-arrow'>&rarr;</div>"
        "<div class='flow-node'>"
        "<strong>persisters</strong>"
        f"<span>db={escape(str(counts.get('db_validated_count', 0)))}/{escape(str(counts.get('db_rejected_count', 0)))}</span>"
        "<small>validés / rejetés en base</small>"
        "</div>"
        "</div>"
    )
    metric_cards_html = "\n".join(
        f"<div class='metric'><strong>{escape(str(label))}</strong><span>{escape(str(value))}</span></div>"
        for label, value in metric_cards
    )
    summary_cards_html = "\n".join(
        [
            f"<div class='metric compact'><strong>services ok</strong><span>{ok_services}</span></div>",
            f"<div class='metric compact'><strong>services en défaut</strong><span>{issue_services}</span></div>",
            f"<div class='metric compact'><strong>alertes actives</strong><span>{alert_count}</span></div>",
            f"<div class='metric compact'><strong>lag total</strong><span>{current_lag_total}</span></div>",
            f"<div class='metric compact'><strong>latence validation</strong><span>{escape(display_value(timings.get('avg_validation_latency_ms'), ' ms'))}</span></div>",
            f"<div class='metric compact'><strong>ancienneté backlog</strong><span>{escape(display_value(timings.get('estimated_oldest_lag_seconds'), ' s'))}</span></div>",
            f"<div class='metric compact'><strong>TTL Pix</strong><span>{escape(display_value((details.get('generator') or {}).get('decision_sla_seconds'), ' s'))}</span></div>",
            f"<div class='metric compact'><strong>PUB Kafka</strong><span>{escape(str((details.get('generator') or {}).get('producer_semantics', 'at_least_once')))}</span></div>",
            f"<div class='metric compact'><strong>SUB Kafka</strong><span>{escape(str(active_processing_detail.get('consumer_semantics', 'at_least_once')))}</span></div>",
            f"<div class='metric compact'><strong>réglage PUB</strong><span>acks={escape(str((details.get('generator') or {}).get('producer_acks', 'all')))} retries={escape(str((details.get('generator') or {}).get('producer_retries', '0')))}</span></div>",
            f"<div class='metric compact'><strong>generator</strong><span>enqueued={escape(str((details.get('generator') or {}).get('messages_enqueued', 0)))} delivered={escape(str((details.get('generator') or {}).get('messages_delivered', 0)))} failed={escape(str((details.get('generator') or {}).get('messages_delivery_failed', 0)))}</span></div>",
            f"<div class='metric compact'><strong>scenario planner</strong><span>plan={escape(str((scenario_planner_detail or {}).get('plan_name', 'n/a')))} phases={escape(str((scenario_planner_detail or {}).get('phase_count', 0)))}</span></div>",
            f"<div class='metric compact'><strong>traffic shaper</strong><span>mode={escape(str((traffic_shaper_detail or {}).get('traffic_model', 'bursty')))} batches={escape(str((traffic_shaper_detail or {}).get('batches_sent', 0)))}</span></div>",
        ]
    )
    trend_cards_html = "\n".join(
        [
            "<div class='trend-card'><strong>Lag Kafka</strong>"
            + render_sparkline(history, "lag_total", "#c46a2d")
            + f"<span>actuel={current_lag_total}</span></div>",
            "<div class='trend-card'><strong>Débit Pix émis/s</strong>"
            + render_sparkline(history, "raw_rate_per_second", "#2a7f62")
            + f"<span>actuel={escape(display_value(rates.get('topic_raw_offsets_per_second', 0), ' msg/s'))}</span></div>",
            "<div class='trend-card'><strong>Reject %</strong>"
            + render_sparkline(history, "reject_rate_percent", "#9d3c2a")
            + f"<span>actuel={escape(display_value(ratios.get('reject_rate_percent', 0), '%'))}</span></div>",
        ]
    )
    service_groups = [
        ("Génération", ["pix-scenario-planner", "pix-traffic-shaper", "generator"]),
        ("Traitement", ["pix-validator", "pix-decision-engine", "pix-outcome-publisher"]),
        ("Persistance", ["persister-valid", "persister-rejected", "postgres"]),
        ("Observabilité", ["service-health", "metrics-collector", "influxdb", "grafana"]),
        ("Middleware", ["kafka", "topic-init"]),
    ]
    service_group_cards_html = "\n".join(
        (
            "<div class='service-group'>"
            f"<h3>{escape(group_name)}</h3>"
            "<div class='services-grid'>"
            + "\n".join(
                (
                    "<div class='service-card'>"
                    f"<strong>{escape(service_name)}</strong>"
                    f"<span class='service-pill service-pill-{escape(str(services.get(service_name, 'unknown')).lower())}'>{escape(format_status_label(services.get(service_name, 'unknown')))}</span>"
                    "</div>"
                )
                for service_name in members
            )
            + "</div></div>"
        )
        for group_name, members in service_groups
    )
    return f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <meta http-equiv="refresh" content="10">
  <title>Simul-Pix Service Health</title>
  <style>
    :root {{
      --bg: #f5efe3;
      --ink: #14213d;
      --card: #fffaf0;
      --accent: #c46a2d;
      --line: #d9c9aa;
    }}
    body {{ margin: 0; font-family: "IBM Plex Sans", "Avenir Next", sans-serif; background: radial-gradient(circle at top left, #fff7ea, var(--bg)); color: var(--ink); }}
    main {{ max-width: 1080px; margin: 0 auto; padding: 32px 20px 48px; }}
    h1, h2 {{ letter-spacing: -0.04em; }}
    .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 16px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(20, 33, 61, 0.08); margin-bottom: 20px; }}
    .badge {{ display: inline-block; padding: 8px 12px; border-radius: 999px; color: white; font-weight: 800; margin-bottom: 12px; letter-spacing: 0.02em; }}
    .badge-ok {{ background: #1f8a4c; }}
    .badge-degraded {{ background: var(--accent); }}
    .badge-critical {{ background: #b42318; }}
    .topbar {{ display: grid; grid-template-columns: auto 1fr auto; align-items: center; gap: 16px; margin-bottom: 18px; }}
    .topbar-run {{ margin: 0; text-align: center; white-space: nowrap; font-weight: 600; }}
    .topbar-meta {{ margin: 0; text-align: right; white-space: nowrap; }}
    .layout {{ display: grid; grid-template-columns: minmax(0, 1.2fr) minmax(0, 0.8fr); gap: 20px; align-items: start; }}
    .column {{ min-width: 0; }}
    .metrics {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(140px, 1fr)); gap: 12px; margin-top: 12px; }}
    .metric {{ border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: rgba(255, 255, 255, 0.65); }}
    .metric.compact span {{ font-size: 1.1rem; }}
    .metric strong {{ display: block; font-size: 0.85rem; text-transform: uppercase; color: #5b6470; margin-bottom: 6px; }}
    .metric span {{ font-size: 1.3rem; font-weight: 700; }}
    .flowline {{ display: grid; grid-template-columns: repeat(6, minmax(120px, 1fr) 28px) minmax(120px, 1fr); gap: 10px; align-items: center; margin-top: 14px; }}
    .flow-node {{ border: 1px solid var(--line); border-radius: 14px; padding: 12px; background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(255,255,255,0.72)); text-align: center; }}
    .flow-node strong {{ display: block; font-size: 0.9rem; margin-bottom: 6px; }}
    .flow-node span {{ display: block; font-size: 1.15rem; font-weight: 700; margin-bottom: 6px; }}
    .flow-node small {{ display: block; color: #5b6470; font-size: 0.82rem; }}
    .flow-arrow {{ text-align: center; font-size: 1.35rem; color: var(--accent); font-weight: 800; }}
    .services-grid {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(170px, 1fr)); gap: 12px; margin-top: 12px; }}
    .service-group {{ margin-top: 14px; }}
    .service-group h3 {{ margin: 0 0 8px; font-size: 0.95rem; text-transform: uppercase; letter-spacing: 0.04em; color: #5b6470; }}
    .service-card {{ border: 1px solid var(--line); border-radius: 14px; padding: 14px; background: linear-gradient(180deg, rgba(255,255,255,0.95), rgba(255,255,255,0.72)); box-shadow: inset 0 1px 0 rgba(255,255,255,0.65); }}
    .service-card strong {{ display: block; font-size: 1rem; margin-bottom: 10px; }}
    .service-pill {{ display: inline-block; padding: 7px 12px; border-radius: 999px; font-size: 0.82rem; font-weight: 800; letter-spacing: 0.03em; text-transform: uppercase; }}
    .service-pill-ok {{ background: #e7f6ea; color: #1f8a4c; border: 1px solid #8dc59c; }}
    .service-pill-pass {{ background: #e7f6ea; color: #1f8a4c; border: 1px solid #8dc59c; }}
    .service-pill-running {{ background: #eef5ff; color: #215ea6; border: 1px solid #9dc0f3; }}
    .service-pill-degraded {{ background: #fff4d7; color: #9a6700; border: 1px solid #e1bf57; }}
    .service-pill-warning {{ background: #fff4d7; color: #9a6700; border: 1px solid #e1bf57; }}
    .service-pill-error {{ background: #fdeaea; color: #b42318; border: 1px solid #e39b9b; }}
    .service-pill-critical {{ background: #fdeaea; color: #b42318; border: 1px solid #e39b9b; }}
    .service-pill-unknown {{ background: #efefef; color: #5b6470; border: 1px solid #d6d6d6; }}
    .trends {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; margin-top: 12px; }}
    .trend-card {{ border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: rgba(255, 255, 255, 0.7); }}
    .trend-card strong {{ display: block; font-size: 0.85rem; text-transform: uppercase; color: #5b6470; margin-bottom: 8px; }}
    .trend-card span {{ display: block; margin-top: 6px; font-size: 0.9rem; font-weight: 600; }}
    .sparkline {{ width: 100%; height: 64px; display: block; }}
    .sparkline-empty {{ height: 64px; display: grid; place-items: center; color: #5b6470; font-size: 0.9rem; }}
    .banner {{ border-radius: 12px; padding: 12px 14px; margin: 12px 0; font-weight: 600; }}
    .banner-success {{ background: #e7f6ea; border: 1px solid #8dc59c; color: #1d5a2d; }}
    .banner-warning {{ background: #fff4d7; border: 1px solid #e1bf57; color: #7a5800; }}
    .banner-error {{ background: #fdeaea; border: 1px solid #e39b9b; color: #8a1f1f; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; font-size: 0.95rem; }}
    th {{ font-size: 0.85rem; text-transform: uppercase; letter-spacing: 0.04em; color: #5b6470; }}
    td {{ overflow-wrap: anywhere; word-break: break-word; }}
    code {{ font-family: "IBM Plex Mono", monospace; font-size: 0.92em; }}
    @media (max-width: 920px) {{
      .topbar {{ display: flex; flex-direction: column; align-items: flex-start; }}
      .topbar-run,
      .topbar-meta {{ text-align: left; }}
      .layout {{ grid-template-columns: 1fr; }}
      .flowline {{ grid-template-columns: 1fr; }}
      .flow-arrow {{ transform: rotate(90deg); }}
    }}
  </style>
</head>
<body>
  <main>
    <div class="topbar">
      <div class="badge {status_badge_class(payload.get('status'))}">simul-pix={escape(format_status_label(payload.get("status")))}</div>
      <p class="topbar-run">run_id=<code>{escape(str(run_state.get("run_id", "default")))}</code></p>
      <p class="topbar-meta">mise a jour=<code>{escape(str(payload.get("timestamp", "")))}</code> rafraichissement=<code>10s</code></p>
    </div>
    {network_banner}
    {control_banner}
    <div class="layout">
      <div class="column">
        <section class="card">
          <div class="metrics">{metric_cards_html}</div>
          <div class="metrics">{summary_cards_html}</div>
          {pipeline_flow_html}
          {("<div class='metrics'>" + pipeline_step_cards_html + "</div><p><strong>Chaîne pédagogique</strong> : on suit ici le plan de flux dans <code>pix-scenario-planner</code>, la forme du trafic dans <code>pix-traffic-shaper</code>, puis le parcours visible d'un Pix sur les topics <code>raw</code> (Pix émis) et <code>checked</code> (Pix contrôlés), avant la persistance.</p>") if pipeline_step_cards_html else ""}
          <p><strong>Architecture du parcours TP</strong> : {architecture_note}</p>
          <p><strong>Lecture</strong> : ces compteurs correspondent à la vue cumulée depuis le démarrage de l'application. Pour une vision plus instantanée, lire les tendances et les débits.</p>
        </section>
        <section class="card">
          <h2>Parcours courant</h2>
          <p><strong>architecture</strong>=<code>{escape(processing_arch)}</code></p>
          <p><strong>chaîne pédagogique</strong>=<code>pix-scenario-planner -&gt; pix-traffic-shaper -&gt; generator -&gt; Pix émis -&gt; Pix contrôlés</code></p>
          <p><strong>pilotage</strong>=<code>http://localhost:8083/scenario</code></p>
        </section>
        {render_scenario_entry_card()}
      </div>
      <div class="column">
        <section class="card">
          <h2>Tendances</h2>
          <div class="trends">{trend_cards_html}</div>
        </section>
        <section class="card">
          <h2>Services</h2>
          <p><strong>Groupes Docker pédagogiques</strong> : les conteneurs sont regroupés ici par rôle métier pour faciliter la lecture de la plateforme.</p>
          {service_group_cards_html}
        </section>
      </div>
    </div>
  </main>
</body>
</html>"""
