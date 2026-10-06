from html import escape
from typing import Any, Callable

from config import ADVANCED_TP_CATALOG, NETWORK_PROFILES, TP_CATALOG
from formatting import display_value
from scenario_control import grafana_dashboard_link


def network_profile_options(selected: str) -> str:
    rows = []
    for name, config in NETWORK_PROFILES.items():
        is_selected = " selected" if name == selected else ""
        rows.append(f"<option value='{escape(name)}'{is_selected}>{escape(str(config.get('title', name)))}</option>")
    return "".join(rows)


def render_scenario_html(
    payload: dict[str, Any],
    *,
    safe_int: Callable[[Any], int],
    read_form_value: Callable[[dict[str, list[str]], str, str], str],
    get_scenario_config: Callable[[str], dict[str, Any]],
    render_network_warning_banner: Callable[[dict[str, Any]], str],
    format_control_message: Callable[[str, str, dict[str, Any]], tuple[str, str]],
) -> str:
    details = payload.get("details", {})
    control = payload.get("control", {})
    ui_state = payload.get("ui", {})
    current_traffic_shaper = details.get("pix-traffic-shaper") or {}
    network_state = details.get("network") or {}
    selected_scenario = "nominal"
    scenario_config = get_scenario_config(selected_scenario)
    defaults = scenario_config.get("defaults", {})
    processing_arch = "split"
    selected_network_profile = str(ui_state.get("network_profile") or network_state.get("profile") or "kafka_latency")
    network_profile = NETWORK_PROFILES.get(selected_network_profile, NETWORK_PROFILES["kafka_latency"])
    network_defaults = network_profile.get("defaults", {})
    control_disabled = "disabled" if control.get("status") in ("running", "busy") else ""
    _ts_status = current_traffic_shaper.get("status")
    _ts_phase = current_traffic_shaper.get("current_phase")
    phase_active = _ts_phase not in (None, "idle", "completed", "n/a")
    is_running = _ts_status == "running" and phase_active
    launch_label = "Modifier le TP" if is_running else "Lancer le TP choisi"
    launch_action = "update_scenario" if is_running else "run_scenario"
    ui_message, ui_level = format_control_message(
        str(ui_state.get("action", "")),
        str(ui_state.get("message", "")),
        control,
    )
    control_banner = f"<div class='banner banner-{escape(ui_level)}'>{escape(ui_message)}</div>" if ui_message else ""
    network_banner = render_network_warning_banner(network_state)
    generator_phase = escape(str(current_traffic_shaper.get("current_phase") or "n/a"))
    phase_progress = safe_int(current_traffic_shaper.get("phase_progress"))
    _counts = (payload.get("metrics") or {}).get("counts") or {}
    _validated = safe_int(_counts.get("validated", 0))
    _rejected = safe_int(_counts.get("rejected", 0))
    if _rejected > 0:
        progress_display = f"{phase_progress} / ({_validated} + {_rejected})"
    else:
        progress_display = f"{phase_progress} / {_validated}"
    current_run_id = escape(str((payload.get("run") or {}).get("run_id", "default")))
    form_values = {
        "total_messages": read_form_value({"v": [str(ui_state.get("total_messages", defaults.get("total_messages", "100")))]}, "v", "100"),
        "rate_per_second": read_form_value({"v": [str(ui_state.get("rate_per_second", defaults.get("rate_per_second", "10")))]}, "v", "10"),
        "producer_semantics": read_form_value({"v": [str(ui_state.get("producer_semantics", defaults.get("producer_semantics", "at_least_once")))]}, "v", "at_least_once"),
        "consumer_semantics": read_form_value({"v": [str(ui_state.get("consumer_semantics", defaults.get("consumer_semantics", "at_least_once")))]}, "v", "at_least_once"),
        "consumer_commit_strategy": read_form_value({"v": [str(ui_state.get("consumer_commit_strategy", defaults.get("consumer_commit_strategy", "after")))]}, "v", "after"),
        "consumer_auto_commit": read_form_value({"v": [str(ui_state.get("consumer_auto_commit", defaults.get("consumer_auto_commit", "off")))]}, "v", "off"),
        "consumer_max_poll_interval_ms": read_form_value({"v": [str(ui_state.get("consumer_max_poll_interval_ms", defaults.get("consumer_max_poll_interval_ms", "300000")))]}, "v", "300000"),
        "consumer_commit_batch_size": read_form_value({"v": [str(ui_state.get("consumer_commit_batch_size", defaults.get("consumer_commit_batch_size", "1")))]}, "v", "1"),
        "producer_acks": read_form_value({"v": [str(ui_state.get("producer_acks", defaults.get("producer_acks", "all")))]}, "v", "all"),
        "producer_retries": read_form_value({"v": [str(ui_state.get("producer_retries", defaults.get("producer_retries", "")))]}, "v", ""),
        "decision_sla_seconds": read_form_value({"v": [str(ui_state.get("decision_sla_seconds", defaults.get("decision_sla_seconds", "10")))]}, "v", "10"),
        "validator_workers": read_form_value({"v": [str(ui_state.get("validator_workers", defaults.get("validator_workers", "1")))]}, "v", "1"),
        "decision_engine_workers": read_form_value({"v": [str(ui_state.get("decision_engine_workers", defaults.get("decision_engine_workers", "1")))]}, "v", "1"),
        "traffic_model": read_form_value({"v": [str(ui_state.get("traffic_model", defaults.get("traffic_model", "bursty")))]}, "v", "bursty"),
        "match_count": read_form_value({"v": [str(ui_state.get("match_count", defaults.get("match_count", "10")))]}, "v", "10"),
        "stadium_capacity": read_form_value({"v": [str(ui_state.get("stadium_capacity", defaults.get("stadium_capacity", "44000")))]}, "v", "44000"),
        "pix_usage_rate": read_form_value({"v": [str(ui_state.get("pix_usage_rate", defaults.get("pix_usage_rate", "0.05")))]}, "v", "0.05"),
        "peak_share": read_form_value({"v": [str(ui_state.get("peak_share", defaults.get("peak_share", "0.30")))]}, "v", "0.30"),
        "peak_window_minutes": read_form_value({"v": [str(ui_state.get("peak_window_minutes", defaults.get("peak_window_minutes", "15")))]}, "v", "15"),
        "total_window_minutes": read_form_value({"v": [str(ui_state.get("total_window_minutes", defaults.get("total_window_minutes", "240")))]}, "v", "240"),
        "time_compression_factor": read_form_value({"v": [str(ui_state.get("time_compression_factor", defaults.get("time_compression_factor", "10")))]}, "v", "10"),
    }
    network_values = {
        "target_service": read_form_value({"v": [str(ui_state.get("network_target_service", network_state.get("target_service") or network_defaults.get("target_service", "pix-decision-engine")))]}, "v", "pix-decision-engine"),
        "delay_ms": read_form_value({"v": [str(ui_state.get("network_delay_ms", network_state.get("delay_ms") if safe_int(network_state.get("active")) else network_defaults.get("delay_ms", "250")))]}, "v", "250"),
        "jitter_ms": read_form_value({"v": [str(ui_state.get("network_jitter_ms", network_state.get("jitter_ms") if safe_int(network_state.get("active")) else network_defaults.get("jitter_ms", "40")))]}, "v", "40"),
        "loss_percent": read_form_value({"v": [str(ui_state.get("network_loss_percent", network_state.get("loss_percent") if safe_int(network_state.get("active")) else network_defaults.get("loss_percent", "0")))]}, "v", "0"),
        "rate_kbit": read_form_value({"v": [str(ui_state.get("network_rate_kbit", network_state.get("rate_kbit") if safe_int(network_state.get("active")) else network_defaults.get("rate_kbit", "0")))]}, "v", "0"),
    }
    parameter_help_html = ""
    football_fields = ""
    producer_tuning_disabled = "disabled" if form_values["producer_semantics"] != "custom" else ""
    consumer_tuning_disabled = "disabled" if form_values["consumer_semantics"] != "custom" else ""
    producer_semantics_label = {
        "at_most_once": "at most once",
        "at_least_once": "at least once",
        "exactly_once": "exactly once",
        "custom": "custom",
    }.get(form_values["producer_semantics"], form_values["producer_semantics"])
    consumer_semantics_label = {
        "at_most_once": "at most once",
        "at_least_once": "at least once",
        "exactly_once_kafka": "exactly once Kafka",
        "custom": "custom",
    }.get(form_values["consumer_semantics"], form_values["consumer_semantics"])
    if form_values["producer_semantics"] == "at_most_once":
        producer_consequence = "publication sans attente d'ack durable, pertes possibles en cas d'incident, débit maximal"
    elif form_values["producer_semantics"] == "at_least_once":
        producer_consequence = "publication confirmée avec retries, livraison renforcée mais doublons possibles après incident"
    elif form_values["producer_semantics"] == "exactly_once":
        producer_consequence = "publication Kafka idempotente, doublons évités sur Kafka, coût de coordination plus élevé"
    else:
        producer_consequence = (
            f"réglage libre avec acks={form_values['producer_acks']} et retries={form_values['producer_retries'] or '0'}, "
            "comportement dépendant de cette combinaison"
        )
    if form_values["consumer_semantics"] == "at_most_once":
        consumer_consequence = "commit avant traitement, perte possible si le service tombe après lecture"
    elif form_values["consumer_semantics"] == "at_least_once":
        consumer_consequence = "commit après traitement, rejeu possible après incident donc risque de doublon"
    elif form_values["consumer_semantics"] == "exactly_once_kafka":
        consumer_consequence = "transaction Kafka lecture-publication-offset, doublons évités sur les topics Kafka, PostgreSQL hors périmètre"
    else:
        consumer_consequence = (
            f"réglage libre : commit {form_values['consumer_commit_strategy']}, auto-commit {form_values['consumer_auto_commit']}, "
            f"max poll {form_values['consumer_max_poll_interval_ms']} ms, batch {form_values['consumer_commit_batch_size']}"
        )
    def render_tp_cards(catalog: list[dict[str, str]]) -> str:
        return "".join(
            (
                "<button type='button' class='tp-card tp-choice'"
                f" data-scenario='{escape(tp.get('scenario', 'nominal'), quote=True)}'"
                f" data-total-messages='{escape(tp.get('total_messages', ''), quote=True)}'"
                f" data-rate-per-second='{escape(tp.get('rate_per_second', ''), quote=True)}'"
                f" data-traffic-model='{escape(tp.get('traffic_model', 'poisson'), quote=True)}'"
                f" data-producer-semantics='{escape(tp.get('producer_semantics', 'at_least_once'), quote=True)}'"
                f" data-consumer-semantics='{escape(tp.get('consumer_semantics', 'at_least_once'), quote=True)}'"
                f" data-decision-sla-seconds='{escape(tp.get('decision_sla_seconds', '10'), quote=True)}'"
                f" data-validator-workers='{escape(tp.get('validator_workers', '1'), quote=True)}'"
                f" data-decision-engine-workers='{escape(tp.get('decision_engine_workers', '1'), quote=True)}'>"
                f"<h3>{escape(tp['title'])}</h3>"
                f"<p>{escape(tp['summary'])}</p>"
                f"<code>{escape(tp['guide_path'])}</code>"
                "</button>"
            )
            for tp in catalog
        )

    basic_tp_cards_html = render_tp_cards([tp for tp in TP_CATALOG if tp.get("category") == "basic"])
    semantic_tp_cards_html = render_tp_cards([tp for tp in TP_CATALOG if tp.get("category") == "semantic"])
    advanced_tp_cards_html = "".join(
        (
            "<article class='tp-card compact'>"
            f"<h3>{escape(tp['title'])}</h3>"
            f"<p>{escape(tp['summary'])}</p>"
            f"<code>{escape(tp['guide_path'])}</code>"
            "</article>"
        )
        for tp in ADVANCED_TP_CATALOG
    )
    return f"""<!doctype html>
<html lang="fr">
<head>
  <meta charset="utf-8">
  <meta name="viewport" content="width=device-width, initial-scale=1">
  <title>Parcours Kafka Simul-Pix</title>
  <style>
    :root {{ --bg: #f5efe3; --ink: #14213d; --card: #fffaf0; --accent: #c46a2d; --line: #d9c9aa; }}
    body {{ margin: 0; font-family: "IBM Plex Sans", "Avenir Next", sans-serif; background: radial-gradient(circle at top left, #fff7ea, var(--bg)); color: var(--ink); }}
    main {{ max-width: 1360px; margin: 0 auto; padding: 28px 20px 40px; }}
    .card {{ background: var(--card); border: 1px solid var(--line); border-radius: 16px; padding: 18px 20px; box-shadow: 0 10px 30px rgba(20, 33, 61, 0.08); margin-bottom: 20px; }}
    .banner {{ border-radius: 12px; padding: 12px 14px; margin: 12px 0; font-weight: 600; }}
    .banner-success {{ background: #e7f6ea; border: 1px solid #8dc59c; color: #1d5a2d; }}
    .banner-warning {{ background: #fff4d7; border: 1px solid #e1bf57; color: #7a5800; }}
    .banner-error {{ background: #fdeaea; border: 1px solid #e39b9b; color: #8a1f1f; }}
    .topline {{ display: grid; grid-template-columns: 1fr auto; gap: 16px; align-items: center; }}
    .quicklinks {{ display: flex; flex-wrap: wrap; gap: 10px; margin-top: 12px; }}
    .quicklinks a, button {{ border: 1px solid var(--line); background: white; color: var(--ink); border-radius: 10px; padding: 10px 12px; text-decoration: none; font: inherit; cursor: pointer; }}
    table {{ width: 100%; border-collapse: collapse; margin-top: 12px; }}
    th, td {{ text-align: left; padding: 10px 8px; border-bottom: 1px solid var(--line); vertical-align: top; }}
    ul {{ margin: 10px 0 0 18px; padding: 0; }}
    .workspace {{ display: grid; grid-template-columns: minmax(0, 1fr) minmax(0, 1fr); gap: 20px; align-items: start; }}
    .grid {{ display: grid; grid-template-columns: minmax(0, 1.15fr) minmax(0, 0.85fr); gap: 20px; }}
    .meta {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(180px, 1fr)); gap: 12px; }}
    .mini {{ border: 1px solid var(--line); border-radius: 12px; padding: 12px; background: rgba(255,255,255,0.7); }}
    .compact-note {{ margin-top: 12px; padding-top: 12px; border-top: 1px solid var(--line); font-size: 0.95rem; }}
    .compact-note p {{ margin: 6px 0; }}
    .tp-list {{ display: grid; grid-template-columns: repeat(auto-fit, minmax(240px, 1fr)); gap: 12px; margin-top: 14px; }}
    .tp-card {{ border: 1px solid var(--line); background: rgba(255,255,255,0.72); border-radius: 12px; padding: 12px; }}
    .tp-choice {{ width: 100%; text-align: left; cursor: pointer; }}
    .tp-choice:hover, .tp-choice.is-selected {{ border-color: var(--accent); box-shadow: 0 0 0 2px rgba(196, 106, 45, 0.18); }}
    .tp-choice.is-selected {{ background: #fff4d7; }}
    .tp-card h3 {{ margin: 0 0 8px; font-size: 1rem; }}
    .tp-card p {{ margin: 0 0 10px; color: #53493c; line-height: 1.45; }}
    .tp-card code {{ display: block; white-space: normal; overflow-wrap: anywhere; font-size: 0.82rem; }}
    .tp-card.compact h3 {{ font-size: 0.95rem; }}
    .tp-section {{ margin-top: 18px; }}
    .tp-section:first-of-type {{ margin-top: 12px; }}
    .tp-section h3 {{ margin: 0; font-size: 1.05rem; }}
    .tp-section > p {{ margin: 6px 0 0; color: #53493c; line-height: 1.45; }}
    .tp-panel .tp-list {{ grid-template-columns: repeat(2, minmax(0, 1fr)); }}
    .tp-panel > p {{ color: #53493c; line-height: 1.45; }}
    details.advanced {{ margin-top: 18px; border: 1px solid var(--line); border-radius: 14px; background: rgba(255,255,255,0.55); }}
    details.advanced[open] {{ background: rgba(255,255,255,0.8); }}
    details.advanced > summary {{ cursor: pointer; padding: 14px 16px; font-weight: 700; }}
    details.advanced > summary::marker {{ color: var(--accent); }}
    .advanced-body {{ padding: 0 16px 16px; }}
    .advanced-intro {{ margin: 0 0 10px; color: #53493c; }}
    select:disabled, input:disabled {{ background: #f1ede4; color: #7b7468; cursor: not-allowed; }}
    code {{ font-family: "IBM Plex Mono", monospace; }}
    @media (max-width: 1120px) {{ .workspace {{ grid-template-columns: 1fr; }} }}
    @media (max-width: 920px) {{ .grid, .topline {{ grid-template-columns: 1fr; }} }}
    @media (max-width: 720px) {{ .tp-panel .tp-list {{ grid-template-columns: 1fr; }} }}
    .info-tip {{ position: relative; display: inline-block; margin-left: 6px; vertical-align: middle; cursor: help; }}
    .info-tip .tip-icon {{ display: inline-flex; align-items: center; justify-content: center; width: 16px; height: 16px; border-radius: 50%; background: var(--accent); color: white; font-size: 11px; font-weight: 700; line-height: 1; }}
    .info-tip .tip-content {{ display: none; position: absolute; left: 22px; top: -6px; width: 340px; background: var(--ink); color: #f5efe3; padding: 12px 14px; border-radius: 10px; font-size: 0.85rem; line-height: 1.55; z-index: 100; box-shadow: 0 4px 20px rgba(0,0,0,0.25); font-weight: 400; }}
    .info-tip:hover .tip-content {{ display: block; }}
  </style>
</head>
<body>
  <main>
    <section class="card">
      <div class="topline">
        <div>
          <h1>Parcours Kafka Simul-Pix</h1>
          <p>Suivre les TP Kafka / PUB-SUB, piloter le flux Pix et observer les effets sur les topics, groupes consommateurs et dashboards.</p>
        </div>
      </div>
      <div class="quicklinks">
        <a href="/">Retour à la météo</a>
        <a href="{escape(grafana_dashboard_link('simulpix-pipeline', 'simul-pix-general-dashboard'))}" target="_blank" rel="noopener noreferrer">General dashboard</a>
        <a href="{escape(grafana_dashboard_link('simulpix-kafka', 'simul-pix-kafka-dashboard'))}" target="_blank" rel="noopener noreferrer">Kafka dashboard</a>
        <a href="{escape(grafana_dashboard_link('simulpix-persistence', 'simul-pix-persistence-dashboard'))}" target="_blank" rel="noopener noreferrer">Persistence dashboard</a>
        <a href="{escape(grafana_dashboard_link('simulpix-incidents', 'simul-pix-incidents'))}" target="_blank" rel="noopener noreferrer">Incidents</a>
      </div>
    </section>
    {network_banner}
    <div class="workspace">
      <div class="control-area">
        <div class="grid">
          <section class="card">
            <h2>Paramètres du flux TP</h2>
            {control_banner}
            <form id="main-scenario-form" method="post" action="/control">
          <input type="hidden" id="js-launch-action" name="action" value="{launch_action}">
          <input type="hidden" name="scenario" value="{escape(selected_scenario)}">
          <input type="hidden" name="return_view" value="scenario">
          <table><tbody>
            <tr><td>Total messages<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Nombre total de Pix à émettre sur la durée du scénario. Laissé vide = flux infini jusqu'au clic sur Stop.<br><br>Ce paramètre détermine la taille observable de la fenêtre de données dans Grafana et le temps estimé de complétion selon le débit choisi.</span></span></td><td><input type="number" name="total_messages" value="{escape(form_values['total_messages'])}" placeholder="vide = illimite"></td></tr>
            <tr><td>Débit / seconde<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Nombre cible de Pix générés par seconde. Laissé vide = pas de limitation, le pipeline tourne à vitesse maximale.<br><br>Ce paramètre interagit avec le mode trafic : en <strong>linear</strong> il est respecté à la seconde près ; en <strong>Poisson</strong> et <strong>bursty</strong> il est la moyenne cible autour de laquelle les valeurs fluctuent.</span></span></td><td><input type="number" name="rate_per_second" value="{escape(form_values['rate_per_second'])}" placeholder="vide = sans limite"></td></tr>
            <tr><td>Mode trafic<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content"><strong>linear</strong> — débit fixe et régulier, N messages par seconde exactement. Idéal pour une première lecture du pipeline.<br><br><strong>Poisson</strong> — tirage stochastique autour de la cible. Fluctuations naturelles d'une seconde à l'autre, moyenne respectée sur la durée.<br><br><strong>bursty</strong> — comme Poisson, avec en plus des pics soudains et des creux temporaires. Le plus réaliste : sorties de stade, paiements groupés, reprises après accalmie.</span></span></td><td><select name="traffic_model"><option value="linear"{' selected' if form_values['traffic_model']=='linear' else ''}>linear</option><option value="poisson"{' selected' if form_values['traffic_model']=='poisson' else ''}>Poisson</option><option value="bursty"{' selected' if form_values['traffic_model']=='bursty' else ''}>bursty</option></select></td></tr>
            <tr><td>Sémantique PUB Kafka<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content"><strong>At most once</strong> — acks=0, aucun accusé de réception. Débit maximal, perte possible si un broker tombe.<br><br><strong>At least once</strong> — acks=all + retries. Aucune perte garantie, mais un retry peut produire un doublon en cas de panne entre envoi et accusé.<br><br><strong>Exactly once</strong> — producteur transactionnel Kafka. Ni perte ni doublon, au prix d'une latence et d'une complexité accrues.</span></span></td><td><select name="producer_semantics"><option value="at_most_once"{' selected' if form_values['producer_semantics']=='at_most_once' else ''}>At most once</option><option value="at_least_once"{' selected' if form_values['producer_semantics']=='at_least_once' else ''}>At least once</option><option value="exactly_once"{' selected' if form_values['producer_semantics']=='exactly_once' else ''}>Exactly once (publication Kafka)</option><option value="custom"{' selected' if form_values['producer_semantics']=='custom' else ''}>Custom</option></select></td></tr>
            <tr><td>Sémantique SUB Kafka<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content"><strong>At most once</strong> — commit avant traitement. Si le service plante après le commit, le message est perdu définitivement.<br><br><strong>At least once</strong> — commit après traitement. En cas de crash entre traitement et commit, le message est relu au redémarrage — risque de doublon.<br><br><strong>Exactly once Kafka</strong> — l'offset est commité dans la même transaction Kafka que la publication. Ni perte ni doublon, même sous incident réseau.</span></span></td><td><select name="consumer_semantics"><option value="at_most_once"{' selected' if form_values['consumer_semantics']=='at_most_once' else ''}>At most once</option><option value="at_least_once"{' selected' if form_values['consumer_semantics']=='at_least_once' else ''}>At least once</option><option value="exactly_once_kafka"{' selected' if form_values['consumer_semantics']=='exactly_once_kafka' else ''}>Exactly once Kafka (topics Kafka)</option><option value="custom"{' selected' if form_values['consumer_semantics']=='custom' else ''}>Custom</option></select></td></tr>
            <tr><td>TTL du Pix<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Durée de vie maximale d'un Pix dans le pipeline, en secondes. Si la décision (valide/rejeté) n'est pas rendue avant cette échéance, le Pix est classé <strong>hors SLA</strong>.<br><br>Réduire le TTL avec un débit élevé ou une perturbation réseau active révèle rapidement des messages hors SLA dans le dashboard — bonne démonstration de l'effet du lag Kafka sur la qualité de service.</span></span></td><td><input type="number" name="decision_sla_seconds" value="{escape(form_values['decision_sla_seconds'])}" min="3" max="3600"></td></tr>
            {football_fields}
          </tbody></table>
          <details class="advanced">
            <summary>Réglages avancés</summary>
            <div class="advanced-body">
              <p class="advanced-intro">Workers Kafka et réglages fins des commits, acks et retries pour les essais de saturation ou de dégradation.</p>
              <table><tbody>
                <tr><td>Workers pix-validator<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Nombre de threads consommateurs parallèles sur pix-validator. Augmenter permet d'absorber plus de messages mais introduit de la concurrence — l'ordre des messages n'est garanti qu'au sein d'une même partition.</span></span></td><td><input type="number" name="validator_workers" value="{escape(form_values['validator_workers'])}" min="1" max="64" {control_disabled}></td></tr>
                <tr><td>Workers pix-decision-engine<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Nombre de threads consommateurs parallèles sur pix-decision-engine. Même logique que pour pix-validator — illustre l'effet du parallélisme sur le débit de décision métier.</span></span></td><td><input type="number" name="decision_engine_workers" value="{escape(form_values['decision_engine_workers'])}" min="1" max="64" {control_disabled}></td></tr>
                <tr><td>Commit offset<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content"><strong>avant traitement</strong> — l'offset est validé avant de traiter le message. Si le service plante ensuite, le message est perdu → at-most-once.<br><br><strong>après traitement</strong> — l'offset est validé après succès. En cas de crash entre les deux, le message est relu → at-least-once, risque de doublon.</span></span></td><td><select name="consumer_commit_strategy" {consumer_tuning_disabled}><option value="before"{' selected' if form_values['consumer_commit_strategy']=='before' else ''}>avant traitement</option><option value="after"{' selected' if form_values['consumer_commit_strategy']=='after' else ''}>après traitement</option></select></td></tr>
                <tr><td>Auto commit<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content"><strong>off</strong> — le commit d'offset est explicite dans le code. Nécessaire pour at-least-once et exactly-once. C'est le mode par défaut dans Simul-Pix.<br><br><strong>on</strong> — Kafka commite automatiquement à intervalle régulier, sans lien avec le succès du traitement. Simplifie le code mais rend la sémantique incontrôlable.</span></span></td><td><select name="consumer_auto_commit" {consumer_tuning_disabled}><option value="off"{' selected' if form_values['consumer_auto_commit']=='off' else ''}>off</option><option value="on"{' selected' if form_values['consumer_auto_commit']=='on' else ''}>on</option></select></td></tr>
                <tr><td>Max poll interval ms<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Délai maximum entre deux appels poll() vers Kafka. Si le traitement d'un batch dépasse ce délai, Kafka considère le consommateur comme mort et déclenche un rebalancing de partition — visible dans le dashboard Kafka.</span></span></td><td><input type="number" name="consumer_max_poll_interval_ms" value="{escape(form_values['consumer_max_poll_interval_ms'])}" min="1000" max="3600000" {consumer_tuning_disabled}></td></tr>
                <tr><td>Batch de commit<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Nombre de messages traités avant de commiter l'offset. 1 = commit message par message (sûr, plus lent). N > 1 = commit groupé (plus rapide, mais en cas de crash N messages peuvent être rejoués).</span></span></td><td><input type="number" name="consumer_commit_batch_size" value="{escape(form_values['consumer_commit_batch_size'])}" min="1" max="1000000" {consumer_tuning_disabled}></td></tr>
                <tr><td>Acks Kafka<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content"><strong>0</strong> — aucun accusé. Débit maximal, perte possible → at-most-once.<br><br><strong>1</strong> — accusé du leader uniquement. Perte possible si le leader tombe avant réplication.<br><br><strong>all</strong> — accusé de tous les réplicas en ISR. Aucune perte garantie → at-least-once ou exactly-once selon les retries.</span></span></td><td><select name="producer_acks" {producer_tuning_disabled}><option value="0"{' selected' if form_values['producer_acks']=='0' else ''}>0</option><option value="1"{' selected' if form_values['producer_acks']=='1' else ''}>1</option><option value="all"{' selected' if form_values['producer_acks']=='all' else ''}>all</option></select></td></tr>
                <tr><td>Retries Kafka<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Nombre de tentatives de renvoi automatique en cas d'erreur transitoire (leader absent, timeout réseau). 0 = aucun retry → at-most-once. N > 0 avec idempotence désactivée → risque de doublon. N > 0 avec <code>enable.idempotence=true</code> (activé par exactly-once) → retry sûr.</span></span></td><td><input type="number" name="producer_retries" value="{escape(form_values['producer_retries'])}" min="0" placeholder="vide = 0" {producer_tuning_disabled}></td></tr>
              </tbody></table>
            </div>
          </details>
            </form>
            <div style="display:flex; gap:10px; padding-top:14px;">
              <button type="submit" form="main-scenario-form" id="js-launch-btn" style="font-weight:700; flex:1;">{launch_label}</button>
              <form method="post" action="/control" style="flex:1;">
                <input type="hidden" name="action" value="stop_platform">
                <input type="hidden" name="return_view" value="scenario">
                <button type="submit" style="font-weight:700; width:100%;">Arrêt général</button>
              </form>
            </div>
          </section>
          <section class="card">
            <h2>État courant</h2>
            <div class="meta">
              <div class="mini"><strong>Phase traffic shaper<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Phase en cours du traffic shaper, le composant qui cadence l'émission des Pix.<br><br><strong>idle</strong> — service démarré, en attente d'un scénario.<br><strong>flux nominal</strong> — scénario standard à débit unique en cours.<br><strong>baseline</strong> — phase de débit de fond (scénario football_match_peak).<br><strong>peak</strong> — phase de pic de trafic (scénario football_match_peak).<br><strong>completed</strong> — toutes les phases terminées.</span></span></strong><br><code id="js-phase">{generator_phase}</code></div>
              <div class="mini"><strong>Progression<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Pix émis par le traffic shaper sur Pix traités par le pipeline.<br><br>Format : <strong>émis / validés</strong> ou <strong>émis / (validés + rejetés)</strong> quand il y a des rejets.<br><br>Un écart entre les deux chiffres indique du lag Kafka : des Pix ont été émis mais pas encore décidés.</span></span></strong><br><code id="js-progress">{progress_display}</code></div>
            </div>
            <div class="compact-note">
              <p><strong><code>at most once</code></strong> = risque de perte</p>
              <p><strong><code>at least once</code></strong> = risque de doublon</p>
              <p><strong><code>exactly once Kafka</code></strong> = ni doublon logique ni perte logique sur la chaîne Kafka, hors PostgreSQL</p>
              <p id="producer-consequence-line"><strong>PUB choisi</strong> : <code id="producer-semantics-label">{escape(producer_semantics_label)}</code> = <span id="producer-consequence-text">{escape(producer_consequence)}</span></p>
              <p id="consumer-consequence-line"><strong>SUB choisi</strong> : <code id="consumer-semantics-label">{escape(consumer_semantics_label)}</code> = <span id="consumer-consequence-text">{escape(consumer_consequence)}</span></p>
            </div>
          </section>
        </div>
      </div>
      <aside class="card tp-panel">
        <h2>Parcours TP</h2>
        <p>Cliquez sur un TP pour injecter ses paramètres dans le formulaire, puis lancez le TP choisi.</p>
        <section class="tp-section">
          <h3>TP basiques — 1 à 10</h3>
          <p>Vous construisez les bases : architecture, topics, groupes, partitions, offsets, DLQ, observabilité et charge.</p>
          <div class="tp-list">
            {basic_tp_cards_html}
          </div>
        </section>
        <section class="tp-section">
          <h3>TP sémantiques — 11 à 13</h3>
          <p>Vous provoquez puis mesurez les pertes, les doublons et la protection exactly-once Kafka.</p>
          <div class="tp-list">
            {semantic_tp_cards_html}
          </div>
        </section>
        <details class="advanced">
          <summary>Pour aller plus loin</summary>
          <div class="advanced-body">
            <p class="advanced-intro">Les anciennes fiches sont conservées comme atelier avancé : incidents, charge, perturbations réseau et sémantiques Kafka.</p>
            <div class="tp-list">
              {advanced_tp_cards_html}
            </div>
          </div>
        </details>
      </aside>
    </div>
    <details class="advanced">
      <summary>Réglages avancés réseau</summary>
      <div class="advanced-body">
        <p class="advanced-intro">Perturbations réseau réversibles pour forcer du lag, des retries ou des expirations TTL sur un service cible.</p>
        <div class="grid">
          <section class="card">
            <h2>Perturbation réseau Kafka</h2>
            <p>Cette action applique une perturbation réseau réversible sur le service cible afin d'observer les effets des sémantiques de lecture et d'écriture Kafka en contexte dégradé.</p>
            <p><strong>Repère avancé</strong> : injecter la perturbation pendant un flux encore actif. Sur la validation terrain, un flux nominal <code>400 / 10</code> puis <code>kafka_latency</code> sur <code>pix-decision-engine</code> ont fait monter le lag total au-delà de <code>200</code> avec apparition de <code>processing_timeout</code>.</p>
            <p><strong>Sécurité</strong> : cette perturbation n'est appliquée que si la confirmation explicite est cochée ci-dessous. Un lancement de scénario ne l'active jamais.</p>
            <form method="post" action="/control">
              <input type="hidden" name="action" value="apply_network_profile">
              <input type="hidden" name="return_view" value="scenario">
              <table><tbody>
                <tr><td>Profil<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Preset de perturbation réseau prédéfini.<br><br><strong>kafka_latency</strong> — ajoute 300 ms de latence + 50 ms de jitter sur le lien réseau du service cible : simule un réseau surchargé.<br><br><strong>kafka_loss</strong> — introduit 10 % de perte de paquets : force les retries et révèle la sémantique PUB choisie.<br><br><strong>kafka_throttle</strong> — bride le débit à 100 kbit/s : ralentit les fetchs et grossit le lag visible dans Grafana.</span></span></td><td><select name="network_profile">{network_profile_options(selected_network_profile)}</select></td></tr>
                <tr><td>Service cible<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Microservice sur lequel la perturbation réseau est injectée via <code>tc qdisc</code> (Traffic Control Linux). Choisir un service au milieu de la chaîne (ex. <code>pix-decision-engine</code>) permet d'observer l'effet sur le lag consumer des topics en aval sans interrompre la production en amont.</span></span></td><td><select name="network_target_service"><option value="pix-scenario-planner"{' selected' if network_values['target_service'] == 'pix-scenario-planner' else ''}>pix-scenario-planner</option><option value="pix-traffic-shaper"{' selected' if network_values['target_service'] == 'pix-traffic-shaper' else ''}>pix-traffic-shaper</option><option value="generator"{' selected' if network_values['target_service'] == 'generator' else ''}>generator</option><option value="pix-validator"{' selected' if network_values['target_service'] == 'pix-validator' else ''}>pix-validator</option><option value="pix-decision-engine"{' selected' if network_values['target_service'] == 'pix-decision-engine' else ''}>pix-decision-engine</option><option value="pix-outcome-publisher"{' selected' if network_values['target_service'] == 'pix-outcome-publisher' else ''}>pix-outcome-publisher</option><option value="persister-valid"{' selected' if network_values['target_service'] == 'persister-valid' else ''}>persister-valid</option><option value="persister-rejected"{' selected' if network_values['target_service'] == 'persister-rejected' else ''}>persister-rejected</option></select></td></tr>
                <tr><td>Delay ms<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Latence artificielle ajoutée à chaque paquet sortant du service cible. Allonge le round-trip Kafka : le producer attend plus longtemps l'ack, le consumer tarde à recevoir ses messages. Visible sur le panel <em>Lag consumer</em> du dashboard Kafka.</span></span></td><td><input type="number" name="network_delay_ms" value="{escape(network_values['delay_ms'])}" min="0" max="5000"></td></tr>
                <tr><td>Jitter ms<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Variation aléatoire (±) autour du délai de base. Un jitter élevé crée une latence irrégulière qui peut déclencher des timeouts même si la latence moyenne reste faible — utile pour illustrer pourquoi <code>max.poll.interval.ms</code> doit tenir compte des pics et pas seulement de la moyenne.</span></span></td><td><input type="number" name="network_jitter_ms" value="{escape(network_values['jitter_ms'])}" min="0" max="5000"></td></tr>
                <tr><td>Loss %<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Probabilité de perte de paquets sur le lien du service cible. Même 5 % de perte force les retries TCP/Kafka et peut faire dépasser le délai d'ack. Avec <strong>acks=all</strong> + retries=0, une perte déclenche directement une exception côté producer → at-most-once visible immédiatement.</span></span></td><td><input type="text" name="network_loss_percent" value="{escape(network_values['loss_percent'])}"></td></tr>
                <tr><td>Rate kbit<span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Débit réseau maximal (en kbit/s) autorisé sur le lien du service cible. Brider à 100 kbit/s simule un lien saturé : les batchs Kafka grossissent en attente d'envoi, le lag consumer monte progressivement — bonne démonstration de l'effet du débit sur la fraîcheur des données.</span></span></td><td><input type="number" name="network_rate_kbit" value="{escape(network_values['rate_kbit'])}" min="0" max="1000000"></td></tr>
                <tr style="background:#fff4d7; border-radius:8px;"><td><strong>Confirmation obligatoire</strong><span class="info-tip"><span class="tip-icon">i</span><span class="tip-content">Garde-fou obligatoire. La perturbation réseau est une action destructive réversible mais immédiatement visible pendant le parcours TP. Cette case force une intention explicite et évite les activations accidentelles lors d'un démarrage de scénario.</span></span></td><td><label style="font-weight:600; cursor:pointer;"><input type="checkbox" name="confirm_network_apply" value="yes"> ✓ je confirme l'application de cette perturbation réseau</label></td></tr>
                <tr><td colspan="2"><button type="submit">Appliquer la perturbation</button></td></tr>
              </tbody></table>
            </form>
            <form method="post" action="/control">
              <input type="hidden" name="action" value="reset_network_profile">
              <input type="hidden" name="return_view" value="scenario">
              <p><button type="submit">Supprimer la perturbation</button></p>
            </form>
          </section>
          <section class="card">
            <h2>État réseau courant</h2>
            <div class="meta">
              <div class="mini"><strong>Actif</strong><br><code>{'oui' if safe_int(network_state.get('active')) else 'non'}</code></div>
              <div class="mini"><strong>Profil</strong><br><code>{escape(str(network_state.get('profile', 'none')))}</code></div>
              <div class="mini"><strong>Cible</strong><br><code>{escape(str(network_state.get('target_service', 'none')))}</code></div>
              <div class="mini"><strong>Appliqué le</strong><br><code>{escape(display_value(network_state.get('applied_at')))}</code></div>
            </div>
            <table>
              <thead><tr><th>Paramètre</th><th>Valeur</th><th>Lecture pédagogique</th></tr></thead>
              <tbody>
                <tr><td>Delay</td><td><code>{escape(display_value(network_state.get('delay_ms'), ' ms'))}</code></td><td>Retard ajouté à chaque échange réseau sortant du service cible.</td></tr>
                <tr><td>Jitter</td><td><code>{escape(display_value(network_state.get('jitter_ms'), ' ms'))}</code></td><td>Variation aléatoire autour de la latence moyenne.</td></tr>
                <tr><td>Loss</td><td><code>{escape(display_value(network_state.get('loss_percent'), ' %'))}</code></td><td>Probabilité de perte de paquets, utile pour discuter retries et acknowledgements.</td></tr>
                <tr><td>Rate</td><td><code>{escape(display_value(network_state.get('rate_kbit'), ' kbit'))}</code></td><td>Débit réseau maximal autorisé sur le lien du service cible.</td></tr>
              </tbody>
            </table>
            <div class="quicklinks">
              <a href="{escape(grafana_dashboard_link('simulpix-kafka', 'simul-pix-kafka-dashboard'))}" target="_blank" rel="noopener noreferrer">Kafka dashboard</a>
              <a href="{escape(grafana_dashboard_link('simulpix-incidents', 'simul-pix-incidents'))}" target="_blank" rel="noopener noreferrer">Incidents Kafka</a>
            </div>
          </section>
        </div>
      </div>
    </details>
    {parameter_help_html}
  </main>
  <script>
    (() => {{
      const semantics = document.querySelector('select[name="producer_semantics"]');
      const consumerSemantics = document.querySelector('select[name="consumer_semantics"]');
      const acks = document.querySelector('select[name="producer_acks"]');
      const retries = document.querySelector('input[name="producer_retries"]');
      const consumerCommitStrategy = document.querySelector('select[name="consumer_commit_strategy"]');
      const consumerAutoCommit = document.querySelector('select[name="consumer_auto_commit"]');
      const consumerMaxPollInterval = document.querySelector('input[name="consumer_max_poll_interval_ms"]');
      const consumerCommitBatch = document.querySelector('input[name="consumer_commit_batch_size"]');
      const producerSemanticsLabel = document.getElementById('producer-semantics-label');
      const producerConsequenceText = document.getElementById('producer-consequence-text');
      const consumerSemanticsLabel = document.getElementById('consumer-semantics-label');
      const consumerConsequenceText = document.getElementById('consumer-consequence-text');
      const tpChoices = document.querySelectorAll('.tp-choice');
      const scenarioInput = document.querySelector('input[name="scenario"]');
      if (!semantics || !acks || !retries || !consumerSemantics || !consumerCommitStrategy || !consumerAutoCommit || !consumerMaxPollInterval || !consumerCommitBatch || !producerSemanticsLabel || !producerConsequenceText || !consumerSemanticsLabel || !consumerConsequenceText) {{
        return;
      }}
      const producerLabels = {{
        at_most_once: 'at most once',
        at_least_once: 'at least once',
        exactly_once: 'exactly once',
        custom: 'custom',
      }};
      const consumerLabels = {{
        at_most_once: 'at most once',
        at_least_once: 'at least once',
        exactly_once_kafka: 'exactly once Kafka',
        custom: 'custom',
      }};
      const getProducerConsequence = () => {{
        if (semantics.value === 'at_most_once') {{
          return "publication sans attente d'ack durable, pertes possibles en cas d'incident, débit maximal";
        }}
        if (semantics.value === 'at_least_once') {{
          return "publication confirmée avec retries, livraison renforcée mais doublons possibles après incident";
        }}
        if (semantics.value === 'exactly_once') {{
          return "publication Kafka idempotente, doublons évités sur Kafka, coût de coordination plus élevé";
        }}
        return 'réglage libre avec acks=' + acks.value + ' et retries=' + (retries.value || '0') + ', comportement dépendant de cette combinaison';
      }};
      const getConsumerConsequence = () => {{
        if (consumerSemantics.value === 'at_most_once') {{
          return 'commit avant traitement, perte possible si le service tombe après lecture';
        }}
        if (consumerSemantics.value === 'at_least_once') {{
          return 'commit après traitement, rejeu possible après incident donc risque de doublon';
        }}
        if (consumerSemantics.value === 'exactly_once_kafka') {{
          return 'transaction Kafka lecture-publication-offset, doublons évités sur les topics Kafka, PostgreSQL hors périmètre';
        }}
        return 'réglage libre : commit ' + consumerCommitStrategy.value + ', auto-commit ' + consumerAutoCommit.value + ', max poll ' + consumerMaxPollInterval.value + ' ms, batch ' + consumerCommitBatch.value;
      }};
      const updateConsequenceState = () => {{
        producerSemanticsLabel.textContent = producerLabels[semantics.value] || semantics.value;
        producerConsequenceText.textContent = getProducerConsequence();
        consumerSemanticsLabel.textContent = consumerLabels[consumerSemantics.value] || consumerSemantics.value;
        consumerConsequenceText.textContent = getConsumerConsequence();
      }};
      const updateProducerTuningState = () => {{
        const locked = semantics.value !== 'custom' || semantics.disabled;
        acks.disabled = locked;
        retries.disabled = locked;
        if (semantics.value === 'at_most_once') {{
          acks.value = '0';
          retries.value = '0';
        }} else if (semantics.value === 'at_least_once') {{
          acks.value = 'all';
          if (!retries.value || retries.value === '0') {{
            retries.value = '1';
          }}
        }} else if (semantics.value === 'exactly_once') {{
          acks.value = 'all';
          if (!retries.value || retries.value === '0') {{
            retries.value = '1';
          }}
        }}
      }};
      const updateConsumerTuningState = () => {{
        const locked = consumerSemantics.value !== 'custom' || consumerSemantics.disabled;
        consumerCommitStrategy.disabled = locked;
        consumerAutoCommit.disabled = locked;
        consumerMaxPollInterval.disabled = locked;
        consumerCommitBatch.disabled = locked;
        if (consumerSemantics.value === 'at_most_once') {{
          consumerCommitStrategy.value = 'before';
          consumerAutoCommit.value = 'off';
          consumerCommitBatch.value = '1';
        }} else if (consumerSemantics.value === 'at_least_once') {{
          consumerCommitStrategy.value = 'after';
          consumerAutoCommit.value = 'off';
          consumerCommitBatch.value = '1';
        }} else if (consumerSemantics.value === 'exactly_once_kafka') {{
          consumerCommitStrategy.value = 'after';
          consumerAutoCommit.value = 'off';
          consumerCommitBatch.value = '1';
        }}
      }};
      const setFieldValue = (name, value) => {{
        const field = document.querySelector('[name="' + name + '"]');
        if (!field || value === undefined) {{
          return;
        }}
        field.value = value;
        field.dispatchEvent(new Event(field.tagName === 'SELECT' ? 'change' : 'input', {{ bubbles: true }}));
      }};
      const selectTpChoice = (choice) => {{
        if (!choice) {{
          return;
        }}
        if (scenarioInput) {{
          scenarioInput.value = choice.dataset.scenario || 'nominal';
        }}
        setFieldValue('total_messages', choice.dataset.totalMessages || '');
        setFieldValue('rate_per_second', choice.dataset.ratePerSecond || '');
        setFieldValue('traffic_model', choice.dataset.trafficModel || 'poisson');
        setFieldValue('producer_semantics', choice.dataset.producerSemantics || 'at_least_once');
        setFieldValue('consumer_semantics', choice.dataset.consumerSemantics || 'at_least_once');
        setFieldValue('decision_sla_seconds', choice.dataset.decisionSlaSeconds || '10');
        setFieldValue('validator_workers', choice.dataset.validatorWorkers || '1');
        setFieldValue('decision_engine_workers', choice.dataset.decisionEngineWorkers || '1');
        tpChoices.forEach((item) => item.classList.toggle('is-selected', item === choice));
        updateProducerTuningState();
        updateConsumerTuningState();
        updateConsequenceState();
      }};
      semantics.addEventListener('change', () => {{ updateProducerTuningState(); updateConsequenceState(); }});
      consumerSemantics.addEventListener('change', () => {{ updateConsumerTuningState(); updateConsequenceState(); }});
      acks.addEventListener('change', updateConsequenceState);
      retries.addEventListener('input', updateConsequenceState);
      consumerCommitStrategy.addEventListener('change', updateConsequenceState);
      consumerAutoCommit.addEventListener('change', updateConsequenceState);
      consumerMaxPollInterval.addEventListener('input', updateConsequenceState);
      consumerCommitBatch.addEventListener('input', updateConsequenceState);
      tpChoices.forEach((choice) => {{
        choice.addEventListener('click', () => selectTpChoice(choice));
      }});
      updateProducerTuningState();
      updateConsumerTuningState();
      updateConsequenceState();
    }})();
    (function() {{
      function pollState() {{
        fetch('/health')
          .then(function(r) {{ return r.json(); }})
          .then(function(data) {{
            var ts = (data.details || {{}})['pix-traffic-shaper'] || {{}};
            var phase = ts.current_phase || 'n/a';
            var progress = ts.phase_progress != null ? ts.phase_progress : 0;
            var counts = (data.metrics || {{}}).counts || {{}};
            var validated = counts.validated || 0;
            var rejected = counts.rejected || 0;
            var progressText = rejected > 0
              ? progress + ' / (' + validated + ' + ' + rejected + ')'
              : progress + ' / ' + validated;
            document.getElementById('js-phase').textContent = phase;
            document.getElementById('js-progress').textContent = progressText;
            var phaseActive = phase !== 'n/a' && phase !== 'idle' && phase !== 'completed';
            var tsRunning = ts.status === 'running' && phaseActive;
            var btn = document.getElementById('js-launch-btn');
            var actionInput = document.getElementById('js-launch-action');
            if (tsRunning) {{
              btn.textContent = "Modifier le TP";
              actionInput.value = 'update_scenario';
            }} else {{
              btn.textContent = "Lancer le TP choisi";
              actionInput.value = 'run_scenario';
            }}
          }})
          .catch(function() {{}});
      }}
      setInterval(pollState, 3000);
    }})();
  </script>
</body>
</html>"""
