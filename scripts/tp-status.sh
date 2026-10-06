#!/bin/sh
set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

HEALTH_URL="${SIMULPIX_HEALTH_URL:-http://127.0.0.1:8082/health/details}"

if ! raw_payload="$(curl -fsS "$HEALTH_URL")"; then
  echo "[simulpix] Impossible de joindre la météo des services: $HEALTH_URL" >&2
  echo "[simulpix] Vérifiez que la plateforme est démarrée avec ./start.sh" >&2
  exit 1
fi

if [ "${SIMULPIX_TP_STATUS_VERBOSE:-0}" = "1" ]; then
  printf "%s" "$raw_payload" | python3 -m json.tool
  exit 0
fi

RAW_PAYLOAD="$raw_payload" python3 <<'PY'
import json
import os
import sys


def value(data, *path, default="n/a"):
    current = data
    for key in path:
        if not isinstance(current, dict) or key not in current:
            return default
        current = current[key]
    if current is None:
        return default
    return current


def count(data, key):
    return value(data, "metrics", "counts", key, default=0)


def status_marker(status):
    if status == "ok" or status == "running" or status == "idle":
        return "OK"
    if status == "degraded":
        return "À surveiller"
    if status == "down" or status == "error":
        return "Problème"
    return str(status)


payload = json.loads(os.environ["RAW_PAYLOAD"])
services = payload.get("services") or {}
details = payload.get("details") or {}
alerts = payload.get("alerts") or []
run = payload.get("run") or {}
groups = run.get("groups") or {}

generated = count(payload, "generated")
processed = count(payload, "processed")
validated = count(payload, "validated")
rejected = count(payload, "rejected")
outcome = count(payload, "outcome_count")
db_validated = count(payload, "db_validated_count")
db_rejected = count(payload, "db_rejected_count")
reject_rate = value(payload, "metrics", "ratios", "reject_rate_percent", default=0.0)

print("============================================================")
print("TP Status - météo lisible de la plateforme simul-pix")
print("============================================================")
print()
print("À retenir :")
print("- cette commande résume la santé de la plateforme ;")
print("- les compteurs métier disent combien de Pix ont traversé le pipeline ;")
print("- le lag Kafka indique si des consommateurs sont en retard ;")
print("- le JSON complet reste disponible en mode verbose.")
print()

print("1) État global")
print("--------------")
print(f"Plateforme : {status_marker(payload.get('status'))} ({payload.get('status', 'n/a')})")
print(f"Run id     : {run.get('run_id', 'default')}")
print(f"Alertes    : {len(alerts)}")
if alerts:
    for alert in alerts[:5]:
        print(f"  - {alert}")
else:
    print("  - aucune alerte active")
print()

print("2) Compteurs métier")
print("-------------------")
print("Ces valeurs permettent de suivre le chemin d'une transaction Pix.")
print(f"Pix générés                : {generated}")
print(f"Pix traités par décision   : {processed}")
print(f"Pix acceptés               : {validated}")
print(f"Pix rejetés                : {rejected}")
print(f"Réponses finales outcome   : {outcome}")
print(f"Persistés en base valides  : {db_validated}")
print(f"Persistés en base rejetés  : {db_rejected}")
print(f"Taux de rejet              : {reject_rate} %")
print()

print("Lecture rapide :")
if generated == 0:
    print("- aucun flux n'a encore été lancé, ou la plateforme vient d'être réinitialisée.")
elif generated > processed:
    print("- des Pix sont générés mais pas encore tous décidés : il peut y avoir du retard dans le pipeline.")
elif rejected > 0:
    print("- des rejets existent : ouvrez l'activité 07 ou observez le topic rejected.")
else:
    print("- le flux semble nominal : les compteurs principaux convergent.")
print()

print("3) Services principaux")
print("----------------------")
service_order = [
    ("kafka", "cluster Kafka"),
    ("postgres", "base PostgreSQL"),
    ("generator", "producteur de Pix"),
    ("pix-validator", "contrôle les messages raw"),
    ("pix-decision-engine", "décide valide ou rejet"),
    ("pix-outcome-publisher", "publie les réponses finales"),
    ("persister-valid", "persiste les Pix acceptés"),
    ("persister-rejected", "persiste les Pix rejetés"),
    ("service-health", "météo et pilotage"),
    ("grafana", "dashboards"),
]
for name, role in service_order:
    status = payload.get("status", "n/a") if name == "service-health" else services.get(name, "n/a")
    print(f"{name:24} {status_marker(status):14} {role}")
print()

print("4) Kafka : lag des groupes applicatifs")
print("--------------------------------------")
lags = [
    ("pix-validator", count(payload, "pix_validator_lag_total"), groups.get("pix-validator", "simulpix-validator-v1")),
    ("pix-decision-engine", count(payload, "pix_decision_engine_lag_total"), groups.get("pix-decision-engine", "simulpix-decision-engine-v1")),
    ("pix-outcome-publisher", count(payload, "pix_outcome_publisher_lag_total"), groups.get("pix-outcome-publisher", "simulpix-outcome-publisher-v1")),
    ("persister-valid", count(payload, "persister_valid_lag_total"), groups.get("persister-valid", "simulpix-persister-valid-v1")),
    ("persister-rejected", count(payload, "persister_rejected_lag_total"), groups.get("persister-rejected", "simulpix-persister-rejected-v1")),
]
for name, lag, group in lags:
    interpretation = "à jour" if int(lag or 0) == 0 else "retard à traiter"
    print(f"{name:24} lag={lag:<6} {interpretation}  groupe={group}")
print()

print("5) État du flux courant")
print("-----------------------")
generator = details.get("generator") or {}
traffic = details.get("pix-traffic-shaper") or {}
network = details.get("network") or {}
validator = details.get("pix-validator") or {}
decision_engine = details.get("pix-decision-engine") or {}
consumer_semantics = (
    validator.get("consumer_semantics")
    or decision_engine.get("consumer_semantics")
    or "n/a"
)
print(f"Scénario generator       : {generator.get('scenario_type', 'n/a')}")
print(f"Phase traffic shaper     : {traffic.get('current_phase', 'n/a')}")
print(f"Progression phase        : {traffic.get('phase_progress', 0)} / {traffic.get('phase_total', 0)}")
print(f"Mode trafic              : {traffic.get('traffic_model', generator.get('traffic_model', 'n/a'))}")
print(f"Sémantique PUB           : {generator.get('producer_semantics', 'n/a')}")
print(f"Sémantique SUB           : {consumer_semantics}")
print(f"Perturbation réseau      : {'active' if network.get('active') else 'inactive'}")
if network.get("active"):
    print(f"  profil={network.get('profile', 'n/a')} cible={network.get('target_service', 'n/a')}")
print()

print("6) Où regarder ensuite")
print("----------------------")
print("- détails Kafka pédagogiques : ./scripts/tp-kafka.sh")
print("- météo HTML                 : http://localhost:8082/")
print("- pilotage des TP            : http://localhost:8083/")
print("- Grafana                    : http://localhost:3000")
print("- JSON complet               : SIMULPIX_TP_STATUS_VERBOSE=1 ./scripts/tp-status.sh")
PY
