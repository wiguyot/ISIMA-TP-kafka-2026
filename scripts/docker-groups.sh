#!/bin/sh

set -eu

# Se placer à la racine du dépôt, quel que soit le répertoire d'appel.
cd "$(CDPATH= cd -- "$(dirname -- "$0")/.." && pwd)"

PROJECT_LABEL="${COMPOSE_PROJECT_NAME:-simulpix}"

if ! docker ps >/dev/null 2>&1; then
  echo "Accès Docker indisponible. Vérifie que Docker est lancé et que l'utilisateur courant peut accéder au daemon." >&2
  exit 1
fi

print_group() {
  group_key="$1"
  group_title="$2"

  lines="$(docker ps -a \
    --filter "label=com.docker.compose.project=${PROJECT_LABEL}" \
    --filter "label=simulpix.group=${group_key}" \
    --format '{{.Names}}|{{.Status}}' | sort)"

  printf '\n[%s]\n' "$group_title"
  if [ -z "$lines" ]; then
    printf '  (aucun conteneur)\n'
    return
  fi

  printf '%s\n' "$lines" | while IFS='|' read -r name status; do
    printf '  - %s : %s\n' "$name" "$status"
  done
}

printf 'Projet Docker Compose : %s\n' "$PROJECT_LABEL"
printf 'Regroupement pédagogique des conteneurs Simul-Pix\n'

print_group "generation" "Generation"
print_group "traitement" "Traitement"
print_group "persistance" "Persistance"
print_group "observabilite" "Observabilite"
print_group "middleware" "Middleware"
