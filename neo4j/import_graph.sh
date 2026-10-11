#!/usr/bin/env bash
# F3.4: bulk-import an exported graph into Neo4j, then verify it.
#
#   bash neo4j/import_graph.sh <graph> --overwrite      # from WSL, in the repo root
#
# <graph> is the export name printed by `python -m musin.export.neo4j_csv`:
# "full" or "sample-f<fraction>" (e.g. sample-f0.05), i.e. $MUSIN_DATA/import/<graph>/.
#
# 1. removes $MUSIN_DATA/neo4j/loaded_graph.json (nothing claims to know the
#    database content until the new import is verified)
# 2. stops Neo4j and runs `neo4j-admin database import full neo4j` in a one-off
#    container of the same image, with the arguments the export wrote to
#    _import_args.txt; bad rows or duplicate nodes abort the import
# 3. starts Neo4j and runs `python -m musin.export.neo4j_check`, which writes
#    loaded_graph.json again only if every check passes
# 4. runs `python -m musin.gds.setup` (F3.5): constraints, indexes, and a
#    build-check-drop of every GDS projection
#
# Community Edition has ONE user database: the import REPLACES the loaded graph
# (sample, full, or the F1.5 toy graph). --overwrite says that is intended.
# Do not run while a heavy Spark job is running (PRD §7.2).
set -euo pipefail

graph="${1:-}"
if [[ -z "$graph" || "${2:-}" != "--overwrite" ]]; then
  echo "usage: bash neo4j/import_graph.sh <full|sample-f<fraction>> --overwrite" >&2
  echo "Replaces the graph in the Neo4j 'neo4j' database with the exported graph." >&2
  exit 2
fi
[[ "$graph" =~ ^(full|sample-f[0-9.]+)$ ]] || { echo "bad graph name: $graph" >&2; exit 2; }

cd "$(dirname "$0")/.."
[[ -f .env ]] || { echo ".env missing (cp .env.example .env)" >&2; exit 1; }
MUSIN_DATA="$(grep -E '^MUSIN_DATA=' .env | cut -d= -f2-)"
export_dir="$MUSIN_DATA/import/$graph"
args_file="$export_dir/_import_args.txt"
[[ -f "$args_file" ]] || {
  echo "$args_file missing: run python -m musin.export.neo4j_csv first" >&2
  exit 1
}
mapfile -t import_args < "$args_file"

start=$(date +%s)
rm -f "$MUSIN_DATA/neo4j/loaded_graph.json"

echo "== stop neo4j and bulk-import '$graph' (${#import_args[@]} file groups)"
docker compose stop neo4j
docker compose run --rm --no-deps neo4j \
  neo4j-admin database import full neo4j \
  --overwrite-destination=true \
  --bad-tolerance=0 \
  --skip-bad-relationships=false \
  --skip-duplicate-nodes=false \
  --report-file="/logs/import-$graph.report" \
  "${import_args[@]}"

echo "== start neo4j and verify"
docker compose up -d neo4j
docker compose exec spark python -m musin.export.neo4j_check --config config/pipeline.yaml --graph "$graph"

echo "== constraints, indexes, and projection checks (F3.5)"
docker compose exec spark python -m musin.gds.setup --config config/pipeline.yaml
echo "== import of '$graph' finished in $(( $(date +%s) - start ))s"
