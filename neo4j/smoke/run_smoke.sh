#!/usr/bin/env bash
# F1.5 Neo4j + GDS smoke test: bulk-import the toy graph, then check it.
#
#   bash neo4j/smoke/run_smoke.sh --overwrite        # from WSL, in the repo root
#
# 1. copies tests/fixtures/neo4j_toy/*.csv to $MUSIN_DATA/import/toy/
# 2. stops Neo4j and runs `neo4j-admin database import full` into the `neo4j`
#    database in a one-off container of the same image (the database name goes
#    first: --nodes/--relationships are greedy and would swallow it)
# 3. starts Neo4j and runs `python -m musin.neo4j_smoke` in the spark container
#
# Community Edition has ONE user database, so the import REPLACES whatever graph
# is loaded (e.g. the sample or full graph from F3.4). --overwrite is required
# to say that is intended. Do not run while a heavy Spark job is running.
set -euo pipefail

if [[ "${1:-}" != "--overwrite" ]]; then
  echo "usage: bash neo4j/smoke/run_smoke.sh --overwrite" >&2
  echo "Replaces the graph in the Neo4j 'neo4j' database with the toy graph." >&2
  exit 2
fi

cd "$(dirname "$0")/../.."
[[ -f .env ]] || { echo ".env missing (cp .env.example .env)" >&2; exit 1; }
MUSIN_DATA="$(grep -E '^MUSIN_DATA=' .env | cut -d= -f2-)"
[[ -n "$MUSIN_DATA" && -d "$MUSIN_DATA/import" ]] || {
  echo "MUSIN_DATA ('$MUSIN_DATA') or its import/ dir missing (run setup/preflight.sh)" >&2
  exit 1
}

start=$(date +%s)
echo "== copy toy CSVs -> $MUSIN_DATA/import/toy/"
rm -rf "$MUSIN_DATA/import/toy"
mkdir -p "$MUSIN_DATA/import/toy"
cp tests/fixtures/neo4j_toy/*.csv "$MUSIN_DATA/import/toy/"

echo "== stop neo4j and bulk-import"
docker compose stop neo4j
docker compose run --rm --no-deps neo4j \
  neo4j-admin database import full neo4j \
  --overwrite-destination=true \
  --report-file=/logs/import-toy.report \
  --nodes=/import/toy/artists.csv \
  --nodes=/import/toy/genres.csv \
  --relationships=/import/toy/in_genre.csv \
  --relationships=/import/toy/edges_co_credited.csv \
  --relationships=/import/toy/edges_featured.csv \
  --relationships=/import/toy/edges_produced_for.csv \
  --relationships=/import/toy/edges_performed_for.csv

echo "== start neo4j and run the checks"
docker compose up -d neo4j
docker compose exec spark python -m musin.neo4j_smoke --config config/pipeline.yaml
echo "== smoke test finished in $(( $(date +%s) - start ))s"
