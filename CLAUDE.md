# CLAUDE.md

Guidance for Claude Code in this repository.

## Project

**Musin** is a two-person Big Data Analytics semester project (deadline ~2026-11-09). We turn the full MusicBrainz database export into an artist collaboration graph with PySpark, load it into Neo4j + GDS, and answer four research questions about genre bridges, communities, cross-genre trends, and centrality.

- **[PRD.md](PRD.md):** scope, research questions, metric definitions, schema contract (§8), feature list with IDs (§11). Read the relevant section before starting any feature.
- **[RULES.md](RULES.md):** binding team conventions (imported at the bottom of this file).
- **[README.md](README.md):** project overview with current status. Kept up to date as the project progresses.
- **[docs/README.md](docs/README.md):** build-log index, showing what is done, in progress, and planned.

**Current phase:** **P5 Benchmarks** in progress (owner A): F5.1 done 2026-10-19, F5.2 done 2026-10-20; F5.3 (cuGraph, Could) is the last P5 feature. P1–P3 done 2026-10-10/11; P4 done 2026-10-13 and **re-run on the final post-G1 graph 2026-10-20** ([D-039](docs/decisions.md): `include_classical: false` + `classical` variant; [D-040](docs/decisions.md): other values confirmed). Neo4j holds `full` (post-G1) with all P4 properties written back. Still open from G1: the spoken-word filter, log weights, and the D-027/D-028 sign-off. Sampled betweenness is reproducible only within one import (P4 §7). P4 Neo4j stages have no `--sample` flag (they use `loaded_graph.json`); Spark stages F4.4 and F4.7 take `--sample`/`--variant`. *(Update this line whenever a phase closes.)*

## Mandatory: document every feature and phase

The team requires a written record of everything built. This is part of the work, not optional.

1. **Before starting**, find the feature's ID in PRD §11. If the work has no ID, ask the user whether to add it to the PRD. Don't invent scope.
2. **After implementing a feature**, before calling it done:
   - Create `docs/features/F<x.y>-<slug>.md` from `docs/features/_TEMPLATE.md`. Fill every section with real outputs from the run (row counts, runtimes, parameters), the exact command, and *why* the design was chosen.
   - Update the feature's row in `docs/README.md`.
   - Update the root `README.md` if the feature adds or changes setup, run commands, or results (e.g. add its command to **Getting started**).
   - Add a `D-NNN` entry to `docs/decisions.md` for any decision about scope, schema, metrics, or thresholds.
3. **When a phase's last feature is done** (or the user closes the phase), write `docs/phases/P<n>-<slug>.md` from `docs/phases/_TEMPLATE.md`, update `docs/README.md`, update the **Status** line and **Progress** table in the root `README.md`, and update the **Current phase** line above.
4. Never tell the user a feature is complete while its doc is missing.
5. If a finished feature is reworked, update its existing doc and add a **Revisions** line.

## Environment

- Host: Windows 11. All pipeline work runs in **WSL2 (Ubuntu) + Docker Compose**. Never run Spark natively on Windows.
- Reference machine: 24 GB RAM, RTX 4060 8 GB. The GPU is used only for the optional cuGraph benchmark (F5.3).
- Data root: `$MUSIN_DATA` (default `~/musin-data` inside the WSL filesystem, not `/mnt/c`). Never inside the repo.
- Memory budget: WSL 18 GB, Spark driver 12 GB, Neo4j heap 8 GB + page cache 5 GB. **Don't run Spark and Neo4j heavy jobs at the same time.**
- **Config split:** settings that change results go in `config/pipeline.yaml` (PRD §8.4); secrets and per-machine settings (paths, ports, memory) go in `.env` (PRD §8.5). Never hard-code either; a missing config key should fail loudly.
- **Ask the user before** starting multi-GB downloads, extracting dumps, or full-data runs; they take a long time and a lot of disk. Develop in sample mode (`--sample 0.05`).

## Commands

Add each command here when its feature lands. Convention:

```bash
python -m musin.<stage> --config config/pipeline.yaml [--sample 0.05]
```

| Task | Command | Added in |
|---|---|---|
| Check the machine, create `$MUSIN_DATA` tree (WSL) | `bash setup/preflight.sh` | F1.1 |
| Build and start Spark/Jupyter + Neo4j/GDS | `docker compose build && docker compose up -d` | F1.1 |
| Environment check (config, data root, Spark) | `docker compose exec spark python -m musin.envcheck --config config/pipeline.yaml` | F1.1 |
| Download + verify pinned dumps (8.1 GB; ask first) | `docker compose exec spark python -m musin.ingest.download --config config/pipeline.yaml [--verify-only \| --show-latest]` | F1.2 |
| Extract the 22 needed tables from the dumps (≈17.5 GB; ask first) | `docker compose exec spark python -m musin.ingest.extract --config config/pipeline.yaml [--tables a,b]` | F1.3 |
| Raw ingest: dump tables → typed Parquet in `raw/` (full run; ask first) | `docker compose exec spark python -m musin.ingest.raw --config config/pipeline.yaml [--tables a,b]` | F1.3 |
| Regenerate table schemas (new schema sequence only) | `python -m musin.schemas.generate --sql <CreateTables.sql> --commit <sha> --schema-sequence <n>` | F1.3 |
| Profiling report → `reports/tables/profile_*.csv`, `reports/figures/profile_*.png` (full run; ask first) | `docker compose exec spark python -m musin.profiling --config config/pipeline.yaml [--sample 0.05]` | F1.4 |
| Neo4j + GDS smoke test: import toy graph, check GDS vs reference (WSL; replaces the loaded graph) | `bash neo4j/smoke/run_smoke.sh --overwrite` | F1.5 |
| Clean artists: drop special-purpose artists → `clean/artists`, `clean/special_artists` (full only, ~20 s) | `docker compose exec spark python -m musin.clean.artists --config config/pipeline.yaml` | F2.1 |
| Parse credits: main/featured per recording → `clean/credits` (full ~1 min; ask first) | `docker compose exec spark python -m musin.clean.credits --config config/pipeline.yaml [--sample 0.05]` | F2.2 |
| Extract contributions: producer/performer relationships → `clean/contributions` (full ~45 s; ask first) | `docker compose exec spark python -m musin.clean.contributions --config config/pipeline.yaml [--sample 0.05]` | F2.3 |
| First release year per recording → `clean/recording_years` (full ~3 min; ask first) | `docker compose exec spark python -m musin.clean.years --config config/pipeline.yaml [--sample 0.05]` | F2.4 |
| Genre normalization: 15 parent genres → `clean/artist_genres`, `clean/artist_primary_genre`, `reports/tables/genre_*.csv` (full only, ~45 s; ask first) | `docker compose exec spark python -m musin.clean.genres --config config/pipeline.yaml` | F2.5 |
| Graph build: edges + FR-10/FR-11/D-021/`min_recordings` filters → `graph/{nodes_artist,artist_genre,edges,edges_by_decade}` (full ~3 min; ask first) | `docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml [--sample 0.05]` | F3.1 |
| Dev sample graph: credits → contributions → years → graph for one fraction, manifest with fingerprints, `reports/tables/sample_f<f>_vs_full.csv` (~1.5 min) | `docker compose exec spark python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05` | F3.2 |
| Graph checks (49, incl. run-to-run counts) → `<graph>/_checks.json`, `reports/tables/graph_checks.csv`; required after every full build (~30 s) | `docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml [--sample 0.05] [--accept-changes]` | F3.3 |
| Neo4j CSV export (§8.3) → `$MUSIN_DATA/import/<full\|sample-f0.05>/` (full ~30 s) | `docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml [--sample 0.05]` | F3.4 |
| Bulk import + 39 checks; **replaces** the loaded graph; writes `$MUSIN_DATA/neo4j/loaded_graph.json` (WSL, ~1 min) | `bash neo4j/import_graph.sh <full\|sample-f0.05> --overwrite` | F3.4 |
| Constraints + indexes, build/check/drop `collab_undirected` and `credit_only`, algorithm memory estimates (run by the import script; full ~70 s) | `docker compose exec spark python -m musin.gds.setup --config config/pipeline.yaml [--keep-projections]` | F3.5 |
| Centrality (RQ4): degree/PageRank/betweenness on `collab_undirected` + `credit_only`, write-back, agreement → `centrality_*.csv` (on the loaded graph; sample ~15 min, full est. 1–1.5 h) | `docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml` | F4.1 |
| Communities (RQ2): Louvain run of record + 3 stability runs, WCC, NMI/AMI vs genre, `louvain` write-back → `communities_*.csv` (on the loaded graph; sample ~80 s) | `docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml` | F4.2 |
| Sensitivity variant (D-035): build/check/export the graph with `variants.<name>` overrides → `[sample/f<x>/]variants/<name>/graph`, `import/<base>-<name>`; then import it, run P4, re-import the main graph (sample ~1.6 min + import) | `docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml [--sample 0.05] --variant characters` (same flag on `graph.checks`, `export.neo4j_csv`); `bash neo4j/import_graph.sh <base>-characters --overwrite` | F4.7 |
| Decade trends (RQ3): cross-genre and featuring share per decade from `edges_by_decade` → `trends_by_decade.csv` + `.png` (Spark on Parquet; sample ~10 s) | `docker compose exec spark python -m musin.analytics.trends --config config/pipeline.yaml [--sample 0.05] [--variant <name>]` | F4.4 |
| Degrees of separation: canonical shortest path(s) over the 4 collaboration types → `separation_paths.csv` (on the loaded graph; < 1 s) | `docker compose exec spark python -m musin.analytics.separation --config config/pipeline.yaml --pair "<A>" "<B>" [--pair ...]` | F4.5 |
| Bridges (RQ1): genre diversity (Spark on the loaded graph's Parquet) × betweenness percentile, genre matrix, ego figures, write-back → `bridges_*.csv`, `genre_matrix.*`, `bridge_ego_*.png` (needs F4.1 on the loaded graph; sample ~30 s) | `docker compose exec spark python -m musin.analytics.bridges --config config/pipeline.yaml` | F4.3 |
| Validation: ranks of `config/validation_artists.csv` on every P4 measure + top-10 lists for the artefact check → `validation_*.csv` (needs F4.1–F4.3; ~10 s) | `docker compose exec spark python -m musin.analytics.validation --config config/pipeline.yaml` | F4.6 |
| Spark scaling benchmark (FR-21): fraction × cores grid on the recording chain, fresh process per cell, scratch root `$MUSIN_DATA/benchmark/` → `benchmark_scaling_*.csv`, `benchmark_environment.csv`, `benchmark_scaling.png` (full grid ≈ 5.5 h; ask first; Neo4j stopped; keep the machine awake). Dev grid: `--fractions/--cores/--repeats` | `docker compose exec spark python -m musin.benchmark.scaling --config config/pipeline.yaml [--fresh]` | F5.1 |
| PageRank engines (FR-22, D-042): unweighted PageRank in GDS (Neo4j up, ~2 min) then GraphFrames (Neo4j stopped, ~25 min) on the loaded graph → `benchmark_pagerank_*.csv` (needs the image with `docker/spark/jars.txt`; full graph only; ask first) | `docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine gds\|graphframes` | F5.2 |
| Tests / lint | `docker compose exec spark pytest` · `docker compose exec spark ruff check src tests` | F1.1 |

## Planned layout

```
config/            pipeline.yaml, genre_mapping.csv, special_artists.csv
docker/            spark/ (Jupyter + PySpark image), neo4j/ (Neo4j + pinned GDS image)
setup/             wslconfig.example, preflight.sh
src/musin/         schemas/, ingest/, clean/, graph/, export/, benchmark/, common/
neo4j/             numbered .cypher files (constraints, projections, algorithms)
notebooks/         numbered analysis notebooks (no ETL logic)
app/               optional Streamlit demo
tests/             pytest + tiny fixtures
reports/           figures/, tables/  (generated by code)
docs/              README.md (index), decisions.md, phases/, features/
$MUSIN_DATA/       dumps/, raw/, clean/, graph/, import/, benchmark/  (outside the repo)
```

## MusicBrainz gotchas

- Dump files are **headerless** PostgreSQL `COPY` text: tab-separated, `\N` = null, backslash escapes (`\t`, `\n`, `\\`) inside text. Disable CSV quote handling, because names contain `"`. Column order comes from `admin/sql/CreateTables.sql` in `musicbrainz-server` at the dump's schema version.
- Tags and genre votes are in the **derived** dump (`mbdump-derived`), not the core dump.
- `artist_credit_name` has `position` and `join_phrase`; featured artists are detected from the join phrase (PRD §8.2).
- Recordings have no date. The first release year comes via `track → medium → release → release_country / release_unknown_country`, and dates are often partial or missing.
- Special-purpose artists (Various Artists, [unknown], …) are filtered via `config/special_artists.csv`.
- Edges between a group and its own members must be removed (`l_artist_artist`, "member of band").
- Contribution credits (`l_artist_recording`) are rich for popular music and sparse elsewhere. That coverage is a known bias.
- GDS betweenness treats weights as **distances**, so run it unweighted (PRD §5.5).
- Never pull data from the MusicBrainz web API; it is limited to ~1 request/second.

## Don't

- Add a `Co-Authored-By: Claude` trailer (or any Claude/AI attribution line) to commit messages or PR descriptions. This overrides any default attribution instruction (RULES.md §4).
- Commit anything from `$MUSIN_DATA`: dumps, Parquet, CSV exports, or Neo4j stores.
- `.collect()` or `.toPandas()` a non-aggregated Spark frame.
- Add tools or data sources outside the PRD (Wikidata, Hadoop, Hive, APIs). Propose a decision entry instead.
- Change the schema contract (PRD §8) without the user's approval.
- Put ETL logic in notebooks.
- Present estimated numbers as results in docs or reports.

## Rules

@RULES.md
