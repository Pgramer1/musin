<!--
Keep this README current (RULES.md §1.3). Update it whenever:
  - a phase changes status          → Status line + Progress table
  - a feature adds or changes setup → Getting started
  - results become available        → Results
-->

# Musin

**Bridging Genres: A Big-Data Analysis of the MusicBrainz Collaboration Network**

A Big Data Analytics semester project. We process the full MusicBrainz database export with PySpark, build an artist collaboration graph, and analyse it in Neo4j to find which artists connect music genres.

> **Status:** P1–P3 done: raw ingest, clean layer, graph build, checks, Neo4j load with constraints and GDS projections (**M2 reached: the sample graph is loaded in Neo4j**); **P4 Graph analytics in progress** (F4.1 centrality, F4.2 communities, F4.4 decade trends, F4.5 degrees of separation and F4.7 sensitivity variants verified on the sample graph); scope gate G1 on Oct 16 · *Last updated 2026-10-11*

## Research questions

1. **Genre bridges:** which artists connect otherwise separate genres?
2. **Communities vs genres:** do the communities formed by collaboration match genre labels?
3. **Trends:** has cross-genre collaboration increased over the decades?
4. **Centrality:** who is most central, and do different centrality measures agree?

Exact metric definitions are in [PRD §5](PRD.md#5-research-questions).

## Progress

| Phase | Dates | Status |
|---|---|---|
| P0 Planning | Oct 10 | Done |
| P1 Environment & ingestion | Oct 10–16 | Done |
| P2 Cleaning & genre normalization | Oct 15–21 | Done |
| P3 Graph build & Neo4j load | Oct 18–24 | Done |
| P4 Graph analytics | Oct 19–30 | In progress |
| P5 Benchmarks | Oct 22–30 | Planned |
| P6 Presentation & report | Oct 28–Nov 9 | Planned |

Feature-level status, with a doc for everything built, is in the [build log](docs/README.md).

## Architecture

```
MusicBrainz full export (mbdump + mbdump-derived)
        │
        ▼
PySpark ETL ── raw → clean → graph Parquet layers
        │
        ▼
Neo4j + Graph Data Science ── PageRank, betweenness, Louvain, shortest paths
        │
        ▼
reports/ ── figures, tables, final report, demo
```

| Layer | Technology |
|---|---|
| Environment | WSL2 (Ubuntu) + Docker Compose |
| Processing | PySpark 3.5 |
| Storage | Parquet |
| Graph database | Neo4j 5 Community + GDS |
| Analysis | Jupyter, pandas, scikit-learn, matplotlib |
| Benchmarks | Spark GraphFrames, RAPIDS cuGraph (stretch) |

## Getting started

Prerequisites:

- Windows 11 with WSL2 (Ubuntu), or Linux
- Docker Desktop (WSL2 backend) or Docker Engine with Compose
- 24 GB RAM for full-data runs (8 GB is enough for sample mode)
- 100 GB+ free SSD space for the dumps and intermediate data

**1. One-time Windows setup** (PowerShell, from the repo root):

```powershell
wsl --install -d Ubuntu-24.04                                    # creates your Linux user
Copy-Item setup\wslconfig.example $env:USERPROFILE\.wslconfig    # caps WSL at 18 GB; merge if you have one
wsl --shutdown                                                   # then restart Docker Desktop
```

In Docker Desktop, go to **Settings → Resources → WSL integration** and enable your Ubuntu distro.

**2. Configure and start** (Ubuntu shell, repo root):

```bash
cp .env.example .env              # set MUSIN_DATA=/home/<you>/musin-data and NEO4J_PASSWORD (8+ chars)
bash setup/preflight.sh           # checks the machine and creates the $MUSIN_DATA folders
docker compose build
docker compose up -d
docker compose exec spark python -m musin.envcheck --config config/pipeline.yaml
```

**3. Get the data** (8.1 GB download of the pinned snapshot `20261007-002147`, GPG- and SHA-256-verified; [F1.2](docs/features/F1.2-dump-download.md)):

```bash
docker compose exec spark python -m musin.ingest.download --config config/pipeline.yaml
```

The official server keeps only the two latest weekly exports, so after about 2026-10-21 copy `$MUSIN_DATA/dumps/20261007-002147/` from a teammate and run the same command with `--verify-only`.

**4. Unpack and convert to Parquet** (the 22 tables we use, ≈17.5 GB of text → typed Parquet in `$MUSIN_DATA/raw/`; [F1.3](docs/features/F1.3-raw-ingest.md)):

```bash
docker compose exec spark python -m musin.ingest.extract --config config/pipeline.yaml
docker compose exec spark python -m musin.ingest.raw --config config/pipeline.yaml
```

**5. Profile the data and check Neo4j** (profiling report for gate G1, [F1.4](docs/features/F1.4-profiling.md); Neo4j + GDS smoke test on a 12-artist toy graph, [F1.5](docs/features/F1.5-neo4j-smoke-test.md)):

```bash
docker compose exec spark python -m musin.profiling --config config/pipeline.yaml   # add --sample 0.05 while developing
bash neo4j/smoke/run_smoke.sh --overwrite    # replaces the graph in Neo4j with the toy graph
```

**6. Clean** (P2; writes `$MUSIN_DATA/clean/`; add `--sample 0.05` to recording-level stages to write `$MUSIN_DATA/sample/f0.05/` instead):

```bash
docker compose exec spark python -m musin.clean.artists --config config/pipeline.yaml   # F2.1: artists minus 15 special-purpose placeholders
docker compose exec spark python -m musin.clean.credits --config config/pipeline.yaml   # F2.2: main/featured artist per recording
docker compose exec spark python -m musin.clean.contributions --config config/pipeline.yaml   # F2.3: producer/performer relationships
docker compose exec spark python -m musin.clean.years --config config/pipeline.yaml   # F2.4: first release year per recording
docker compose exec spark python -m musin.clean.genres --config config/pipeline.yaml   # F2.5: 15 parent genres per artist
```

**7. Build the graph** (P3; writes `$MUSIN_DATA/graph/` with nodes, genres, typed edges, and per-decade edges; `--sample 0.05` writes `$MUSIN_DATA/sample/f0.05/graph/`):

```bash
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml   # F3.1: edges + FR-10/FR-11/D-021/min_recordings filters
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml  # F3.3: 49 checks, required after every build
```

The checks compare row counts with the last passing check and fail on a change above 5%. After a deliberate change (e.g. a G1 decision), run them once with `--accept-changes` and record why in `docs/decisions.md`.

**8. Load into Neo4j** ([F3.4](docs/features/F3.4-neo4j-export-import.md); ~1.5 min for the full graph). Neo4j Community holds one graph at a time, so the import **replaces** the loaded graph. `$MUSIN_DATA/neo4j/loaded_graph.json` says which graph is loaded.

```bash
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --sample 0.05   # -> $MUSIN_DATA/import/sample-f0.05
bash neo4j/import_graph.sh sample-f0.05 --overwrite   # bulk import + 39 checks + constraints/indexes + GDS projection checks (F3.5)
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml               # full graph -> import/full
bash neo4j/import_graph.sh full --overwrite
```

**Dev sample graph** (F3.2; ~1.5 min, works with `SPARK_DRIVER_MEMORY=4g`). It rebuilds the whole recording-level chain for 5% of recordings into `$MUSIN_DATA/sample/f0.05/` and writes `graph/_manifest.json` with content fingerprints. Two members hold the same graph when the fingerprints match. Needs the full `clean/artists` and genre tables from step 6.

```bash
docker compose exec spark python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05
```

**9. Analyse** (P4; runs on whichever graph is loaded in Neo4j and names it in every output; tables go to `reports/tables/` for the full graph and to `$MUSIN_DATA/sample/f0.05/reports/tables/` for the sample):

```bash
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml   # F4.1: degree, PageRank, betweenness + agreement (RQ4)
docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml  # F4.2: Louvain communities vs genres (RQ2)
docker compose exec spark python -m musin.analytics.trends --config config/pipeline.yaml       # F4.4: cross-genre and featuring share by decade (RQ3; Spark on Parquet, add --sample 0.05)
docker compose exec spark python -m musin.analytics.separation --config config/pipeline.yaml --pair "Frank Sinatra" "Snoop Dogg"   # F4.5: degrees of separation (MBID or exact name)
```

**Sensitivity variants** ([F4.7](docs/features/F4.7-sensitivity-variants.md), D-035). A variant graph is the main graph with some F3.1 filters overridden under `variants.<name>` in `config/pipeline.yaml` (first variant: `characters`, which keeps `Character` artists, D-021). It is built next to the main graph in `$MUSIN_DATA/[sample/f0.05/]variants/<name>/` and loaded as `<full|sample-f0.05>-<name>`. Its P4 tables go to `reports/tables/variants/<name>/` (full) or the sample tree. Load the main graph again afterwards:

```bash
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml --sample 0.05 --variant characters
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml --sample 0.05 --variant characters
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --sample 0.05 --variant characters
bash neo4j/import_graph.sh sample-f0.05-characters --overwrite
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml   # tables -> sample/f0.05/variants/characters/reports/tables
bash neo4j/import_graph.sh sample-f0.05 --overwrite                                           # back to the main dev graph
```

| Service | URL |
|---|---|
| Jupyter Lab (PySpark 3.5.9, Python 3.11) | http://localhost:8888 |
| Spark UI (while a job runs) | http://localhost:4040 |
| Neo4j Browser (5.26.30 + GDS 2.13.12), user `neo4j` | http://localhost:7474 |

Ports listen on `127.0.0.1` only. Spark and Neo4j heavy jobs must not run at the same time (18 GB budget). Settings that change results live in [`config/pipeline.yaml`](config/pipeline.yaml); per-machine settings and secrets live in `.env`. Details: [F1.1 build log](docs/features/F1.1-dev-environment.md).

## Verify the build

How to check, look at, and reproduce what has been built. Run everything from WSL Ubuntu in the repo root (`wsl -d Ubuntu-24.04 --cd <repo>` from PowerShell, since the default WSL distro may be `docker-desktop`), with `docker compose up -d spark neo4j`. The expected numbers below are from the 2026-10-11 build with the provisional G1 values; they change once G1 decisions are applied.

**1. Health check** (about 2 min, changes no data):

```bash
export MUSIN_DATA="$(grep -E '^MUSIN_DATA=' .env | cut -d= -f2-)"                                     # for the cat/grep lines below
docker compose exec spark pytest -q                                                                  # all tests pass
docker compose exec spark ruff check src tests                                                       # "All checks passed!"
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml                 # full graph: 49 checks pass, run_to_run +0.00%
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml --sample 0.05   # same for the dev sample graph
cat $MUSIN_DATA/neo4j/loaded_graph.json                                                              # which graph Neo4j holds now
```

**2. Look at the graph in Neo4j Browser** (http://localhost:7474, user `neo4j`, password `NEO4J_PASSWORD` from `.env`). On an edge, `weight` is the number of shared recordings, and `firstYear`/`lastYear` give their span.

```cypher
// what's loaded
MATCH ()-[r]->() RETURN type(r) AS type, count(*) AS n ORDER BY n DESC;

// one artist's strongest collaborations, drawn as a graph
MATCH (a:Artist {name: 'Snoop Dogg'})-[r]-(b:Artist)
RETURN a, r, b ORDER BY r.weight DESC LIMIT 40;

// degrees of separation (on the sample: Tom Petty → Prince → Erick Sermon → Snoop Dogg)
MATCH (a:Artist {name: 'Tom Petty'}), (b:Artist {name: 'Snoop Dogg'})
MATCH p = shortestPath((a)-[:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR*..10]-(b))
RETURN p;

// genre profile of an artist
MATCH (a:Artist {name: 'Daft Punk'})-[r:IN_GENRE]->(g:Genre)
RETURN g.name, r.share ORDER BY r.share DESC;
```

**3. Look at the Parquet tables** (Jupyter, http://localhost:8888):

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
edges = spark.read.parquet("/data/graph/edges")            # sample: /data/sample/f0.05/graph/edges
nodes = spark.read.parquet("/data/graph/nodes_artist")
edges.groupBy("edge_type").count().show()
nodes.orderBy(nodes.recording_count.desc()).show(10)
```

Generated tables are in `reports/tables/`:
- what each graph filter removed: `graph_build_summary.csv`
- the 49 check results: `graph_checks.csv`
- sample vs full: `sample_f0.05_vs_full.csv`
- projection sizes and memory: `gds_projections.csv`
- the earlier phases: `profile_*` (F1.4) and `genre_*` (F2.5)

The whole graph phase is summarised in [docs/phases/P3-graph-build-neo4j.md](docs/phases/P3-graph-build-neo4j.md).

**4. Prove a rebuild is identical.** Every build records order-independent content fingerprints; equal fingerprints mean identical tables.

```bash
docker compose exec spark python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05   # ~95 s, safe while Neo4j runs
grep -A2 '"edges"' $MUSIN_DATA/sample/f0.05/graph/_manifest.json                                    # rows + fingerprint, unchanged between builds
docker compose stop neo4j                                                                            # full rebuild is a heavy Spark job
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml                  # ~3 min
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml                 # run_to_run +0.00% = same counts
```

**5. Re-verify Neo4j.** Each import runs 39 checks against the export and the Parquet source, then F3.5's constraint and projection checks; every line should say `PASS`. An import **replaces** the loaded graph.

```bash
bash neo4j/import_graph.sh sample-f0.05 --overwrite                                    # member B's dev graph (~1.5 min)
bash neo4j/import_graph.sh full --overwrite                                            # full graph (~2 min)
docker compose exec spark python -m musin.gds.setup --config config/pipeline.yaml      # re-check schema + projections only
```

After a deliberate config change (e.g. a G1 decision), `graph.checks` fails on purpose, because row counts move by more than 5 %. Re-run it once with `--accept-changes` and record the reason in [docs/decisions.md](docs/decisions.md).

## Repository layout

```
PRD.md             project requirements: scope, metrics, schema, schedule
RULES.md           team conventions
CLAUDE.md          guidance for Claude Code
docs/              build log: phase docs, feature docs, decision log
config/            pipeline.yaml (settings that change results)
docker/            Spark/Jupyter and Neo4j/GDS images
setup/             wslconfig.example, preflight.sh
src/musin/         pipeline code (python -m musin.<stage>)
neo4j/             numbered Cypher files (smoke/: F1.5 toy-graph test)
reports/           generated tables and figures (profile_*: F1.4, genre_*: F2.5, graph_build_*: F3.1, graph_checks: F3.3, sample_*: F3.2, centrality_*: F4.1, communities_*: F4.2, trends_*: F4.4, separation_*: F4.5)
tests/             pytest + fixtures (neo4j_toy/: F1.5 toy graph)
docker-compose.yml two services: spark, neo4j
.env.example       template for per-machine settings and secrets
```

More folders (`notebooks/`, `app/`) are added as the phases progress.

## Results

*Added during P4 (analysis) and P6 (report).*

**Data profile (F1.4):** 40.5 M recordings, 2.0 M credited artists; 86% of recordings have one credited artist; 92.6% have a first release year; genre tags cover 27% of credited artists (72% of artist appearances); producer credits cover 4.4% of recordings. Details and G1 evidence: [F1.4 build log](docs/features/F1.4-profiling.md), tables in `reports/tables/profile_*.csv`.

**Genres (F2.5):** 15 parent genres cover 97.5% of genre votes; 26.6% of credited artists (69.9% of appearances) get a primary genre, led by electronic (26.7%) and rock (18.0%). Details: [F2.5 build log](docs/features/F2.5-genre-normalization.md), tables in `reports/tables/genre_*.csv`.

**Graph (F3.1, provisional G1 thresholds):** 1.18 M artists and 3.54 M edges (1.73 M co-credited, 0.73 M featured, 0.29 M produced-for, 0.78 M performed-for), each recording counted once per artist pair; 23.4% of artists have a primary genre and 35.2% of edges are labelled at both ends. Classical artists are 2.2% of nodes but touch 13.1% of edges. Details: [F3.1 build log](docs/features/F3.1-edge-building.md), `reports/tables/graph_build_summary.csv`.

## Documentation

| Document | What it covers |
|---|---|
| [PRD.md](PRD.md) | Scope, research questions, data, architecture, schema contract, features, schedule, risks |
| [RULES.md](RULES.md) | How we work: documentation, git, code, data, Neo4j, analysis integrity |
| [docs/README.md](docs/README.md) | Build log: what was built in each phase and feature, and why |
| [docs/decisions.md](docs/decisions.md) | Every major decision, with its reasoning |
| [CLAUDE.md](CLAUDE.md) | Instructions for Claude Code in this repo |

## Team

| Member | Name | Role |
|---|---|---|
| A | `<name>` | Data engineering: environment, Spark ETL, Neo4j import, benchmarks |
| B | `<name>` | Graph analytics: Neo4j/GDS, research questions, validation, demo |

## Data and attribution

Data comes from [MusicBrainz](https://musicbrainz.org/), run by the MetaBrainz Foundation. Core data is CC0; tags come from the derived dump under CC BY-NC-SA 3.0. No MusicBrainz data is stored in this repository.
