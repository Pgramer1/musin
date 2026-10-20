<!--
Keep this README current (RULES.md §1.3). Update it whenever:
  - a phase changes status          → Status line + Progress table
  - a feature adds or changes setup → Getting started
  - results become available        → Results
-->

# Musin

**Bridging Genres: A Big-Data Analysis of the MusicBrainz Collaboration Network**

A Big Data Analytics semester project. We process the full MusicBrainz database export with PySpark, build an artist collaboration graph, and analyse it in Neo4j to find which artists connect music genres.

> **Status:** P1–P4 done; G1 decisions recorded ([D-039](docs/decisions.md): classical analysed separately; [D-040](docs/decisions.md): other values confirmed), the graph rebuilt and **all four research questions re-answered on the final graph** (2026-10-20); **P5 Benchmarks done** (Spark scaling, GDS vs GraphFrames vs cuGraph); **P6 built**: report figures and tables (F6.1), demo queries (F6.2), report draft ([`report/report.md`](report/report.md), F6.3), 5-slide deck (F6.4), clean-clone check (F6.5). Team review, demo video and rehearsal pending · *Last updated 2026-10-20*

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
| P4 Graph analytics | Oct 19–30 | Done (final graph) |
| P5 Benchmarks | Oct 22–30 | Done |
| P6 Presentation & report | Oct 28–Nov 9 | Built; team review, video and rehearsal pending |

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
| Benchmarks | Spark GraphFrames, RAPIDS cuGraph (optional GPU container) |

## Getting started

Prerequisites:

- Windows 11 with WSL2 (Ubuntu), or Linux
- Docker Desktop (WSL2 backend) or Docker Engine with Compose
- 24 GB RAM for full-data runs (8 GB is enough for sample mode)
- 100 GB+ free SSD space for the dumps and intermediate data

**1. One-time Windows setup** (PowerShell):

```powershell
wsl --install -d Ubuntu-24.04     # creates your Linux user
```

In Docker Desktop, go to **Settings → Resources → WSL integration** and enable your Ubuntu distro. Every command from here on runs in that Ubuntu shell (`wsl -d Ubuntu-24.04` from PowerShell; the default distro may be `docker-desktop`).

**2. Clone and cap WSL's memory** (Ubuntu shell). The GitHub repo is private and WSL's `git` has no GitHub login of its own, so `git clone` stops at a username prompt: sign in first with `gh auth login` (`sudo apt install gh`) or give a personal access token as the password. Clone into the WSL filesystem (e.g. `~/musin`); a checkout under `/mnt/c/` also works but its bind mount is slower.

```bash
git clone https://github.com/Pgramer1/musin.git ~/musin && cd ~/musin
cp setup/wslconfig.example /mnt/c/Users/<windows-user>/.wslconfig   # caps WSL at 18 GB; merge if you have one
```

Then run `wsl --shutdown` in PowerShell, restart Docker Desktop, and open the Ubuntu shell in `~/musin` again.

**3. Configure and start** (Ubuntu shell, repo root):

```bash
cp .env.example .env              # set MUSIN_DATA=/home/<you>/musin-data and NEO4J_PASSWORD (8+ chars)
bash setup/preflight.sh           # checks the machine and creates the $MUSIN_DATA folders
docker compose build              # ~7 min on a cold cache (PySpark is a 318 MB download)
docker compose up -d
docker compose exec spark python -m musin.envcheck --config config/pipeline.yaml
```

If `docker compose build` stops with `ReadTimeoutError` from `files.pythonhosted.org`, run it again: the finished layers are cached, so it resumes at the pip step. On a machine with less than 24 GB RAM, lower the three memory values in `.env` as its comment says (sample mode only). Jupyter (8888) and the Spark UI (4040) use fixed host ports, so stop any other `musin` checkout's containers first.

**4. Get the data** (8.1 GB download of the pinned snapshot `20261007-002147`, GPG- and SHA-256-verified; [F1.2](docs/features/F1.2-dump-download.md)):

```bash
docker compose exec spark python -m musin.ingest.download --config config/pipeline.yaml
```

The official server keeps only the two latest weekly exports, so from about 2026-10-21 on the download fails: copy the folder `$MUSIN_DATA/dumps/20261007-002147/` from a teammate and run the same command with `--verify-only` (re-checks the GPG signature and both SHA-256 sums, under a minute). `--show-latest` lists what the server has now.

**5. Unpack and convert to Parquet** (the 22 tables we use, ≈17.5 GB of text → typed Parquet in `$MUSIN_DATA/raw/`; [F1.3](docs/features/F1.3-raw-ingest.md)). From here on, stop Neo4j while a heavy Spark stage runs (18 GB budget) and start it again for steps 6, 9 and 10:

```bash
docker compose stop neo4j
docker compose exec spark python -m musin.ingest.extract --config config/pipeline.yaml
docker compose exec spark python -m musin.ingest.raw --config config/pipeline.yaml
```

**6. Profile the data and check Neo4j** (profiling report for gate G1, [F1.4](docs/features/F1.4-profiling.md); Neo4j + GDS smoke test on a 12-artist toy graph, [F1.5](docs/features/F1.5-neo4j-smoke-test.md)):

```bash
docker compose exec spark python -m musin.profiling --config config/pipeline.yaml   # add --sample 0.05 while developing
bash neo4j/smoke/run_smoke.sh --overwrite    # replaces the graph in Neo4j with the toy graph
```

**7. Clean** (P2; writes `$MUSIN_DATA/clean/`; add `--sample 0.05` to recording-level stages to write `$MUSIN_DATA/sample/f0.05/` instead):

```bash
docker compose exec spark python -m musin.clean.artists --config config/pipeline.yaml   # F2.1: artists minus 15 special-purpose placeholders
docker compose exec spark python -m musin.clean.credits --config config/pipeline.yaml   # F2.2: main/featured artist per recording
docker compose exec spark python -m musin.clean.contributions --config config/pipeline.yaml   # F2.3: producer/performer relationships
docker compose exec spark python -m musin.clean.years --config config/pipeline.yaml   # F2.4: first release year per recording
docker compose exec spark python -m musin.clean.genres --config config/pipeline.yaml   # F2.5: 15 parent genres per artist
```

**8. Build the graph** (P3; writes `$MUSIN_DATA/graph/` with nodes, genres, typed edges, and per-decade edges; `--sample 0.05` writes `$MUSIN_DATA/sample/f0.05/graph/`):

```bash
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml   # F3.1: edges + FR-10/FR-11/D-021/min_recordings filters
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml  # F3.3: 49 checks, required after every build
```

The checks compare row counts with the last passing check and fail on a change above 5%. After a deliberate change (e.g. a G1 decision), run them once with `--accept-changes` and record why in `docs/decisions.md`.

**9. Load into Neo4j** ([F3.4](docs/features/F3.4-neo4j-export-import.md); ~1.5 min for the full graph). Neo4j Community holds one graph at a time, so the import **replaces** the loaded graph. `$MUSIN_DATA/neo4j/loaded_graph.json` says which graph is loaded.

```bash
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --sample 0.05   # -> $MUSIN_DATA/import/sample-f0.05
bash neo4j/import_graph.sh sample-f0.05 --overwrite   # bulk import + 39 checks + constraints/indexes + GDS projection checks (F3.5)
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml               # full graph -> import/full
bash neo4j/import_graph.sh full --overwrite
```

**Dev sample graph** (F3.2; ~1.5 min, works with `SPARK_DRIVER_MEMORY=4g`). It rebuilds the whole recording-level chain for 5% of recordings into `$MUSIN_DATA/sample/f0.05/` and writes `graph/_manifest.json` with content fingerprints. Two members hold the same graph when the fingerprints match. Needs the full `clean/artists` and genre tables from step 7.

```bash
docker compose exec spark python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05
```

**Sample mode only** (member B, or a machine under 24 GB). `--sample 0.05` samples recordings, not the input, so a sample run still needs the **full** `raw/` layer (steps 4–5) and the full artist-level tables: `clean/artists` and `clean/special_artists` (F2.1, ~20 s) and the genre tables (F2.5, ~45 s), and F2.5 reads the full `clean/credits` (F2.2, ~1 min). Run those once in full mode (or copy `raw/` and `clean/` from a teammate), then everything else with `--sample 0.05`:

```bash
docker compose stop neo4j
docker compose exec spark python -m musin.clean.artists --config config/pipeline.yaml
docker compose exec spark python -m musin.clean.credits --config config/pipeline.yaml        # full: F2.5 needs it
docker compose exec spark python -m musin.clean.genres --config config/pipeline.yaml
docker compose exec spark python -m musin.clean.credits --config config/pipeline.yaml --sample 0.05
docker compose exec spark python -m musin.clean.contributions --config config/pipeline.yaml --sample 0.05
docker compose exec spark python -m musin.clean.years --config config/pipeline.yaml --sample 0.05
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml --sample 0.05
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml --sample 0.05
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --sample 0.05
docker compose start neo4j
bash neo4j/import_graph.sh sample-f0.05 --overwrite   # replaces the loaded graph
```

**10. Analyse** (P4; runs on whichever graph is loaded in Neo4j and names it in every output; tables go to `reports/tables/` for the full graph and to `$MUSIN_DATA/sample/f0.05/reports/tables/` for the sample). A full F4.1 run takes about 3 h (single-threaded betweenness), so keep the machine awake. The full command order is in [P4 §8](docs/phases/P4-graph-analytics.md#8-how-to-reproduce-this-phase):

```bash
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml   # F4.1: degree, PageRank, betweenness + agreement (RQ4)
docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml  # F4.2: Louvain communities vs genres (RQ2)
docker compose exec spark python -m musin.analytics.bridges --config config/pipeline.yaml      # F4.3: genre diversity, bridge score, genre matrix (RQ1; needs F4.1 first)
docker compose exec spark python -m musin.analytics.validation --config config/pipeline.yaml   # F4.6: ranks of the pre-registered artists + top-10 spot-check lists (needs F4.1-F4.3)
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

The `classical` variant ([D-039](docs/decisions.md), G1/OD1) keeps classical artists, which the main graph leaves out. Use it the same way with `--variant classical`.

**11. Benchmark** (P5). [F5.1](docs/features/F5.1-spark-scaling.md) times the recording chain (credits → contributions → years → graph build) for every data fraction × core count in `benchmark.scaling` (D-041). Each cell runs in its own process against a scratch copy under `$MUSIN_DATA/benchmark/`, so pipeline data and `reports/` are never touched. A re-run resumes from the last finished cell. Stop Neo4j and keep the machine idle and awake while it runs:

```bash
docker compose stop neo4j
docker compose exec spark python -m musin.benchmark.scaling --config config/pipeline.yaml        # full grid, 48 runs (~5.5 h); tables + figure in reports/
docker compose exec spark python -m musin.benchmark.scaling --config config/pipeline.yaml     --fractions 0.01,0.02 --cores 2,4 --repeats 1                                                  # quick dev grid -> $MUSIN_DATA/benchmark/dev/
```

[F5.2](docs/features/F5.2-pagerank-gds-vs-graphframes.md) compares unweighted PageRank in Neo4j GDS and Spark GraphFrames on the loaded graph (D-042). After `docker compose build spark`, which adds the pinned GraphFrames jars, run the GDS half with Neo4j up and the GraphFrames half with it stopped:

```bash
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine gds           # ~2 min
docker compose stop neo4j
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine graphframes   # ~25 min; writes benchmark_pagerank_*.csv
docker compose start neo4j
```

[F5.3](docs/features/F5.3-pagerank-cugraph.md) adds RAPIDS cuGraph on the GPU as a third engine (D-043). This is optional: it needs Docker Desktop's NVIDIA GPU support, a 5.83 GiB image, and accepting the NVIDIA Deep Learning Container License. It runs in the `rapids` container (compose profile `gpu`); the report is then rewritten from the Spark container:

```bash
docker compose --profile gpu build rapids
docker compose --profile gpu run --rm rapids python -m musin.benchmark.pagerank_gpu --config config/pipeline.yaml
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --report
```

**Report figures and tables** ([F6.1](docs/features/F6.1-report-figures-tables.md), D-044). The stage reads only `reports/tables/`, so it needs neither Neo4j nor a Spark session and takes about 2 s. It adds the figures the analysis stages don't draw, the main-graph-vs-variant comparison, report-ready tables in Markdown and LaTeX (`reports/tables/report/`), and the manifest [`reports/README.md`](reports/README.md), which lists every figure and table with the command that makes it. Re-run it after any stage that rewrites a report table. It stops if the tables come from different builds of a graph:

```bash
docker compose exec spark python -m musin.report --config config/pipeline.yaml
```

**Demo** ([F6.2](docs/features/F6.2-demo.md)). The 8 saved Neo4j Browser queries are in `neo4j/10_demo.cypher`, with their parameters as `:param` lines in its header. Drag the file into the Browser's Favorites. Before presenting, run every query once with those parameters on the loaded graph (read-only, about 6 s):

```bash
docker compose exec spark python -m musin.demo_check --config config/pipeline.yaml
```

| Service | URL |
|---|---|
| Jupyter Lab (PySpark 3.5.9, Python 3.11) | http://localhost:8888 |
| Spark UI (while a job runs) | http://localhost:4040 |
| Neo4j Browser (5.26.30 + GDS 2.13.12), user `neo4j` | http://localhost:7474 |

Ports listen on `127.0.0.1` only. Spark and Neo4j heavy jobs must not run at the same time (18 GB budget). Settings that change results live in [`config/pipeline.yaml`](config/pipeline.yaml); per-machine settings and secrets live in `.env`. Details: [F1.1 build log](docs/features/F1.1-dev-environment.md).

## Verify the build

How to check, look at, and reproduce what has been built. Run everything from WSL Ubuntu in the repo root (`wsl -d Ubuntu-24.04 --cd <repo>` from PowerShell, since the default WSL distro may be `docker-desktop`), with `docker compose up -d spark neo4j`. Expected numbers are for the final post-G1 settings: the full graph is in Results, and the 5 % dev sample has 283,056 artists and 408,056 edges (F6.5 clean-clone run, 2026-10-20).

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

**4. Prove a rebuild is identical.** Every build records order-independent content fingerprints; equal fingerprints mean identical tables. The post-G1 dev sample's `edges` fingerprint is `598257284874031480146`. A sample graph built before G1 (2026-10-11, 535,372 edges) fails the first rebuild's run-to-run check (−24 % edges); run `graph.sample` once with `--accept-changes` to replace it.

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

**6. Re-run the clean-clone check** ([F6.5](docs/features/F6.5-readme-reproducibility.md), NFR-01). A second checkout on the same machine must not share the running `musin` project's containers, images, ports or data. Clone into a scratch folder, give it its own data root (hard links to `dumps/`, `raw/` and the four full artist-level `clean/` tables cost no disk), and add a compose project name and an override file:

```bash
git clone https://github.com/Pgramer1/musin.git ~/f65-check/musin && cd ~/f65-check/musin
mkdir -p ~/f65-check/data/clean && cp -al ~/musin-data/{dumps,raw} ~/f65-check/data/
for t in artists special_artists artist_genres artist_primary_genre; do cp -al ~/musin-data/clean/$t ~/f65-check/data/clean/; done
cp .env.example .env    # MUSIN_DATA=$HOME/f65-check/data, a password, NEO4J ports 17474/17687, sample-mode memory
echo COMPOSE_PROJECT_NAME=musin-f65 >> .env
cat > docker-compose.override.yml <<'YAML'
services:
  spark: {image: "musin-spark:f65check", ports: !override ["127.0.0.1:18888:8888", "127.0.0.1:14040:4040"]}
  neo4j: {image: "musin-neo4j:f65check"}
  rapids: {image: "musin-rapids:f65check"}
YAML
```

Then follow Getting started steps 3 and 4 (`--verify-only`) and the sample-mode-only block from `clean.credits --sample 0.05` on. Remove it afterwards with `docker compose down`, `docker image rm musin-spark:f65check musin-neo4j:f65check` and by deleting `~/f65-check`.

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
reports/           generated tables and figures (profile_*: F1.4, genre_*: F2.5, graph_build_*: F3.1, graph_checks: F3.3, sample_*: F3.2, centrality_*: F4.1, communities_*: F4.2, trends_*: F4.4, separation_*: F4.5, bridges_*/genre_matrix: F4.3, validation_*: F4.6, variants/: F4.7, benchmark_*: P5, sensitivity_*/pipeline_funnel/report/: F6.1; manifest in reports/README.md)
tests/             pytest + fixtures (neo4j_toy/: F1.5 toy graph)
docker-compose.yml services spark and neo4j, plus rapids (profile gpu, F5.3 only)
.env.example       template for per-machine settings and secrets
```

`notebooks/` and `app/` (PRD layout) are added if P6 needs them; nothing in the pipeline depends on them.

## Results

*Added during P4 (analysis) and P6 (report).* Every figure and table, by report section, is listed in [`reports/README.md`](reports/README.md) (F6.1); report-ready versions are in `reports/tables/report/`.

**Data profile (F1.4):** 40.5 M recordings, 2.0 M credited artists; 86% of recordings have one credited artist; 92.6% have a first release year; genre tags cover 27% of credited artists (72% of artist appearances); producer credits cover 4.4% of recordings. Details and G1 evidence: [F1.4 build log](docs/features/F1.4-profiling.md), tables in `reports/tables/profile_*.csv`.

**Genres (F2.5):** 15 parent genres cover 97.5% of genre votes; 26.6% of credited artists (69.9% of appearances) get a primary genre, led by electronic (26.7%) and rock (18.0%). Details: [F2.5 build log](docs/features/F2.5-genre-normalization.md), tables in `reports/tables/genre_*.csv`.

**Graph (F3.1, final G1 settings, rebuilt 2026-10-19):** 1.14 M artists and 3.08 M edges (1.43 M co-credited, 0.73 M featured, 0.24 M produced-for, 0.68 M performed-for), each recording counted once per artist pair. 22.0% of artists have a primary genre, and 33.2% of edges are labelled at both ends. Classical artists (32,202) are left out of the main graph (D-039) and kept in the `classical` variant (1.18 M artists, 3.54 M edges, the old provisional graph). Details: [F3.1 build log](docs/features/F3.1-edge-building.md), `reports/tables/graph_build_summary.csv`.

**Spark scaling (F5.1, FR-21):** the full recording chain (credits → contributions → years → graph build, 40.5 M recordings → 3.08 M edges) takes 20.4 min on 1 core and 6.4 min on 8 (median of 3). Speedup is 1.8× at 2 cores, 2.7× at 4 and 3.2× at 8 (efficiency 0.40). About a quarter of the runtime is fixed cost, because every sample still scans the full raw tables, so 10 % of the data takes 27–32 % of the full time. Details: [F5.1 build log](docs/features/F5.1-spark-scaling.md), `reports/tables/benchmark_scaling_summary.csv`, `reports/figures/benchmark_scaling.png`.

**PageRank engines (F5.2, FR-22):** on the final graph (1.14 M artists, 2.86 M pairs, unweighted, 4 cores each), Neo4j GDS and Spark GraphFrames give the same ranking: identical top 100, Spearman ρ 0.99999, scores within 1.2 × 10⁻⁶. GDS computes it in 3.8 s, GraphFrames in 470 s (123×); end to end it's 30 s against 8 min. GraphFrames saves the one-time Neo4j import (about 2.5 min) but loses far more on every algorithm run, which supports the Spark-for-ETL, GDS-for-algorithms split. RAPIDS cuGraph on the laptop GPU (F5.3) gives the same top 100 in 0.2 s (1.1 s end to end), 19× faster than GDS, but the project needs Neo4j anyway for the demo, Cypher paths and write-back. Details: [F5.2](docs/features/F5.2-pagerank-gds-vs-graphframes.md), [F5.3](docs/features/F5.3-pagerank-cugraph.md), [P5](docs/phases/P5-benchmarks.md), `reports/tables/benchmark_pagerank_*.csv`.

*P4 results below are from the final post-G1 full graph (classical excluded, D-039; spoken word still included, D-040), re-run 2026-10-19/20. The classical-included and Character-included sensitivity runs are in `reports/tables/variants/`.*

**RQ1 — genre bridges (F4.3, F4.6):** 75.8 % of labelled collaboration weight stays within one genre (jazz 81 %, hip hop 73 %, electronic 62 % within-genre; r&b & soul, punk, experimental and metal send most of their weight to other genres; see `genre_matrix.png`). The top bridges are the world-music label Putumayo, producers and session players (Adrian Quesada, Gabe Noel, Keefus Ciancia), and roots and world-music artists (Ry Cooder, Amadou & Mariam, Baaba Maal, Youssou N'Dour). Of the 15 pre-registered cross-genre artists, 14 are in the graph (Yo‐Yo Ma is classical); 13 rank in the top 1,000 by betweenness (median rank 103), and 3 are in the bridge top 100 (Ry Cooder #10, Santana #66, Mark Ronson #80). Weight-based diversity under-rates artists with a large home genre (Quincy Jones, Herbie Hancock). Details: [F4.3](docs/features/F4.3-genre-bridges.md), [F4.6](docs/features/F4.6-validation.md).

**RQ2 — communities vs genres (F4.2):** 72,258 Louvain communities (modularity 0.868). Communities match primary genres only partly: AMI 0.224 inside the largest component (NMI 0.241 overall). Some communities are near-pure genres (EDM 83 %, jazz 77 %); others are scenes spanning genres (US hip hop with electronic, Japanese pop and anime music, reggae and dub with UK grime, German hip hop, European jazz with experimental). Details: [F4.2](docs/features/F4.2-communities.md).

**RQ3 — trends (F4.4):** the cross-genre share of labelled weight rose from 16 % (1960s) to 24–25 % (1970s–80s), dipped to 21 % (1990s) and climbed to 28 % in the 2020s. Featuring credits rose from 3–4 % of billed collaborations before 1990 to 23 % in the 2010s. Genre coverage halves over the same period (61 % → 29 %). With classical included, the cross-genre share was U-shaped instead, so the answer depends on the OD1 choice. Details: [F4.4](docs/features/F4.4-cross-genre-trends.md), `reports/figures/trends_by_decade.png`.

**RQ4 — centrality (F4.1):** weighted degree, PageRank and betweenness agree moderately overall (Spearman ρ 0.51–0.67) but barely at the top (degree and betweenness have 9 artists in common in their top 100, Jaccard 0.047). Weighted degree is dominated by content artefacts: Springsteen's live archive and audiobook authors and narrators, pending the open spoken-word decision. Betweenness surfaces cross-scene connectors (Snoop Dogg, Ye, Busta Rhymes, Pitbull, Sting, Elton John). Sampled betweenness moves between re-imports of the same graph (top-20 overlap 0.67), so only ranks and percentiles should be quoted. With Character artists included, the Vocaloid 初音ミク (Hatsune Miku) is #1 by PageRank, which supports excluding them (D-021). Details: [F4.1](docs/features/F4.1-centrality.md), [F4.7](docs/features/F4.7-sensitivity-variants.md).

## Documentation

| Document | What it covers |
|---|---|
| [PRD.md](PRD.md) | Scope, research questions, data, architecture, schema contract, features, schedule, risks |
| [RULES.md](RULES.md) | How we work: documentation, git, code, data, Neo4j, analysis integrity |
| [docs/README.md](docs/README.md) | Build log: what was built in each phase and feature, and why |
| [docs/decisions.md](docs/decisions.md) | Every major decision, with its reasoning |
| [report/report.md](report/report.md) | Final report draft (F6.3), every number from `reports/tables/`; slides in `report/musin-slides.pptx` (F6.4) |
| [reports/README.md](reports/README.md) | Every report figure and table, the stage that makes it, and the command (generated by F6.1) |
| [CLAUDE.md](CLAUDE.md) | Instructions for Claude Code in this repo |

## Team

| Member | Name | Role |
|---|---|---|
| A | `<name>` | Data engineering: environment, Spark ETL, Neo4j import, benchmarks |
| B | `<name>` | Graph analytics: Neo4j/GDS, research questions, validation, demo |

## Data and attribution

Data comes from [MusicBrainz](https://musicbrainz.org/), run by the MetaBrainz Foundation. Core data is CC0; tags come from the derived dump under CC BY-NC-SA 3.0. No MusicBrainz data is stored in this repository.
