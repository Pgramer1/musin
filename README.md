# Musin

**Bridging Genres: A Big-Data Analysis of the MusicBrainz Collaboration Network**

Musin turns the complete [MusicBrainz](https://musicbrainz.org/) database export (40.5 million recordings) into a network of who makes music with whom, then asks which artists connect genres that otherwise keep to themselves. PySpark does the heavy lifting on the raw data, Neo4j with the Graph Data Science library runs the graph algorithms, and everything runs in Docker on a single 24 GB machine.

![Share of each genre's collaborations, by partner genre](reports/figures/genre_matrix.png)

## What it answers

1. **Genre bridges:** which artists connect otherwise separate genres?
2. **Communities vs genres:** do the communities formed by collaboration match genre labels?
3. **Trends:** has cross-genre collaboration increased over the decades?
4. **Centrality:** who is most central, and do different centrality measures agree?

The exact metric definitions are in [PRD §5](PRD.md#5-research-questions).

## Key findings

All numbers come from the generated tables in [`reports/tables/`](reports/tables/); [`reports/README.md`](reports/README.md) lists every figure and table with the command that makes it.

- **The graph:** 1.14 M artists and 3.08 M collaborations of four kinds: co-credited (1.43 M), featured (0.73 M), produced-for (0.24 M) and performed-for (0.68 M). Classical music is analysed separately, because composers billed on every recording of their works would otherwise dominate the graph.
- **Genres mostly stay home.** 75.8 % of collaboration stays within one genre: jazz 81 %, hip hop 73 %, electronic 62 %. R&B & soul, punk, experimental and metal send most of theirs to other genres.
- **Bridges are rarely the stars.** The top genre bridges are producers and session players (Adrian Quesada, Gabe Noel, Keefus Ciancia), the world-music label Putumayo, and roots and world-music artists (Ry Cooder, Amadou & Mariam, Baaba Maal, Youssou N'Dour).
- **Communities are scenes, not genres.** Louvain finds 72,258 communities (modularity 0.868) that match genre labels only partly (AMI 0.224). Some are near-pure genres (EDM, jazz); others are scenes such as Japanese pop with anime music, or reggae and dub with UK grime.
- **Cross-genre work is growing.** Its share rose from 16 % in the 1960s to 28 % in the 2020s, and featuring credits went from 3–4 % of billed collaborations before 1990 to 23 % in the 2010s. Genre tags thin out for recent artists, so read the trend with that in mind.
- **"Most central" depends on the measure.** Weighted degree, PageRank and betweenness agree moderately overall (Spearman ρ 0.51–0.67) but share only 9 artists in their top 100s. Betweenness surfaces cross-scene connectors such as Snoop Dogg, Busta Rhymes, Pitbull, Sting and Elton John.
- **Right tool for each job.** On the same graph, PageRank takes 470 s in Spark GraphFrames, 3.8 s in Neo4j GDS and 0.2 s on a laptop GPU with RAPIDS cuGraph, all with the same top 100. The full Spark pipeline (40.5 M recordings → 3.08 M edges) takes 20.4 min on 1 core and 6.4 min on 8.

## How it works

```
MusicBrainz full export (mbdump + mbdump-derived, 8.1 GB compressed)
        │
        ▼
PySpark ETL ── raw → clean → graph   (Parquet layers under $MUSIN_DATA)
        │
        ▼
Neo4j + Graph Data Science ── PageRank, betweenness, Louvain, shortest paths
        │
        ▼
reports/ ── tables, figures, report
```

1. **Ingest:** download and verify a pinned MusicBrainz export, and convert the 22 tables we need into typed Parquet.
2. **Clean:** drop placeholder artists (Various Artists, [unknown], …), split credits into main and featured artists, extract producer and performer credits, find each recording's first release year, and map free-form tags to 15 parent genres.
3. **Build the graph:** one node per artist, one typed, weighted edge per collaborating pair (weight = shared recordings), skipping recordings with more than 10 credited artists and edges between a band and its own members. 49 automatic checks run after every build.
4. **Analyse:** bulk-import into Neo4j and run centrality, communities, genre-bridge scores, decade trends and shortest paths. Results are written back to the graph and saved as CSV and PNG under `reports/`.

| Layer | Technology |
|---|---|
| Environment | WSL2 (Ubuntu) + Docker Compose |
| Processing | PySpark 3.5, Python 3.11 |
| Storage | Parquet |
| Graph database | Neo4j 5 Community + Graph Data Science 2.13 |
| Analysis | pandas, scikit-learn, matplotlib, Jupyter |
| Benchmarks (optional) | Spark GraphFrames, RAPIDS cuGraph (GPU) |

## Getting started

### Requirements

- Windows 11 with WSL2 (Ubuntu), or Linux
- Docker Desktop (WSL2 backend) or Docker Engine with Compose
- 24 GB RAM for the full dataset (8 GB is enough for sample mode)
- 100 GB+ free SSD space for the dumps and intermediate data

### 1. Set up WSL (Windows only)

In PowerShell:

```powershell
wsl --install -d Ubuntu-24.04
```

In Docker Desktop, go to **Settings → Resources → WSL integration** and enable the Ubuntu distro. Every command from here on runs in that Ubuntu shell (`wsl -d Ubuntu-24.04`).

### 2. Clone and cap WSL's memory

Clone into the WSL filesystem (e.g. `~/musin`); a checkout under `/mnt/c/` works but is slower.

```bash
git clone https://github.com/Pgramer1/musin.git ~/musin && cd ~/musin
cp setup/wslconfig.example /mnt/c/Users/<windows-user>/.wslconfig   # caps WSL at 18 GB; merge if you already have one
```

Then run `wsl --shutdown` in PowerShell, restart Docker Desktop, and reopen the Ubuntu shell in `~/musin`.

### 3. Configure and start the containers

```bash
cp .env.example .env              # set MUSIN_DATA=/home/<you>/musin-data and NEO4J_PASSWORD (8+ chars)
bash setup/preflight.sh           # checks the machine and creates the $MUSIN_DATA folders
docker compose build              # ~7 min the first time
docker compose up -d
docker compose exec spark python -m musin.envcheck --config config/pipeline.yaml
```

On a machine with less than 24 GB RAM, lower the three memory values in `.env` as its comments say, and use sample mode. If the build stops with a `ReadTimeoutError` from PyPI, run it again; finished layers are cached.

### 4. Get the data

```bash
docker compose exec spark python -m musin.ingest.download --config config/pipeline.yaml
```

This downloads 8.1 GB and checks the GPG signature and SHA-256 sums. The results in this repo come from the snapshot `20261007-002147`, pinned as `snapshot_date` in [`config/pipeline.yaml`](config/pipeline.yaml). MusicBrainz keeps only the two most recent weekly exports online, so that snapshot is no longer downloadable:

- **To use a newer export,** run the command with `--show-latest`, put the name it prints in `snapshot_date`, and download. Your numbers will differ slightly from the ones above. If the export's schema sequence has changed, regenerate the table schemas first ([F1.3](docs/features/F1.3-raw-ingest.md)).
- **If you already have the pinned dumps,** copy them into `$MUSIN_DATA/dumps/20261007-002147/` and run the command with `--verify-only`.

## Running the pipeline

Run each stage in order with `docker compose exec spark python -m musin.<stage> --config config/pipeline.yaml`. Every stage reads only the layer before it and rewrites its whole output, so any stage can be re-run.

**Memory rule:** Spark and Neo4j share an 18 GB budget, so stop Neo4j (`docker compose stop neo4j`) during heavy Spark stages and start it again (`docker compose start neo4j`) for the import and analysis steps.

| # | Stage | Module | Full-data time |
|---|---|---|---|
| 1 | Unpack the 22 needed tables (≈17.5 GB of text) | `musin.ingest.extract` | ~2.5 min |
| 2 | Convert them to typed Parquet in `raw/` | `musin.ingest.raw` | ~3 min |
| 3 | Profile the data (optional) → `reports/tables/profile_*` | `musin.profiling` | ~6.5 min |
| 4 | Clean artists (drop placeholders) | `musin.clean.artists` | ~20 s |
| 5 | Parse credits into main and featured artists | `musin.clean.credits` | ~1 min |
| 6 | Extract producer and performer credits | `musin.clean.contributions` | ~45 s |
| 7 | First release year per recording | `musin.clean.years` | ~3 min |
| 8 | Map tags to 15 parent genres | `musin.clean.genres` | ~45 s |
| 9 | Build the graph | `musin.graph.build` | ~3 min |
| 10 | Run the 49 graph checks | `musin.graph.checks` | ~30 s |
| 11 | Export CSVs for Neo4j | `musin.export.neo4j_csv` | ~30 s |

Then load the graph into Neo4j (this **replaces** whatever graph is loaded) and run the analyses:

```bash
docker compose start neo4j
bash neo4j/import_graph.sh full --overwrite        # bulk import + 39 checks + indexes and GDS projections

docker compose exec spark python -m musin.analytics.centrality  --config config/pipeline.yaml   # degree, PageRank, betweenness (≈3 h on the full graph)
docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml   # Louvain communities vs genres
docker compose exec spark python -m musin.analytics.bridges     --config config/pipeline.yaml   # genre-bridge scores and genre matrix (needs centrality)
docker compose exec spark python -m musin.analytics.validation  --config config/pipeline.yaml   # where well-known cross-genre artists rank
docker compose exec spark python -m musin.analytics.trends      --config config/pipeline.yaml   # cross-genre share by decade
docker compose exec spark python -m musin.analytics.separation  --config config/pipeline.yaml --pair "Frank Sinatra" "Snoop Dogg"
docker compose exec spark python -m musin.report                --config config/pipeline.yaml   # report-ready figures and tables
```

Tables and figures land in `reports/tables/` and `reports/figures/`.

### Sample mode

For development or smaller machines, add `--sample 0.05` to the recording-level stages (credits, contributions, years, graph build, checks, export, trends). It keeps 5 % of recordings, chosen by a hash of the recording id, so every run picks the same ones. Outputs go to `$MUSIN_DATA/sample/f0.05/`.

Sample mode still needs the full `raw/` layer and the full artist-level tables, so run stages 1–2, 4, 5 and 8 once on the full data first. Then build the whole sample graph in one go (~1.5 min) and load it:

```bash
docker compose exec spark python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --sample 0.05
bash neo4j/import_graph.sh sample-f0.05 --overwrite
```

The analysis commands then run on the sample graph and write their tables to `$MUSIN_DATA/sample/f0.05/reports/tables/`.

### Sensitivity variants

A variant is the main graph with a few filters overridden under `variants.<name>` in `config/pipeline.yaml`. Two are defined: `characters` keeps fictional "Character" artists, and `classical` keeps classical music. Add `--variant <name>` to `graph.build`, `graph.checks` and `export.neo4j_csv`, import it as `full-<name>` (or `sample-f0.05-<name>`), run the analyses, then re-import the main graph. Details: [F4.7](docs/features/F4.7-sensitivity-variants.md).

### Benchmarks (optional)

```bash
docker compose exec spark python -m musin.benchmark.scaling  --config config/pipeline.yaml                      # Spark scaling, data fraction × cores (~5.5 h; Neo4j stopped)
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine gds          # PageRank in Neo4j GDS (~2 min)
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine graphframes  # PageRank in Spark GraphFrames (~25 min; Neo4j stopped)
```

The GPU run with RAPIDS cuGraph needs Docker's NVIDIA GPU support and a 5.83 GiB image: see [F5.3](docs/features/F5.3-pagerank-cugraph.md).

## Exploring the graph

| Service | URL |
|---|---|
| Neo4j Browser (user `neo4j`, password from `.env`) | http://localhost:7474 |
| Jupyter Lab (PySpark) | http://localhost:8888 |
| Spark UI (while a job runs) | http://localhost:4040 |

Ports listen on `127.0.0.1` only. A few queries to try in Neo4j Browser (on an edge, `weight` is the number of shared recordings and `firstYear`/`lastYear` give their span):

```cypher
// one artist's strongest collaborations, drawn as a graph
MATCH (a:Artist {name: 'Snoop Dogg'})-[r]-(b:Artist)
RETURN a, r, b ORDER BY r.weight DESC LIMIT 40;

// degrees of separation between two artists
MATCH (a:Artist {name: 'Tom Petty'}), (b:Artist {name: 'Snoop Dogg'})
MATCH p = shortestPath((a)-[:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR*..10]-(b))
RETURN p;

// genre profile of an artist
MATCH (a:Artist {name: 'Daft Punk'})-[r:IN_GENRE]->(g:Genre)
RETURN g.name, r.share ORDER BY r.share DESC;
```

More saved queries, with their parameters, are in [`neo4j/10_demo.cypher`](neo4j/10_demo.cypher); drag the file into the Browser's Favorites. `docker compose exec spark python -m musin.demo_check --config config/pipeline.yaml` runs them all once as a check.

The Parquet layers can be read directly from Jupyter:

```python
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
edges = spark.read.parquet("/data/graph/edges")
nodes = spark.read.parquet("/data/graph/nodes_artist")
edges.groupBy("edge_type").count().show()
```

## Tests and checks

```bash
docker compose exec spark pytest -q
docker compose exec spark ruff check src tests
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml   # add --sample 0.05 for the sample graph
```

The graph checks compare row counts with the last passing run and fail on a change above 5 %. After a deliberate config change, run them once with `--accept-changes`.

## Configuration

- [`config/pipeline.yaml`](config/pipeline.yaml) holds every setting that changes results: snapshot, filters, genre mapping, algorithm parameters. A missing key fails loudly; there are no hidden defaults in code.
- `.env` (from [`.env.example`](.env.example)) holds per-machine settings and secrets: data path, Neo4j password, ports and memory.

## Repository layout

```
src/musin/         pipeline code, one module per stage (python -m musin.<stage>)
config/            pipeline.yaml, genre mapping, special-artist list, validation list
neo4j/             numbered Cypher files and the import script
docker/            Spark/Jupyter and Neo4j/GDS images
setup/             WSL memory template and machine check
tests/             pytest suite and small fixtures
reports/           generated tables and figures (manifest: reports/README.md)
docs/              build log: a document per feature and phase, and the decision log
PRD.md             requirements: scope, metric definitions, schema
```

## Further reading

| Document | What it covers |
|---|---|
| [reports/README.md](reports/README.md) | Every figure and table, and the command that makes it |
| [PRD.md](PRD.md) | Scope, research questions, metric definitions, data model |
| [docs/README.md](docs/README.md) | Build log: what was built at each step, and why |
| [docs/decisions.md](docs/decisions.md) | Every design decision, with its reasoning |

## Data and attribution

Data comes from [MusicBrainz](https://musicbrainz.org/), run by the MetaBrainz Foundation. Core data is CC0; tags come from the derived dump under CC BY-NC-SA 3.0. No MusicBrainz data is stored in this repository. Figures and tables in `reports/` are derived from that data, so the genre-based ones carry the same CC BY-NC-SA 3.0 terms.

## License

The code is released under the [MIT License](LICENSE). The MusicBrainz data it processes is not covered by this license; see above.
