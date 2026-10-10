<!--
Keep this README current (RULES.md §1.3). Update it whenever:
  - a phase changes status          → Status line + Progress table
  - a feature adds or changes setup → Getting started
  - results become available        → Results
-->

# Musin

**Bridging Genres: A Big-Data Analysis of the MusicBrainz Collaboration Network**

A Big Data Analytics semester project. We process the full MusicBrainz database export with PySpark, build an artist collaboration graph, and analyse it in Neo4j to find which artists connect music genres.

> **Status:** P1 Environment & ingestion in progress (dev environment done; dump download next) · *Last updated 2026-10-10*

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
| P1 Environment & ingestion | Oct 10–16 | In progress |
| P2 Cleaning & genre normalization | Oct 15–21 | Planned |
| P3 Graph build & Neo4j load | Oct 18–24 | Planned |
| P4 Graph analytics | Oct 19–30 | Planned |
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

| Service | URL |
|---|---|
| Jupyter Lab (PySpark 3.5.9, Python 3.11) | http://localhost:8888 |
| Spark UI (while a job runs) | http://localhost:4040 |
| Neo4j Browser (5.26.30 + GDS 2.13.12), user `neo4j` | http://localhost:7474 |

Ports listen on `127.0.0.1` only. Spark and Neo4j heavy jobs must not run at the same time (18 GB budget). Settings that change results live in [`config/pipeline.yaml`](config/pipeline.yaml); per-machine settings and secrets live in `.env`. Details: [F1.1 build log](docs/features/F1.1-dev-environment.md).

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
tests/             pytest
docker-compose.yml two services: spark, neo4j
.env.example       template for per-machine settings and secrets
```

More folders (`neo4j/`, `notebooks/`, `reports/`) are added as the phases progress.

## Results

*Added during P4 (analysis) and P6 (report).*

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
