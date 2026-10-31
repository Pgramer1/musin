# P0 — Planning

| | |
|---|---|
| **Status** | Done |
| **Planned dates** | 2026-10-10 |
| **Actual dates** | 2026-10-10 |
| **Data snapshot** | Not chosen yet (OD6, decided in F1.2) |

## 1. Goal

Turn the initial idea into a scoped, buildable plan for a one-month Big Data Analytics project, and set up how the project is run and documented.

## 2. What was built

No code was written in this phase. It produced the project's planning documents:

| File | Purpose |
|---|---|
| [README.md](../../README.md) | Project overview and live status, updated as the project progresses |
| [PRD.md](../../PRD.md) | Scope, research questions, metric definitions, schema contract, features, schedule, risks |
| [RULES.md](../../RULES.md) | Working conventions, including these documentation rules |
| [CLAUDE.md](../../CLAUDE.md) | Guidance for Claude Code working in the repo |
| [docs/README.md](../README.md) | Build-log index of every phase and feature |
| [docs/decisions.md](../decisions.md) | Decision log, D-001 to D-010 |
| [features/_TEMPLATE.md](../features/_TEMPLATE.md), [phases/_TEMPLATE.md](_TEMPLATE.md) | Templates for the build log |

## 3. How it fits together

**Starting point.** The original pitch was:

> **Music Collaboration Network.** Discover which artists connect different music genres and who has the most influential collaboration network. Tech: Neo4j · Data: free Wikidata query/API · Difficulty: Easy–Medium.

A written review of that pitch came with it; its findings are summarised below. The original file (`idea.md.txt`) was deleted on 2026-10-10 once its content had been folded into this doc and the PRD.

**What the review found, and how the plan responds:**

| Finding | Response in the plan |
|---|---|
| As written, it was a graph project, not a big-data project | Full MusicBrainz dumps processed with Spark (D-002, D-003) |
| Billing credits and typed relationships were conflated | Four separate edge types (D-008, PRD §8.2) |
| Genre tags are noisy user votes | Official-genre filter + parent-genre mapping (FR-08) |
| Hub artists and N² edge explosion | Special-artist filter, `max_credit_size`, band-member filter (FR-04, FR-10, FR-11) |
| "Influence" and "bridge" were undefined | Formal metrics (PRD §5) |
| No validation | Pre-registered validation list (PRD §5.6) |

**Where we departed from the review's suggestions:**

- *Keep the bipartite Artist–Recording graph in Neo4j* → the projection is done in Spark instead, because tens of millions of recording nodes won't fit comfortably on a 24 GB laptop (D-005).
- *Spark vs pandas benchmark* → replaced by Spark scaling plus a graph-engine comparison (D-009).
- *Streamlit dashboard* → made optional (D-010).

**Constraints recorded:** about one month (2026-10-10 to ~2026-11-09); no data-size requirement; Spark/Hadoop/Hive optional but valued; reference machine with 24 GB RAM and an RTX 4060 (8 GB).

**How the build log works from here.** Each feature gets a doc in `docs/features/` when it's done, each phase gets a doc in `docs/phases/` when it closes, and `docs/README.md` tracks the status of all of them (RULES.md §1).

## 4. Key numbers

n/a: no data has been processed yet. The first dataset sizes arrive with F1.3 and F1.4.

## 5. Decisions made

- [D-001](../decisions.md#d-001--musicbrainz-is-the-only-data-source): MusicBrainz is the only data source
- [D-002](../decisions.md#d-002--full-database-dumps-not-the-web-api): full dumps, not the API
- [D-003](../decisions.md#d-003--pyspark-for-etl-layered-parquet-storage): PySpark ETL, layered Parquet
- [D-004](../decisions.md#d-004--no-hadoophdfs-or-hive): no Hadoop/HDFS or Hive
- [D-005](../decisions.md#d-005--project-to-artistartist-edges-in-spark): project to artist–artist edges in Spark
- [D-006](../decisions.md#d-006--neo4j-5-community--gds-for-graph-analytics): Neo4j 5 Community + GDS
- [D-007](../decisions.md#d-007--wsl2--docker-compose-environment): WSL2 + Docker Compose
- [D-008](../decisions.md#d-008--separate-credit-edges-from-contribution-edges): separate credit and contribution edges
- [D-009](../decisions.md#d-009--benchmarks-spark-scaling--graph-engine-comparison): benchmark design
- [D-010](../decisions.md#d-010--demo-in-neo4j-browser-streamlit-optional): demo in Neo4j Browser, Streamlit optional
- [D-011](../decisions.md#d-011--contract-v1-clarifications): contract v1 clarifications (config and `.env` contracts, decade weights, genre attribution, import CSVs, provisional values for the open decisions)

## 6. Deviations from the plan

n/a: this is the first phase.

## 7. Known issues and limitations

- Open decisions OD1–OD7 (PRD §14) are unresolved until gate G1 on Oct 16.
- The exact submission deadline still needs confirming.

## 8. How to reproduce this phase

n/a.

## 9. Handoff to the next phase

Before P1 work starts:

- [ ] Confirm the deadline
- [x] Create the Git repo (remote: `github.com/Pgramer1/musin`) and add `.gitignore` + `.gitattributes`
- [ ] Install WSL2 (Ubuntu) and Docker on both machines
- [ ] Check that the reference machine has ≥ 100 GB of free disk
- [ ] Agree schema contract v1 (PRD §8) by Oct 13 (M1)
