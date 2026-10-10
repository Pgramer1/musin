# P1 — Environment & ingestion

| | |
|---|---|
| **Status** | Done |
| **Planned dates** | 2026-10-10 – 2026-10-16 |
| **Actual dates** | 2026-10-10 (all five features in one day) |
| **Owners** | A (F1.1–F1.3), A + B (F1.4), B (F1.5) |
| **Data snapshot** | MusicBrainz full export `20261007-002147` (schema sequence 31, replication sequence 189552) |

## 1. Goal

From PRD §11: a pinned, reproducible environment on the reference machine; the MusicBrainz full export downloaded and verified; every table the project needs parsed with an explicit schema into typed Parquet; a profiling report that gives gate G1 the evidence for the open decisions; and a working Neo4j + GDS path, proven on a toy graph, so member B can build analysis code before the real graph exists.

## 2. What was built

| Feature | Status | Doc |
|---|---|---|
| F1.1 — Dev environment | Done | [F1.1-dev-environment.md](../features/F1.1-dev-environment.md) |
| F1.2 — Dump download and snapshot pinning | Done | [F1.2-dump-download.md](../features/F1.2-dump-download.md) |
| F1.3 — Table schemas and raw ingest | Done | [F1.3-raw-ingest.md](../features/F1.3-raw-ingest.md) |
| F1.4 — Data profiling report | Done | [F1.4-profiling.md](../features/F1.4-profiling.md) |
| F1.5 — Neo4j + GDS smoke test | Done | [F1.5-neo4j-smoke-test.md](../features/F1.5-neo4j-smoke-test.md) |

## 3. How it fits together

```
Windows 11 ─ WSL2 Ubuntu (18 GB cap, .wslconfig)                                  F1.1
   └─ Docker Compose
        ├─ spark  (musin-spark:f1.1, PySpark 3.5.9, Jupyter)   /workspace = repo, /data = $MUSIN_DATA
        └─ neo4j  (musin-neo4j:f1.1, Neo4j 5.26.30 + GDS 2.13.12)   /import = $MUSIN_DATA/import (ro)

data.metabrainz.org  ──F1.2 download, GPG-signed SHA-256──▶  dumps/20261007-002147/{mbdump,mbdump-derived}.tar.bz2  (8.1 GB)
                     ──F1.3 extract 22 tables (lbzip2)────▶  dumps/…/extracted/  (17.6 GB COPY text)
                     ──F1.3 parse, explicit schemas───────▶  raw/<table>/  (9.1 GB Parquet, 172.4 M rows)
raw/                 ──F1.4 profiling─────────────────────▶  reports/tables/profile_*.csv, reports/figures/profile_*.png
tests/fixtures/neo4j_toy ─F1.5 neo4j-admin import─────────▶  Neo4j `neo4j` db ─ GDS projections, algorithms vs reference
```

- **F1.1** sets up two containers and the two settings contracts: `config/pipeline.yaml` for anything that changes results, `.env` for secrets and per-machine values. A missing key fails loudly.
- **F1.2** pins the snapshot (OD6). Checksums are verified against MusicBrainz's signed `SHA256SUMS`, so the download is trusted end to end.
- **F1.3** turns headerless COPY text into typed Parquet. It uses built-in Spark functions only and is strict: wrong field counts or malformed values fail the stage, so `raw/` is either an exact copy of the dump or doesn't exist.
- **F1.4** reads `raw/` once and writes the numbers G1 needs: credit sizes (OD3), the threshold grid (OD2), classical share (OD1), genres (OD4), contribution coverage (OD5, OD7).
- **F1.5** proves the downstream half, from §8.3 CSVs through `neo4j-admin` to GDS. Every algorithm result is checked against an independent Python implementation. It found two contract gaps, fixed by D-016 and D-017, before any real data was loaded.

## 4. Key numbers

| Item | Value |
|---|---|
| Download | 8.1 GB (2 archives), 757 s at ≈ 10.5 MB/s; `--verify-only` 8 s |
| Extraction | 22 tables, 17,569,799,300 bytes of text, 136 s |
| Raw ingest | 172,355,893 rows, 9,127,621,763 bytes Parquet, 157 s |
| Largest tables | `track` 58,292,678 · `recording` 40,475,049 · `l_artist_recording` 19,009,084 |
| Profiling | full 383 s, 5 % sample 185 s; 12 CSVs + 3 figures |
| Credited artists | 2,020,979 (of 3,005,875 in `artist`); 48,270,286 artist–recording appearances |
| Credit graph preview (`min_recordings` 1, credits ≤ 10) | 910,299 nodes with an edge, 2,406,037 credit-pair edges |
| Coverage | first release year 92.6 % of recordings; genre tag 27.2 % of credited artists (72.3 % of appearances); producer credit 4.4 % of recordings |
| Smoke test | 31/31 checks, 30–31 s end to end; GDS = reference within 1e-9 |
| Tests | 62 `pytest` tests, `ruff` clean |

## 5. Decisions made

- [D-012](../decisions.md#d-012--fixed-jupyterspark-ports-jupyter-without-a-token): fixed Jupyter/Spark UI ports; Jupyter without a token, bound to 127.0.0.1.
- [D-013](../decisions.md#d-013--snapshot-20261007-002147-verified-by-signed-checksums-od6): snapshot `20261007-002147`, verified by signed checksums (resolves OD6).
- [D-014](../decisions.md#d-014--raw-layer-22-tables-schema-31-generated-explicit-schemas): raw layer of 22 tables (adds `l_artist_release` for OD7), schema 31, generated explicit schemas, strict parsing.
- [D-015](../decisions.md#d-015--profiling-report-parameters-and-definitions): profiling parameters (`profiling.*` keys) and fixed definitions of the coverage figures.
- [D-016](../decisions.md#d-016--artistid-imported-as-an-integer-via-id-typelong): `artistId:ID(Artist){id-type:long}` in the §8.3 import header.
- [D-017](../decisions.md#d-017--collab-projection-one-relationship-per-artist-pair-weights-summed): COLLAB = one undirected relationship per artist pair, weights summed across types and directions.

## 6. Deviations from the plan

- **Schedule:** planned Oct 10–16, finished Oct 10. Gate G1 stays on Oct 16; the open decisions OD1–OD5 and OD7 are *not* resolved by this phase. F1.4 supplies the evidence and the team decides at G1.
- **Scope added within existing features:** `l_artist_release` joined the raw layer (D-014) so OD7 could be measured. F1.5 changed the schema contract text (D-016, still draft v1, so no version bump) and fixed the COLLAB merge rule (D-017).
- **Owner split:** F1.5 (owner B) was implemented in the same session as F1.1–F1.4.
- **No PRs:** features were merged directly to `main` at the team's request, instead of through the reviewed PRs in RULES §2.

## 7. Known issues and limitations

- **The snapshot expires from the server around 2026-10-21.** Keep `$MUSIN_DATA/dumps/20261007-002147/`; a teammate copies it and runs `--verify-only`.
- **`raw/` keeps Postgres types** (32-bit IDs, short years). `clean/` must widen them to the §8.1 types.
- **Data-quality issues seen in profiling,** to be handled in P2: special artists (`[unknown]` is the largest hub), 30,801 downvoted artist tags, implausible years (206 … 3030), voice-actor `(CV:` credits, and genre tags matched by exact name only.
- **D-016 awaits the second member's sign-off** at M1 (Oct 13), together with the rest of contract v1.
- **PageRank at `max_iterations` 20 did not converge on the toy graph,** and Louvain concurrency has no config key. F4.1/F4.2 must address both.
- **The concurrency rule is a working rule,** not enforced: nothing stops a heavy Spark job and Neo4j work from running together. Profiling peaked at a 13.1 GiB JVM.

## 8. How to reproduce this phase

From WSL Ubuntu, repo root, after the one-time setup in the root [README](../../README.md#getting-started):

```bash
bash setup/preflight.sh
docker compose build && docker compose up -d
docker compose exec spark python -m musin.envcheck --config config/pipeline.yaml
docker compose exec spark python -m musin.ingest.download --config config/pipeline.yaml   # or --verify-only on a copied dump
docker compose exec spark python -m musin.ingest.extract --config config/pipeline.yaml
docker compose exec spark python -m musin.ingest.raw --config config/pipeline.yaml
docker compose exec spark python -m musin.profiling --config config/pipeline.yaml
bash neo4j/smoke/run_smoke.sh --overwrite
docker compose exec spark pytest
```

## 9. Handoff to the next phase

P2 (cleaning and genre normalization) can rely on:

- **`$MUSIN_DATA/raw/<table>/`:** 22 Parquet datasets whose schemas equal `musin.schemas.tables.SCHEMAS`, with row counts in `raw/_manifest.json`. They are faithful copies: nothing is filtered, `\N` → null, escapes decoded.
- **`musin.common`:** `load_config` (fails on missing keys), `get_spark`, `sample_by_hash` (deterministic FR-12 sampling), and, for P3/P4, `cypher.load_queries` and `neo4j.get_driver`.
- **`reports/tables/profile_*.csv`:** the evidence for G1 (OD1–OD5, OD7) and the reference sizes for P2/P3 sanity checks, e.g. 2,406,037 credit pairs before any filter.
- **The Neo4j path:** the §8.3 headers (with `{id-type:long}`), the `neo4j-admin` invocation in Docker, and the COLLAB projection query (`neo4j/smoke/03_projections.cypher`) are proven. F3.4 and F3.5 can reuse them. Remember that the database currently holds the toy graph.

Watch out for: the provisional OD values in `config/pipeline.yaml` until G1; widening raw types in `clean/`; handling downvoted tags and implausible years in F2.4/F2.5; never running Spark and Neo4j heavy jobs together.
