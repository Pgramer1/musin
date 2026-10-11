# P3 — Graph build & Neo4j load

| | |
|---|---|
| **Status** | Done |
| **Planned dates** | 2026-10-18 – 2026-10-24 |
| **Actual dates** | 2026-10-11 (all five features) |
| **Owners** | A (F3.1–F3.4); F3.5 assigned to B, built by A at the team's request |
| **Data snapshot** | MusicBrainz full export `20261007-002147` (schema sequence 31) |

## 1. Goal

From PRD §11: turn the P2 `clean/` layer into the artist collaboration graph of PRD §8 (typed, aggregated edges, edges per decade, nodes with genres), apply every graph-level filter (FR-09–FR-11, D-021, OD1/OD2), guarantee its quality (FR-13), make a deterministic dev sample (FR-12), and load both into Neo4j with constraints and the GDS projections P4 needs (FR-14). Milestones served: **M2** (sample graph in Neo4j for member B) and the groundwork for **M3** (full graph in Neo4j).

## 2. What was built

| Feature | Status | Doc |
|---|---|---|
| F3.1 — Edge building, aggregation, graph filters | Done (reworked once, D-028) | [F3.1-edge-building.md](../features/F3.1-edge-building.md) |
| F3.2 — Sample mode and dev sample graph | Done | [F3.2-dev-sample-graph.md](../features/F3.2-dev-sample-graph.md) |
| F3.3 — Graph-layer quality checks | Done | [F3.3-graph-quality-checks.md](../features/F3.3-graph-quality-checks.md) |
| F3.4 — Neo4j CSV export and bulk import | Done | [F3.4-neo4j-export-import.md](../features/F3.4-neo4j-export-import.md) |
| F3.5 — Constraints, indexes, GDS projections | Done | [F3.5-constraints-indexes-projections.md](../features/F3.5-constraints-indexes-projections.md) |

Also produced in this phase: the G1 evidence note [docs/g1-scope-gate.md](../g1-scope-gate.md) (OD1–OD7, spoken-word proposal, contract items for M1).

## 3. How it fits together

```
clean/credits, contributions, recording_years  ─┐
clean/artists, artist_genres, primary_genre    ─┼─F3.1 graph.build──▶ graph/{nodes_artist, artist_genre, edges, edges_by_decade}
raw/l_artist_artist ("member of band")         ─┘   FR-10/FR-11/D-021/OD1/OD2 filters, D-028 one count per recording and pair
                                                             │
                                          F3.3 graph.checks  │ 49 checks + run-to-run guard → graph/_checks.json
                                                             ▼
                              F3.4 export.neo4j_csv ──▶ import/<full|sample-f0.05>/  (§8.3 CSVs, read back, gated on _checks.json)
                                                             │
                     neo4j/import_graph.sh ──▶ neo4j-admin import ─▶ 39 checks (export.neo4j_check) ─▶ loaded_graph.json
                                                             │
                                          F3.5 gds.setup     ▼ constraints + indexes; collab_undirected and credit_only
                                                               built, checked against Parquet, estimated, dropped

F3.2 graph.sample --sample 0.05: credits → contributions → years → graph.build → graph.checks → _manifest.json
                                 (everything under $MUSIN_DATA/sample/f0.05/; compared with the full graph)
```

- **Each stage refuses unchecked input.** The export requires a passing F3.3 check of exactly the current tables (by content fingerprint). The import requires the export's argument file. The GDS setup requires a verified `loaded_graph.json`.
- **Content fingerprints identify every graph** (Σ xxhash64 per row, F3.2): in the sample manifest, in `_checks.json`, in the export record, and in `loaded_graph.json`. P4 results can therefore name the exact graph they came from (RULES §7).
- **All filters live in F3.1**, never in `clean/`. A G1 decision therefore means: change `config/pipeline.yaml`, re-run F3.1 → F3.5, accept the count change once in F3.3.

## 4. Key numbers

| Output | Full graph | Sample 0.05 |
|---|---:|---:|
| Artists (`nodes_artist`) | 1,180,939 | 310,253 |
| Edges | 3,541,659 | 535,372 |
| … `CO_CREDITED` / `FEATURED` / `PRODUCED_FOR` / `PERFORMED_FOR` | 1,731,537 / 734,524 / 291,319 / 784,279 | 263,641 / 61,528 / 66,162 / 144,041 |
| `edges_by_decade` rows | 3,829,423 | 555,833 |
| `artist_genre` rows | 442,104 | 198,771 |
| Shared recordings (Σ weight) | 17,363,002 | 865,430 |
| COLLAB pairs (GDS) | 3,301,392 | 524,330 |
| Artists with a primary genre | 276,338 (23.4 %) | 39.9 % |
| Neo4j store | 505 MB; 1,180,954 nodes, 3,983,763 relationships | 310,268 nodes, 734,143 relationships |

| Runtime (reference machine) | Full | Sample |
|---|---:|---:|
| Graph build (F3.1) | 168 s | 49 s |
| Graph checks (F3.3) | 27 s | a few s |
| Whole sample chain (F3.2) | — | ≈ 95 s (4 GB driver works) |
| Export (F3.4) | 30 s | 17 s |
| `neo4j-admin` import | 6.9 s | 2.6–3.3 s |
| Import script incl. verification and GDS setup | 128 s | 83 s |

- **Filters (full build):**
  - FR-10: 12,769 recordings over 10 artists skipped.
  - FR-11: 2,464,512 band-member instances removed (80.8 % `PERFORMED_FOR`).
  - D-021: 18,052 `Character` artists, 90,825 edges.
  - D-028: 4,054,365 `PERFORMED_FOR` and 134,833 `PRODUCED_FOR` instances that repeated a pair on one recording (−15.8 % edges).
- **Memory for P4:** COLLAB projection 193 MiB; projection plus the largest algorithm estimate (betweenness) is 0.64 GiB of the 8 GiB heap.
- **Tests:** 155 `pytest` tests (P2 ended at 93), `ruff` clean.

## 5. Decisions made

- [D-026](../decisions.md#d-026--graph-build-rules-credit-size-filter-scope-and-drop-order): credit size counts Character artists; the FR-10 cap applies to every edge type; band-member removal covers all types; the classical-off definition; the drop order.
- [D-027](../decisions.md#d-027--recording_count-counts-billed-and-contribution-credits): `recording_count` counts billed **and** contribution credits (interpretation of §8.1, M1 sign-off pending).
- [D-028](../decisions.md#d-028--each-recording-counts-once-per-artist-pair-no-double-counting-in-collab): each recording counts once per artist pair (billed → produced → performed); PRD §8.2 changed (M1 sign-off pending).
- [D-029](../decisions.md#d-029--dev-sample-graph-fraction-005-identified-by-content-fingerprints): dev sample fraction 0.05, identified by content fingerprints; for development only.
- [D-030](../decisions.md#d-030--graph-quality-checks-run-to-run-tolerance-and-baseline): `checks.max_row_change` 0.05 against the last passing check; `--accept-changes` for deliberate changes.
- [D-031](../decisions.md#d-031--neo4j-export-layout-csv-dialect-and-the-loaded-graph-marker): export layout under `import/<name>/`, explicit CSV dialect, strict import, `loaded_graph.json` marker.
- [D-032](../decisions.md#d-032--neo4j-schema-and-the-two-production-projections): Neo4j schema and the two production projections.

## 6. Deviations from the plan

- **Schedule.** Planned Oct 18–24, done Oct 11, before gate G1 (Oct 16). The whole graph uses the **provisional** G1 values (`min_recordings` 1, `max_credit_size` 10, classical included, release-level credits off). G1 will change some of them; re-running F3.1 → F3.5 for both graphs takes about 9 minutes (build 168 s, checks 27 s, sample chain 95 s, exports 47 s, imports with setup 211 s).
- **Contract changes inside the draft:** D-027 and D-028 interpret or change PRD §8.1/§8.2 before the M1 freeze (Oct 13). Both are listed in the G1 note for sign-off.
- **F3.5 ownership.** Built by A, not B, at the team's request. Member B should review the F3.5 doc and the `project()`/`drop()` API before P4.
- **Rework.** F3.1 was reworked once in this phase (D-028, double counting in COLLAB), and its doc has a revision line.
- **New config key** `checks.max_row_change` (F3.3; PRD §8.4 updated).
- **Sample exports live under `import/`**, not in the D-019 sample tree, because Neo4j can only see `import/` (D-031).
- **No PRs:** merged directly to `main`, as in P1 and P2.

## 7. Known issues and limitations

- **Content artefacts dominate weighted rankings** (G1 note): classical artists (57 of the top 100 by weighted degree), audiobook author–narrator pairs (14 of the 25 heaviest pairs), and Springsteen's live archive. They are decisions for G1 and P4, not bugs. A spoken-word filter needs a PRD addition (draft in the G1 note).
- **Type-specific edges mean "strongest relationship per recording"** after D-028: a billed artist's own instrument credits are not `PERFORMED_FOR` edges.
- **Backing bands** are not caught by FR-11 (E Street Band members → Springsteen).
- **The sample over-represents hubs and labelled artists** (labelled nodes 1.7×, classical 2.6×). It is for code development, not estimates.
- **`credit_only` holds fewer artists** (888,062) than COLLAB; RQ4's sensitivity comparison must account for it.
- **Coverage biases from P2 carry over:** genres label 23.4 % of nodes; contribution credits cover about 10 % of recordings.

## 8. How to reproduce this phase

From WSL Ubuntu, repo root, after P2 (`clean/` present). Stop Neo4j during Spark runs:

```bash
docker compose stop neo4j
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml                 # F3.1
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml                # F3.3 (--accept-changes after a deliberate change)
docker compose exec spark python -m musin.graph.sample --config config/pipeline.yaml --sample 0.05  # F3.2 (runs F3.3 on the sample)
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml            # F3.4, full
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --sample 0.05
bash neo4j/import_graph.sh full --overwrite           # F3.4 import + verify, F3.5 schema + projections
bash neo4j/import_graph.sh sample-f0.05 --overwrite   # leave member B's dev graph loaded
docker compose exec spark pytest
```

## 9. Handoff to the next phase

P4 (member B) can rely on:

- **Neo4j holds `sample-f0.05`** with constraints on `artistId` / `mbid` / `Genre.name` and an index on `Artist.name`. `$MUSIN_DATA/neo4j/loaded_graph.json` names the loaded graph and its fingerprints; quote the `edges` fingerprint with results. Switching to the full graph takes one command (≈ 2 min) and replaces the sample.
- **Projections:** `musin.gds.setup.project(driver, "collab_undirected" | "credit_only")` builds them exactly as D-017 defines, already checked against Parquet; `drop()` removes them. The weight is the number of shared recordings (D-028).
- **Memory:** every §5.5 algorithm fits the heap with large margin (0.64 GiB of 8 GiB on the full COLLAB). Betweenness *runtime* with sampling 10,000 is not yet measured.
- **Spark-side data for RQ3:** `graph/edges_by_decade` (full) and `sample/f0.05/graph/edges_by_decade`. The F3.3 checks guarantee the §8.1 invariant and first/last-decade consistency.
- **Watch out for:**
  - G1 (Oct 16) will likely change filters (classical, spoken word). Results computed before then are on the provisional graph and must be re-run.
  - The weighted-degree top lists are dominated by the artefacts in §7.
  - Validation list (PRD §5.6) must be committed **before** looking at RQ1 results.
