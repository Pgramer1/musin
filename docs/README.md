# Musin build log

This folder records what was built, in which phase, and why. The rules for writing these docs are in [RULES.md §1](../RULES.md#1-documentation--the-build-log).

| Path | What goes there |
|---|---|
| `phases/` | One doc per phase, written when the phase closes ([template](phases/_TEMPLATE.md)) |
| `features/` | One doc per feature, written when the feature is done ([template](features/_TEMPLATE.md)) |
| [decisions.md](decisions.md) | Numbered decision log (`D-NNN`) |
| [validation-list.md](validation-list.md) | Pre-registered cross-genre artists, committed before any RQ1 result (PRD §5.6, D-037) |
| [g1-scope-gate.md](g1-scope-gate.md) | Evidence and proposals for scope gate G1 (Oct 16): OD1–OD7, spoken word, contract sign-off. Outcome: D-039, D-040 |

**Status values:** Planned · In progress · Done · Cut

## Phases

| Phase | Dates | Status | Doc |
|---|---|---|---|
| P0 Planning | Oct 10 | Done | [P0-planning.md](phases/P0-planning.md) |
| P1 Environment & ingestion | Oct 10–16 | Done | [P1-environment-ingestion.md](phases/P1-environment-ingestion.md) |
| P2 Cleaning & genre normalization | Oct 15–21 | Done | [P2-cleaning-genres.md](phases/P2-cleaning-genres.md) |
| P3 Graph build & Neo4j load | Oct 18–24 | Done | [P3-graph-build-neo4j.md](phases/P3-graph-build-neo4j.md) |
| P4 Graph analytics | Oct 19–30 | Done (provisional pre-G1 graph) | [P4-graph-analytics.md](phases/P4-graph-analytics.md) |
| P5 Benchmarks | Oct 22–30 | In progress | — |
| P6 Presentation & report | Oct 28–Nov 9 | Planned | — |

## Features

| ID | Feature | Phase | Owner | Priority | Status | Doc |
|---|---|---|---|---|---|---|
| F1.1 | Dev environment | P1 | A | Must | Done | [F1.1-dev-environment.md](features/F1.1-dev-environment.md) |
| F1.2 | Dump download and snapshot pinning | P1 | A | Must | Done | [F1.2-dump-download.md](features/F1.2-dump-download.md) |
| F1.3 | Table schemas and raw ingest | P1 | A | Must | Done | [F1.3-raw-ingest.md](features/F1.3-raw-ingest.md) |
| F1.4 | Data profiling report | P1 | A + B | Must | Done | [F1.4-profiling.md](features/F1.4-profiling.md) |
| F1.5 | Neo4j + GDS smoke test | P1 | B | Must | Done | [F1.5-neo4j-smoke-test.md](features/F1.5-neo4j-smoke-test.md) |
| F2.1 | Artist cleaning and special-purpose filter | P2 | A | Must | Done | [F2.1-artist-cleaning.md](features/F2.1-artist-cleaning.md) |
| F2.2 | Credit parsing (main / featured) | P2 | A | Must | Done | [F2.2-credit-parsing.md](features/F2.2-credit-parsing.md) |
| F2.3 | Contribution relationship extraction | P2 | A | Must | Done | [F2.3-contributions.md](features/F2.3-contributions.md) |
| F2.4 | Recording first release year | P2 | A | Must | Done | [F2.4-first-release-year.md](features/F2.4-first-release-year.md) |
| F2.5 | Genre normalization | P2 | A + B | Must | Done | [F2.5-genre-normalization.md](features/F2.5-genre-normalization.md) |
| F3.1 | Edge building and filters | P3 | A | Must | Done | [F3.1-edge-building.md](features/F3.1-edge-building.md) |
| F3.2 | Sample mode and dev sample graph | P3 | A | Must | Done | [F3.2-dev-sample-graph.md](features/F3.2-dev-sample-graph.md) |
| F3.3 | Graph-layer quality checks | P3 | A | Must | Done | [F3.3-graph-quality-checks.md](features/F3.3-graph-quality-checks.md) |
| F3.4 | Neo4j CSV export and bulk import | P3 | A | Must | Done | [F3.4-neo4j-export-import.md](features/F3.4-neo4j-export-import.md) |
| F3.5 | Constraints, indexes, GDS projections | P3 | B (built by A) | Must | Done | [F3.5-constraints-indexes-projections.md](features/F3.5-constraints-indexes-projections.md) |
| F4.1 | Centrality and agreement (RQ4) | P4 | B | Must | Done (provisional graph) | [F4.1-centrality.md](features/F4.1-centrality.md) |
| F4.2 | Communities vs genres (RQ2) | P4 | B | Must | Done (provisional graph) | [F4.2-communities.md](features/F4.2-communities.md) |
| F4.3 | Genre bridges (RQ1) | P4 | B | Must | Done (provisional graph) | [F4.3-genre-bridges.md](features/F4.3-genre-bridges.md) |
| F4.4 | Cross-genre trends by decade (RQ3) | P4 | B | Must | Done (provisional graph) | [F4.4-cross-genre-trends.md](features/F4.4-cross-genre-trends.md) |
| F4.5 | Degrees of separation | P4 | B | Should | Done (provisional graph) | [F4.5-degrees-of-separation.md](features/F4.5-degrees-of-separation.md) |
| F4.6 | Validation | P4 | B | Must | Done (provisional graph) | [F4.6-validation.md](features/F4.6-validation.md) |
| F4.7 | Sensitivity graph variants (D-021) | P4 | B | Must | Done (sample + provisional full graph) | [F4.7-sensitivity-variants.md](features/F4.7-sensitivity-variants.md) |
| F5.1 | Spark scaling benchmark | P5 | A | Must | Done | [F5.1-spark-scaling.md](features/F5.1-spark-scaling.md) |
| F5.2 | PageRank: GDS vs GraphFrames | P5 | A | Should | Planned | — |
| F5.3 | PageRank on GPU (cuGraph) | P5 | A | Could | Planned | — |
| F6.1 | Report figures and tables | P6 | Both | Must | Planned | — |
| F6.2 | Demo | P6 | B | Must | Planned | — |
| F6.3 | Final report | P6 | Both | Must | Planned | — |
| F6.4 | Slides and rehearsal | P6 | Both | Must | Planned | — |
| F6.5 | README and reproducibility check | P6 | A | Must | Planned | — |
