# Musin build log

This folder records what was built, in which phase, and why. The rules for writing these docs are in [RULES.md §1](../RULES.md#1-documentation--the-build-log).

| Path | What goes there |
|---|---|
| `phases/` | One doc per phase, written when the phase closes ([template](phases/_TEMPLATE.md)) |
| `features/` | One doc per feature, written when the feature is done ([template](features/_TEMPLATE.md)) |
| [decisions.md](decisions.md) | Numbered decision log (`D-NNN`) |
| `validation-list.md` | Pre-registered cross-genre artists, written before RQ1 results (PRD §5.6) |

**Status values:** Planned · In progress · Done · Cut

## Phases

| Phase | Dates | Status | Doc |
|---|---|---|---|
| P0 Planning | Oct 10 | Done | [P0-planning.md](phases/P0-planning.md) |
| P1 Environment & ingestion | Oct 10–16 | Planned | — |
| P2 Cleaning & genre normalization | Oct 15–21 | Planned | — |
| P3 Graph build & Neo4j load | Oct 18–24 | Planned | — |
| P4 Graph analytics | Oct 19–30 | Planned | — |
| P5 Benchmarks | Oct 22–30 | Planned | — |
| P6 Presentation & report | Oct 28–Nov 9 | Planned | — |

## Features

| ID | Feature | Phase | Owner | Priority | Status | Doc |
|---|---|---|---|---|---|---|
| F1.1 | Dev environment | P1 | A | Must | Planned | — |
| F1.2 | Dump download and snapshot pinning | P1 | A | Must | Planned | — |
| F1.3 | Table schemas and raw ingest | P1 | A | Must | Planned | — |
| F1.4 | Data profiling report | P1 | A + B | Must | Planned | — |
| F1.5 | Neo4j + GDS smoke test | P1 | B | Must | Planned | — |
| F2.1 | Artist cleaning and special-purpose filter | P2 | A | Must | Planned | — |
| F2.2 | Credit parsing (main / featured) | P2 | A | Must | Planned | — |
| F2.3 | Contribution relationship extraction | P2 | A | Must | Planned | — |
| F2.4 | Recording first release year | P2 | A | Must | Planned | — |
| F2.5 | Genre normalization | P2 | A + B | Must | Planned | — |
| F3.1 | Edge building and filters | P3 | A | Must | Planned | — |
| F3.2 | Sample mode and dev sample graph | P3 | A | Must | Planned | — |
| F3.3 | Graph-layer quality checks | P3 | A | Must | Planned | — |
| F3.4 | Neo4j CSV export and bulk import | P3 | A | Must | Planned | — |
| F3.5 | Constraints, indexes, GDS projections | P3 | B | Must | Planned | — |
| F4.1 | Centrality and agreement (RQ4) | P4 | B | Must | Planned | — |
| F4.2 | Communities vs genres (RQ2) | P4 | B | Must | Planned | — |
| F4.3 | Genre bridges (RQ1) | P4 | B | Must | Planned | — |
| F4.4 | Cross-genre trends by decade (RQ3) | P4 | B | Must | Planned | — |
| F4.5 | Degrees of separation | P4 | B | Should | Planned | — |
| F4.6 | Validation | P4 | B | Must | Planned | — |
| F5.1 | Spark scaling benchmark | P5 | A | Must | Planned | — |
| F5.2 | PageRank: GDS vs GraphFrames | P5 | A | Should | Planned | — |
| F5.3 | PageRank on GPU (cuGraph) | P5 | A | Could | Planned | — |
| F6.1 | Report figures and tables | P6 | Both | Must | Planned | — |
| F6.2 | Demo | P6 | B | Must | Planned | — |
| F6.3 | Final report | P6 | Both | Must | Planned | — |
| F6.4 | Slides and rehearsal | P6 | Both | Must | Planned | — |
| F6.5 | README and reproducibility check | P6 | A | Must | Planned | — |
