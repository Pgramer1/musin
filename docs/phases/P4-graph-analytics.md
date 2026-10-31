# P4 — Graph analytics

| | |
|---|---|
| **Status** | Done. Final results on the post-G1 full graph (D-039, D-040), re-run 2026-10-19/20 with unchanged code |
| **Planned dates** | 2026-10-19 – 2026-10-30 |
| **Actual dates** | 2026-10-11 – 2026-10-13 (provisional graph); final re-run 2026-10-19 – 2026-10-20 |
| **Built with** | Claude Code, on request |
| **Data snapshot** | MusicBrainz full export `20261007-002147` (schema sequence 31); main graph `full`, edges fingerprint `-2227712706449027820962` (1,135,461 artists, 3,078,519 edges); variants `full-characters` (`-1131787253908781684974`) and `full-classical` (`-3716276720548320596788`, identical to the provisional graph) |

## 1. Goal

From PRD §11: answer the four research questions on the collaboration graph loaded in P3. The phase had to:

- answer RQ4 centrality (F4.1), RQ2 communities (F4.2), RQ1 genre bridges (F4.3) and RQ3 trends by decade (F4.4);
- provide degrees of separation (F4.5, Should);
- validate the bridge results against a pre-registered list (F4.6);
- produce the sensitivity runs the PRD asks for, which needed a new feature (F4.7).

All results are report-bound tables in `reports/tables/`.

## 2. What was built

| Feature | Status | Doc |
|---|---|---|
| F4.1 — Centrality measures and agreement (RQ4) | Done (final graph) | [F4.1-centrality.md](../features/F4.1-centrality.md) |
| F4.2 — Communities vs genres (RQ2) | Done (final graph) | [F4.2-communities.md](../features/F4.2-communities.md) |
| F4.3 — Genre bridges (RQ1) | Done (final graph) | [F4.3-genre-bridges.md](../features/F4.3-genre-bridges.md) |
| F4.4 — Cross-genre trends by decade (RQ3) | Done (final graph) | [F4.4-cross-genre-trends.md](../features/F4.4-cross-genre-trends.md) |
| F4.5 — Degrees of separation (Should) | Done (final graph; report pairs chosen in F6) | [F4.5-degrees-of-separation.md](../features/F4.5-degrees-of-separation.md) |
| F4.6 — Validation | Done (final graph) | [F4.6-validation.md](../features/F4.6-validation.md) |
| F4.7 — Sensitivity graph variants (new, D-035) | Done (`characters` and `classical`, final graph) | [F4.7-sensitivity-variants.md](../features/F4.7-sensitivity-variants.md) |

Also produced in this phase: the pre-registered [validation list](../validation-list.md) (D-037).

## 3. How it fits together

```
graph/ (Parquet, P3) ──F4.4 analytics.trends (Spark)──▶ trends_by_decade.csv/.png                       RQ3
     │
     └─ neo4j/import_graph.sh <graph> ─▶ Neo4j + GDS (one graph at a time, named in loaded_graph.json)
                │
                ├─ F4.1 analytics.centrality ─▶ :Artist.{degreeWeighted, pagerank, betweenness}     RQ4
                │                                centrality_{top,agreement,runs}.csv
                ├─ F4.2 analytics.communities ─▶ :Artist.louvain; communities_*.csv                  RQ2
                ├─ F4.3 analytics.bridges ──────▶ :Artist.{genreDiversity, bridgeScore}              RQ1
                │      (Spark on the loaded graph's own Parquet + F4.1 betweenness)
                │                                bridges_*.csv, genre_matrix.csv/.png, bridge_ego_*.png
                ├─ F4.6 analytics.validation ──▶ validation_{ranks,summary,top10}.csv  (config/validation_artists.csv)
                └─ F4.5 analytics.separation ──▶ separation_paths.csv  (any --pair, by MBID or unique name)

F4.7: graph.build/checks/export --variant <characters|classical> ─▶ graph full-<v> ─▶ F4.1 + F4.3 ─▶ reports/*/variants/<v>/
```

- **One graph at a time.** Neo4j Community holds one graph. Every P4 Neo4j stage reads `loaded_graph.json`, stamps the graph name and edges fingerprint on every output row, and discards its results if the marker changes during the run (`check_still_loaded`).
- **Same graph in Spark and Neo4j.** F4.3 reads the Parquet path from the marker, so diversity (Spark) and betweenness (GDS) describe the same graph. An artist-count check confirms it.
- **Where outputs go** (`musin.common.layers.results_dir` / `figures_dir`):
  - the full graph writes to `reports/{tables,figures}`;
  - a variant writes to `reports/{tables,figures}/variants/<name>`;
  - a sample writes inside its sample tree, never to the committed reports.
- **Reproducible by construction within one import** (D-033, D-034, D-036): betweenness and the Louvain run of record are single-threaded; PageRank must converge; the tied shortest paths get a canonical pick. Two sample runs of F4.1 and F4.2 on one import gave identical rankings. **Across re-imports of the same graph, sampled betweenness is not reproducible** (§7).

## 4. Key numbers

Main graph `full` after G1: 1,135,461 artists, 2,863,538 COLLAB pairs, 12,329,531 shared recordings (classical excluded, D-039; spoken word still included, D-040).

| Question | Headline (full graph) | Table |
|---|---|---|
| RQ4 centrality agreement | ρ 0.51–0.67 between measures; top-100 Jaccard degree–betweenness 0.047 (COLLAB) | `centrality_agreement.csv` |
| RQ4 top by betweenness | Snoop Dogg, Vineyard Music, Ye, Busta Rhymes, Pitbull, Sting, Elton John, Akon (COLLAB) | `centrality_top.csv` |
| RQ4 top by weighted degree | Springsteen's live archive (ranks 1–4) and audiobook authors/narrators (Stephen King, David Nathan, J. K. Rowling, Terry Pratchett) | `centrality_top.csv` |
| RQ2 communities | 72,258 communities, modularity 0.868; genre NMI 0.241 / AMI 0.186 (largest component: 1,180 communities, AMI 0.224) | `communities_runs.csv` |
| RQ2 stability | run-to-run NMI 0.885–0.897 overall, 0.805–0.825 inside the largest component | `communities_stability.csv` |
| RQ1 bridges | 126,293 artists with a defined diversity; 75.8 % of labelled weight is within-genre; top bridges are a world-music label (Putumayo), producers and session players, roots and world-music artists | `bridges_top.csv`, `genre_matrix.csv` |
| RQ1 validation | 14/15 listed artists in the graph (Yo‐Yo Ma is classical); median betweenness rank 103; bridge rank median 96th percentile, 3 in the top 100 | `validation_summary.csv` |
| RQ3 trends | cross-genre share 16 % (1960s) → 24–25 % (1970s–80s) → 21 % (1990s) → 28 % (2020s); featuring share 3–4 % before 1990 → 23 % (2010s) | `trends_by_decade.csv` |
| F4.5 separation | example pairs 2–3 hops apart (Sinatra → Snoop Dogg via Stevie Wonder) | `separation_paths.csv` |
| F4.7 `characters` | 初音ミク #1 by PageRank, #2 by betweenness; ρ within 0.004 of main; bridge top-25 Jaccard 0.85, no Character enters | `variants/characters/` |
| F4.7 `classical` | 18 classical artists enter the centrality top 20s (top-20 Jaccard vs main 0.25–0.48); bridge top-25 Jaccard 0.61, no classical artist enters | `variants/classical/` |

| Runtime (reference machine, full graph) | |
|---|---:|
| F4.1 centrality | 9,792 s by the stage's own timer (betweenness 5,775 s + 3,864 s, single-threaded); the wall time of 24,750 s includes about 4 h of machine sleep |
| F4.2 communities | 246 s |
| F4.3 bridges | 50 s |
| F4.4 trends | 13 s |
| F4.6 validation | 30 s |
| F4.5 separation (3 pairs) | < 1 s |
| F4.7 `characters`: build + checks + export + import | 322 s, then F4.1 11,513 s and F4.3 58 s |
| F4.7 `classical`: import (built in the G1 rebuild) | 124 s, then F4.1 11,088 s and F4.3 54 s |

**Tests:** 238 `pytest` tests (P3 ended at 155; F5.1 added 18), `ruff` clean.

**Effect of G1.** Removing classical (D-039) changed every RQ:
- **RQ3:** the cross-genre share is no longer U-shaped.
- **RQ2:** the classical community is gone; modularity rose from 0.840 to 0.868 and genre AMI fell from 0.223 to 0.186.
- **RQ1:** the bridge top 25 changed noticeably (Jaccard 0.61 with the classical-included graph).
- **RQ4:** orchestras left the top lists, and audiobook artists took their place in weighted degree.

The provisional numbers survive as the `classical` sensitivity run, because that variant is the provisional graph.

## 5. Decisions made

- [D-033](../decisions.md): fixed GDS concurrency 4; betweenness single-threaded (the only reproducible setting); PageRank runs to convergence (max 1000 iterations as a cap); `top_k_centrality`.
- [D-034](../decisions.md): Louvain run of record single-threaded plus stability runs; AMI next to NMI; genre agreement also inside the largest connected component; community ids as size ranks.
- [D-035](../decisions.md): variant graphs built side by side with `--variant` (the `variants:` section in PRD §8.4); new feature F4.7.
- [D-036](../decisions.md): degrees of separation over the four collaboration types only (never through `:Genre`), a canonical path among ties, artists by MBID or unique name.
- [D-037](../decisions.md): validation list chosen by Claude Code on request, committed before any RQ1 result.
- [D-038](../decisions.md): mid-rank betweenness percentile, write-back only where diversity is defined, ego-figure settings, CJK fonts.
- [D-039](../decisions.md), [D-040](../decisions.md) (G1, recorded 2026-10-19): classical out of the main graph and analysed as the `classical` variant; the other provisional values confirmed. Spoken word, log weights and the D-027/D-028 sign-off remain open.

## 6. Deviations from the plan

- **Schedule and graph.** Planned Oct 19–30; first done Oct 11–13 on the **provisional** graph, before gate G1. After G1 was recorded (2026-10-19), P3 was rebuilt and every stage re-ran unchanged in the order of §8, plus the new `classical` variant (2026-10-19/20, about 13.5 h of wall time including about 4 h of machine sleep).
- **Validation list** chosen by Claude Code rather than through the independent selection PRD §5.6 describes (D-037). It was still committed before any bridge result existed.
- **New feature F4.7** (sensitivity variants), added to PRD §11 with D-035, because the PRD's `Character` sensitivity runs could not be produced with one `graph/` folder.
- **Metric additions,** reported next to the PRD metrics, which stay unchanged:
  - AMI and largest-component views for RQ2 (D-034);
  - a labelled-coverage column for RQ3 (F4.4).
- **Configuration:** PageRank `max_iterations` 20 → 1000 as a cap (D-033). New keys:
  - `analysis.concurrency`, `analysis.betweenness_concurrency`, `analysis.top_k_centrality`;
  - `analysis.louvain_concurrency`, `analysis.top_k_communities`, `analysis.community_example_artists`;
  - `analysis.separation_max_paths`, `analysis.ego_figures`, `analysis.ego_max_neighbours`;
  - `validation.artists_file`, `variants`.

  All are listed in PRD §8.4.
- **Environment:** `fonts-noto-cjk` added to the Spark image (CJK artist names in figures). F4.3 runs with a 4 GB Spark driver on the full graph, so Spark and Neo4j fit WSL's 18 GB together. That setting lives in `.env`/the command line and doesn't change results.
- **No PRs:** merged directly to `main`, as in P1–P3.

## 7. Known issues and limitations

- **Spoken word is still in the graph** (G1 left it open, D-040). Without classical, audiobook authors and narrators (Stephen King, David Nathan, J. K. Rowling, Terry Pratchett) and Springsteen's live archive take 8 of the COLLAB weighted-degree top 10; in `credit_only`, 8 of the top 10 are audiobook-related. The proposed recording-level filter (g1-scope-gate.md §4, F3.6/FR-28) is not in the PRD.
- **Sampled betweenness is not reproducible across re-imports.** `full-classical` is the provisional graph exactly (same edges fingerprint), yet its COLLAB betweenness top 20 shares only 16 of 20 artists with the provisional run (Jaccard 0.667), and Vineyard Music's score fell from 93.9 M to 2.8 M. Weighted degree and PageRank were identical. The likely cause (not verified) is that the export doesn't sort rows (`neo4j_csv.py`), so a rebuild assigns different Neo4j node ids and the seeded 10,000-source sample changes. Within one import, D-033's reproducibility holds. Report betweenness ranks and percentiles, not the exact top-20 order. The bridge top 25 moved less (Jaccard 0.92), because it uses percentiles.
- **Record labels credited as artists** (Putumayo, #1 bridge; Vineyard Music, #2 betweenness) are not on the special-artists list. G1 did not add them.
- **Weight-based diversity under-rates artists with a big home genre:** Quincy Jones and Herbie Hancock have top-50 betweenness but bridge ranks around 60,000 of 126,293 (F4.6).
- **The bridge score ≈ the diversity score** for every artist that has one, because those artists all have betweenness percentiles near 1 (D-038).
- **Genres cover a minority:** 22.0 % of artists have a primary genre, and labelled coverage falls from 61 % (1960s) to 29 % (2020s) of decade weight. This is the main caveat for RQ1–RQ3.
- **Spot-check verdicts:** one entry is still unverified (Luke Pickman), and 16 new top-10 entries are not yet reviewed (F4.6). All verdicts are Claude Code's judgement and need review.
- **Yo‐Yo Ma**, on the pre-registered list, is not in the main graph (classical); the list stays unchanged (D-037).
- **Betweenness is slow** (about 2.7 h of compute per full F4.1 run, single-threaded), and a sleeping machine pauses the run. Keep the machine awake for full runs.

## 8. How to reproduce this phase

From WSL Ubuntu, repo root, after P3 (`graph/` built and checked, `import/full` exported; the `classical` variant built, checked and exported with `--variant classical`). Spark-only steps run with Neo4j stopped. A variant's first rebuild after a main-config change needs `graph.checks --accept-changes`:

```bash
docker compose stop neo4j
docker compose exec spark python -m musin.analytics.trends --config config/pipeline.yaml                      # F4.4
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml --variant characters      # F4.7
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml --variant characters
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --variant characters
bash neo4j/import_graph.sh full-characters --overwrite                                                          # sensitivity graphs first
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml                  # ~3.2 h
docker compose exec -e SPARK_DRIVER_MEMORY=4g spark python -m musin.analytics.bridges --config config/pipeline.yaml
bash neo4j/import_graph.sh full-classical --overwrite                                                           # built with --variant classical
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml                  # ~3.1 h
docker compose exec -e SPARK_DRIVER_MEMORY=4g spark python -m musin.analytics.bridges --config config/pipeline.yaml
bash neo4j/import_graph.sh full --overwrite                                                                     # main graph last; stays loaded
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml                  # F4.1, ~2.7 h
docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml                 # F4.2
docker compose exec -e SPARK_DRIVER_MEMORY=4g spark python -m musin.analytics.bridges --config config/pipeline.yaml   # F4.3
docker compose exec spark python -m musin.analytics.validation --config config/pipeline.yaml                  # F4.6
docker compose exec spark python -m musin.analytics.separation --config config/pipeline.yaml --pair "<A>" "<B>"   # F4.5
docker compose exec spark pytest
```

## 9. Handoff to the next phase

P5 (benchmarks) and P6 (report) can rely on:

- **Neo4j holds the full graph** (`loaded_graph.json`: `full`) with every P4 property written back: `degreeWeighted`, `pagerank`, `betweenness`, `louvain`, `genreDiversity`, `bridgeScore`. These serve the F6.2 demo. The dev sample graph needs `bash neo4j/import_graph.sh sample-f0.05 --overwrite`.
- **Report tables and figures** are in `reports/tables/` and `reports/figures/` (final full graph), with the sensitivity versions under `variants/characters/` and `variants/classical/`. Every row names its graph and edges fingerprint.
- **For F5.2 (GDS vs GraphFrames PageRank):** F4.1's PageRank runs to convergence (122 iterations on COLLAB of the final graph), not 20 (D-033). Compare on the same settings or state the difference.
- **Watch out for:**
  - **Spoken word.** If the spoken-word filter is adopted (F3.6), P3 and §8 must run again (about 10 h of compute with both variants).
  - **Betweenness variance across imports** (§7): don't compare raw betweenness scores between graphs or imports.
  - **Write-backs don't survive a re-import.** Importing another graph wipes the Neo4j properties.
  - **Review the F4.6 spot-check verdicts**, the unverified entry and the 16 not-yet-reviewed entries before quoting top lists.
