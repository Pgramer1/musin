# P4 — Graph analytics

| | |
|---|---|
| **Status** | Done on the **provisional** full graph (pre-G1 filters). Every stage re-runs unchanged on the post-G1 graph. |
| **Planned dates** | 2026-10-19 – 2026-10-30 |
| **Actual dates** | 2026-10-11 – 2026-10-13 |
| **Owners** | B; built by Claude Code with member A at the team's request |
| **Data snapshot** | MusicBrainz full export `20261007-002147` (schema sequence 31); graph `full`, edges fingerprint `-3716276720548320596788` |

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
| F4.1 — Centrality measures and agreement (RQ4) | Done (provisional graph) | [F4.1-centrality.md](../features/F4.1-centrality.md) |
| F4.2 — Communities vs genres (RQ2) | Done (provisional graph) | [F4.2-communities.md](../features/F4.2-communities.md) |
| F4.3 — Genre bridges (RQ1) | Done (provisional graph) | [F4.3-genre-bridges.md](../features/F4.3-genre-bridges.md) |
| F4.4 — Cross-genre trends by decade (RQ3) | Done (provisional graph) | [F4.4-cross-genre-trends.md](../features/F4.4-cross-genre-trends.md) |
| F4.5 — Degrees of separation (Should) | Done (provisional graph; report pairs chosen in F6) | [F4.5-degrees-of-separation.md](../features/F4.5-degrees-of-separation.md) |
| F4.6 — Validation | Done (provisional graph) | [F4.6-validation.md](../features/F4.6-validation.md) |
| F4.7 — Sensitivity graph variants (new, D-035) | Done (sample and provisional full graph) | [F4.7-sensitivity-variants.md](../features/F4.7-sensitivity-variants.md) |

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

F4.7: graph.build/checks/export --variant characters ─▶ graph full-characters ─▶ the same stages ─▶ reports/*/variants/characters/
```

- **One graph at a time.** Neo4j Community holds one graph. Every P4 Neo4j stage reads `loaded_graph.json`, stamps the graph name and edges fingerprint on every output row, and discards its results if the marker changes during the run (`check_still_loaded`).
- **Same graph in Spark and Neo4j.** F4.3 reads the Parquet path from the marker, so diversity (Spark) and betweenness (GDS) describe the same graph. An artist-count check confirms it.
- **Where outputs go** (`musin.common.layers.results_dir` / `figures_dir`):
  - the full graph writes to `reports/{tables,figures}`;
  - a variant writes to `reports/{tables,figures}/variants/<name>`;
  - a sample writes inside its sample tree, never to the committed reports.
- **Reproducible by construction** (D-033, D-034, D-036): betweenness and the Louvain run of record are single-threaded; PageRank must converge; the tied shortest paths get a canonical pick. Two sample runs of F4.1 and F4.2 gave identical rankings.

## 4. Key numbers

All full-graph numbers are from the provisional pre-G1 graph: 1,180,939 artists, 3,301,392 COLLAB pairs, 17,363,002 shared recordings.

| Question | Headline (full graph) | Table |
|---|---|---|
| RQ4 centrality agreement | ρ 0.52–0.65 between measures; top-100 Jaccard degree–betweenness 0.058 | `centrality_agreement.csv` |
| RQ4 top by betweenness | London Symphony Orchestra, Snoop Dogg, Royal Philharmonic, Vineyard Music, Paul McCartney, Sting, Eminem, Ye | `centrality_top.csv` |
| RQ2 communities | 69,671 communities, modularity 0.840; genre NMI 0.275 / AMI 0.223 (largest component: 983 communities, AMI 0.268) | `communities_runs.csv` |
| RQ2 stability | run-to-run NMI 0.87–0.90 overall, 0.78–0.83 inside the largest component | `communities_stability.csv` |
| RQ1 bridges | 154,578 artists with a defined diversity; 82.2 % of labelled weight is within-genre; top bridges are world-music, session and roots collaborators | `bridges_top.csv`, `genre_matrix.csv` |
| RQ1 validation | 15/15 listed artists in the graph; median betweenness rank 88; bridge rank median 96th percentile, 3 in the top 100 | `validation_summary.csv` |
| RQ3 trends | cross-genre share 21 % (1970s) → 12 % (1990s) → 23 % (2020s); featuring share 1–2 % before 1990 → 18 % (2010s) | `trends_by_decade.csv` |
| F4.5 separation | example pairs 2–3 hops apart (Sinatra → Snoop Dogg via Stevie Wonder) | `separation_paths.csv` |
| F4.7 sensitivity | with Characters: Hatsune Miku is #1 by PageRank and betweenness; agreement statistics change by ≤ 0.004 (ρ); bridge top-25 Jaccard 0.92 | `variants/characters/` |

| Runtime (reference machine, full graph) | |
|---|---:|
| F4.1 centrality | 11,029 s (betweenness 6,563 s + 4,279 s, single-threaded) |
| F4.2 communities | 280 s |
| F4.3 bridges | 58 s |
| F4.4 trends | 11 s |
| F4.6 validation | 29 s |
| F4.5 separation (4 pairs) | < 1 s |
| F4.7 full variant: build + checks + export + import | 344 s, then F4.1 12,095 s and F4.3 65 s |

**Tests:** 220 `pytest` tests (P3 ended at 155), `ruff` clean.

## 5. Decisions made

- [D-033](../decisions.md): fixed GDS concurrency 4; betweenness single-threaded (the only reproducible setting); PageRank runs to convergence (max 1000 iterations as a cap); `top_k_centrality`.
- [D-034](../decisions.md): Louvain run of record single-threaded plus stability runs; AMI next to NMI; genre agreement also inside the largest connected component; community ids as size ranks.
- [D-035](../decisions.md): variant graphs built side by side with `--variant` (the `variants:` section in PRD §8.4); new feature F4.7.
- [D-036](../decisions.md): degrees of separation over the four collaboration types only (never through `:Genre`), a canonical path among ties, artists by MBID or unique name.
- [D-037](../decisions.md): validation list chosen by Claude Code at member B's request, committed before any RQ1 result.
- [D-038](../decisions.md): mid-rank betweenness percentile, write-back only where diversity is defined, ego-figure settings, CJK fonts.

## 6. Deviations from the plan

- **Schedule and graph.** Planned Oct 19–30; done Oct 11–13, before gate G1 (Oct 16). The team decided on 2026-10-12 to close P4 on the **provisional** graph (OD1–OD7 still open; classical and spoken word included). After G1, every stage re-runs with the same commands, in the order of §8.
- **Validation list** chosen by Claude Code, not by both members independently (D-037). It was still committed before any bridge result existed.
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

- **Provisional graph.** Classical recording volume, audiobooks (Stephen King, J. K. Rowling, narrator David Nathan) and Springsteen's live archive dominate the weighted rankings. These are G1 decisions (OD1, G1 note §2.1/§2.3), and all P4 tables must be refreshed after G1.
- **Record labels credited as artists** (Putumayo, #1 bridge; Vineyard Music, top-10 betweenness and PageRank) are not on the special-artists list. A candidate for G1.
- **Weight-based diversity under-rates artists with a big home genre:** Quincy Jones, Herbie Hancock and Yo‐Yo Ma have top-50 betweenness but bridge ranks around the 57th–65th percentile (F4.6).
- **The bridge score ≈ the diversity score** for every artist that has one, because those artists all have betweenness percentiles near 1 (D-038).
- **Genres cover a minority:** 23.4 % of artists have a primary genre, and labelled coverage falls from 61 % (1960s) to 32 % (2020s) of decade weight. This is the main caveat for RQ1–RQ3.
- **Two spot-check entries are unverified** (JohnStacy, Luke Pickman) and need a look in MusicBrainz. All spot-check verdicts are Claude Code's judgement and should be reviewed by both members.
- **Betweenness is slow** (≈ 3 h per full F4.1 run, single-threaded) and the run pauses when the machine sleeps. Keep the machine awake for full runs.

## 8. How to reproduce this phase

From WSL Ubuntu, repo root, after P3 (`graph/` built and checked, `import/full` exported). Spark-only steps run with Neo4j stopped:

```bash
docker compose stop neo4j
docker compose exec spark python -m musin.analytics.trends --config config/pipeline.yaml                      # F4.4
docker compose exec spark python -m musin.graph.build --config config/pipeline.yaml --variant characters      # F4.7
docker compose exec spark python -m musin.graph.checks --config config/pipeline.yaml --variant characters
docker compose exec spark python -m musin.export.neo4j_csv --config config/pipeline.yaml --variant characters
bash neo4j/import_graph.sh full-characters --overwrite                                                          # sensitivity graph first
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml                  # ~3.4 h
docker compose exec -e SPARK_DRIVER_MEMORY=4g spark python -m musin.analytics.bridges --config config/pipeline.yaml
bash neo4j/import_graph.sh full --overwrite                                                                     # main graph last; stays loaded
docker compose exec spark python -m musin.analytics.centrality --config config/pipeline.yaml                  # F4.1, ~3.1 h
docker compose exec spark python -m musin.analytics.communities --config config/pipeline.yaml                 # F4.2
docker compose exec -e SPARK_DRIVER_MEMORY=4g spark python -m musin.analytics.bridges --config config/pipeline.yaml   # F4.3
docker compose exec spark python -m musin.analytics.validation --config config/pipeline.yaml                  # F4.6
docker compose exec spark python -m musin.analytics.separation --config config/pipeline.yaml --pair "<A>" "<B>"   # F4.5
docker compose exec spark pytest
```

## 9. Handoff to the next phase

P5 (benchmarks, member A) and P6 (report) can rely on:

- **Neo4j holds the full graph** (`loaded_graph.json`: `full`) with every P4 property written back: `degreeWeighted`, `pagerank`, `betweenness`, `louvain`, `genreDiversity`, `bridgeScore`. These serve the F6.2 demo. Member B's sample graph needs `bash neo4j/import_graph.sh sample-f0.05 --overwrite`.
- **Report tables and figures** are in `reports/tables/` and `reports/figures/` (full graph), with the Character sensitivity versions under `variants/characters/`. Every row names its graph and edges fingerprint.
- **For F5.2 (GDS vs GraphFrames PageRank):** F4.1's PageRank runs to convergence (121 iterations on COLLAB), not 20 (D-033). Compare on the same settings or state the difference.
- **Watch out for:**
  - **G1 (Oct 16).** After it, rebuild P3 and re-run §8 (≈ 7 h of compute, mostly betweenness), then refresh the docs' result sections.
  - **Write-backs don't survive a re-import.** Importing another graph wipes the Neo4j properties.
  - **Review the F4.6 spot-check verdicts** and the two unverified entries before quoting top lists.
