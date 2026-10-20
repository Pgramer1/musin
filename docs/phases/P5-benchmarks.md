# P5 — Benchmarks

| | |
|---|---|
| **Status** | Done (all three features, including the Could F5.3) |
| **Planned dates** | 2026-10-22 – 2026-10-30 |
| **Actual dates** | 2026-10-19 – 2026-10-20 |
| **Owners** | A; built by Claude Code with member A |
| **Data snapshot** | MusicBrainz full export `20261007-002147`; final post-G1 graph `full`, edges fingerprint `-2227712706449027820962` (1,135,461 artists, 3,078,519 edges) |

## 1. Goal

From PRD §11 and goal G4: justify the architecture with measured benchmarks.

- **F5.1 (Must, FR-21):** Spark scaling over data fraction (10/25/50/100 %) × cores (1/2/4/8).
- **F5.2 (Should, FR-22):** PageRank on identical input in Neo4j GDS vs Spark GraphFrames, comparing runtime and top-100 overlap.
- **F5.3 (Could, FR-23):** the same comparison on the GPU with RAPIDS cuGraph.

Success criterion 3 (PRD §16) requires FR-21 to be "reported with environment details".

P5 started right after G1 was recorded (D-039, D-040). G1 required a rebuild of the graph and a P4 re-run, both done in the same period (P3/P4 revisions, P4 phase doc). All benchmarks therefore ran on the final graph.

## 2. What was built

| Feature | Status | Doc |
|---|---|---|
| F5.1 — Spark scaling benchmark | Done | [F5.1-spark-scaling.md](../features/F5.1-spark-scaling.md) |
| F5.2 — PageRank: GDS vs GraphFrames | Done | [F5.2-pagerank-gds-vs-graphframes.md](../features/F5.2-pagerank-gds-vs-graphframes.md) |
| F5.3 — PageRank on GPU (cuGraph) | Done | [F5.3-pagerank-cugraph.md](../features/F5.3-pagerank-cugraph.md) |

## 3. How it fits together

```
config/pipeline.yaml benchmark.*  (D-041, D-042)          $MUSIN_DATA/benchmark/  (scratch, never the pipeline layers)
            │
F5.1  musin.benchmark.scaling ── fresh process per cell, local[n], event log ──▶ scaling/runs/<cell>.json
      (F2.2 → F2.3 → F2.4 → F3.1 on a symlinked scratch root)                    ─▶ benchmark_scaling_{runs,summary}.csv,
                                                                                    benchmark_environment.csv, benchmark_scaling.png
F5.2  musin.benchmark.pagerank --engine gds          (Neo4j up)      ─┐
      musin.benchmark.pagerank --engine graphframes  (Neo4j stopped) ─┼▶ pagerank/<graph>/run_<engine>_r<n>.json + scores
F5.3  musin.benchmark.pagerank_gpu  (rapids container, GPU)          ─┘
      musin.benchmark.pagerank --report ─▶ benchmark_pagerank_{runs,summary,agreement,top}.csv
```

- **Isolation:** no benchmark writes the pipeline's `clean/`, `graph/` or committed tables except its own report tables. F5.1 runs the real stages against a scratch root whose inputs are symlinks. F5.2/F5.3 only read the loaded graph and keep GDS scores in the in-memory projection.
- **Same discipline everywhere:**
  - three repetitions, median reported with min–max;
  - a fresh process per run, with a cold JVM or CUDA context;
  - one engine at a time (RULES §10);
  - every finished run saved at once, so an interrupted benchmark resumes;
  - results tied to the graph's edges fingerprint.
- **Identical input for the engines** (D-042, D-043): every engine computes unweighted PageRank on the same 2,863,538 COLLAB pairs, because GraphFrames can't weight it. The report refuses to run if any repetition saw different node or edge counts.

## 4. Key numbers

**Environment:**
- **CPU:** i5-13450HX, 16 logical CPUs, 17.6 GiB in WSL2.
- **GPU:** RTX 4060 Laptop, 8 GB.
- **Software:** Spark 3.5.9 local mode, Java 17, Neo4j 5.26.30 Community + GDS 2.13.12, GraphFrames 0.12.3, RAPIDS 26.08.

**F5.1 — Spark scaling** (median of 3; [`benchmark_scaling_summary.csv`](../../reports/tables/benchmark_scaling_summary.csv)):

| Fraction | 1 core | 2 cores | 4 cores | 8 cores | Speedup at 8 |
|---:|---:|---:|---:|---:|---:|
| 10 % | 334 s | 192 s | 137 s | 123 s | 2.73 |
| 25 % | 531 s | 292 s | 203 s | 191 s | 2.78 |
| 50 % | 745 s | 427 s | 302 s | 265 s | 2.81 |
| 100 % | 1,224 s | 680 s | 461 s | 385 s | 3.18 |

**F5.2 / F5.3 — PageRank on the final graph** (median of 3; [`benchmark_pagerank_summary.csv`](../../reports/tables/benchmark_pagerank_summary.csv), [`benchmark_pagerank_agreement.csv`](../../reports/tables/benchmark_pagerank_agreement.csv)):

| Engine | Load | Compute | Output | Total | Top-100 vs GDS | ρ vs GDS |
|---|---:|---:|---:|---:|---:|---:|
| Neo4j GDS (4 cores) | 11.8 s | 3.8 s | 14.7 s | 30.3 s | — | — |
| Spark GraphFrames (4 cores) | 9.5 s | 469.7 s | 2.4 s | 482.1 s | identical | 0.99999 |
| RAPIDS cuGraph (GPU) | 0.8 s | 0.2 s | 0.1 s | 1.1 s | identical | 0.99993 |

**Total measured benchmark time:**
- F5.1: 19,765 s (5.5 h);
- F5.2: 127 s (GDS) + 24.7 min (GraphFrames);
- F5.3: 13 s.

**Tests:** 248 `pytest` tests (P4 re-run ended at 238), `ruff` clean.

## 5. Decisions made

- [D-041](../decisions.md): F5.1 design: the recording chain as workload, a symlinked scratch root, a fresh process per cell, median of 3, repetition-major order, resume.
- [D-042](../decisions.md): F5.2 design: unweighted PageRank on identical pairs, the same damping, tolerance (new key `benchmark.pagerank.tolerance` 1e-7) and 4 cores, three timed phases, GraphFrames 0.12.3 pinned by SHA-1.
- [D-043](../decisions.md): F5.3: cuGraph in its own `rapids` container (profile `gpu`, host UID + `conda` group), double precision via unit float64 weights, scores rescaled ×N, output via PyArrow (cuDF's writer fails under WSL). The 5.83 GiB download and the NVIDIA Deep Learning Container License were approved by the team.
- [D-039](../decisions.md), [D-040](../decisions.md) (G1) were recorded at the start of this phase, so that P5 runs on the final graph.

## 6. Deviations from the plan

- **Dates:** done Oct 19–20, before the planned Oct 22–30. This leaves the time before the feature freeze (M4, Oct 30) for the open G1 items and P6.
- **G1 work was folded into this phase's period:** recording D-039/D-040, rebuilding the graph, re-running P4 with the new `classical` variant, and refreshing the P4 docs. None of it is P5 scope, but the benchmarks had to run on the final graph.
- **F5.2 compares unweighted PageRank,** not F4.1's weighted measure, because GraphFrames can't weight it (D-042). F4.1's weighted scores are a reference row.
- **F5.3 was planned as a stretch** ("Could"). It was attempted once a ten-minute check showed the GPU visible to Docker, and finished the same day.
- **New config keys:** `paths.benchmark`, `benchmark.scaling.{fractions, cores, repeats}`, `benchmark.pagerank.{tolerance, repeats}` (PRD §8.4).
- **Image changes:** GraphFrames jars and `graphframes-py` in the Spark image, plus a new optional `rapids` image.
- **No PRs:** merged directly to `main`, as in P1–P4.

## 7. Known issues and limitations

- **One machine.** Every number describes this laptop in WSL2: GDS Community capped at 4 cores, Spark in local mode, a laptop GPU. They justify *this project's* architecture, not general claims about the engines.
- **F5.1's fraction axis isn't pure data volume.** Sample mode reads all raw tables before filtering, so about a quarter of the runtime is fixed. The 8-core point mixes performance and efficiency cores.
- **Run-to-run spread:** up to 31 % in F5.1's multi-core cells and 17 % in one GraphFrames repetition, most likely from background load on the Windows host. The reports use medians.
- **Machine sleep:** during F5.1 and the P4 re-run, the host slept for 70 min and 4 h. Monotonic timers didn't count it, so timings are unaffected, but keep the machine awake for long runs.
- **F5.2's first run stamped finish times as start times.** Fixed in the code; the stored start times of that run are reconstructed and approximate (F5.2 §8). The timings are unaffected.
- **cuDF's GPU Parquet writer fails under WSL** (F5.3 §8); output goes through PyArrow.
- **Disk in `$MUSIN_DATA/benchmark/`:** 6.3 GB (F5.1) + 3.3 GB (F5.2/F5.3) of scratch data and event logs, plus the 17.6 GB RAPIDS image in Docker. All can be deleted once the report is final; nothing downstream reads them.

## 8. How to reproduce this phase

From WSL Ubuntu, repo root, with the final graph built (P3), loaded in Neo4j (`bash neo4j/import_graph.sh full --overwrite`) and P4 run (F5.2's reference row reads F4.1's `pagerank` property). Keep the machine awake:

```bash
docker compose build spark                                             # GraphFrames jars (F5.2)
docker compose stop neo4j
docker compose exec spark python -m musin.benchmark.scaling --config config/pipeline.yaml        # F5.1, ~5.5 h, resumes
docker compose start neo4j
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine gds          # F5.2, ~2 min
docker compose stop neo4j
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --engine graphframes  # F5.2, ~25 min
docker compose --profile gpu build rapids                                                                      # F5.3, 5.83 GiB pull
docker compose --profile gpu run --rm rapids python -m musin.benchmark.pagerank_gpu --config config/pipeline.yaml   # F5.3, ~15 s
docker compose exec spark python -m musin.benchmark.pagerank --config config/pipeline.yaml --report
docker compose start neo4j
docker compose exec spark pytest
```

## 9. Handoff to the next phase

P6 (report, slides, demo; both members) can rely on:

- **Report tables** in `reports/tables/`:
  - `benchmark_scaling_{runs,summary}.csv`;
  - `benchmark_environment.csv`, the environment details PRD §16 (success criterion 3) asks for;
  - `benchmark_pagerank_{runs,summary,agreement,top}.csv`.
- **Figure:** `reports/figures/benchmark_scaling.png`. The PageRank comparison has no figure yet; F6.1 can draw one from `benchmark_pagerank_summary.csv`.
- **The architecture argument for the report:**
  - Spark handles the full dump in 6.4 min on 8 cores.
  - GDS beats GraphFrames by about 120× on PageRank with the same answer.
  - A GPU is a further 19× faster but isn't needed at this scale.

  Each claim has a table behind it.
- **Neo4j holds `full`** with all P4 properties written back. The benchmarks didn't change it: GDS scores stayed in the projection.
- **Watch out for:**
  - Several G1 items are still open: the spoken-word filter, log weights, the D-027/D-028 sign-off, and the betweenness reproducibility across imports (P4 §7). If the graph is rebuilt again, F5.1 doesn't need re-running (it measures the pipeline, not results), but F5.2/F5.3 should be re-run for the new fingerprint (about 30 min). Their saved runs are refused for a different graph build.
  - Using the RAPIDS image means accepting the NVIDIA Deep Learning Container License.
