# Open G1 items — decision brief (2026-10-20)

**Status:** open for decision. Nothing in this note is decided. It was prepared by Claude Code on 2026-10-20, after P4 was re-run on the final graph and P5 was closed. Each decision taken gets its own `D-NNN` entry.

**Why now:** the feature freeze is Oct 30 (M4) and P6 starts Oct 28. Options that rebuild the graph need about 11 h of machine time, so they must be decided this week. If both items lead to a rebuild, doing them **together** saves one full P4 re-run.

Numbers below come from the committed tables of the final graph `full` (edges fingerprint `-2227712706449027820962`) unless marked otherwise. Runtimes are measured values from P3–P5 runs; costs of work not yet done are **estimates** and say so.

---

## 1. Spoken word (audiobooks, audio dramas)

### 1.1 The problem, now that classical is gone

Audiobook author–narrator pairs are credited like performers, and each audiobook has dozens to hundreds of tracks, so the pairs get very heavy weights. With classical removed (D-039), they are the main remaining content artefact:

- **RQ4, COLLAB weighted degree:** 4 of the top 10 (Stephen King #5, David Nathan #7, J. K. Rowling #8, Terry Pratchett #9). The other places go to Springsteen's live archive (#1–#4), Frank Sinatra and the Count Basie Orchestra (`centrality_top.csv`).
- **RQ4, `credit_only` weighted degree:** 8 of the top 10 are audiobook or audio-drama artists (F4.1 §7).
- **Unweighted PageRank** (F5.2): the author Agatha Christie is #17.
- **Not visibly affected:** betweenness top 10 (hip hop and EDM connectors), the RQ1 bridge top 25 (world-music and crossover artists), and the communities' top-10 genre profiles. These are judged from the committed tables; no measurement isolated spoken word.
- **RQ3 (trends):** unknown. Audiobook pairs are mostly CO_CREDITED, so they add to the denominator of the featuring share in recent decades. The size of the effect can't be computed without the filter's data.

The size of the problem was only estimated before G1, on the provisional graph with an artist-tag proxy: 2,860 artists, 100,183 edges (2.8 %), 7.2 % of COLLAB weight (g1-scope-gate.md §2.1). That proxy is unusable as a filter, because it flags Bruce Springsteen for his *Born to Run* audiobook.

### 1.2 What a proper filter needs

The G1 proposal (g1-scope-gate.md §4) drops **recordings** whose release groups are **all** of an excluded secondary type (Audiobook, Audio drama, Spoken word). A music recording that also appears on a spoken-word compilation keeps its edges. Springsteen's music stays; his audiobook goes.

It needs two dump tables we don't ingest yet, `release_group_secondary_type` and `release_group_secondary_type_join`. They would come from `mbdump.tar.bz2`, which is already on disk; F1.3 extracted 22 tables in 136 s. It is new scope: F3.6 / FR-28 and a new `filters.exclude_secondary_types` key need an approved PRD change (RULES §3, §11).

### 1.3 Options

| | Option | What changes in the results | Work (estimate) | Machine time |
|---|---|---|---|---|
| **S0** | **Measure first:** extract and ingest the two tables, then count the recordings, edges and weight the filter would remove and which top-list artists it touches. No graph change. Can precede S1–S3 | none yet; turns the 7.2 % proxy into a real number | 1–2 h | about 5 min |
| **S1** | **Filter the main graph** (F3.6 as proposed) | audiobook artefacts leave all main results; every P4 table and doc is refreshed | about 1 day of code, tests and docs, plus half a day to refresh docs | rebuild 5 min + P4 on `full`, `characters`, `classical` about 10–11 h (P4 §4) + F5.2/F5.3 about 30 min |
| **S2** | **Filter as a sensitivity variant only:** the main graph keeps spoken word, and a `nospoken` variant (D-035 mechanism) gets F4.1 + F4.3 | main results unchanged; the report shows how far spoken word moves RQ4/RQ1 | as S1, plus allowing the new key as a variant override (a D-035 change) | build 5 min + F4.1/F4.3 on the variant about 3.2 h |
| **S3** | **Keep and document:** no data change. The report marks the audiobook rows in weighted-degree lists and leans on betweenness and the bridge score, which aren't affected | none | about 1 h of writing | none |

**Trade-offs:**
- **S1** gives the cleanest RQ4 answer, but costs the most and puts a rebuild in the last week before the freeze.
- **S2** proves the effect without changing the headline numbers, but the headline weighted-degree list stays artefact-heavy.
- **S3** is free, but weighted degree, one of RQ4's three measures, keeps showing audiobooks in its top 10.

**S0 is cheap and makes the choice evidence-based.** If the filter turns out to remove, say, under 1 % of weight, S3 becomes much easier to defend.

---

## 2. Betweenness reproducibility across re-imports

### 2.1 The problem

`full-classical` is exactly the provisional graph (same edges fingerprint), imported again on 2026-10-19. Its F4.1 run differs from the provisional run on 2026-10-13:

- **COLLAB betweenness top 20:** 16 of 20 artists shared (Jaccard 0.667). Vineyard Music's score went from 93.9 M (#4) to 2.8 M (outside the top 20).
- **`credit_only` betweenness top 20:** the same 20 artists, in a different order.
- **Agreement statistics:** ρ moved in the 4th decimal (degree–betweenness 0.65022 → 0.65009).
- **RQ1 bridge top 25:** Jaccard 0.92. The bridge score uses betweenness *percentiles*, which are robust.
- **Weighted degree, PageRank and Louvain:** unaffected.

Within one import, D-033's reproducibility holds: two runs give identical scores.

### 2.2 Likely cause (not yet verified)

Betweenness is approximated from 10,000 source artists drawn with a fixed seed (`analysis.betweenness_sampling_size`, `analysis.random_seed`), and the draw is over Neo4j's internal node ids. Those ids come from the row order of the import CSVs. Neither `graph.build` nor `export.neo4j_csv` sorts the rows (`neo4j_csv.py:201` only `coalesce`s), so a rebuild of identical content can reach Neo4j in a different order, get different ids, and therefore a different sample.

Two separate things are going on:
- **Reproducibility:** a rebuild doesn't reproduce the published numbers bit for bit.
- **Sampling error:** any single 10,000-source sample gives a top 20 that is only about 2/3 stable, and the Vineyard Music case shows single scores can be far off.

Fixing the first doesn't fix the second.

### 2.3 Options

| | Option | Fixes | Work (estimate) | Machine time |
|---|---|---|---|---|
| **B0** | **Verify the cause:** rebuild and re-export one graph, then compare the CSV row order with the current export | (evidence) | 15 min | about 5 min |
| **B1** | **Deterministic export:** sort nodes by `artistId` and relationships by endpoints before writing the CSVs. The same content then always gets the same node ids | reproducibility | about 1 h including a test | none by itself; the published numbers would only come from a sorted export after the next re-import and P4 run (free if combined with S1) |
| **B2** | **Several seeds, report the stability:** run betweenness with, for example, 4 more seeds on the main graph's COLLAB projection, and report how often each artist is in the top 20 and a rank range | sampling error is shown, not reduced | about 2 h (a `--seeds` mode or a small stage) | about 1.6 h per seed on COLLAB (5,775 s measured), so 4 seeds ≈ 6.4 h; `credit_only` adds 1.1 h per seed |
| **B3** | **Larger sample:** `betweenness_sampling_size` 10,000 → 40,000 | sampling error is reduced | config only (a metric change: decision entry) | runtime grows roughly with the sample: about 4× on every graph and projection, ≈ 30 h+ for `full`, `characters` and `classical`. Too much before the freeze |
| **B4** | **Document only:** say in the report that betweenness top lists carry sampling error and quote ranks and percentiles, not exact positions. Already written in F4.1 §8 and P4 §7 | neither | done | none |

Exact betweenness (all 1.1 M sources) is out of reach: about 113 × the sampled runtime.

**Trade-offs:**
- **B1** is cheap and good practice whatever else is chosen; its full benefit needs one re-import and P4 run.
- **B2** gives the report an honest error bar on RQ4's betweenness lists, and on Vineyard Music in particular, for an overnight run.
- **B3** isn't affordable before the freeze.

---

## 3. Combinations

| Plan | Contents | Machine time before Oct 30 (estimate) | Result |
|---|---|---|---|
| **Minimal** | S3 + B4 | none | no data changes; the report explains both artefacts |
| **Evidence first** | S0 + B0, then decide | about 10 min | real numbers for both questions before committing to a rebuild |
| **Clean rebuild** | S0 → S1 + B1 together (+ B2 overnight on the new main graph) | about 11 h (+ 6.4 h) | audiobooks gone, rebuilds reproducible, betweenness stability reported; one P4 re-run covers everything |
| **Sensitivity only** | S0 → S2 + B1 + B2 | about 3.2 h + 6.4 h | headline numbers unchanged; the report shows both effects as sensitivity results |

**Claude Code's suggestion** (not a decision): start with **Evidence first**. It costs about 10 min of compute and an afternoon of work. Then choose between **Clean rebuild** and **Sensitivity only** based on how much weight spoken word really carries. If there is a rebuild, B1 should ride along, because it makes that rebuild the last non-reproducible one.

---

## 4. Also still open from G1 (not covered here)

- **`log(1 + weight)` for the weighted P4 measures** (g1-scope-gate.md §2.3). Without classical, Springsteen's live archive (#1–#4 by weighted degree) is the remaining weight outlier. A log transform would also shrink the audiobook pairs' dominance, so it partly overlaps with §1.
- **M1 sign-off of D-027/D-028** (g1-scope-gate.md §2.2): already implemented, it only needs sign-off.
