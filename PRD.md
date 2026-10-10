# Musin — Product Requirements Document

**Bridging Genres: A Big-Data Analysis of the MusicBrainz Collaboration Network**

| | |
|---|---|
| **Course** | Big Data Analytics — semester project |
| **Team** | Member A: `<name>` (data engineering) · Member B: `<name>` (graph analytics) |
| **Version** | 1.0 (2026-10-10) |
| **Submission** | ~2026-11-09 — **confirm the exact deadline** |
| **Current phase** | P0 Planning ✅ → P1 Environment & ingestion |

Companion documents: [RULES.md](RULES.md) (how we work) · [CLAUDE.md](CLAUDE.md) (AI assistant guide) · [docs/README.md](docs/README.md) (build log) · [docs/decisions.md](docs/decisions.md)

---

## 1. Summary

Musin turns the full MusicBrainz database export into a weighted, typed artist–artist collaboration graph and analyses it to find the artists who connect music genres. PySpark processes the raw dump (tens of GB of relational data) into a projected graph. Neo4j with the Graph Data Science (GDS) library runs the graph algorithms. Benchmarks justify the architecture.

## 2. Problem

Genres are usually studied one artist at a time, but music is made together. A collaboration graph shows which artists link scenes that would otherwise be separate.

MusicBrainz is the largest open music database, but it is relational, large, and noisy: genres are user-voted tags, credit coverage is uneven, and a few "artists" (Various Artists, orchestras, compilations) connect to everything. Turning it into a graph you can trust is a real big-data processing problem, and that processing is half the project.

## 3. Goals

| ID | Goal |
|---|---|
| G1 | Process the **complete** MusicBrainz export with Spark, not a sample or the web API. |
| G2 | Build a clean, typed, weighted collaboration graph in which every filtering choice is documented. |
| G3 | Answer RQ1–RQ4 (§5) with formally defined metrics, figures, and a validation check. |
| G4 | Justify the architecture with measured benchmarks (FR-21 to FR-23). |
| G5 | Be reproducible from a clean clone, and documented phase by phase in `docs/`. |

Non-goals are listed in §15.

## 4. Deliverables

1. **Git repository:** pipeline code, Neo4j scripts, analysis notebooks, config.
2. **Build log:** one doc per feature and per phase in `docs/` (see [RULES.md §1](RULES.md#1-documentation--the-build-log)).
3. **Final report:** architecture, data processing, results, benchmarks, limitations.
4. **Slides and a live demo.**

---

## 5. Research questions

All metrics run on the **COLLAB projection** (every edge type merged into one undirected graph, `weight` = summed shared-recording counts) unless stated otherwise. Genre analyses use only artists with a known primary genre.

### RQ1 — Which artists bridge genres?

- **Genre diversity** of artist *i*: `D_i = 1 − Σ_g p_ig²`, where `p_ig` is the share of *i*'s collaboration weight going to neighbours whose primary genre is *g*. Defined only for artists with ≥ 5 genre-labelled neighbours. 0 means every collaborator is in one genre; it approaches 1 as collaborators spread evenly across many genres.
- **Bridge score:** `B_i = D_i × percentile_rank(betweenness_i)`. A bridge is both diverse *and* structurally between groups.
- **Outputs:** top-25 bridge table; genre × genre collaboration heatmap (edge weight between parent genres); ego-network figures for 3 bridges.

### RQ2 — Do collaboration communities match genres?

- Louvain communities on COLLAB (weighted). Report modularity, the community-size distribution, and **NMI** (normalized mutual information) between community and primary genre.
- **Outputs:** NMI value; genre composition of the 10 largest communities; a short description of each.

### RQ3 — Has cross-genre collaboration changed over time?

- Per decade (1960s–2020s), using the first release year of each shared recording:
  - **Cross-genre share** = weight of edges whose endpoints have different primary genres ÷ weight of edges where both endpoints are labelled.
  - **Featuring share** = FEATURED weight ÷ (FEATURED + CO_CREDITED) weight.
- Limitation to state: primary genre is fixed per artist, not per period.
- **Outputs:** line charts and a table by decade.

### RQ4 — Who is most central, and do the measures agree?

- Weighted degree, PageRank, betweenness (settings in §5.5).
- **Agreement:** Spearman ρ across all artists, and Jaccard overlap of the top-100 sets, for each pair of measures.
- **Sensitivity:** repeat on a credit-only projection (CO_CREDITED + FEATURED).
- **Outputs:** top-20 per measure; agreement table; discussion.

### 5.5 Algorithm settings

| Measure | Engine | Settings |
|---|---|---|
| Weighted degree | GDS `degree` | weight = `weight`, undirected |
| PageRank | GDS `pageRank` | damping 0.85, max 20 iterations, weighted, undirected |
| Betweenness | GDS `betweenness` | **unweighted**: GDS treats weights as distances, so a strong tie would look "long". `samplingSize` and `samplingSeed` fixed and recorded |
| Communities | GDS `louvain` | weighted; fixed concurrency; run 3× to check stability |
| Shortest path | Cypher `shortestPath` | unweighted hops ("degrees of separation") |
| NMI | scikit-learn | on the exported (artist, community, primary_genre) table |

The exact parameter values used are recorded in each feature doc.

### 5.6 Validation

Before RQ1 results are looked at, both members independently list ~15 artists widely known for cross-genre collaboration and commit the combined list to `docs/validation-list.md`. This pre-registration stops us from picking examples after seeing the results. The report states where those artists rank. The top 10 of every ranking is also spot-checked for data artefacts.

---

## 6. Data

### 6.1 Source

- **MusicBrainz full export**, pinned to one snapshot date (chosen in F1.2 and recorded in `config/pipeline.yaml`):
  - `mbdump.tar.bz2`: core data (CC0)
  - `mbdump-derived.tar.bz2`: tags, ratings, annotations (CC BY-NC-SA 3.0)
- Files are headerless PostgreSQL `COPY` text: tab-separated, `\N` = null, backslash escapes. Column names and order come from `admin/sql/CreateTables.sql` in the `musicbrainz-server` repository at the matching schema version.
- The web API is **not** used (limited to ~1 request/second).

### 6.2 Tables used

| Table(s) | Purpose |
|---|---|
| `artist`, `artist_type`, `area` | Artist nodes: name, type (Person/Group/Orchestra/…), area, dates |
| `artist_credit`, `artist_credit_name` | Who is billed on each recording; `position` and `join_phrase` (featuring detection) |
| `recording` | The unit of collaboration |
| `track`, `medium`, `release`, `release_group` | Link recordings → releases → release groups |
| `release_country`, `release_unknown_country` | Release dates → first release year per recording |
| `l_artist_recording`, `link`, `link_type` | Contribution credits: producer, performer, instrument, vocal |
| `l_artist_artist` | Band membership (used to remove group ↔ member edges) |
| `genre`, `genre_alias` | Official MusicBrainz genre list |
| `tag`, `artist_tag`, `release_group_tag` | User-voted tags (derived dump) |

F1.3 confirms the exact file locations and any extra tables needed.

### 6.3 Known data issues and handling

| Issue | Handling |
|---|---|
| Special-purpose artists (Various Artists, [unknown], [anonymous], [traditional], [no artist], …) link to everything | Excluded via `config/special_artists.csv`, MBIDs verified against the dump (FR-04) |
| Large credits (orchestras, cast recordings) create N² edges | Recordings with more than `max_credit_size` credited artists produce no pairwise edges (FR-10) |
| A group and its own members look like collaborators | Remove edges between a group and its members (`l_artist_artist`, "member of band") (FR-11) |
| Classical music creates giant hubs (composers, orchestras, conductors) | Open decision OD1 at the scope gate: exclude, or analyse separately |
| Tags are noisy and sparse ("hip hop" / "hip-hop" / "rap") | Keep official genres and aliases only, map to 12–15 parent genres, weight by votes, report coverage (FR-08) |
| Contribution credits are well documented mainly for popular releases | Measure coverage in F1.4; state as a limitation; analyse PRODUCED_FOR separately or drop it (OD5) |
| Recordings have no date; release dates are partial or missing | First release year = earliest known year across releases, null if none; nulls are excluded from RQ3 only (FR-07) |
| Results reflect what volunteers chose to document | Stated in the report's limitations section |

### 6.4 Licensing

Core data is CC0. Tags come from the derived dump (CC BY-NC-SA 3.0), which is fine for coursework. Cite MusicBrainz / MetaBrainz Foundation in the report and README.

---

## 7. Architecture

```
MusicBrainz full export (pinned snapshot): mbdump + mbdump-derived
        │  F1.2 download, verify checksums
        ▼
PySpark, local[*], inside WSL2 / Docker
  raw/     headerless TSV → typed Parquet, one dataset per table          (P1)
  clean/   artists, credits, contributions, recording years, genres       (P2)
  graph/   nodes_artist, artist_genre, edges, edges_by_decade             (P3)
        │  F3.4 export CSV in neo4j-admin import format
        ▼
Neo4j 5 Community + GDS → centrality, communities, bridges, paths         (P4)
        │  results streamed to reports/tables/*.csv
        ▼
reports/ (tables, figures) → report, slides, demo                         (P6)

Benchmarks (P5): Spark scaling · PageRank in GDS vs GraphFrames (vs cuGraph)
```

### 7.1 Stack

| Layer | Choice | Why |
|---|---|---|
| Environment | WSL2 (Ubuntu) + Docker Compose | Identical setup for both members; avoids Spark-on-Windows problems |
| Processing | PySpark 3.5, local mode | Handles the full dump on one machine; 3.5 is compatible with GraphFrames |
| Storage | Parquet, layered (raw → clean → graph) | Columnar, compressed, Spark-native; layers make stages independent |
| Graph DB | Neo4j 5 Community + GDS | Free; GDS covers every algorithm we need (the 4-core limit is acceptable) |
| Analysis | Jupyter, pandas (aggregated outputs only), scikit-learn, matplotlib | |
| Benchmark | Spark GraphFrames; RAPIDS cuGraph (stretch) | Compare graph engines on identical input |
| Demo | Neo4j Browser saved queries (Bloom if available); Streamlit optional | |

Hadoop/HDFS and Hive are deliberately not used ([D-004](docs/decisions.md)).

### 7.2 Reference machine and resource budget

24 GB DDR5 RAM, RTX 4060 (8 GB VRAM), Windows 11 + WSL2.

| Setting | Value |
|---|---|
| `.wslconfig` memory | 18 GB |
| Spark driver memory | 12 GB |
| Neo4j heap / page cache | 8 GB / 5 GB (GDS projections live on the heap) |
| Free disk | ≥ 100 GB SSD; data inside the WSL filesystem, not `/mnt/c` |
| Concurrency rule | Spark and Neo4j heavy jobs never run at the same time |

---

## 8. Data model — schema contract v1

This contract is the interface between Member A (producer) and Member B (consumer). Changing it requires both members' approval, a version bump here, and a decision-log entry.

### 8.1 Parquet graph layer (`$MUSIN_DATA/graph/`)

**`nodes_artist`**

| Column | Type | Notes |
|---|---|---|
| `artist_id` | long | MusicBrainz internal `artist.id`; the graph key |
| `mbid` | string | `artist.gid`, the public UUID; used in reports |
| `name` | string | |
| `artist_type` | string | Person, Group, Orchestra, Choir, Character, Other, or null |
| `area_name` | string | nullable |
| `begin_year` | int | nullable |
| `recording_count` | long | distinct recordings the artist is credited on |
| `primary_genre` | string | parent genre with the highest share; null if untagged |
| `genre_votes` | long | tag votes behind the genre profile |

**`artist_genre`**

| Column | Type | Notes |
|---|---|---|
| `artist_id` | long | |
| `parent_genre` | string | |
| `share` | double | sums to 1 per artist |

**`edges`**

| Column | Type | Notes |
|---|---|---|
| `src_id` | long | |
| `dst_id` | long | for undirected types, `src_id < dst_id` |
| `edge_type` | string | `CO_CREDITED`, `FEATURED`, `PRODUCED_FOR`, `PERFORMED_FOR` |
| `weight` | long | distinct shared recordings |
| `first_year` | int | nullable |
| `last_year` | int | nullable |

**`edges_by_decade`**: `src_id`, `dst_id`, `edge_type`, `decade` (int, e.g. 1990), `weight`. Feeds RQ3.

### 8.2 Edge semantics

For each recording, the credited artists come from `artist_credit_name` in `position` order.

- An artist is **featured** if it comes after a join phrase matching `feat.`, `ft.`, or `featuring` (case-insensitive). Otherwise it is **main**.
- main × main → `CO_CREDITED` (undirected)
- main → featured → `FEATURED` (directed)
- featured × featured → `CO_CREDITED`
- A contribution relationship (`l_artist_recording` with a link type in the allowlist in `config/pipeline.yaml`) creates `PRODUCED_FOR` or `PERFORMED_FOR` from the contributor to each **main** artist of that recording.
- Self-edges are dropped. Edges are aggregated per (src, dst, type) as distinct-recording counts.
- Ambiguous phrases (`with`, `&`, `x`, `vs.`) count as main. This is a documented limitation.

### 8.3 Neo4j graph

```
(:Artist {artistId, mbid, name, type, areaName, beginYear, recordingCount, primaryGenre})
(:Genre  {name})
(:Artist)-[:IN_GENRE      {share}]->(:Genre)
(:Artist)-[:CO_CREDITED   {weight, firstYear, lastYear}]->(:Artist)   // stored once, read as undirected
(:Artist)-[:FEATURED      {weight, firstYear, lastYear}]->(:Artist)   // main → featured
(:Artist)-[:PRODUCED_FOR  {weight, firstYear, lastYear}]->(:Artist)   // producer → billed artist
(:Artist)-[:PERFORMED_FOR {weight, firstYear, lastYear}]->(:Artist)   // performer → billed artist
```

Algorithm outputs are written back as Artist properties: `degreeWeighted`, `pagerank`, `betweenness`, `louvain`, `genreDiversity`, `bridgeScore`.

Only artists with `recording_count ≥ min_recordings` and at least one edge are loaded. The threshold is set at the scope gate (OD2).

---

## 9. Functional requirements

Priority: **Must** (the project fails without it) · **Should** (expected) · **Could** (stretch).

| ID | Requirement | Priority |
|---|---|---|
| | **Ingestion** | |
| FR-01 | Download a pinned full-export snapshot (core + derived) and verify checksums | Must |
| FR-02 | Parse every table in §6.2 with an explicit schema into raw Parquet; `\N` → null | Must |
| FR-03 | Produce a profiling report: row counts, null rates, tag / relationship / date coverage, credit-size distribution | Must |
| | **Cleaning** | |
| FR-04 | Exclude special-purpose artists everywhere downstream | Must |
| FR-05 | Parse credits into (recording, artist, position, role ∈ {main, featured}) | Must |
| FR-06 | Extract contribution credits through a configurable link-type allowlist | Must |
| FR-07 | Derive each recording's first release year (null if unknown) | Must |
| FR-08 | Map tags → official genres → parent genres; compute each artist's genre profile and primary genre | Must |
| | **Graph build** | |
| FR-09 | Build typed, aggregated edges per §8.2, plus `edges_by_decade` | Must |
| FR-10 | Skip pairwise edges for recordings with more than `max_credit_size` credited artists | Must |
| FR-11 | Remove edges between a group and its own members | Must |
| FR-12 | Support a deterministic sample mode (`--sample <fraction>`, by hashed recording id) | Must |
| FR-13 | Quality checks: no self-loops, no duplicate (src, dst, type), every endpoint exists | Must |
| | **Load** | |
| FR-14 | Export to neo4j-admin CSV, bulk import, create constraints and indexes | Must |
| | **Analytics** | |
| FR-15 | Centrality: weighted degree, PageRank, betweenness, and their agreement (RQ4) | Must |
| FR-16 | Louvain communities and NMI against genres (RQ2) | Must |
| FR-17 | Genre diversity, bridge score, genre × genre matrix (RQ1) | Must |
| FR-18 | Cross-genre and featuring share by decade (RQ3) | Must |
| FR-19 | Shortest path ("degrees of separation") between two artists | Should |
| FR-20 | Validation against the pre-registered list (§5.6) | Must |
| | **Benchmarks** | |
| FR-21 | Spark scaling: data fraction (10 / 25 / 50 / 100%) × cores (1 / 2 / 4 / 8) | Must |
| FR-22 | PageRank on identical input in Neo4j GDS vs Spark GraphFrames: runtime and top-100 overlap | Should |
| FR-23 | The same comparison on GPU with RAPIDS cuGraph | Could |
| | **Presentation** | |
| FR-24 | Every report figure and table generated by code into `reports/` | Must |
| FR-25 | Live demo through saved Neo4j Browser queries (Bloom if available) | Must |
| FR-26 | Streamlit ego-network explorer (at most a few hundred nodes per view) | Could |
| FR-27 | Final report and slides | Must |

## 10. Non-functional requirements

| ID | Requirement |
|---|---|
| NFR-01 | **Reproducible:** each stage runs with one documented command from `config/pipeline.yaml`; container images, library versions, and the snapshot date are pinned. |
| NFR-02 | **Fits the reference machine** (§7.2) without swapping; heavy engines run one at a time. |
| NFR-03 | **Idempotent stages:** re-running a stage overwrites its whole output; a stage reads only the previous layer. |
| NFR-04 | **Observable:** every stage logs input/output row counts and runtime; Spark event logs are kept for benchmarks. |
| NFR-05 | **Portable:** sample mode runs on an 8 GB RAM machine, so both members can develop. |
| NFR-06 | **Documented:** every feature and phase has a build-log doc ([RULES.md §1](RULES.md#1-documentation--the-build-log)). |
| NFR-07 | **Attributed and clean:** MusicBrainz is cited; data is never committed to the repo. |

---

## 11. Phases, features, schedule

Feature IDs are `F<phase>.<n>`. Each feature gets a doc in `docs/features/` when it is done; each phase gets a doc in `docs/phases/` when it closes.

| Phase | Dates | Lead |
|---|---|---|
| P0 Planning | Oct 10 | Both |
| P1 Environment & ingestion | Oct 10–16 | A |
| P2 Cleaning & genre normalization | Oct 15–21 | A |
| P3 Graph build & Neo4j load | Oct 18–24 | A (load: B) |
| P4 Graph analytics | Oct 19–30 | B |
| P5 Benchmarks | Oct 22–30 | A |
| P6 Presentation & report | Oct 28–Nov 9 | Both |

### Features

| ID | Feature | Covers | Owner | Priority |
|---|---|---|---|---|
| F1.1 | Dev environment: `.wslconfig`, Docker Compose (Spark + Jupyter, Neo4j + GDS) | NFR-01, NFR-02 | A | Must |
| F1.2 | Dump download, checksum verification, snapshot pinning | FR-01 | A | Must |
| F1.3 | Table schemas and raw ingest to Parquet | FR-02 | A | Must |
| F1.4 | Data profiling report | FR-03 | A + B | Must |
| F1.5 | Neo4j + GDS smoke test on a toy graph | FR-14 (prep) | B | Must |
| F2.1 | Artist cleaning and special-purpose filter | FR-04 | A | Must |
| F2.2 | Credit parsing (main / featured) | FR-05 | A | Must |
| F2.3 | Contribution relationship extraction | FR-06 | A | Must |
| F2.4 | Recording first release year | FR-07 | A | Must |
| F2.5 | Genre normalization (1-day timebox) | FR-08 | A + B | Must |
| F3.1 | Edge building, aggregation, hub and band-member filters | FR-09, FR-10, FR-11 | A | Must |
| F3.2 | Sample mode and dev sample graph | FR-12 | A | Must |
| F3.3 | Graph-layer quality checks | FR-13 | A | Must |
| F3.4 | Neo4j CSV export and bulk import | FR-14 | A | Must |
| F3.5 | Constraints, indexes, GDS projections | FR-14 | B | Must |
| F4.1 | Centrality measures and agreement (RQ4) | FR-15 | B | Must |
| F4.2 | Communities vs genres (RQ2) | FR-16 | B | Must |
| F4.3 | Genre bridges (RQ1) | FR-17 | B | Must |
| F4.4 | Cross-genre trends by decade (RQ3) | FR-18 | B | Must |
| F4.5 | Degrees of separation | FR-19 | B | Should |
| F4.6 | Validation | FR-20 | B | Must |
| F5.1 | Spark scaling benchmark | FR-21 | A | Must |
| F5.2 | PageRank: GDS vs GraphFrames | FR-22 | A | Should |
| F5.3 | PageRank on GPU (cuGraph) | FR-23 | A | Could |
| F6.1 | Report figures and tables | FR-24 | Both | Must |
| F6.2 | Demo: saved Browser queries (+ optional Streamlit) | FR-25, FR-26 | B | Must |
| F6.3 | Final report | FR-27 | Both | Must |
| F6.4 | Slides and rehearsal | FR-27 | Both | Must |
| F6.5 | README and clean-clone reproducibility check | NFR-01 | A | Must |

### Milestones

| ID | Date | Milestone |
|---|---|---|
| M0 | Oct 10 | Planning docs complete |
| M1 | Oct 13 | Schema contract v1 frozen (§8) |
| G1 | Oct 16 | **Scope gate:** profiling done, open decisions (§14) resolved |
| M2 | Oct 19 | Sample graph loaded in Neo4j; B develops against it |
| M3 | Oct 24 | Full graph loaded in Neo4j |
| M4 | Oct 30 | **Feature freeze:** analyses and benchmarks done |
| M5 | Nov 5 | Report and slides drafted |
| M6 | Nov 9 | Submission (confirm the date) |

Nov 5–9 is buffer. Could-priority work starts only when every Must in its phase is done.

## 12. Team

| Member | Owns | Report sections |
|---|---|---|
| A — `<name>` | P1, P2, P3 build, P5, F6.5 | Architecture, ETL, benchmarks |
| B — `<name>` | F1.5, F3.5, P4, F6.2 | Analysis, results |
| Both | Genre mapping, validation list, report, slides | Each reviews the other's PRs |

## 13. Risks

| Risk | Likelihood | Impact | Mitigation |
|---|---|---|---|
| Genre tags too sparse | High | RQ1–RQ3 weaker | Add release-group tags; analyse the labelled subset; report coverage |
| Hubs dominate metrics | High | Meaningless rankings | FR-04, FR-10, FR-11, OD1 |
| Genre mapping becomes a rabbit hole | High | Days lost | 1-day timebox; imperfect is fine if documented |
| Producer credits too sparse | Medium | Contribution analysis thin | Measure at G1; downgrade or drop (OD5) |
| Out of memory | Medium | Stalled runs | Sample mode; one engine at a time; GDS `.estimate` first |
| Betweenness too slow | Medium | P4 delayed | Sampling |
| Environment setup on Windows | Medium | Days lost in week 1 | WSL2 + Docker from day 1 |
| Member B idle waiting for data | Medium | Lost parallelism | Contract by M1, toy graph (F1.5), sample graph by M2 |
| Deadline slip | Medium | Rushed report | Feature freeze at M4; 4-day buffer |

## 14. Open decisions (resolve at gate G1, Oct 16)

| ID | Question | Input needed |
|---|---|---|
| OD1 | Include classical music, exclude it, or analyse it separately? | Hub sizes from F1.4 |
| OD2 | `min_recordings` threshold for the Neo4j graph | Node/edge counts at several thresholds |
| OD3 | `max_credit_size` | Credit-size distribution |
| OD4 | Final parent-genre list (12–15) | Top official genres by tag volume |
| OD5 | Is producer coverage enough to analyse PRODUCED_FOR on its own? | Relationship coverage |
| OD6 | Snapshot date | Latest full export available |
| OD7 | Also use release-level credits (`l_artist_release`)? | Coverage compared with recording level |

Each resolution is recorded in [docs/decisions.md](docs/decisions.md).

## 15. Out of scope

- Wikidata, Discogs, Spotify, or any second data source
- The MusicBrainz web API, or crawling
- Audio features, recommendation, machine learning on the graph
- Real-time or streaming ingestion
- Hadoop/HDFS, Hive, cloud clusters
- Full-graph force-directed visualisation (unreadable at this scale)

## 16. Success criteria

The project is done when:

1. The full pipeline runs from a clean clone on the pinned snapshot (sample mode on any member's machine, full mode on the reference machine).
2. RQ1–RQ4 each have defined metrics, figures, a written answer, and a validation note.
3. The FR-21 benchmark (and FR-22 if reached) is reported with environment details.
4. Every Must feature and every phase has a build-log doc.
5. Report, slides, and demo are delivered by M6.

## 17. Glossary

| Term | Meaning |
|---|---|
| Artist credit | How a recording is billed ("A feat. B"), stored in `artist_credit_name` |
| Contribution | A typed relationship (producer, performer, …) from `l_artist_recording` |
| COLLAB projection | All edge types merged into one undirected, weighted GDS graph |
| Primary genre | The artist's parent genre with the highest vote share |
| Special-purpose artist | A placeholder artist such as Various Artists or [unknown] |
| MBID | MusicBrainz's public UUID for an entity |
