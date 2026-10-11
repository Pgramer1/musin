# Decision log

Each decision gets the next `D-NNN` number. Never edit or delete an old entry. If a decision changes, add a new entry that supersedes it, and change the old entry's status to "Superseded by D-NNN".

Entry format:

```
### D-NNN — Title
- **Date / phase:**
- **Status:** Accepted | Superseded by D-NNN
- **Context:** why a decision was needed
- **Decision:** what we chose
- **Consequences:** trade-offs and what follows from it
```

---

### D-001 — MusicBrainz is the only data source
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** The original idea used Wikidata. Linking entities across two sources would take days and add little.
- **Decision:** Use MusicBrainz only. Drop Wikidata.
- **Consequences:** No cross-source enrichment. Genres come from MusicBrainz tags.

### D-002 — Full database dumps, not the web API
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** The API is limited to ~1 request/second, which cannot deliver millions of entities within a month.
- **Decision:** Download the full export (`mbdump` + `mbdump-derived`), pinned to one snapshot date.
- **Consequences:** Tens of GB to process, which is what makes this a big-data project. Headerless `COPY` files must be parsed with explicit schemas.

### D-003 — PySpark for ETL, layered Parquet storage
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** The joins (recordings × credits × releases × relationships) are too large for pandas on one machine, and the course rewards big-data tooling.
- **Decision:** PySpark 3.5 in local mode; Parquet layers `raw → clean → graph`.
- **Consequences:** Explicit schemas are needed for every table. Spark's scaling is measured in F5.1.

### D-004 — No Hadoop/HDFS or Hive
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** They are optional for the course. On a single machine they add setup work and no performance benefit.
- **Decision:** Not used. Parquet + Spark SQL fills the warehouse role.
- **Consequences:** Simpler setup. The report must explain the choice.

### D-005 — Project to artist–artist edges in Spark
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** The review of the original idea suggested keeping the Artist–Recording bipartite graph in Neo4j. That means tens of millions of recording nodes, which is too heavy for a 24 GB laptop.
- **Decision:** Spark aggregates artist–recording links into weighted artist–artist edges. Neo4j stores only artists, genres, and those edges.
- **Consequences:** Recording-level questions are answered in Spark, not Neo4j. `edges_by_decade` is added so RQ3 doesn't need recordings in Neo4j.

### D-006 — Neo4j 5 Community + GDS for graph analytics
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** We need PageRank, betweenness, Louvain, shortest paths, and an interactive demo, for free.
- **Decision:** Neo4j 5 Community Edition with the Graph Data Science plugin.
- **Consequences:** GDS algorithms are limited to 4 cores. Community has one user database, so switching between sample and full graph means re-importing.

### D-007 — WSL2 + Docker Compose environment
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** The host is Windows 11. Spark on native Windows needs Hadoop binary workarounds and is unreliable, and two members need identical setups.
- **Decision:** All pipeline work runs in WSL2 (Ubuntu) + Docker Compose.
- **Consequences:** Data must live in the WSL filesystem for acceptable I/O speed.

### D-008 — Separate credit edges from contribution edges
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** MusicBrainz separates billing (artist credit) from typed relationships (producer, performer). Mixing them hides what an edge means.
- **Decision:** Four edge types: `CO_CREDITED` and `FEATURED` (credit), `PRODUCED_FOR` and `PERFORMED_FOR` (contribution). They are merged only inside GDS projections.
- **Consequences:** Enables the credit-only sensitivity analysis in RQ4.

### D-009 — Benchmarks: Spark scaling + graph-engine comparison
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** The review suggested Spark vs pandas, but pandas would simply run out of memory on the full joins, which gives a one-line result.
- **Decision:** Spark scaling (data fraction × cores) as a Must. PageRank in GDS vs GraphFrames as a Should, with cuGraph on the GPU as a Could.
- **Consequences:** The graph layer must also export as a plain edge list for GraphFrames and cuGraph.

### D-010 — Demo in Neo4j Browser; Streamlit optional
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted
- **Context:** With one month, a custom dashboard is the lowest value per hour.
- **Decision:** Must = saved Neo4j Browser queries (Bloom if available). Streamlit = Could.
- **Consequences:** The demo needs prepared queries on small subgraphs.

### D-011 — Contract v1 clarifications
- **Date / phase:** 2026-10-10 · P0
- **Status:** Accepted, pending both members' sign-off at M1 (Oct 13)
- **Context:** A review of the PRD found places where the contract was implied but not defined, so Member A and Member B could implement the same thing differently.
- **Decision:**
  1. **Config contract** (PRD §8.4): `config/pipeline.yaml` key list v1. **Environment contract** (§8.5): `.env` keys. Rule: settings that change results go in `pipeline.yaml`; secrets and per-machine settings go in `.env`.
  2. **`edges_by_decade.weight`** = distinct shared recordings whose first release year falls in that decade. One edge can span several decades; recordings with no known year are excluded; summed weights must not exceed `edges.weight` (checked in F3.3). `first_year` / `last_year` = earliest / latest first-release year of the shared recordings.
  3. **Genre attribution rule** (PRD §5): artists count only by their primary genre; only labelled edges enter genre metrics; the genre × genre matrix is symmetric, with within-genre collaboration on the diagonal.
  4. **Import CSV headers** pinned in PRD §8.3.
  5. **Provisional values** for OD1–OD7 until gate G1 (PRD §14).
  6. **Primary-genre ties** broken by alphabetical parent-genre name; F2.5 reports how many artists tied.
  7. **Bridge-score percentile** is taken over all artists in the loaded graph.
  8. **Combining artist tags with release-group tags** is left to F2.5, which records its own decision.
  9. **`min_recordings` filter** is applied when building `graph/`, so `graph/` and Neo4j hold the same graph.
- **Consequences:** These edits are part of drafting v1, because the contract isn't frozen until M1, so there's no version bump. F1.1 creates `.env.example` and the first `config/pipeline.yaml`. F3.3 gains the `edges_by_decade` check.

### D-012 — Fixed Jupyter/Spark ports, Jupyter without a token
- **Date / phase:** 2026-10-10 · P1 (F1.1)
- **Status:** Accepted
- **Context:** The Spark + Jupyter container needs a Jupyter port, a Spark UI port, and a way to authenticate to Jupyter. None of these is in the PRD §8.5 `.env` contract. Adding them would extend the contract; leaving them out means hard-coding per-machine settings.
- **Decision:** Keep §8.5 unchanged. Fix Jupyter at port 8888 and the Spark UI at 4040 in `docker-compose.yml`, and run Jupyter without a token. Every published port (Neo4j's included) is bound to `127.0.0.1` only.
- **Consequences:** Nothing is reachable from the network, but any local process can use Jupyter, and through it the repo and `$MUSIN_DATA`. If 8888 or 4040 is taken on a member's machine, they edit the port mapping locally. Revisit (add `JUPYTER_PORT`, `JUPYTER_TOKEN`, `SPARK_UI_PORT` to §8.5) if either becomes a problem.

### D-013 — Snapshot 20261007-002147, verified by signed checksums (OD6)
- **Date / phase:** 2026-10-10 · P1 (F1.2)
- **Status:** Accepted
- **Context:** OD6 asked which MusicBrainz full export to pin. The download also needed a source URL, a file list, and a way to trust the files, none of which were config keys yet. The server keeps only the two most recent weekly exports (20260930-002222 and 20261007-002147 on 2026-10-10).
- **Decision:**
  1. Pin `snapshot_date: "20261007-002147"`, the latest full export when F1.2 ran (as provisionally agreed for OD6). The value is the export's directory name, because that is the snapshot's unique ID.
  2. Download only `mbdump.tar.bz2` (core, CC0) and `mbdump-derived.tar.bz2` (tags, CC BY-NC-SA 3.0). The other archives (edit history 16 GB, editors, stats, cover art, CD stubs) are not needed by any RQ.
  3. Trust chain: `SHA256SUMS` must carry a valid GPG signature from the MusicBrainz data-dump signing key (fingerprint `D5E63B4BDCCE195642948684B8FC2375C777580F`, public key committed as `config/musicbrainz-signing-key.asc`), and every file must match its SHA-256.
  4. New `source.*` keys in `config/pipeline.yaml` and PRD §8.4: `base_url`, `files`, `signing_key_file`, `signing_key_fingerprint`.
- **Consequences:** This snapshot leaves the official server about two weeks after 2026-10-10. After that, our verified copy in `$MUSIN_DATA/dumps/20261007-002147/` is the only source; a member who needs the files copies them from the reference machine and runs `--verify-only`. All report numbers refer to this snapshot.

### D-014 — Raw layer: 22 tables, schema 31, generated explicit schemas
- **Date / phase:** 2026-10-10 · P1 (F1.3)
- **Status:** Accepted
- **Context:** F1.3 had to confirm which dump tables to ingest and where their column order comes from. Dump files are headerless; a wrong column order would silently shift every value.
- **Decision:**
  1. Ingest the 21 tables of PRD §6.2 plus `l_artist_release`, which F1.4 needs to measure release-level credit coverage for OD7. Nothing else (no `recording_tag`, aliases, or link attributes) until a feature needs it.
  2. Column names, order, and types come from `admin/sql/CreateTables.sql` at musicbrainz-server commit `c93f815cbfe10f1c5ba0b752a8ef878a15ee68df` (tag `v-2026-09-21.0`, the `production` head at dump time, declaring `DB_SCHEMA_SEQUENCE 31`, matching both archives' `SCHEMA_SEQUENCE`). A generator turns it into the committed module `src/musin/schemas/tables.py`; the pipeline never parses SQL at run time, and stages refuse to run if the extracted `SCHEMA_SEQUENCE` differs.
  3. `raw/` keeps Postgres types faithfully (`SMALLINT` → short, `INTEGER`/`SERIAL` → int, `UUID` → string, `TIMESTAMP WITH TIME ZONE` → timestamp in UTC). Widening to the §8.1 types (e.g. `artist_id` long) happens in `clean/`.
  4. Parsing is strict: wrong field counts and malformed values fail the stage (ANSI casts), so `raw/` either matches the dump exactly or does not exist.
- **Consequences:** A new snapshot with a different schema sequence requires regenerating the schema module. Contract §8 is unaffected.

### D-015 — Profiling report parameters and definitions
- **Date / phase:** 2026-10-10 · P1 (F1.4)
- **Status:** Accepted
- **Context:** The profiling report (FR-03) feeds the G1 open decisions. Its own parameters (threshold grid, top-k sizes, histogram cap) must come from config (RULES §5), and its coverage figures need fixed definitions so the numbers mean the same thing at G1 and in the report.
- **Decision:**
  1. New `profiling:` keys in `config/pipeline.yaml` and PRD §8.4: `min_recordings_grid [1, 2, 3, 5, 10]`, `credit_size_cap 20`, `top_k_hubs 30`, `top_k_genres 150`, `top_k_join_phrases 30`. They shape only the report, never the graph.
  2. Definitions: a **credited artist** appears in the `artist_credit_name` of at least one recording's credit; **credit size** = distinct artists in a recording's credit; **pair-instances** = n(n−1)/2 per recording; a **genre tag** is a tag whose name equals (case-insensitively) an official genre name or genre alias, counted only with vote count > 0; **first release year** = earliest `date_year` over all releases containing the recording (country and unknown-country dates); **contribution coverage** uses the `contributions.*` allowlists at recording level, and for OD7 the same names at release level, mapped to recordings through track → medium → release.
  3. The `min_recordings` grid counts **credit-pair edges only** (credits ≤ `max_credit_size`), before the F2/F3 filters (special artists, band members). It is a preview for OD2, not the final graph size.
- **Consequences:** Profiling numbers are reproducible from `python -m musin.profiling` and land in `reports/tables/profile_*.csv`; G1 decisions cite those files.

### D-016 — `artistId` imported as an integer via `{id-type:long}`
- **Date / phase:** 2026-10-10 · P1 (F1.5)
- **Status:** Accepted (the contract is draft v1 until M1, so no version bump)
- **Context:** PRD §8.3 requires `artistId` to be an integer property and left the importer option to F3.4. The F1.5 smoke test imported the exact §8.3 header `artistId:ID(Artist)`: Neo4j 5.26 stored `artistId` as `STRING` for every artist. The global `--id-type=integer` cannot be used, because the `Genre` ID group uses genre names.
- **Decision:** The `artists.csv` header in PRD §8.3 becomes `artistId:ID(Artist){id-type:long},…`. The per-group override stores `artistId` as `INTEGER` (verified on the toy graph) and leaves `Genre` IDs as strings. Edge and `IN_GENRE` files keep `:START_ID(Artist)` / `:END_ID(Artist)` unchanged.
- **Consequences:** F3.4 writes this header. Cypher lookups such as `{artistId: 177}` match integers, and joins with Spark output (`artist_id` long) need no casts. The import command lists the database name before `--nodes`/`--relationships`, which are greedy (F1.5 doc §3.2).

### D-017 — COLLAB projection: one relationship per artist pair, weights summed
- **Date / phase:** 2026-10-10 · P1 (F1.5)
- **Status:** Accepted
- **Context:** PRD §5 and the glossary define COLLAB as all edge types merged into one undirected weighted graph, but not how parallel edges are merged. A pair can have several edges: `CO_CREDITED` plus `FEATURED`, or `FEATURED` a → b plus `PERFORMED_FOR` b → a. A native GDS projection of the four types keeps them as parallel relationships. On the toy graph that left weighted degree unchanged but changed betweenness by up to 2.8 (e.g. 17.5 → 20.3), because GDS counts each parallel edge as a separate shortest path. Native `aggregation: 'SUM'` merges only within one type and direction.
- **Decision:** COLLAB is built as a Cypher-aggregation projection. Each edge's endpoints are ordered by `artistId`, weights are summed per unordered pair across all four types and both directions, and the result is projected with `undirectedRelationshipTypes: ['*']`. Reference implementation: `neo4j/smoke/03_projections.cypher` (`collab_undirected`).
- **Consequences:** Weighted degree, PageRank, betweenness, and Louvain all see one relationship per pair. GDS reports `relationshipCount` = 2 × pairs for this undirected graph. F3.5 builds the production `collab_undirected` this way and runs `.estimate` first; a Cypher projection over the full graph is slower to build than a native one, and F3.5 records the build time. Type-specific analyses use their own projections.

### D-018 — Special-purpose artist list and artist cleaning rules
- **Date / phase:** 2026-10-11 · P2 (F2.1)
- **Status:** Accepted
- **Context:** FR-04 excludes special-purpose artists everywhere downstream, but the PRD only gives examples. F1.4 showed them as the largest hubs (`[unknown]` on 225,951 recordings). The list must be reproducible and must not drop real artists who share a placeholder-like name.
- **Decision:**
  1. `config/special_artists.csv` lists 15 artists by MBID: the 12 whose MusicBrainz comment marks them "Special Purpose Artist", the reserved IDs 1 (Various Artists) and 2 (Deleted Artist), and `[mechanical instruments]` (type-`Other` sound-source placeholder). Bracketed names without the MusicBrainz mark, and real artists named "Anonymous"/"Unknown", stay in.
  2. The stage fails if a listed MBID is missing from the dump or its name differs from the file.
  3. `clean/artists` is the allowed artist set for every later stage; `clean/special_artists` holds the excluded IDs for cheap anti-joins.
  4. `begin_year` later than the snapshot year is set to null (25 artists); years ≤ 0 are kept (historical people).
- **Consequences:** 12,426 credit rows (12,076 artist credits) lose a placeholder artist; F2.2 handles what that means for featuring detection. Unmarked placeholders (`[blank]`, `[announcer]`) remain and are watched in the top-10 spot checks. A new snapshot needs the list re-checked.

### D-019 — Sample-mode output layout
- **Date / phase:** 2026-10-11 · P2 (F2.2)
- **Status:** Accepted
- **Context:** FR-12 requires a `--sample <fraction>` mode for every pipeline stage, and RULES §2 requires each stage to run in sample and full mode. F2.2 is the first `clean/` stage that reads recordings, so sample output needs a home that can never overwrite full data. F1.4 used an ad-hoc `tmp/profiling_sample/`.
- **Decision:** Recording-level outputs of a sample run go to `$MUSIN_DATA/sample/f<fraction>/<paths.layer>/` (e.g. `sample/f0.05/clean/credits`), via `musin.common.layers.layer_path`. Artist-level tables (`clean/artists`, `clean/special_artists`) are not sampled and are always read from the full layer. A sampled stage reads its recording-level inputs from the same sample tree.
- **Consequences:** Several fractions can coexist, and a sample run is never mistaken for full output. F3.2 builds the dev sample graph from this tree (`sample/f<fraction>/graph/`). F1.4's profiling sample location is unchanged (report-only output).

### D-020 — Credit roles decided before removing special artists
- **Date / phase:** 2026-10-11 · P2 (F2.2)
- **Status:** Accepted
- **Context:** PRD §8.2 defines featured artists by their position after a featuring join phrase, and FR-04 removes special-purpose artists. The order matters for credits such as `[unknown] ft. X`: removing the placeholder first would make X the first slot and therefore main.
- **Decision:** Roles are computed on the complete MusicBrainz credit; special artists are removed afterwards, and the remaining artists keep their role and original position. When one credit lists the same artist twice, it gets one row: the first position, and `main` if any slot is main.
- **Consequences:** 117 recordings keep featured artists with no main artist (e.g. guests on `[theatre]` cast recordings); they produce no `FEATURED` edge, because the main collaborator is unknown. 5,204 repeated slots are collapsed. Positions can have gaps where a placeholder was removed.

### D-021 — `Character` artists left out of the graph, with a sensitivity run
- **Date / phase:** 2026-10-11 · P2 (after F2.2)
- **Status:** Accepted. Reversible: set `filters.exclude_artist_types: []` and re-run F3.1 onward.
- **Context:** F2.2 found singing-synthesizer voicebanks (type `Character`) among the most-featured artists. An exploratory query on `clean/credits` (credits of 2–10 artists, PRD §8.2 edge rules, 2026-10-11) measured the effect:
  - 15,908 `Character` artists on 194,044 recordings (242,042 credit rows). Most are not voicebanks but fictional characters: German audio-drama characters (Benjamin Blümchen 9,123 recordings, Bibi Blocksberg 7,650, John Sinclair 6,086), the Smurfs, Barbie, Peppa Pig, and anime characters. Voicebanks (初音ミク 12,565, 鏡音リン, GUMI, 巡音ルカ, IA) are the ones credited as featured.
  - Edges touching a Character: 62,653 of 2,550,363 pairs (2.5 %); CO weight 205,393 of 9,770,581 (2.1 %); FEATURED weight 37,234 of 1,448,859 (2.6 %).
  - **RQ3:** the featuring share per decade changes by at most 0.0007 (2010s 0.1828 with, 0.1829 without), so it is negligible.
  - **RQ4 / RQ1:** 初音ミク is the largest hub in the graph (2,282 distinct collaborators, ahead of the London Symphony Orchestra's 1,952), and 19 Characters have ≥ 100 collaborators. Her collaborators span j-pop, electronic, EDM, hip hop, and rock (only 196 of 2,282 have an artist-level genre tag), so she could rank as a top "genre bridge" although she is a software instrument.
  - **Cost of excluding:** 3,121 artists (0.35 % of 896,706 with an edge) have only Character partners and would leave the graph; 11,469 lose at least one partner.
- **Decision:**
  1. Artists whose `artist_type` is in `filters.exclude_artist_types` (value `[Character]`) are left out of the graph. An edge to a Character is not a human collaboration: it is either an instrument (voicebank) or a role voiced by a real performer.
  2. The filter is applied when **building edges (F3.1)**, not in `clean/`. `clean/credits` keeps Character artists, so flipping the key only needs F3.1 onward to re-run. The excluded artists are removed the same way as special artists (D-020): roles are already decided, the remaining artists keep their roles, and "producer feat. 初音ミク" yields no edge.
  3. **Sensitivity run:** RQ1's bridge table and RQ4's top-20 lists are also produced from a graph built with `exclude_artist_types: []`, and the report states where the main results differ. How the second build is stored is decided in F3.2/F4.1.
- **Consequences:** Main results exclude 2.5 % of pairs and about 3.1 k mostly Vocaloid producers; the report states this J-pop/Vocaloid coverage loss as a limitation. Changing the decision later costs one config line plus a re-run of F3.1 → F3.4 and the P4 scripts; it becomes expensive only once P4 figures and report text exist (after ≈ Oct 30). F3.1 reports the final counts from the real edge build.

### D-022 — Contribution extraction rules
- **Date / phase:** 2026-10-11 · P2 (F2.3)
- **Status:** Accepted
- **Context:** FR-06 extracts contribution credits through the `contributions.*` allowlists. The PRD leaves open how link types are matched, what one output row means, and how release-level credits (OD7) map to recordings.
- **Decision:**
  1. Allowlisted names are matched against `link_type` for the exact entity pair (artist → recording; artist → release for OD7). A configured name that doesn't exist stops the stage. The MusicBrainz link-type hierarchy is not followed automatically.
  2. `clean/contributions` has one row per (recording, artist, edge type), with the sorted link-type names behind it and a `level` (`recording` wins over `release`), so F3.1 never counts one recording twice for the same pair.
  3. If `filters.use_release_level_credits` is true, an artist → release relationship applies to every recording on that release.
  4. Special artists are removed here (FR-04); excluded artist types (D-021), group members (FR-11), and self-edges are handled in F3.1.
- **Consequences:** Default run: 11,097,331 rows on 4,011,210 recordings. With release level: 16,948,022 rows (+52.7 %) but recording coverage only 9.91 % → 11.63 %, because album-wide credits repeat on every track and inflate album producers' edge weights. This is evidence for OD7 at G1. `conductor` and `performing orchestra` (1.8 M relationships) stay out unless the allowlist changes (OD1/OD5).

### D-023 — Plausible release years: 1877–snapshot year
- **Date / phase:** 2026-10-11 · P2 (F2.4)
- **Status:** Accepted
- **Context:** FR-07 derives each recording's first release year from release dates. F1.4 found impossible values (minimum 206; dates up to 3036). The earliest date decides a recording's year, so one typo can move a recording by centuries.
- **Decision:** A release date counts only if its year is in [`years.min_release_year` = 1877, snapshot year]. 1877 is the phonograph, so no sound recording can have been released earlier. The snapshot year is the latest year that can have happened at dump time. Out-of-range dates are ignored, not clamped; a recording with no counted date gets `first_year` null. New config key `years.min_release_year` (PRD §8.4).
- **Consequences:** On the full dump, 1 date before 1877 and 225 after 2026 are ignored. 1,473 recordings change: 1,472 become null and 1 moves to its next real year. Genuine early recordings (1878, 1888, 1890s) are kept; plausible typos (e.g. 1884 for a 1980s single) survive. The F1.4 profile keeps reporting unbounded raw years.

### D-024 — Parent genres and the genre mapping (OD4)
- **Date / phase:** 2026-10-11 · P2 (F2.5)
- **Status:** Accepted as the OD4 draft; G1 (Oct 16) may still adjust single rows of the mapping.
- **Context:** RQ1–RQ3 need 12–15 parent genres (OD4), and MusicBrainz has 2,212 official genres. F2.5 has a 1-day timebox.
- **Decision:**
  1. 15 parents: rock, metal, punk, pop, electronic, hip hop, r&b & soul, jazz, blues, classical, folk & world, country, latin, reggae, experimental. Loosely Discogs' top-level styles, with metal and punk split from rock and experimental kept as its own parent.
  2. `config/genre_mapping.csv` lists **all** 2,212 official genres with their parent (empty = deliberately unmapped) and the basis: a head-noun rule (the last word decides) plus 308 manual decisions, covering every genre above about 100 votes. Non-style tags (instrumental, lo-fi, comedy, spoken word, christmas music…) are unmapped.
  3. The stage fails unless the CSV lists exactly the dump's official genres.
- **Consequences:** 97.49 % of positive genre votes map to a parent; the unmapped rest is mostly non-style tags (about 0.4 % of votes are small real genres). Boundary cases (synth-pop, post-punk, disco, new age) are documented per row and can be changed by editing one line and re-running F2.5 onward.

### D-025 — Genre profile source: artist tags first, then main-artist release-group tags
- **Date / phase:** 2026-10-11 · P2 (F2.5)
- **Status:** Accepted
- **Context:** PRD §8.1 left open how artist tags and release-group tags combine into `artist_genre.share`. Artist tags alone label only 9.9 % of credited artists; release-group tags are about 10× more numerous, and propagating them to every credited artist mislabels featured guests (F1.4).
- **Decision:** Each artist's profile comes from one source. It uses the artist's own genre tags if any map to a parent genre; otherwise it uses the tags of release groups on which the artist is a **main** credited artist (F2.2 featured rule). Only positive votes count. `share` = votes / artist total; the primary genre is the highest share with the alphabetical tie-break (PRD §8.1), and a `tied` flag is stored. No minimum-vote threshold.
- **Consequences:** 558,473 artists get a primary genre (213,418 from artist tags, 345,055 from release-group tags), i.e. 26.6 % of credited artists and 69.9 % of appearances. 29.8 % of labelled artists rest on one vote and 11.4 % are tied; G1 may add a minimum-vote rule via a new `genres.*` key.

### D-026 — Graph build rules: credit size, filter scope, and drop order
- **Date / phase:** 2026-10-11 · P3 (F3.1)
- **Status:** Accepted
- **Context:** FR-09–FR-11, D-021, and OD1 say *which* edges leave the graph, but not how credit size is counted, whether the `max_credit_size` cap also applies to contribution edges, which edge types the band-member filter covers, or what `include_classical: false` means. The P2 phase doc asked F3.1 to decide the credit-size count explicitly.
- **Decision:**
  1. **Credit size** = distinct artists of the recording in `clean/credits`: special artists already removed (F2.1/D-020), repeated slots collapsed, **`Character` artists still counted**. A recording's size therefore does not change when `filters.exclude_artist_types` changes, so the D-021 sensitivity build differs from the main build only by Character edges.
  2. A recording with more than `filters.max_credit_size` credited artists produces **no edges of any type**, contribution edges included: contributor × main is also a product, and these recordings are orchestral, cast, and compilation credits where "who worked with whom" is not readable from the credit.
  3. **Band members (FR-11):** a `member of band` relationship (artist → artist, any period) removes edges between the two artists for **all four edge types**, in both directions. The link-type name is matched in `raw/link_type`; the stage fails if it is missing.
  4. **`include_classical: false`** (not the current value) removes artists whose F2.5 primary genre is `classical`, as an endpoint filter like D-021. `true` (provisional OD1 value) removes nothing. G1 may still choose a different definition.
  5. **Drop order** for counting: self-loop → excluded artist type → excluded classical → band-member pair. Every edge instance is counted under the first rule that removes it; the result does not depend on the order.
- **Consequences:** Full build (2026-10-11): 12,769 recordings (0.03 %) over the cap skip 1,464,888 pair-instances and 37,940 contribution rows; 2,464,512 instances are removed as band-member pairs, 80.8 % of them `PERFORMED_FOR` (members credited for instruments on their own band's recordings). Filters are applied in F3.1 only, so changing any of these keys means re-running F3.1 onward.

### D-027 — `recording_count` counts billed and contribution credits
- **Date / phase:** 2026-10-11 · P3 (F3.1)
- **Status:** Accepted (interpretation of PRD §8.1 within draft contract v1; no version bump)
- **Context:** PRD §8.1 defines `nodes_artist.recording_count` as "distinct recordings the artist is credited on", and §8.3 keeps an artist only if `recording_count ≥ filters.min_recordings`. If "credited" meant billed only, producers and session musicians who never appear in an artist credit would have `recording_count` 0 and be removed at any threshold ≥ 1, together with every `PRODUCED_FOR` / `PERFORMED_FOR` edge they start. D-008 keeps contribution edges precisely to study those people.
- **Decision:** `recording_count` = distinct recordings on which the artist is billed (`clean/credits`) **or** holds a contribution credit (`clean/contributions`), counted over all input recordings (including those over `max_credit_size`) before any edge filter. In sample mode it is counted within the sample.
- **Consequences:** 267,862 artists have only contribution credits; 225,693 of them are nodes of the full graph (19.1 % of 1,180,939 nodes). F1.4's OD2 preview counted billed recordings only, so its grid is not directly comparable with F3.1's node counts; G1 should read OD2 from F3.1's numbers.

### D-028 — Each recording counts once per artist pair (no double counting in COLLAB)
- **Date / phase:** 2026-10-11 · P3 (F3.1 rework)
- **Status:** Accepted by member A; contract change within draft v1 (no version bump, as D-011), to be confirmed by both members at M1 (Oct 13)
- **Context:** PRD §8.2 created a contribution edge to every main artist even when the contributor was also billed on the same recording, and D-017 sums all four types per pair for COLLAB. The first F3.1 build showed that 4,026,709 of 10,339,747 non-self `PERFORMED_FOR` instances (38.9 %) and 138,726 of 2,762,816 `PRODUCED_FOR` (5.0 %) came from a billed contributor (e.g. Springsteen's vocal credit on "Bruce Springsteen & The E Street Band" recordings). Such recordings counted twice for the pair in COLLAB, inflating weighted degree, PageRank, and Louvain weights for well-documented acts.
- **Decision:** For each recording, an unordered artist pair gets **at most one edge instance**, chosen by precedence: billed type (`CO_CREDITED` / `FEATURED`, at most one per pair) → `PRODUCED_FOR` → `PERFORMED_FOR`. Lower-precedence instances are dropped in F3.1 (`drop_reason = same_recording_pair`) and the stage fails if any (recording, pair) remains twice. PRD §8.2 gains this rule.
- **Consequences:** Summing type weights per pair (D-017) now equals the distinct shared recordings of the pair. Full build: edges 4,204,209 → 3,541,659; 4,054,365 `PERFORMED_FOR` and 134,833 `PRODUCED_FOR` instances dropped; nodes unchanged (1,180,939). Type-specific edges now mean "the strongest documented relationship on those recordings": `PERFORMED_FOR` no longer records a billed artist's own instrument credits, and a producer who also played counts as `PRODUCED_FOR` only. Billing outranks production because it is the explicit, public statement of collaboration and exists for every recording; production outranks performance because producer credits are rarer and are the subject of OD5.

### D-029 — Dev sample graph: fraction 0.05, identified by content fingerprints
- **Date / phase:** 2026-10-11 · P3 (F3.2)
- **Status:** Accepted
- **Context:** Member B develops P4 against a sample graph before the full graph is loaded (M2/M3). FR-12 fixes the sampling unit (hashed recording id), but not the fraction member B uses, how two members can tell they hold the same graph, or what the sample may be used for. D-021 also left the storage of its Characters-included sensitivity build to "F3.2/F4.1".
- **Decision:**
  1. The shared dev graph is **fraction 0.05**, built with `python -m musin.graph.sample --sample 0.05` into `$MUSIN_DATA/sample/f0.05/` (D-019). It has 310,253 artists and 535,372 edges and builds in about 95 s with a 4 GB driver (NFR-05).
  2. Every sample build writes `graph/_manifest.json` with row counts and order-independent content fingerprints (Σ xxhash64 per row) of the four graph tables, plus the result-changing config. Two graphs are the same when their fingerprints match; results quote the `edges` fingerprint to say which graph they came from (RULES §7).
  3. The sample is for **developing and debugging** code. Weight-based shares (edge-type and decade weight shares) are within a few percent of the full graph, but counts, degrees, and label coverage are biased (labelled nodes 1.7×, classical nodes 2.6×). No report number comes from the sample.
  4. The D-021 sensitivity build stays with F4.1, which is its only consumer.
- **Consequences:** Both members can confirm they hold an identical dev graph by comparing four numbers. Other fractions can be built side by side if needed, e.g. for the F5.1 scaling benchmark. Results computed on the sample must not be compared numerically with full-graph results.

### D-030 — Graph quality checks: run-to-run tolerance and baseline
- **Date / phase:** 2026-10-11 · P3 (F3.3)
- **Status:** Accepted
- **Context:** RULES §6 requires, after every graph build, "row counts within the expected range of the previous run", without defining the range or the previous run. PRD §8.4 forbids thresholds in code.
- **Decision:**
  1. New config key `checks.max_row_change` = **0.05**. A check fails when a table's row count, or an edge type's count, differs from the baseline by more than 5 % (relative).
  2. The baseline is the **last passing check of the same graph folder** (full and each sample fraction separately), stored in `<graph>/_checks.json` with fingerprints. A failing check never overwrites it.
  3. A deliberate change is accepted with `--accept-changes`. The accepted counts are listed in `_checks.json`, and the change must be explained in a decision entry (e.g. each G1 decision).
  4. `python -m musin.graph.checks` is the required step after every full `graph.build`; the sample chain (F3.2) runs it automatically.
- **Consequences:** Rebuilding the same inputs gives 0 % change, so any drift has to be explained. In a test on the sample graph, `max_credit_size: 20` moved total edges by +4.7 % (inside 5 %), but `CO_CREDITED` by +8.86 %. The per-type check caught it. After G1 the first rebuild will need `--accept-changes`. The key never changes results, so changing it needs no rebuild.

### D-031 — Neo4j export layout, CSV dialect, and the loaded-graph marker
- **Date / phase:** 2026-10-11 · P3 (F3.4)
- **Status:** Accepted
- **Context:** PRD §8.3 fixes the import headers but leaves file splitting to F3.4. Several things had to be settled:
  - where sample exports go, given that D-019 sample trees live outside the folder Neo4j can see
  - how Spark's CSV writer is made to produce what `neo4j-admin` parses identically
  - how anyone can tell which graph the single Community-Edition database holds (RULES §7)
- **Decision:**
  1. **Layout.** Exports go to `$MUSIN_DATA/<paths.import>/<name>/`, where `<name>` is `full` or `sample-f<fraction>`. Sample exports live there too, not in the D-019 sample tree, because docker-compose mounts only `import/` into Neo4j. Each §8.3 file is `<file>.header.csv` plus `<file>/part-*.csv` (≤ 1 M rows per part). The exact `neo4j-admin` arguments are written to `_import_args.txt`, so the import script lists no files itself.
  2. **CSV dialect.** RFC 4180 quoting with `""` inside quotes (Spark's default escape is a backslash). Whitespace is not trimmed (Spark's writer trims by default). Null is an empty unquoted field, so Neo4j omits the property. Empty strings are exported as null. A value containing a line break stops the export instead of switching on `--multiline-fields`.
  3. **Gates.** The export refuses a graph whose content fingerprints differ from its last passing F3.3 check, and it reads every file back with the header's types: the content must match the source exactly. The import runs with `--bad-tolerance=0`, no skipping of bad relationships or duplicate nodes, and must leave an empty report.
  4. **Marker.** `$MUSIN_DATA/neo4j/loaded_graph.json` names the loaded graph and its fingerprints. Every import (F3.4 script and the F1.5 smoke test) deletes it first and rewrites it only after its checks pass. P4 results quote it.
  5. **Default loaded graph until M3:** `sample-f0.05`, for member B (M2). The full graph is imported for verification and again at M3.
- **Consequences:** Export plus import of the full graph takes about 1.5 min (30 s export, 7 s `neo4j-admin`, 41 s verification), so switching graphs is cheap. A missing marker means "unknown database content".

### D-032 — Neo4j schema and the two production projections
- **Date / phase:** 2026-10-11 · P3 (F3.5)
- **Status:** Accepted. F3.5 is assigned to member B, who should confirm this at the P3 review.
- **Context:** FR-14 asks for constraints and indexes without naming them. PRD §5 needs COLLAB for every RQ and a credit-only projection for RQ4's sensitivity run. RULES §7 asks for named projections, `.estimate` before full runs, and dropping projections after use. Every import replaces the database, so a hand-made schema would be lost.
- **Decision:**
  1. **Schema:** unique `Artist.artistId`, unique `Artist.mbid`, unique `Genre.name`, and a range index on `Artist.name` (`neo4j/01_constraints.cypher`). It is applied by `python -m musin.gds.setup`, which `neo4j/import_graph.sh` runs after every verified import.
  2. **Projections:** `collab_undirected` (all four edge types) and `credit_only` (`CO_CREDITED + FEATURED`), both built by one Cypher-aggregation statement as D-017 defines (one undirected relationship per pair, summed weight). Only artists with an edge of the projected types are included, so `credit_only` has fewer artists.
  3. **Gates:**
     - The native-projection estimate must fit the heap before building.
     - Node count, relationship count (2 × pairs), weight total, and the 5 heaviest pairs must equal the Parquet graph.
     - Projection plus the largest §5.5 algorithm estimate must fit `NEO4J_HEAP_SIZE`.
     - Projections are dropped afterwards. P4 builds them with `musin.gds.setup.project()`.
- **Consequences:** On the full graph, COLLAB builds in 15 s (193 MiB), and the worst case for P4 (betweenness) needs 0.64 GiB of the 8 GiB heap. RQ4 must compare COLLAB and credit-only rankings on their common artists (888,062).
