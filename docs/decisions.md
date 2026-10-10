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
