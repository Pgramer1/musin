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
