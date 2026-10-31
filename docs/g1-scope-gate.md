# Gate G1 — evidence and proposals (Oct 16)

**Status:** prepared 2026-10-11 for the G1 meeting. **Outcome, recorded 2026-10-19:** the §1 recommendations were accepted ([D-039](decisions.md) OD1, [D-040](decisions.md) OD2–OD5, OD7). §2.1/§4 (spoken word), §2.2 (D-027/D-028 sign-off) and §2.3 (log weights) are still open. The rest of this note is the original draft, unchanged. Each decision taken at G1 gets its own `D-NNN` entry, and provisional values in `config/pipeline.yaml` change only after that.

**Graph used:** full build of snapshot `20261007-002147` at commit `2ac5547` (F3.1 with D-028): 1,180,939 artists, 3,541,659 edges.

**Where the numbers come from:**
- **Feature docs** and `reports/tables/` (F1.4, F2.3, F2.5, F3.1, F3.2)
- **An exploratory query run on 2026-10-11** (marked *(q)*). It removes artists or edges from the built graph instead of rebuilding it. For classical and `min_recordings` this gives exactly what a rebuild would, because both are endpoint filters (D-026 §5). The spoken-word figures are a tag-based proxy (§2.1).

"COLLAB" below means all edge types merged per artist pair with summed weights (D-017). With D-028 that weight is the number of shared recordings.

## 1. Open decisions OD1–OD5, OD7

| ID | Provisional | Evidence | Recommendation |
|---|---|---|---|
| **OD1** classical | `include_classical: true` | See §1.1 | **Analyse separately:** main graph with `include_classical: false`, plus a classical-included sensitivity run, as for D-021 |
| **OD2** `min_recordings` | `1` | See §1.2 | **Keep 1** |
| **OD3** `max_credit_size` | `10` | ≤ 10 keeps 99.97 % of recordings and 88.5 % of pair-instances (F1.4). In F3.1, 12,769 recordings over 10 skip 1,464,888 pair-instances and 37,940 contribution rows. | **Keep 10** |
| **OD4** parent genres | 15-parent draft (D-024) | 97.5 % of genre votes map to a parent. 23.4 % of graph artists have a primary genre, and 35.2 % of edges are labelled at both ends (F3.1). Of labelled artists, 29.8 % rest on one vote and 11.4 % are tied (F2.5). | **Accept the draft**; review single rows only on objection |
| **OD5** `PRODUCED_FOR` | own edge type | Producer credits on 4.4 % of recordings, peaking in the 1960s–80s (F1.4). In the graph: 291,319 edges carrying 13.8 % of COLLAB weight (F3.1/F3.2). | **Keep it as its own type**, and keep RQ4's credit-only sensitivity (PRD §5) |
| **OD7** release-level credits | `false` | +52.7 % contribution rows, but recording coverage only 9.91 % → 11.63 %. Album credits repeat on every track and inflate producer weights (F2.3). | **Keep `false`** |

### 1.1 OD1 — classical

What classical does to the graph:

- 25,717 artists (2.2 %) have primary genre classical, and they touch 463,140 edges (13.1 %) (F3.1).
- They take **57 of the top 100** places by COLLAB weighted degree *(q)*, and 6 of the top 10 by distinct collaborators (F3.1).
- The 25 heaviest pairs include Marriner–Academy of St Martin (6,107) and Karajan–Berliner Philharmoniker (5,859) *(q)*.
- Composers are billed as artists on recordings of their works, so George Frideric Handel is in the top 25 by weighted degree, with 757 collaborators *(q)*. An edge between a dead composer and a living orchestra is not a collaboration in the sense of the research questions.

With `include_classical: false` *(q)*:

- **Size:** 1,135,461 nodes (−3.9 %) and 3,078,519 edges (−13.1 %).
- **Isolated artists:** 19,761 non-classical artists lose all their partners.
- **Top 100 by weighted degree:** 51 unlabelled, 25 jazz, 10 rock, 5 electronic, 4 hip hop, 3 pop, 1 r&b & soul, 1 folk & world.
- **What fills the top 10:** Springsteen and the E Street Band (§2.3) and audiobook authors and narrators (§2.1).

**Cost:** one config line and a ≈ 3 min rebuild (F3.1). The classical branch is already implemented (D-026 §4: primary genre `classical`).

### 1.2 OD2 — `min_recordings`

Real graph sizes at each threshold *(q)*. F1.4's grid counted billed recordings only (D-027):

| `min_recordings` | Nodes | Edges |
|---:|---:|---:|
| 1 | 1,180,939 | 3,541,659 |
| 2 | 845,385 | 3,087,290 |
| 3 | 715,515 | 2,859,982 |
| 5 | 593,942 | 2,597,158 |
| 10 | 451,767 | 2,236,900 |

Raising the threshold to 2 removes 28 % of nodes but only 13 % of edges; the removed nodes are mostly one-recording artists on the edge of the graph. Every row fits the reference machine. Keeping 1 loses nothing, and a GDS projection can still filter by `recordingCount` if one algorithm is too slow.

## 2. New issues found in P3 (no open decision yet)

### 2.1 Spoken word (audiobooks, audio dramas)

**Evidence:**
- Author–narrator pairs are **14 of the 25 heaviest COLLAB pairs** *(q)*: William Dufris–D. J. MacHale 11,222, Stephen Fry–J. K. Rowling 9,297, Stephen King–David Nathan 8,902, Diana Gabaldon–Birgitta Assheuer 6,741, Tom Clancy–Frank Arnold 5,063, …
- Stephen King, David Nathan, J. K. Rowling and Terry Pratchett are in the top 20 by weighted degree *(q)*.
- They are not music collaborations, and they would surface as "genre bridges" in RQ1. F1.4 already flagged audiobook authors among the top hubs.

**Tag proxy *(q)*:** artists with a positive `spoken word`, `audio drama`, `audiobook`, `hörbuch` or `hörspiel` tag, on the artist or on a release group they are credited on.
- It flags 2,860 graph artists, who touch 100,183 edges (2.8 %) and 7.2 % of COLLAB weight. It catches 11 of the 25 heaviest pairs, but 5 of those 11 are Springsteen pairs (false positives, see the next bullet), so only 6 of the 14 audiobook pairs are caught.
- **It is not usable as a filter.** It flags **Bruce Springsteen** (his *Born to Run* audiobook) and misses narrator William Dufris. Removing the flagged artists would delete Springsteen's music, and the weighted top 10 would become 10 classical artists.

**Proposal:** filter **recordings**, not artists, by MusicBrainz's release-group secondary type, which marks audiobooks and audio dramas explicitly. The draft PRD entry is in §4.

### 2.2 Contract changes made in P3, for the M1 sign-off (Oct 13)

Both are already implemented and documented. They need sign-off because they interpret or change PRD §8.

- **[D-027](decisions.md#d-027--recording_count-counts-billed-and-contribution-credits)**
  - *What changed:* `recording_count` counts billed **and** contribution credits.
  - *Why:* otherwise 225,693 producer and session-musician nodes (19.1 %) drop out at any threshold.
- **[D-028](decisions.md#d-028--each-recording-counts-once-per-artist-pair-no-double-counting-in-collab)**
  - *What changed:* each recording counts once per artist pair (billed → produced → performed).
  - *Why:* without it, COLLAB double counts, for example a billed artist's own vocal credit. The rule removed 15.8 % of edges.
  - *Side effect:* PRD §8.2 now says this, and `PERFORMED_FOR` no longer shows billed artists' own instrument credits.

### 2.3 Weight concentration (Springsteen live archive), a P4 question

**Evidence:**
- Bruce Springsteen is #1 by weighted degree (145,087), from about 75 k recordings, mostly live releases. His three pairs with the E Street Band and its members are the heaviest in the graph (31–58 k shared recordings) *(q)*.
- The skew is narrow: the median pair weight is 1, the 99.9th percentile 238, and the top 100 pairs hold 2.3 % of COLLAB weight *(q)*. It mainly distorts weighted degree, and probably weighted PageRank around those artists.

**Option (not a data change):** use `log(1 + weight)` as the weight in the weighted P4 measures. On weighted degree *(q)*, this moves Springsteen out of the top 10 and gives LSO, Philharmonia, RPO, Frank Sinatra, Wiener Philharmoniker, LPO, Berliner, English Chamber Orchestra, Snoop Dogg and Handel. That is close to the ranking by distinct collaborators.

Changing the weight is a metric change (PRD §5) and needs a decision entry. Recommendation: decide together with OD1, because classical still leads under log weights.

**Not proposed:** capping weights or removing live recordings. Both need new rules about what a "real" recording is, which the data cannot support.

## 3. Suggested G1 agenda

1. M1 items: sign off D-027 and D-028 (§2.2) together with contract v1.
2. OD1 (§1.1), then the spoken-word proposal (§2.1 and §4), because each makes the other's effect visible.
3. OD2, OD3, OD4, OD5, OD7 (§1): confirm or change the provisional values.
4. Weight transform for P4 (§2.3).
5. After G1, the agreed config is set, F3.1 → F3.2 is rebuilt, and F3.3 is re-run. Each changed value gets a `D-NNN` entry.

## 4. Draft PRD entry: spoken-word recording filter

*Proposed text for PRD.md, to be added through the normal review only if G1 accepts it. Not yet in the PRD.*

**§6.2 Tables used — add a row:**

| Table(s) | Purpose |
|---|---|
| `release_group_secondary_type`, `release_group_secondary_type_join` | Release-group secondary types (Audiobook, Audio drama, Spoken word, …) for the spoken-word filter |

**§6.3 Known data issues — add a row:**

| Issue | Handling |
|---|---|
| Audiobooks and audio dramas credit authors and narrators like performers and form the heaviest edges | Recordings whose release groups are **all** of a secondary type in `filters.exclude_secondary_types` produce no edges (FR-28) |

**§8.4 Configuration — add a key under `filters`:**

```yaml
  exclude_secondary_types: [Audiobook, Audio drama, Spoken word]   # FR-28; [] keeps all
```

**§9 Functional requirements — add:**

| ID | Requirement | Priority |
|---|---|---|
| FR-28 | Exclude spoken-word recordings (all release groups of an excluded secondary type) from edge building | Should |

**§11 Features — add:**

| ID | Feature | Covers | Priority |
|---|---|---|---|
| F3.6 | Spoken-word recording filter: ingest the two secondary-type tables (F1.3 path), flag recordings, apply in F3.1 | FR-28 | Should |

**Design notes for F3.6** (to be confirmed when it is built):

- **"All" rather than "any" release group.** A music recording that also appears on a spoken-word compilation keeps its edges. A recording that only appears on audiobooks is dropped.
- **Recordings, not artists.** Springsteen's *Born to Run* audiobook is dropped and his music is kept. An author who also records music keeps those edges.
- **Recording → release group** goes via `track → medium → release → release_group`, the same path as F2.4.
- **Where it applies:** in F3.1 next to FR-10, as one more recording-level drop with its own count. `clean/` stays unfiltered, so switching it off is a config change, as for D-021.
- **Estimated effort:** about one day:
  - extend `musin.schemas` and `musin.ingest.extract`/`raw` for two small tables
  - one new F3.1 step and its tests
  - a full rebuild (≈ 3 min)
- **To verify:** that both tables are in `mbdump.tar.bz2` at schema sequence 31, and the exact secondary-type names, before the config values are fixed.
