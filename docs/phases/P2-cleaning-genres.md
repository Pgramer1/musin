# P2 — Cleaning & genre normalization

| | |
|---|---|
| **Status** | Done |
| **Planned dates** | 2026-10-15 – 2026-10-21 |
| **Actual dates** | 2026-10-11 (all five features) |
| **Data snapshot** | MusicBrainz full export `20261007-002147` (schema sequence 31) |

## 1. Goal

From PRD §11: turn the faithful `raw/` copy into a `clean/` layer that F3.1 can build edges from directly. That means: artists without special-purpose placeholders (FR-04), billed artists split into main and featured (FR-05), contribution credits via a configurable allowlist (FR-06), a first release year per recording (FR-07), and a genre profile and primary genre per artist (FR-08). Recording-level stages also gain a deterministic sample mode (FR-12).

## 2. What was built

| Feature | Status | Doc |
|---|---|---|
| F2.1 — Artist cleaning and special-purpose filter | Done | [F2.1-artist-cleaning.md](../features/F2.1-artist-cleaning.md) |
| F2.2 — Credit parsing (main / featured) | Done | [F2.2-credit-parsing.md](../features/F2.2-credit-parsing.md) |
| F2.3 — Contribution relationship extraction | Done | [F2.3-contributions.md](../features/F2.3-contributions.md) |
| F2.4 — Recording first release year | Done | [F2.4-first-release-year.md](../features/F2.4-first-release-year.md) |
| F2.5 — Genre normalization | Done | [F2.5-genre-normalization.md](../features/F2.5-genre-normalization.md) |

## 3. How it fits together

```
raw/artist ──F2.1──▶ clean/artists (3,005,860)         clean/special_artists (15)
                         │ allowed artist set                │ anti-joined everywhere (FR-04)
raw/recording + artist_credit_name ──F2.2──▶ clean/credits          (recording, artist, position, main|featured)
raw/l_artist_recording (+ release level if OD7) ──F2.3──▶ clean/contributions   (recording, artist, PRODUCED_FOR|PERFORMED_FOR)
raw/track → medium → release dates ──F2.4──▶ clean/recording_years  (recording, first_year | null)
raw tags + config/genre_mapping.csv ──F2.5──▶ clean/artist_genres + clean/artist_primary_genre
                                              reports/tables/genre_*.csv
```

- **Artist-level stages** (F2.1, F2.5) run full only, because every sampled stage needs the complete artist set.
- **Recording-level stages** (F2.2–F2.4) take `--sample <f>` and write to `$MUSIN_DATA/sample/f<f>/clean/` (D-019), so a sample never overwrites full data.
- **Role order:** roles are decided on the complete MusicBrainz credit before placeholders are removed (D-020). The same featured rule decides which artists inherit release-group genres (D-025).
- **What `clean/` does not filter:** `max_credit_size` (FR-10), group-member edges (FR-11), `Character` artists (D-021), classical (OD1), and `min_recordings` (OD2) are all applied when building edges in F3.1. A filter change therefore only re-runs F3.1 onward.

## 4. Key numbers

| Output | Rows (full) | Size | Full runtime |
|---|---:|---:|---:|
| `clean/artists` | 3,005,860 | 177 MB | 18 s |
| `clean/special_artists` | 15 | 16 KB | (F2.1) |
| `clean/credits` | 47,887,343 (1,323,347 featured) | 240 MB | 53 s |
| `clean/contributions` | 11,097,331 | 95 MB | 43 s |
| `clean/recording_years` | 40,475,049 (37,468,137 dated, 92.6 %) | 193 MB | 179 s |
| `clean/artist_genres` / `artist_primary_genre` | 890,827 / 558,473 | 6.0 / 3.4 MB | 45 s |

- **Recordings:** 40,104,545 keep at least one credited artist; 370,504 (0.92 %) were credited only to placeholders.
- **Contributions:** 4,011,210 recordings (9.9 %); producer credits on 1,786,572 (4.4 %).
- **Genres:** 26.6 % of credited artists have a primary genre, covering 69.9 % of appearances. 97.5 % of genre votes map to one of the 15 parents.
- **Cross-checks with F1.4's independent code all match exactly:** 1,048,882 featuring recordings, 382,943 special-artist appearances, recording-level contribution counts per link type, and dated recordings after the year bounds.
- **Tests:** 93 `pytest` tests (P1 ended at 62), `ruff` clean.

## 5. Decisions made

- [D-018](../decisions.md#d-018--special-purpose-artist-list-and-artist-cleaning-rules): 15 special-purpose artists by MBID, verified against the dump; future `begin_year` → null.
- [D-019](../decisions.md#d-019--sample-mode-output-layout): sample outputs under `$MUSIN_DATA/sample/f<fraction>/`.
- [D-020](../decisions.md#d-020--credit-roles-decided-before-removing-special-artists): credit roles decided before removing special artists; repeated slots collapsed.
- [D-021](../decisions.md#d-021--character-artists-left-out-of-the-graph-with-a-sensitivity-run): `Character` artists left out of the graph (`filters.exclude_artist_types`), with an RQ1/RQ4 sensitivity run.
- [D-022](../decisions.md#d-022--contribution-extraction-rules): contribution extraction rules; release-level credits map to every recording of the release.
- [D-023](../decisions.md#d-023--plausible-release-years-1877snapshot-year): release years count only within 1877 – snapshot year.
- [D-024](../decisions.md#d-024--parent-genres-and-the-genre-mapping-od4): 15 parent genres and the full mapping CSV (OD4 draft).
- [D-025](../decisions.md#d-025--genre-profile-source-artist-tags-first-then-main-artist-release-group-tags): genre profile from artist tags first, else main-artist release-group tags.

## 6. Deviations from the plan

- **Schedule:** planned Oct 15–21, finished Oct 11, before gate G1 (Oct 16). The G1 open decisions still apply: P2 used the provisional values (`use_release_level_credits: false`; the others only matter from F3.1), and G1 can still adjust the D-024 mapping.
- **Scope added within existing features:** `filters.exclude_artist_types` (D-021) came out of an F2.2 finding (Vocaloid voicebanks as top hubs); `years.min_release_year` (D-023) is a new config key; the OD7 release-level path was implemented and measured on full data, but stays off.
- **No sample mode for artist-level stages** (F2.1, F2.5): RULES §2 asks every pipeline stage for sample and full mode, but these stages have no recordings to sample, and sampled downstream stages need the full artist set. Justified in each feature doc.
- **No PRs:** merged directly to `main`, as in P1.

## 7. Known issues and limitations

- **Coverage biases:** genres cover 26.6 % of credited artists (well-documented artists dominate labelled edges); contribution credits cover about 10 % of recordings with an era bias; electronic and rock are 45 % of labelled artists.
- **Thin genre evidence:** 29.8 % of labelled artists rest on one vote and 11.4 % have tied top shares. A minimum-vote rule is a G1 option.
- **Credit semantics:** ambiguous join phrases count as main (PRD §8.2); voice-actor `(CV:` credits pair characters with actors (moot for the main graph once D-021 removes Characters).
- **Years** are first-release years (compilations make old recordings look younger); 7.4 % of recordings have none; plausible typos survive.
- **Unmarked placeholders** (`[blank]`, `[announcer]`) remain in `clean/artists`.

## 8. How to reproduce this phase

From WSL Ubuntu, repo root, after P1 (`raw/` present); Neo4j stopped:

```bash
docker compose exec spark python -m musin.clean.artists --config config/pipeline.yaml
docker compose exec spark python -m musin.clean.credits --config config/pipeline.yaml        # [--sample 0.05]
docker compose exec spark python -m musin.clean.contributions --config config/pipeline.yaml  # [--sample 0.05]
docker compose exec spark python -m musin.clean.years --config config/pipeline.yaml          # [--sample 0.05]
docker compose exec spark python -m musin.clean.genres --config config/pipeline.yaml
docker compose exec spark pytest
```

F2.1 must run first (`clean/special_artists`); F2.5 needs `clean/credits` for its coverage report.

## 9. Handoff to the next phase

P3 (graph build and Neo4j load) can rely on:

- **`clean/credits`:** (recording_id long, artist_id long, position int, role `main`|`featured`), one row per (recording, artist), placeholders removed. It implements PRD §8.2's main × main / featured × featured / main → featured pairing input.
- **`clean/contributions`:** (recording_id, artist_id, edge_type `PRODUCED_FOR`|`PERFORMED_FOR`, link_types, level), one row per (recording, artist, edge type). F3.1 links each to every **main** artist of the recording and drops self-edges.
- **`clean/recording_years`:** one row per recording, `first_year` nullable, for `edges.first_year/last_year` and `edges_by_decade`.
- **`clean/artists`:** the §8.1 node columns (artist_id, mbid, name, artist_type, area_name, begin_year), plus `disambiguation`.
- **`clean/artist_genres` and `clean/artist_primary_genre`:** `artist_genre` (share sums to 1) and `primary_genre` / `genre_votes` for `nodes_artist`.
- **Sample trees** under `$MUSIN_DATA/sample/f0.05/clean/` for credits, contributions, and years (F3.2 builds the dev sample graph from them).

F3.1 must apply: `max_credit_size` (FR-10), group-member removal (FR-11, `l_artist_artist` "member of band"), `filters.exclude_artist_types` (D-021), classical handling (OD1), `min_recordings` (OD2), and the "at least one edge" node rule (PRD §8.3). It should also count credit size consistently: decide whether excluded or special artists count towards `max_credit_size`, and record that decision.
