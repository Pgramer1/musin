# Musin — Project Rules

These rules bind everyone working on the repo, **including** Claude Code. If a rule gets in the way, change it explicitly in this file. Don't quietly ignore it.

Scope, schema, and metric definitions live in [PRD.md](PRD.md). This file is about **how** we work.

---

## 1. Documentation — the build log

Every feature and every phase gets a written record of what was built, how, and why. The build log lets an examiner (or any new contributor) understand the project without reading the code, and it is most of the final report, already written.

### 1.1 Feature docs

- **When:** as part of finishing a feature. A feature without a doc is not done (§2).
- **Where:** `docs/features/F<phase>.<n>-<slug>.md`, e.g. `docs/features/F2.5-genre-normalization.md`.
- **How:** copy [docs/features/_TEMPLATE.md](docs/features/_TEMPLATE.md) and fill every section. If a section doesn't apply, write "n/a" and why. Don't delete it.
- **Must include:**
  - the phase and PRD requirement(s) it serves
  - what was built
  - how it works, and *why* it was done this way, including alternatives rejected
  - inputs and outputs, with real row counts
  - config keys and the values used
  - the exact command to run it
  - verification results
  - known issues

### 1.2 Phase docs

- **When:** when the last feature of a phase is done, or when the phase is closed with features cut or moved.
- **Where:** `docs/phases/P<n>-<slug>.md`, from [docs/phases/_TEMPLATE.md](docs/phases/_TEMPLATE.md).
- **Must include:** the phase goal; every feature built (linked); how the pieces fit together end to end; key numbers; decisions; deviations from the PRD and why; known issues; what the next phase can now rely on.

### 1.3 Index, README, and decisions

- Update [docs/README.md](docs/README.md) (status + link) **in the same commit** as the doc.
- Keep the root [README.md](README.md) current, in the same commit:
  - a phase changes status → the **Status** line and **Progress** table
  - a feature adds or changes setup or run commands → **Getting started**
  - results become available → **Results**
- Any decision that changes scope, the schema, a metric, or a threshold goes in [docs/decisions.md](docs/decisions.md) as a new `D-NNN` entry. Never edit an old decision; supersede it with a new one.

### 1.4 Writing standard

- **Explain why, not only what.** "Recordings with more than 10 credited artists are skipped because they produced `<n>`% of all edges in the profiling run" beats "added filter".
- **Use real numbers from actual runs** (row counts, runtimes, sizes). Never present an estimate as a result.
- **Link everything:** code paths (`src/musin/clean/credits.py`), config keys, output paths.
- **Write for an outsider:** someone who knows Spark and graphs but has never seen this repo.
- **Be honest:** "Known issues and limitations" is a required section, not an admission of failure.
- **One doc per feature:** if a feature is reworked later, update its doc and add a line under **Revisions**. Don't create a second doc.

## 2. Definition of Done

A feature is done only when all of these are true:

- [ ] Code merged to `main`
- [ ] Runs end-to-end with one documented command, in sample mode and (for pipeline stages) full mode
- [ ] Tests for pure logic pass; stage quality checks pass
- [ ] Feature doc written and `docs/README.md` updated
- [ ] Root `README.md` updated if setup, commands, status, or results changed
- [ ] Decisions logged in `docs/decisions.md`
- [ ] Report-bound outputs saved under `reports/`

## 3. Scope

- **The PRD is the source of truth.** Work on something only if it has a feature ID. New ideas go into the PRD through a PR first.
- **Timeboxes are hard.** Genre mapping gets 1 day. When a timebox runs out, ship what works and document the gap.
- **Must before Could.** Could-priority work starts only when every Must in that phase is done.
- **Feature freeze (Oct 30, M4):** after it, no new features. Only fixes, figures, and writing.

## 4. Git

- One repo; `main` always runs.
- Branch names: `feat/F2.5-genre-normalization`, `fix/F3.1-duplicate-edges`, `docs/P2`.
- Commit messages start with the ID: `F2.5: map tags to parent genres`.
- **No AI attribution:** commits and PRs carry no `Co-Authored-By: Claude …` trailer and no "Generated with Claude Code" line. Only the project's human authors are named.
- **Never commit data:** dumps, Parquet, CSV exports, Neo4j stores, or large notebook outputs. Small hand-made config files (`config/*.csv`, `config/*.yaml`) are committed.
- No secrets in the repo. Secrets and per-machine settings go in `.env` (gitignored; keys listed in [PRD §8.5](PRD.md#85-environment-contract-env)); keep `.env.example` current. Settings that change results go in `config/pipeline.yaml` ([PRD §8.4](PRD.md#84-configuration-contract-configpipelineyaml)).

## 5. Code (Python / PySpark)

- Python 3.11, type hints, `ruff` for lint and format.
- Pipeline code lives in `src/musin/`. Entry points are thin: `python -m musin.<stage> --config config/pipeline.yaml [--sample 0.05]`.
- **No hard-coded paths, thresholds, or analysis parameters.** Paths come from `MUSIN_DATA` + `config/pipeline.yaml`; every threshold and parameter comes from the config ([PRD §8.4](PRD.md#84-configuration-contract-configpipelineyaml)). A missing key fails loudly; there are no hidden defaults in code.
- **Explicit schemas** for every dump table (`src/musin/schemas/`). Never `inferSchema`.
- **Prefer built-in Spark functions.** A Python UDF needs a written reason in the feature doc (UDFs are slow); use a pandas UDF if one is unavoidable.
- **Never `.collect()` or `.toPandas()`** a frame that isn't already aggregated (rule of thumb: under 1 M rows).
- `cache()` only what is reused, and `unpersist()` it afterwards.
- **Deterministic output:** sample by hashed `recording_id`, fix seeds, sort explicitly where order matters.
- Log input/output row counts and runtime at every stage boundary.
- **Unit-test pure logic** on tiny fixtures in `tests/fixtures/`: join-phrase parsing, edge pairing, genre mapping, metric formulas. `pytest` must pass before merging.
- Each transformation function's docstring states its input and output columns.

## 6. Data

- **Layers:** `raw/` → `clean/` → `graph/` → `import/` → `reports/`. A stage reads only the layer before it, and rewrites its whole output.
- `raw/` is a faithful copy of the dump: types and nulls only, no filtering.
- **Never edit data by hand.** If data is wrong, fix the code or config and re-run.
- The snapshot date is pinned in `config/pipeline.yaml` and stated in every phase doc.
- Keep both `artist_id` (internal key) and `mbid` (public UUID). Reports use MBIDs and names.
- **Quality checks after every graph build:** no self-loops, no duplicate `(src, dst, type)`, every endpoint exists in `nodes_artist`, row counts within the expected range of the previous run.
- **Develop in sample mode.** Full runs are for verification and final numbers.

## 7. Neo4j and GDS

- All Cypher lives in numbered files under `neo4j/` (e.g. `neo4j/01_constraints.cypher`). A query shown in the report must exist in a file.
- Load with `neo4j-admin database import full`, not `LOAD CSV`, which is too slow at this size.
- Community Edition has one user database, so switching between the sample and full graph means re-importing. Note which graph each result came from.
- Run the algorithm's `.estimate` procedure before running it on the full graph.
- Name GDS projections clearly (`collab_undirected`, `credit_only`) and drop them when done; they live on the heap.
- Record every algorithm parameter (damping, iterations, sampling size, seed, concurrency) in the feature doc.
- Write final results back as node properties **and** stream them to `reports/tables/*.csv`.

## 8. Notebooks

- Notebooks are for exploration and analysis only. Reusable logic moves to `src/musin/`.
- Numbered names: `notebooks/04_rq1_bridges.ipynb`.
- A notebook must pass *Restart & Run All* before it is committed. Clear large outputs.
- Code saves figures to `reports/figures/` and tables to `reports/tables/`.

## 9. Analysis integrity

- Metrics follow the definitions in [PRD §5](PRD.md#5-research-questions). Changing one needs a decision-log entry and a doc update.
- Numbers in the report come from files in `reports/tables/`, never typed from memory.
- The validation list ([PRD §5.6](PRD.md#56-validation)) is committed **before** anyone looks at RQ1 results.
- Report what didn't work and what the data can't support. Limitations are a required report section.
- Graph figures show ego networks or subgraphs of at most a few hundred nodes.

## 10. Environment and resources

- Work inside WSL2 + Docker Compose. Don't run Spark natively on Windows.
- Data lives in the WSL filesystem (`MUSIN_DATA=~/musin-data`), never under `/mnt/c/`.
- **Memory budget:** WSL 18 GB · Spark driver 12 GB · Neo4j heap 8 GB + page cache 5 GB. **Never run Spark and Neo4j heavy jobs at the same time.**
- Full-data runs happen on the reference machine (24 GB RAM). Smaller machines use sample mode.
- Check free disk space before downloading or extracting dumps (keep ≥ 100 GB free).

## 11. Changes and reviews

- **Schema contract** ([PRD §8](PRD.md#8-data-model--schema-contract-v1)) changes need explicit approval, a version bump, and a decision entry.
- Hold a review at each phase gate.

## 12. Claude Code

Claude follows every rule in this file. In particular, it writes the feature doc as part of every feature, never adds scope that has no feature ID, and asks before multi-GB downloads or full-data runs. See [CLAUDE.md](CLAUDE.md).
