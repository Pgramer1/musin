"""Spark scaling benchmark: data fraction x cores on the recording chain (F5.1, FR-21).

    python -m musin.benchmark.scaling --config config/pipeline.yaml [--fresh]
    python -m musin.benchmark.scaling --config config/pipeline.yaml \
        --fractions 0.01,0.02 --cores 1,2 --repeats 1          # dev run, see below

Times the recording-level chain that F3.2 runs for a sample: clean.credits
(F2.2) -> clean.contributions (F2.3) -> clean.years (F2.4) -> graph.build
(F3.1), once per cell of the grid benchmark.scaling.{fractions x cores x
repeats} (D-041). A fraction of 1 is a full-mode run; smaller fractions are
sample mode (FR-12, hashed recording ids).

Each cell runs in a fresh Python + JVM process with local[<cores>] and the
Spark event log on (NFR-04). It reads and writes a scratch data root,
$MUSIN_DATA/<paths.benchmark>/scaling/data, whose raw/ and full artist tables
are symlinks to the real ones. All outputs, including the F3.1 summary table,
land in that root, so the pipeline's own layers and reports/ are never touched.

Outputs in $MUSIN_DATA/<paths.benchmark>/scaling/:
  runs/<cell>.json   one per finished cell. A re-run skips finished cells
                     (resume); --fresh deletes them first.
  spark-events/      Spark event logs, one per stage and cell
Once every cell has finished:
  <reports_dir>/tables/benchmark_scaling_runs.csv     seconds per cell and stage
  <reports_dir>/tables/benchmark_scaling_summary.csv  median per fraction x cores,
                                                      speedup, efficiency
  <reports_dir>/tables/benchmark_environment.csv      machine, versions, Spark settings
  <reports_dir>/figures/benchmark_scaling.png

Dev run: --fractions, --cores and --repeats replace the config grid. Everything
(scratch data, runs, tables, figure) then goes to
$MUSIN_DATA/<paths.benchmark>/dev/, so report numbers only ever come from the
config grid.
"""

from __future__ import annotations

import argparse
import copy
import json
import logging
import os
import platform
import shutil
import statistics
import subprocess
import sys
import time
from collections.abc import Callable
from dataclasses import dataclass
from datetime import UTC, datetime
from pathlib import Path

from musin.clean import contributions, credits, years
from musin.common.config import Config, ConfigError, load_config, require_env
from musin.common.provenance import repo_commit
from musin.common.spark import get_spark, use_benchmark_settings
from musin.graph import build
from musin.graph.build import write_table
from musin.graph.sample import FULL_ARTIST_TABLES

log = logging.getLogger("musin.benchmark.scaling")

# The timed chain, in pipeline order: every stage that reads recordings (F3.2).
STAGES: tuple[tuple[str, Callable[[Config, float | None], dict]], ...] = (
    ("clean.credits", credits.run),
    ("clean.contributions", contributions.run),
    ("clean.years", years.run),
    ("graph.build", build.run),
)
TOTAL = "total"
# Spark settings recorded per cell: what the benchmark varies, plus the defaults
# it deliberately leaves alone (D-041).
RECORDED_CONF = (
    "spark.master",
    "spark.driver.memory",
    "spark.sql.shuffle.partitions",
    "spark.sql.adaptive.enabled",
    "spark.sql.adaptive.coalescePartitions.enabled",
)
# Ordinal blue ramp (steps 250 -> 650 of the reference sequential hue): a larger
# fraction is a darker line. Step 250 is the lightest that clears 2:1 on the surface.
FRACTION_RAMP = ("#86b6ef", "#5598e7", "#3987e5", "#256abf", "#184f95", "#104281")
IDEAL = "#8a8985"


@dataclass(frozen=True)
class Cell:
    """One run of the chain: a data fraction, a core count, a repetition number."""

    fraction: float
    cores: int
    repeat: int

    @property
    def name(self) -> str:
        return f"f{self.fraction:g}_c{self.cores}_r{self.repeat}"

    @property
    def sample(self) -> float | None:
        """--sample value for the stages: None (full mode) at fraction 1."""
        return None if self.fraction == 1 else self.fraction


def grid(fractions: list[float], cores: list[int], repeats: int) -> list[Cell]:
    """Every cell, repetition-major, then fraction and cores ascending.

    Repetition-major order spreads each cell's repeats over the whole run, so
    drift (thermal state, page cache, background load) hits every cell alike
    instead of one cell's three repeats. ConfigError on an invalid grid.
    """
    if not fractions or not cores:
        raise ConfigError("benchmark grid needs at least one fraction and one core count")
    if len(set(fractions)) != len(fractions) or len(set(cores)) != len(cores):
        raise ConfigError(f"duplicate grid values: fractions {fractions}, cores {cores}")
    for f in fractions:
        if isinstance(f, bool) or not isinstance(f, int | float) or not 0 < f <= 1:
            raise ConfigError(f"benchmark fraction must be in (0, 1], got {f!r}")
    for c in cores:
        if isinstance(c, bool) or not isinstance(c, int) or c < 1:
            raise ConfigError(f"benchmark cores must be integers >= 1, got {c!r}")
    if isinstance(repeats, bool) or not isinstance(repeats, int) or repeats < 1:
        raise ConfigError(f"benchmark repeats must be an integer >= 1, got {repeats!r}")
    return [
        Cell(float(f), c, r)
        for r in range(1, repeats + 1)
        for f in sorted(fractions)
        for c in sorted(cores)
    ]


def _link(target: Path, link: Path) -> None:
    """Relative directory symlink link -> target; refuses to replace a real directory."""
    if not target.is_dir():
        raise ConfigError(f"benchmark input missing: {target}")
    link.parent.mkdir(parents=True, exist_ok=True)
    if link.is_symlink():
        link.unlink()
    elif link.exists():
        raise ConfigError(f"{link} exists and is not a symlink; remove it by hand")
    link.symlink_to(os.path.relpath(target, link.parent), target_is_directory=True)


def prepare_root(cfg: Config, root: Path) -> None:
    """Scratch data root whose inputs are links to the real raw/ and full artist tables.

    The chain reads raw/ and the four artist-level clean tables (F2.1, F2.5),
    which are not sampled (F3.2). It writes clean/{credits,contributions,
    recording_years}, graph/ and sample/ next to those links.
    """
    _link(cfg.data_path("raw"), root / cfg.get("paths.raw"))
    for table in FULL_ARTIST_TABLES:
        _link(cfg.data_path("clean") / table, root / cfg.get("paths.clean") / table)


def bench_config(cfg: Config, root: Path) -> Config:
    """The main config, reading and writing under `root`, with report tables there too.

    graph.build writes reports_dir/tables/graph_build_summary.csv in full mode;
    an absolute reports_dir keeps that inside the scratch root.
    """
    if cfg.variant is not None:
        raise ConfigError("the benchmark runs on the main graph config, not a variant")
    raw = copy.deepcopy(cfg.raw)
    raw["reports_dir"] = str(root / "reports")
    return Config(raw, cfg.source, root)


def run_cell(cfg: Config, cell: Cell, base: Path) -> dict:
    """Run the chain once in this process at cell.cores cores; write runs/<cell>.json.

    Call in a fresh process only. Every cell should start from a cold JVM, and
    spark.driver.memory only applies when the JVM starts (musin.common.spark).
    """
    use_benchmark_settings(cell.cores, base / "spark-events")
    root = base / "data"
    bcfg = bench_config(cfg, root)
    started = datetime.now(UTC)
    seconds: dict[str, float] = {}
    counts: dict[str, dict] = {}
    for name, stage in STAGES:
        log.info("=== %s  %s", cell.name, name)
        t0 = time.monotonic()
        counts[name] = stage(bcfg, cell.sample)
        seconds[name] = round(time.monotonic() - t0, 2)
        log.info("    %s: %.1f s", name, seconds[name])
    seconds[TOTAL] = round(sum(seconds.values()), 2)

    spark = get_spark("musin-benchmark-conf")  # after the timer: settings, not cost
    # conf.get(key) without a fallback returns Spark's built-in default for SQL keys.
    conf = {k: spark.conf.get(k) for k in RECORDED_CONF}
    conf["spark.version"] = spark.version
    conf["defaultParallelism"] = spark.sparkContext.defaultParallelism
    spark.stop()

    result = {
        "cell": cell.name,
        "fraction": cell.fraction,
        "cores": cell.cores,
        "repeat": cell.repeat,
        "started_at_utc": started.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "seconds": seconds,
        "spark_conf": conf,
        "counts": counts,
    }
    target = base / "runs" / f"{cell.name}.json"
    target.parent.mkdir(parents=True, exist_ok=True)
    tmp = target.with_suffix(".tmp")
    tmp.write_text(json.dumps(result, indent=2, default=str) + "\n")
    tmp.replace(target)  # a crashed cell leaves no result, so a resume re-runs it
    log.info("%s done in %.1f s -> %s", cell.name, seconds[TOTAL], target)
    return result


def run_rows(results: list[dict]) -> list[dict]:
    """One row per cell and stage (plus 'total'), in grid order."""
    rows = []
    for r in results:
        for stage, secs in r["seconds"].items():
            rows.append(
                {
                    "fraction": r["fraction"],
                    "cores": r["cores"],
                    "repeat": r["repeat"],
                    "stage": stage,
                    "seconds": secs,
                    "started_at_utc": r["started_at_utc"],
                }
            )
    return rows


def summary_rows(results: list[dict]) -> list[dict]:
    """Median over repeats per fraction x cores, with speedup and efficiency.

    speedup     = median total at the smallest core count / median total
    efficiency  = speedup / (cores / smallest core count)
    vs_full     = median total / median total at the largest fraction, same cores
    Per-stage medians are kept so the report can say where the time goes.
    """
    by_cell: dict[tuple[float, int], list[dict]] = {}
    for r in results:
        by_cell.setdefault((r["fraction"], r["cores"]), []).append(r)
    stages = [name for name, _ in STAGES]

    def median(key: tuple[float, int], stage: str = TOTAL) -> float:
        return statistics.median(r["seconds"][stage] for r in by_cell[key])

    min_cores = min(c for _, c in by_cell)
    max_fraction = max(f for f, _ in by_cell)
    rows = []
    for fraction, cores in sorted(by_cell):
        runs = by_cell[(fraction, cores)]
        totals = [r["seconds"][TOTAL] for r in runs]
        med = median((fraction, cores))
        speedup = median((fraction, min_cores)) / med
        row = {
            "fraction": fraction,
            "cores": cores,
            "repeats": len(runs),
            "median_s": round(med, 2),
            "min_s": round(min(totals), 2),
            "max_s": round(max(totals), 2),
            "speedup": round(speedup, 3),
            "efficiency": round(speedup / (cores / min_cores), 3),
            "vs_full": round(med / median((max_fraction, cores)), 3),
        }
        for stage in stages:
            row[f"{stage.split('.')[1]}_median_s"] = round(median((fraction, cores), stage), 2)
        rows.append(row)
    return rows


def _first_line(path: str, prefix: str) -> str | None:
    try:
        for line in Path(path).read_text().splitlines():
            if line.startswith(prefix):
                return line.split(":", 1)[1].strip()
    except OSError:
        return None
    return None


def environment_rows(cfg: Config, results: list[dict]) -> list[dict]:
    """Machine, software and Spark settings the numbers were measured with."""
    java = subprocess.run(["java", "-version"], capture_output=True, text=True, check=False)
    mem_kb = _first_line("/proc/meminfo", "MemTotal")
    partitions = {str(r["spark_conf"]["spark.sql.shuffle.partitions"]) for r in results}
    first = results[0]["spark_conf"]
    starts = sorted(r["started_at_utc"] for r in results)
    values = {
        "cpu_model": _first_line("/proc/cpuinfo", "model name"),
        "logical_cpus_visible": os.cpu_count(),
        "memory_total_gib": round(int(mem_kb.split()[0]) / 2**20, 1) if mem_kb else None,
        "os": platform.platform(),
        "python": platform.python_version(),
        "spark": first["spark.version"],
        "java": (java.stderr.splitlines() or ["?"])[0],
        "spark_driver_memory": require_env("SPARK_DRIVER_MEMORY"),
        "spark_sql_shuffle_partitions": ", ".join(sorted(partitions)),
        "spark_sql_adaptive_enabled": first["spark.sql.adaptive.enabled"],
        "snapshot_date": cfg.get("snapshot_date"),
        "repo_commit": repo_commit(cfg.source.resolve().parent.parent),
        "cells": len(results),
        "first_cell_started_utc": starts[0],
        "last_cell_started_utc": starts[-1],
    }
    return [{"key": k, "value": v} for k, v in values.items()]


def plot(summary: list[dict], target: Path) -> Path:
    """Median runtime (log scale) and speedup against cores, one line per fraction."""
    from musin.profiling.figures import DPI, TEXT_2, _style, plt

    _style()
    fractions = sorted({r["fraction"] for r in summary})
    cores = sorted({r["cores"] for r in summary})
    step = (len(FRACTION_RAMP) - 1) / max(len(fractions) - 1, 1)
    colors = {f: FRACTION_RAMP[round(i * step)] for i, f in enumerate(fractions)}
    fig, (left, right) = plt.subplots(1, 2, figsize=(9.6, 3.9))

    for f in fractions:
        rows = [r for r in summary if r["fraction"] == f]
        xs = [r["cores"] for r in rows]
        label = f"{f:.0%}"
        left.plot(
            xs,
            [r["median_s"] / 60 for r in rows],
            color=colors[f],
            linewidth=2,
            marker="o",
            markersize=5,
            label=label,
        )
        left.vlines(
            xs,
            [r["min_s"] / 60 for r in rows],
            [r["max_s"] / 60 for r in rows],
            color=colors[f],
            linewidth=1,
        )
        right.plot(
            xs,
            [r["speedup"] for r in rows],
            color=colors[f],
            linewidth=2,
            marker="o",
            markersize=5,
            label=label,
        )
        for ax, y in ((left, rows[-1]["median_s"] / 60), (right, rows[-1]["speedup"])):
            ax.annotate(
                label,
                (xs[-1], y),
                xytext=(6, 0),
                textcoords="offset points",
                va="center",
                color=TEXT_2,
                fontsize=8,
            )
    right.plot(
        cores,
        [c / cores[0] for c in cores],
        color=IDEAL,
        linewidth=1,
        linestyle="--",
        label="ideal (linear)",
    )

    for ax in (left, right):
        ax.set_xscale("log", base=2)
        ax.set_xticks(cores, [str(c) for c in cores])
        ax.minorticks_off()
        ax.set_xlabel("cores (Spark local[n])")
    left.set_yscale("log")
    # A narrow range has no decade tick, so minor ticks carry the labels too.
    left.yaxis.set_major_formatter(lambda y, _pos: f"{y:g}")
    left.yaxis.set_minor_formatter(lambda y, _pos: f"{y:.3g}")
    left.set_ylabel("minutes, median (bar: min-max)")
    left.set_title("Recording chain runtime")
    right.set_ylabel(f"speedup vs {cores[0]} core{'s' if cores[0] > 1 else ''}")
    right.set_title("Speedup")
    right.legend(title="data fraction", loc="upper left", fontsize=8, title_fontsize=8)
    target.parent.mkdir(parents=True, exist_ok=True)
    fig.tight_layout()
    fig.savefig(target, dpi=DPI)
    plt.close(fig)
    return target


def report(cfg: Config, cells: list[Cell], base: Path, dev: bool) -> bool:
    """Write the tables and the figure if every cell has a result; False otherwise."""
    runs = base / "runs"
    missing = [c.name for c in cells if not (runs / f"{c.name}.json").is_file()]
    if missing:
        log.info("%d of %d cells unfinished, no report yet: %s", len(missing), len(cells), missing)
        return False
    results = [json.loads((runs / f"{c.name}.json").read_text()) for c in cells]
    if dev:
        tables, figures = base / "reports" / "tables", base / "reports" / "figures"
    else:
        repo_reports = cfg.source.resolve().parent.parent / cfg.get("reports_dir")
        tables, figures = repo_reports / "tables", repo_reports / "figures"
    summary = summary_rows(results)
    write_table(tables / "benchmark_scaling_runs.csv", run_rows(results))
    write_table(tables / "benchmark_scaling_summary.csv", summary)
    write_table(tables / "benchmark_environment.csv", environment_rows(cfg, results))
    plot(summary, figures / "benchmark_scaling.png")
    log.info("tables -> %s, figure -> %s", tables, figures / "benchmark_scaling.png")
    for r in summary:
        log.info(
            "  fraction %-5g cores %-2d median %8.1f s  speedup %5.2f  efficiency %4.2f",
            r["fraction"],
            r["cores"],
            r["median_s"],
            r["speedup"],
            r["efficiency"],
        )
    return True


def run(cfg: Config, config_path: str, cells: list[Cell], dev: bool, fresh: bool) -> bool:
    """Run every unfinished cell in its own process, then report. True if the grid is complete."""
    base = cfg.data_path("benchmark") / ("dev" if dev else "scaling")
    runs = base / "runs"
    if fresh and runs.is_dir():
        shutil.rmtree(runs)
        log.info("--fresh: removed %s", runs)
    prepare_root(cfg, base / "data")
    for i, cell in enumerate(cells, 1):
        if (runs / f"{cell.name}.json").is_file():
            log.info("[%d/%d] %s already done, skipped", i, len(cells), cell.name)
            continue
        log.info("[%d/%d] %s", i, len(cells), cell.name)
        cmd = [
            sys.executable, "-m", "musin.benchmark.scaling", "--config", config_path,
            "--cell", f"{cell.fraction:g}", str(cell.cores), str(cell.repeat),
            "--base", str(base),
        ]  # fmt: skip
        subprocess.run(cmd, check=True)
    return report(cfg, cells, base, dev)


def _csv(cast: Callable[[str], float | int]) -> Callable[[str], list]:
    return lambda text: [cast(x) for x in text.split(",") if x.strip()]


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--fresh", action="store_true", help="discard finished cells first")
    parser.add_argument("--fractions", type=_csv(float), help="dev run: e.g. 0.01,0.02")
    parser.add_argument("--cores", type=_csv(int), help="dev run: e.g. 1,2")
    parser.add_argument("--repeats", type=int, help="dev run: repetitions per cell")
    # Internal: one cell in this process, started by run().
    parser.add_argument(
        "--cell", nargs=3, metavar=("FRACTION", "CORES", "REPEAT"), help=argparse.SUPPRESS
    )
    parser.add_argument("--base", help=argparse.SUPPRESS)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)

    if args.cell:
        cell = Cell(float(args.cell[0]), int(args.cell[1]), int(args.cell[2]))
        run_cell(cfg, cell, Path(args.base))
        return 0

    dev = any(v is not None for v in (args.fractions, args.cores, args.repeats))
    cells = grid(
        args.fractions or cfg.get("benchmark.scaling.fractions"),
        args.cores or cfg.get("benchmark.scaling.cores"),
        args.repeats or cfg.get("benchmark.scaling.repeats"),
    )
    log.info("%s grid: %d cells", "dev" if dev else "config", len(cells))
    start = time.monotonic()
    complete = run(cfg, args.config, cells, dev, args.fresh)
    log.info("done in %.0fs", time.monotonic() - start)
    return 0 if complete else 1


if __name__ == "__main__":
    sys.exit(main())
