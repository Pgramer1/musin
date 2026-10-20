"""PageRank on the GPU with RAPIDS cuGraph, the third engine of the comparison (F5.3, FR-23).

    docker compose --profile gpu run --rm rapids \
        python -m musin.benchmark.pagerank_gpu --config config/pipeline.yaml [--fresh]
    docker compose exec spark python -m musin.benchmark.pagerank \
        --config config/pipeline.yaml --report

Runs in the `rapids` container (RAPIDS image, GPU passed through), which has no
PySpark and no Neo4j driver, so it imports only musin.benchmark.common and
musin.common.config. Input and settings are those of F5.2 (D-042, D-043): the
COLLAB pairs of the graph named in loaded_graph.json, read from its Parquet,
unweighted, damping `analysis.pagerank.damping`, tolerance
`benchmark.pagerank.tolerance`, `benchmark.pagerank.repeats` repetitions, each
in a fresh process (a cold CUDA context, as a user's run would have).

  load     read nodes_artist and edges with cuDF, form the distinct pairs,
           build an undirected cugraph.Graph (one edge per pair)
  compute  cugraph.pagerank, then a device synchronise
  output   copy the scores to the host and write them as Parquet (PyArrow)

cuGraph normalises PageRank to sum 1; the scores are multiplied by the number
of artists so they are on GDS's and GraphFrames' scale (they sum to N there,
since no artist is isolated). Every edge carries weight 1.0 as float64: that
leaves PageRank unweighted but makes cuGraph compute in double precision, as
the other two engines do.

Writes run_cugraph_r<n>.json and scores_cugraph_r<n>.parquet to
$MUSIN_DATA/<paths.benchmark>/pagerank/<graph>/; finished repetitions are
skipped on a re-run. The report is written by musin.benchmark.pagerank --report
in the Spark container.
"""

from __future__ import annotations

import argparse
import logging
import subprocess
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from musin.benchmark.common import (
    TIMESTAMP,
    base_dir,
    collect,
    finished,
    read_loaded,
    run_file,
    settings,
    write_json,
)
from musin.common.config import load_config

log = logging.getLogger("musin.benchmark.pagerank_gpu")
ENGINE = "cugraph"


def run_once(loaded: dict, opts: dict, base: Path, repeat: int) -> dict:
    """One cuGraph repetition in this (fresh) process; returns its run row."""
    import cudf
    import cugraph
    import cupy
    import pyarrow.parquet as pq

    source = Path(loaded["source"])
    started = datetime.now(UTC)
    t0 = time.monotonic()
    nodes = cudf.read_parquet(str(source / "nodes_artist"), columns=["artist_id"])
    edges = cudf.read_parquet(str(source / "edges"), columns=["src_id", "dst_id"])
    pairs = cudf.DataFrame(
        {
            "a": cupy.minimum(edges["src_id"].values, edges["dst_id"].values),
            "b": cupy.maximum(edges["src_id"].values, edges["dst_id"].values),
        }
    ).drop_duplicates()
    pairs["w"] = cupy.ones(len(pairs), dtype=cupy.float64)
    graph = cugraph.Graph(directed=False)
    graph.from_cudf_edgelist(pairs, source="a", destination="b", weight="w")
    n_nodes = graph.number_of_vertices()
    cupy.cuda.runtime.deviceSynchronize()
    t1 = time.monotonic()
    ranked = cugraph.pagerank(
        graph,
        alpha=opts["damping"],
        max_iter=opts["max_iterations"],
        tol=opts["tolerance"],
    )
    cupy.cuda.runtime.deviceSynchronize()
    t2 = time.monotonic()
    out = cudf.DataFrame(
        {"artistId": ranked["vertex"], "score": ranked["pagerank"].astype("float64") * n_nodes}
    )
    # cuDF's own Parquet writer (GPU-side I/O through KvikIO) fails under WSL with
    # CUDA_ERROR_ILLEGAL_ADDRESS (D-043), so copy to the host and write with PyArrow.
    pq.write_table(out.to_arrow(), str(base / f"scores_{ENGINE}_r{repeat}.parquet"))
    t3 = time.monotonic()
    finished_at = datetime.now(UTC)
    if n_nodes != len(nodes):
        raise RuntimeError(f"{n_nodes} vertices with a pair, {len(nodes)} artists in the graph")
    device = cupy.cuda.runtime.getDeviceProperties(0)
    return {
        "engine": ENGINE,
        "repeat": repeat,
        "started_at_utc": started.strftime(TIMESTAMP),
        "finished_at_utc": finished_at.strftime(TIMESTAMP),
        "load_s": round(t1 - t0, 3),
        "compute_s": round(t2 - t1, 3),
        "output_s": round(t3 - t2, 3),
        "total_s": round(t3 - t0, 3),
        "engine_compute_ms": None,
        "iterations": None,  # cugraph.pagerank does not report its iteration count
        "nodes": int(n_nodes),
        "directed_edges": 2 * len(pairs),
        "score_sum": float(out["score"].sum()),
        "graph": loaded["graph"],
        "edges_fingerprint": loaded["graph_fingerprints"]["edges"],
        **opts,
        "cores": None,  # GPU run; analysis.concurrency does not apply
        "gpu": device["name"].decode() if isinstance(device["name"], bytes) else device["name"],
        "cugraph_version": cugraph.__version__,
    }


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument("--fresh", action="store_true", help="discard saved cuGraph runs")
    parser.add_argument("--repeat", type=int, help=argparse.SUPPRESS)  # internal: one run
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    loaded = read_loaded(cfg)
    opts = settings(cfg)
    base = base_dir(cfg, loaded["graph"])
    fingerprint = loaded["graph_fingerprints"]["edges"]

    if args.repeat is not None:
        write_json(run_file(base, ENGINE, args.repeat), run_once(loaded, opts, base, args.repeat))
        return 0

    start = time.monotonic()
    log.info("graph %s (edges fingerprint %s); %s", loaded["graph"], fingerprint, opts)
    if args.fresh:
        for path in base.glob(f"run_{ENGINE}_r*.json"):
            path.unlink()
    for repeat in range(1, opts["repeats"] + 1):
        if finished(base, ENGINE, repeat, fingerprint):
            log.info("cugraph r%d already saved, skipped", repeat)
            continue
        cmd = [
            sys.executable, "-m", "musin.benchmark.pagerank_gpu", "--config", args.config,
            "--repeat", str(repeat),
        ]  # fmt: skip
        subprocess.run(cmd, check=True)
        run = finished(base, ENGINE, repeat, fingerprint)
        log.info(
            "cugraph r%d: load %.2fs compute %.2fs output %.2fs",
            repeat, run["load_s"], run["compute_s"], run["output_s"],
        )  # fmt: skip
    collect(base, ENGINE, opts["repeats"], fingerprint)
    log.info(
        "done in %.0fs; write the report with musin.benchmark.pagerank --report",
        time.monotonic() - start,
    )
    return 0


if __name__ == "__main__":
    sys.exit(main())
