"""Dry-run of the Neo4j Browser demo (F6.2, FR-25): every demo query, with the demo parameters.

    docker compose exec spark python -m musin.demo_check --config config/pipeline.yaml

Reads:  neo4j/10_demo.cypher (statements and the `:param` lines in its header)
        the graph loaded in Neo4j ($MUSIN_DATA/neo4j/loaded_graph.json)
Writes: nothing (log only)

The demo runs live in front of an audience, so this check runs each statement
exactly as the Browser will, with the parameters the demo script sets, and
fails if one errors or returns no rows. The parameters are read from the
file's own `:param` comment lines, so the file stays the single source.
Read-only and light (seconds); it can run whenever Neo4j is up.
"""

from __future__ import annotations

import argparse
import ast
import logging
import re
import sys
import time
from pathlib import Path

from musin.analytics.common import loaded_graph
from musin.common.config import load_config
from musin.common.cypher import load_queries
from musin.common.neo4j import get_driver

log = logging.getLogger("musin.demo_check")

DEMO_FILE = "10_demo.cypher"
# `//   :param name => 'Ry Cooder';` in the file header.
PARAM_LINE = re.compile(r"^\s*//\s*:param\s+(\w+)\s*=>\s*(.+?)\s*;\s*$")


def demo_params(text: str) -> dict[str, object]:
    """The `:param` values in the comment lines of a demo file (strings and numbers only)."""
    params: dict[str, object] = {}
    for line in text.splitlines():
        m = PARAM_LINE.match(line)
        if m:
            params[m.group(1)] = ast.literal_eval(m.group(2))
    if not params:
        raise ValueError("no ':param' lines found in the demo file header")
    return params


def run(cypher_dir: Path) -> int:
    """Run every demo statement; return the number of failures."""
    path = cypher_dir / DEMO_FILE
    text = path.read_text(encoding="utf-8")
    queries, params = load_queries(path), demo_params(text)
    log.info("%d demo statements, parameters %s", len(queries), params)
    failures = 0
    driver = get_driver(60)
    try:
        with driver.session() as session:
            for name, query in queries.items():
                start = time.monotonic()
                try:
                    records = list(session.run(query, params))
                except Exception as exc:  # noqa: BLE001 - report every failing statement
                    log.error("%s FAILED: %s", name, exc)
                    failures += 1
                    continue
                seconds = time.monotonic() - start
                if not records:
                    log.error("%s returned no rows (%.2fs)", name, seconds)
                    failures += 1
                    continue
                log.info("%s: %d rows in %.2fs", name, len(records), seconds)
                for record in records[:3]:
                    log.info("    %s", _short(record.data()))
    finally:
        driver.close()
    return failures


def _short(row: dict) -> str:
    """Rows with nodes or paths would print whole objects; show names instead."""
    out = {}
    for k, v in row.items():
        if isinstance(v, tuple) and len(v) == 3:  # a relationship: (start, type, end)
            out[k] = v[1]
        elif isinstance(v, dict) and "name" in v:
            out[k] = v["name"]
        elif isinstance(v, list) and v and isinstance(v[0], dict):
            out[k] = " > ".join(str(x.get("name", "·")) for x in v if isinstance(x, dict))
        else:
            out[k] = v
    return str(out)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    cfg = load_config(args.config)
    loaded = loaded_graph(cfg)
    log.info(
        "graph %s (edges fingerprint %s)", loaded["graph"], loaded["graph_fingerprints"]["edges"]
    )
    start = time.monotonic()
    failures = run(cfg.source.resolve().parent.parent / "neo4j")
    log.info("done in %.1fs, %d failures", time.monotonic() - start, failures)
    return 1 if failures else 0


if __name__ == "__main__":
    sys.exit(main())
