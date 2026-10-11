"""Verify a bulk-imported graph in Neo4j against its export and Parquet source (F3.4, FR-14).

    docker compose exec spark python -m musin.export.neo4j_check \
        --config config/pipeline.yaml --graph full

Run by neo4j/import_graph.sh right after `neo4j-admin database import`.
Reads:  $MUSIN_DATA/<paths.import>/<graph>/_export.json  (musin.export.neo4j_csv)
        the Parquet graph named in it (F3.1), the importer report in
        $MUSIN_DATA/neo4j/logs/import-<graph>.report, and the Neo4j database
Writes: $MUSIN_DATA/neo4j/loaded_graph.json  which graph Neo4j holds (only if every check passes)

Checks: an empty importer report; exactly the Artist and Genre labels; node
and relationship counts per label/type equal to the exported rows; integer
artistId/weights/years and float shares (D-016); missing-property counts
equal to the exported nulls; no unexpected properties; and a property-by-
property comparison with Parquet for artists with quotes, commas, or
backslashes in their names plus a hashed sample of artists and edges.
Cypher: neo4j/00_import_checks.cypher. Exits 1 if anything fails.
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import time
from datetime import UTC, datetime
from pathlib import Path

from neo4j import Driver
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.common.config import Config, ConfigError, load_config
from musin.common.cypher import load_queries
from musin.common.neo4j import get_driver
from musin.common.spark import get_spark
from musin.export.neo4j_csv import EXPORT_FILE, HEADERS
from musin.graph.build import EDGE_TYPES

log = logging.getLogger("musin.export.neo4j_check")

CYPHER_FILE = Path(__file__).resolve().parents[3] / "neo4j" / "00_import_checks.cypher"
LOADED_MARKER = "loaded_graph.json"  # in $MUSIN_DATA/neo4j/
CONNECT_WAIT_S = 180
# Spot-check sizes: enough to hit every CSV hazard class, small enough to collect.
TRICKY_PER_CLASS = 200
HASHED_ARTISTS = 500
HASHED_SOURCES = 300
ARTIST_PROPS = {  # Neo4j property -> nodes_artist column
    "artistId": "artist_id",
    "mbid": "mbid",
    "name": "name",
    "type": "artist_type",
    "areaName": "area_name",
    "beginYear": "begin_year",
    "recordingCount": "recording_count",
    "primaryGenre": "primary_genre",
}


def header_property(field: str) -> str:
    """Property name of an import-header field: 'beginYear:int' -> 'beginYear'."""
    return field.split(":", 1)[0]


def expected_missing(export: dict) -> dict[str, int]:
    """{'<file>.<property>': rows exported as null}, i.e. properties Neo4j must lack."""
    return {
        f"{stem}.{header_property(field)}": n
        for stem, info in export["files"].items()
        for field, n in info["nulls"].items()
        if header_property(field)
    }


def compare_rows(expected: dict, actual: dict, key: str) -> list[str]:
    """Differences between {key: row} maps; a missing Neo4j property equals None."""
    diffs = []
    for k in sorted(expected.keys() - actual.keys())[:5]:
        diffs.append(f"{key} {k} missing in Neo4j")
    for k in sorted(expected.keys() & actual.keys()):
        e, a = expected[k], actual[k]
        bad = {c: (e[c], a.get(c)) for c in e if e[c] != a.get(c)}
        if bad:
            diffs.append(f"{key} {k}: {bad}")
    return diffs


def spot_artists(nodes: DataFrame) -> list[dict]:
    """Artists whose names stress CSV quoting, plus a deterministic hashed sample."""
    picks = [
        nodes.filter(F.col("name").contains(ch)).orderBy("artist_id").limit(TRICKY_PER_CLASS)
        for ch in ('"', ",", "\\")
    ]
    picks.append(nodes.orderBy(F.xxhash64("artist_id"), "artist_id").limit(HASHED_ARTISTS))
    df = picks[0]
    for p in picks[1:]:
        df = df.unionByName(p)
    return [r.asDict() for r in df.distinct().collect()]  # at most ~1,100 rows


def run(cfg: Config, graph: str) -> dict:
    """Run every check; write the loaded-graph marker; raise RuntimeError on failure."""
    export_dir = cfg.data_path("import") / graph
    export_file = export_dir / EXPORT_FILE
    if not export_file.is_file():
        raise ConfigError(f"{export_file} missing: run musin.export.neo4j_csv first")
    export = json.loads(export_file.read_text())
    files = export["files"]
    queries = load_queries(CYPHER_FILE)
    problems: list[str] = []

    def check(name: str, ok: bool, detail: str = "") -> None:
        (log.info if ok else log.error)("%s %-34s %s", "PASS" if ok else "FAIL", name, detail)
        if not ok:
            problems.append(f"{name} {detail}".strip())

    report = cfg.data_root / "neo4j" / "logs" / f"import-{graph}.report"
    check(
        "importer_report_empty",
        report.is_file() and report.stat().st_size == 0,
        f"{report} ({report.stat().st_size if report.is_file() else 'missing'} bytes)",
    )

    driver: Driver = get_driver(CONNECT_WAIT_S)

    def q(name: str, **params) -> list[dict]:
        records, _, _ = driver.execute_query(queries[name], params, database_="neo4j")
        return [r.data() for r in records]

    labels = set(q("labels")[0]["labels"])
    check("labels", labels == {"Artist", "Genre"}, str(sorted(labels)))
    nodes = {tuple(r["labels"]): r["n"] for r in q("node_counts")}
    check(
        "node_counts",
        nodes == {("Artist",): files["artists"]["rows"], ("Genre",): files["genres"]["rows"]},
        str(nodes),
    )
    rels = {r["type"]: r["n"] for r in q("relationship_counts")}
    want_rels = {"IN_GENRE": files["in_genre"]["rows"]} | {
        t: files[f"edges_{t.lower()}"]["rows"] for t in EDGE_TYPES
    }
    want_rels = {t: n for t, n in want_rels.items() if n}
    check("relationship_counts", rels == want_rels, str(rels))

    missing = expected_missing(export)
    a = q("artist_report")[0]
    check("artist_ids_distinct", a["distinct_ids"] == a["n"], f"{a['distinct_ids']} / {a['n']}")
    for prop in ("artistId", "recordingCount", "beginYear"):
        check(f"artist_{prop}_integer", a[f"bad_{prop}"] == 0, f"{a[f'bad_{prop}']} wrong-typed")
    for prop in ("mbid", "name", "type", "areaName", "beginYear", "primaryGenre"):
        want = missing.get(f"artists.{prop}", 0)
        check(
            f"artist_{prop}_missing",
            a[f"missing_{prop}"] == want,
            f"{a[f'missing_{prop}']} (export nulls {want})",
        )
    keys = {r["key"] for r in q("artist_property_keys")}
    allowed = {header_property(f) for f in HEADERS["artists"] if header_property(f)}
    check("artist_property_keys", keys <= allowed, str(sorted(keys - allowed)))

    genres = q("genre_names")
    check(
        "genres",
        len(genres) == files["genres"]["rows"] and all(g["keys"] == ["name"] for g in genres),
        str(sorted(g["name"] for g in genres)),
    )
    for r in q("relationship_report"):
        t = r["type"]
        for prop in ("weight", "firstYear", "lastYear"):
            check(f"{t}_{prop}_integer", r[f"bad_{prop}"] == 0, f"{r[f'bad_{prop}']} wrong-typed")
        for prop in ("firstYear", "lastYear"):
            want = missing.get(f"edges_{t.lower()}.{prop}", 0)
            check(
                f"{t}_{prop}_missing",
                r[f"missing_{prop}"] == want,
                f"{r[f'missing_{prop}']} (export nulls {want})",
            )
    ig = q("in_genre_report")[0]
    check("in_genre_share_float", ig["bad_share"] == 0, f"{ig['bad_share']} wrong-typed")

    # --- property-by-property spot checks against Parquet ----------------------
    spark = get_spark("musin-export-neo4j-check")
    source = Path(export["source"])
    parquet_nodes = spark.read.parquet(str(source / "nodes_artist"))
    sample = spot_artists(parquet_nodes)
    expected = {r["artist_id"]: {c: r[c] for c in ARTIST_PROPS.values()} for r in sample}
    actual = {r["artist_id"]: r for r in q("artists_by_id", ids=list(expected))}
    diffs = compare_rows(expected, actual, "artist")
    check("spot_artists", not diffs, f"{len(expected)} compared; " + "; ".join(diffs[:3]))

    edges = spark.read.parquet(str(source / "edges"))
    srcs = [
        r["src_id"]
        for r in edges.select("src_id")
        .distinct()
        .orderBy(F.xxhash64("src_id"), "src_id")
        .limit(HASHED_SOURCES)
        .collect()
    ]
    cols = ["weight", "first_year", "last_year"]
    want_edges = {
        (r["src_id"], r["dst_id"], r["edge_type"]): {c: r[c] for c in cols}
        for r in edges.filter(F.col("src_id").isin(srcs)).collect()  # out-edges of 300 artists
    }
    got_edges = {(r["src_id"], r["dst_id"], r["edge_type"]): r for r in q("edges_from", ids=srcs)}
    diffs = compare_rows(want_edges, got_edges, "edge")
    extra = len(got_edges.keys() - want_edges.keys())
    check(
        "spot_edges",
        not diffs and not extra,
        f"{len(want_edges)} edges of {len(srcs)} artists; {extra} extra; " + "; ".join(diffs[:3]),
    )
    spark.stop()
    driver.close()

    if problems:
        raise RuntimeError(f"{len(problems)} import checks failed: " + "; ".join(problems))
    marker = {
        "graph": graph,
        "source": export["source"],
        "graph_fingerprints": export["graph_fingerprints"],
        "exported_at_utc": export["exported_at_utc"],
        "verified_at_utc": datetime.now(UTC).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "nodes": {"Artist": files["artists"]["rows"], "Genre": files["genres"]["rows"]},
        "relationships": rels,
    }
    (cfg.data_root / "neo4j" / LOADED_MARKER).write_text(json.dumps(marker, indent=2) + "\n")
    log.info(
        "Neo4j now holds graph '%s'; marker -> %s", graph, cfg.data_root / "neo4j" / LOADED_MARKER
    )
    return marker


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    parser.add_argument(
        "--graph", required=True, help="export name: 'full' or 'sample-f<fraction>'"
    )
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    try:
        run(load_config(args.config), args.graph)
    except RuntimeError as exc:
        log.error("%s", exc)
        return 1
    finally:
        log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
