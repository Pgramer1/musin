"""Artist cleaning and special-purpose filter (F2.1, FR-04).

    python -m musin.clean.artists --config config/pipeline.yaml

Reads:  $MUSIN_DATA/<paths.raw>/{artist,artist_type,area,artist_credit_name}  (F1.3)
        <filters.special_artists_file>  (committed, hand-curated)
Writes: $MUSIN_DATA/<paths.clean>/artists/          every artist except the special ones
        $MUSIN_DATA/<paths.clean>/special_artists/  the excluded artists, with ids

`clean/artists` is the set of artists every later stage may use: F2.2, F2.3
and F2.5 drop any row whose artist is not in it (or, cheaper, anti-join the
small `clean/special_artists`). There is no --sample: the artist table is
small and sampling happens by recording in the stages that read credits.
"""

from __future__ import annotations

import argparse
import csv
import logging
import re
import sys
import time
from pathlib import Path

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

from musin.common.config import Config, load_config
from musin.common.spark import get_spark

log = logging.getLogger("musin.clean.artists")

SPECIAL_COLUMNS = ["mbid", "name", "reason"]
UUID = re.compile(r"^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$")


class SpecialArtistsError(ValueError):
    """The special-artists file is malformed or does not match the dump."""


def load_special_artists(path: str | Path) -> list[dict]:
    """Read and validate the special-artists CSV (header mbid,name,reason).

    Raises SpecialArtistsError on a wrong header, a malformed or duplicate
    MBID, or an empty name or reason. Returns rows in file order.
    """
    with Path(path).open(encoding="utf-8", newline="") as fh:
        reader = csv.DictReader(fh)
        if reader.fieldnames != SPECIAL_COLUMNS:
            raise SpecialArtistsError(f"{path}: header must be {','.join(SPECIAL_COLUMNS)}")
        rows = list(reader)
    seen: set[str] = set()
    for i, row in enumerate(rows, start=2):
        mbid = row["mbid"]
        if not UUID.match(mbid or ""):
            raise SpecialArtistsError(f"{path}:{i}: malformed mbid {mbid!r}")
        if mbid in seen:
            raise SpecialArtistsError(f"{path}:{i}: duplicate mbid {mbid}")
        if not (row["name"] or "").strip() or not (row["reason"] or "").strip():
            raise SpecialArtistsError(f"{path}:{i}: empty name or reason")
        seen.add(mbid)
    if not rows:
        raise SpecialArtistsError(f"{path}: no special artists listed")
    return rows


def resolve_special_artists(specials: list[dict], artist: DataFrame) -> DataFrame:
    """Look up each listed MBID in raw `artist` and check the name matches.

    In: special rows (mbid, name, reason); raw artist(id, gid, name).
    Out: (artist_id long, mbid, name, reason), one row per listed artist.
    Raises SpecialArtistsError if an MBID is not in the dump or its name in
    the dump differs from the file, so a typo cannot silently drop nothing.
    """
    spark = artist.sparkSession
    listed = spark.createDataFrame(
        [(r["mbid"], r["name"], r["reason"]) for r in specials],
        "mbid string, listed_name string, reason string",
    )
    found = listed.join(
        artist.select(F.col("id").alias("artist_id"), F.col("gid").alias("mbid"), "name"),
        "mbid",
        "left",
    )
    rows = found.collect()  # one row per listed artist (tens)
    missing = [r["mbid"] for r in rows if r["artist_id"] is None]
    if missing:
        raise SpecialArtistsError(f"special artists not in the dump: {missing}")
    renamed = [
        (r["mbid"], r["listed_name"], r["name"]) for r in rows if r["name"] != r["listed_name"]
    ]
    if renamed:
        raise SpecialArtistsError(
            f"special artist names differ from the dump (mbid, file, dump): {renamed}"
        )
    return found.select(
        F.col("artist_id").cast("long").alias("artist_id"), "mbid", "name", "reason"
    ).orderBy("artist_id")


def clean_artists(
    artist: DataFrame,
    artist_type: DataFrame,
    area: DataFrame,
    special: DataFrame,
    max_begin_year: int,
) -> DataFrame:
    """Typed artist table without special-purpose artists.

    In: raw artist(id, gid, name, comment, type, area, begin_date_year),
    raw artist_type(id, name), raw area(id, name), special(artist_id),
    `max_begin_year` (the snapshot year).
    Out: (artist_id long, mbid, name, disambiguation, artist_type, area_name,
    begin_year int), one row per non-special artist. `disambiguation` is the
    MusicBrainz comment, null when empty. `begin_year` is null when unknown or
    later than `max_begin_year` (a begin date in the future is an entry error);
    years before 1 are kept, MusicBrainz stores historical people.
    """
    year = F.col("begin_date_year").cast("int")
    types = artist_type.select(F.col("id").alias("type"), F.col("name").alias("artist_type"))
    areas = area.select(F.col("id").alias("area"), F.col("name").alias("area_name"))
    return (
        artist.join(F.broadcast(special.select(F.col("artist_id").alias("id"))), "id", "left_anti")
        .join(F.broadcast(types), "type", "left")
        .join(areas, "area", "left")
        .select(
            F.col("id").cast("long").alias("artist_id"),
            F.col("gid").alias("mbid"),
            "name",
            F.when(F.col("comment") != "", F.col("comment")).alias("disambiguation"),
            "artist_type",
            "area_name",
            F.when(year <= max_begin_year, year).alias("begin_year"),
        )
    )


def run(cfg: Config) -> dict:
    """Run the stage; return the logged counts."""
    repo_root = cfg.source.resolve().parent.parent
    specials = load_special_artists(repo_root / cfg.get("filters.special_artists_file"))
    snapshot_year = int(str(cfg.get("snapshot_date"))[:4])
    spark = get_spark("musin-clean-artists")

    def raw(table: str) -> DataFrame:
        return spark.read.parquet(str(cfg.data_path("raw") / table))

    out = cfg.data_path("clean")
    artist = raw("artist")
    n_in = artist.count()
    special = resolve_special_artists(specials, artist)
    special.write.mode("overwrite").parquet(str(out / "special_artists"))
    special = spark.read.parquet(str(out / "special_artists"))

    cleaned = clean_artists(artist, raw("artist_type"), raw("area"), special, snapshot_year)
    cleaned.write.mode("overwrite").parquet(str(out / "artists"))
    result = spark.read.parquet(str(out / "artists"))

    stats = result.agg(
        F.count("*").alias("rows"),
        F.countDistinct("artist_id").alias("ids"),
        F.countDistinct("mbid").alias("mbids"),
        F.sum(F.col("begin_year").isNull().cast("long")).alias("begin_year_null"),
        F.sum(F.col("artist_type").isNull().cast("long")).alias("artist_type_null"),
        F.sum(F.col("area_name").isNull().cast("long")).alias("area_name_null"),
    ).collect()[0]
    future = artist.filter(F.col("begin_date_year") > snapshot_year).count()
    leaked = result.join(special.select("artist_id"), "artist_id").count()
    acn = raw("artist_credit_name").select("artist_credit", F.col("artist").alias("artist_id"))
    special_acn = acn.join(F.broadcast(special.select("artist_id")), "artist_id")
    counts = {
        "artists_in": n_in,
        "special_listed": len(specials),
        "special_removed": special.count(),
        "artists_out": stats["rows"],
        "credit_name_rows_special": special_acn.count(),
        "artist_credits_with_special": special_acn.select("artist_credit").distinct().count(),
        "begin_year_future_nulled": future,
        "begin_year_null": stats["begin_year_null"],
        "artist_type_null": stats["artist_type_null"],
        "area_name_null": stats["area_name_null"],
    }
    for key, value in counts.items():
        log.info("  %-26s %s", key, value)

    problems = []
    if stats["rows"] != n_in - counts["special_removed"]:
        problems.append("artists_out != artists_in - special_removed")
    if stats["ids"] != stats["rows"] or stats["mbids"] != stats["rows"]:
        problems.append("artist_id or mbid not unique")
    if leaked:
        problems.append(f"{leaked} special artists in clean/artists")
    spark.stop()
    if problems:
        raise RuntimeError("quality checks failed: " + "; ".join(problems))
    return counts


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    start = time.monotonic()
    run(load_config(args.config))
    log.info("done in %.0fs", time.monotonic() - start)
    return 0


if __name__ == "__main__":
    sys.exit(main())
