"""Environment check (F1.1): config, data root, and a Spark Parquet round trip.

    python -m musin.envcheck --config config/pipeline.yaml

Exits non-zero on the first failure. Writes only to $MUSIN_DATA/tmp/envcheck,
which it deletes again.
"""

from __future__ import annotations

import argparse
import logging
import platform
import shutil
import sys
import time

from musin.common.config import load_config
from musin.common.spark import get_spark

log = logging.getLogger("musin.envcheck")

LAYERS = ("dumps", "raw", "clean", "graph", "import")
CHECK_ROWS = 1_000_000


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--config", required=True, help="path to config/pipeline.yaml")
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cfg = load_config(args.config)
    log.info("config ok: %s (snapshot_date=%s)", cfg.source, cfg.get("snapshot_date"))

    missing = [layer for layer in LAYERS if not cfg.data_path(layer).is_dir()]
    if missing:
        log.error("missing data dirs under %s: %s (run setup/preflight.sh)", cfg.data_root, missing)
        return 1
    usage = shutil.disk_usage(cfg.data_root)
    log.info("data root ok: %s, %.0f GB free", cfg.data_root, usage.free / 1e9)

    start = time.monotonic()
    spark = get_spark("musin-envcheck")
    sc = spark.sparkContext
    log.info(
        "spark %s, python %s, java %s, master %s, cores %d, driver memory %s",
        spark.version,
        platform.python_version(),
        sc._jvm.java.lang.System.getProperty("java.version"),
        sc.master,
        sc.defaultParallelism,
        sc.getConf().get("spark.driver.memory"),
    )

    out = cfg.data_root / "tmp" / "envcheck"
    df = spark.range(CHECK_ROWS).selectExpr("id", "id % 7 AS bucket")
    df.write.mode("overwrite").parquet(str(out))
    counts = spark.read.parquet(str(out)).groupBy("bucket").count().collect()  # 7 rows
    total = sum(row["count"] for row in counts)
    shutil.rmtree(out)
    spark.stop()

    if total != CHECK_ROWS or len(counts) != 7:
        log.error("parquet round trip wrong: %d rows in %d buckets", total, len(counts))
        return 1
    log.info("parquet round trip ok: %d rows in %.1fs", total, time.monotonic() - start)
    log.info("environment OK")
    return 0


if __name__ == "__main__":
    sys.exit(main())
