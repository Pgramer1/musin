"""Download the jars listed in jars.txt from Maven Central into pyspark/jars (F5.2).

Run at image build time: python fetch_jars.py jars.txt
Every jar is checked against its pinned SHA-1; a mismatch stops the build.
"""

import hashlib
import sys
import urllib.request
from pathlib import Path

import pyspark

MAVEN = "https://repo1.maven.org/maven2/"


def main(listing: str) -> None:
    target = Path(pyspark.__file__).parent / "jars"
    for line in Path(listing).read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        path, sha1 = line.split()
        data = urllib.request.urlopen(MAVEN + path, timeout=60).read()
        got = hashlib.sha1(data).hexdigest()
        if got != sha1:
            sys.exit(f"SHA-1 mismatch for {path}: expected {sha1}, got {got}")
        (target / Path(path).name).write_bytes(data)
        print(f"{Path(path).name}  {len(data):,} bytes  sha1 ok")


if __name__ == "__main__":
    main(sys.argv[1])
