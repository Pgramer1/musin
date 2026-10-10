"""Neo4j driver factory shared by the smoke test and the P3/P4 stages."""

from __future__ import annotations

import logging
import time

from neo4j import Driver, GraphDatabase
from neo4j.exceptions import AuthError, ServiceUnavailable

from musin.common.config import require_env

log = logging.getLogger("musin.neo4j")

USER = "neo4j"


def get_driver(wait_seconds: float) -> Driver:
    """Connect to $NEO4J_URI as user neo4j with $NEO4J_PASSWORD.

    NEO4J_URI is set by docker-compose.yml for the spark service (the service
    name inside the compose network); NEO4J_PASSWORD comes from .env. Waits up
    to `wait_seconds` for the server to accept connections, because Neo4j takes
    a few seconds to start. A wrong password fails at once.
    """
    uri = require_env("NEO4J_URI")
    driver = GraphDatabase.driver(uri, auth=(USER, require_env("NEO4J_PASSWORD")))
    deadline = time.monotonic() + wait_seconds
    while True:
        try:
            driver.verify_connectivity()
            return driver
        except AuthError:
            driver.close()
            raise
        except (ServiceUnavailable, OSError) as exc:
            if time.monotonic() > deadline:
                driver.close()
                raise ServiceUnavailable(f"Neo4j at {uri} not reachable: {exc}") from exc
            log.info("waiting for Neo4j at %s ...", uri)
            time.sleep(2)
