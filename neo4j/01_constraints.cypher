// F3.5: constraints and indexes of the production graph (PRD §8.3).
// Applied by `python -m musin.gds.setup` after every import (neo4j/import_graph.sh).
// Statement names equal the constraint/index names. All statements are
// idempotent (IF NOT EXISTS). A uniqueness constraint fails to create if the
// imported data violates it, so it doubles as a data check.

// @name artist_id_unique
// Every P4 lookup, write-back and report join goes through artistId (D-016: integer).
CREATE CONSTRAINT artist_id_unique IF NOT EXISTS
FOR (a:Artist) REQUIRE a.artistId IS UNIQUE;

// @name artist_mbid_unique
// Reports identify artists by MBID (RULES §6); F3.3 checks it is unique.
CREATE CONSTRAINT artist_mbid_unique IF NOT EXISTS
FOR (a:Artist) REQUIRE a.mbid IS UNIQUE;

// @name genre_name_unique
CREATE CONSTRAINT genre_name_unique IF NOT EXISTS
FOR (g:Genre) REQUIRE g.name IS UNIQUE;

// @name artist_name
// Lookups by name in the demo and for degrees of separation (F4.5, F6.2).
// Names are not unique, so this is a plain range index.
CREATE INDEX artist_name IF NOT EXISTS
FOR (a:Artist) ON (a.name);

// @name await_indexes
CALL db.awaitIndexes(600);

// @name show_constraints
SHOW CONSTRAINTS YIELD name, type, labelsOrTypes, properties
RETURN name, type, labelsOrTypes, properties
ORDER BY name;

// @name show_indexes
SHOW INDEXES YIELD name, type, state, populationPercent, labelsOrTypes, properties
WHERE type <> 'LOOKUP'
RETURN name, type, state, populationPercent, labelsOrTypes, properties
ORDER BY name;
