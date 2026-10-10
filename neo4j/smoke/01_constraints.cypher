// F1.5 smoke test: constraints and indexes on the toy graph.
// The production versions belong to F3.5 (neo4j/01_constraints.cypher).
// Statements are named with "// @name <name>" and end with ";".

// @name artist_id_unique
CREATE CONSTRAINT smoke_artist_id IF NOT EXISTS
FOR (a:Artist) REQUIRE a.artistId IS UNIQUE;

// @name genre_name_unique
CREATE CONSTRAINT smoke_genre_name IF NOT EXISTS
FOR (g:Genre) REQUIRE g.name IS UNIQUE;

// @name artist_mbid_index
CREATE INDEX smoke_artist_mbid IF NOT EXISTS
FOR (a:Artist) ON (a.mbid);

// @name await_indexes
CALL db.awaitIndexes(60);
