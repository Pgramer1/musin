// F4.3: genre bridges (RQ1, PRD §5, D-038). Run by `python -m musin.analytics.bridges`.
// Genre diversity and the genre matrix are computed in Spark from the loaded
// graph's Parquet; Neo4j supplies F4.1's betweenness (written back by
// `musin.analytics.centrality`) and receives the bridge results (§8.3).

// @name betweenness
// Every artist with its F4.1 betweenness; the percentile rank is taken over
// all of them (PRD §5 RQ1), not only artists with a defined diversity.
MATCH (a:Artist)
RETURN a.artistId AS artistId, a.mbid AS mbid, a.name AS name, a.type AS type,
       a.primaryGenre AS primaryGenre, a.betweenness AS betweenness;

// @name write_bridges
// genreDiversity and bridgeScore for artists with a defined diversity, in
// batches of $rows. Other artists keep no such property (§8.3: undefined).
UNWIND $rows AS r
MATCH (a:Artist {artistId: r.artistId})
SET a.genreDiversity = r.genreDiversity, a.bridgeScore = r.bridgeScore
RETURN count(a) AS written;

// @name count_written
MATCH (a:Artist)
RETURN count(a.genreDiversity) AS genreDiversity, count(a.bridgeScore) AS bridgeScore;
