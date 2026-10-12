// F4.5: degrees of separation between two artists (FR-19, PRD §5.5, D-036).
// Run by `python -m musin.analytics.separation`; also usable in the Neo4j
// Browser demo (F6.2) with the same parameters. Paths are unweighted hops over
// the four collaboration relationship types only, in either direction (COLLAB,
// PRD §5). An untyped pattern would also walk through :Genre nodes via
// IN_GENRE ("Frank Sinatra - pop - Snoop Dogg"), which is not a collaboration.

// @name resolve
// An artist by MBID or exact name. Names are not unique; the caller refuses
// an ambiguous name and lists these rows so the user can pick an MBID.
MATCH (a:Artist)
WHERE a.mbid = $key OR a.name = $key
RETURN a.artistId AS artistId, a.mbid AS mbid, a.name AS name, a.type AS type,
       a.recordingCount AS recordingCount, a.primaryGenre AS primaryGenre
ORDER BY recordingCount DESC, artistId;

// @name shortest_paths
// Every shortest path, up to $maxPaths relationship paths. Parallel
// relationships of different types between the same two artists give several
// relationship paths over the same artists; the caller deduplicates the
// artist sequences and picks the smallest as the canonical path (D-036).
MATCH (x:Artist {artistId: $from}), (y:Artist {artistId: $to})
MATCH p = allShortestPaths((x)-[:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR*]-(y))
WITH p LIMIT $maxPaths
WITH collect([n IN nodes(p) | n.artistId]) AS sequences
RETURN size(sequences) AS relationshipPaths, sequences;

// @name hop_relations
// The collaboration relationships between consecutive artists of a path, so
// each hop can be shown with its types, directions and shared recordings.
UNWIND range(0, size($ids) - 2) AS i
MATCH (a:Artist {artistId: $ids[i]})-[r:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR]-(b:Artist {artistId: $ids[i + 1]})
RETURN i AS hop, type(r) AS type, r.weight AS weight, startNode(r) = a AS forward
ORDER BY hop, type;

// @name artists_by_id
MATCH (a:Artist)
WHERE a.artistId IN $ids
RETURN a.artistId AS artistId, a.mbid AS mbid, a.name AS name;
