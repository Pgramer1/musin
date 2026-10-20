// F6.2: live demo in the Neo4j Browser (FR-25, PRD §7.1). Runs on the loaded
// graph `full` with every P4 property written back (F4.1–F4.3), read-only.
//
// How to use in the Browser (http://localhost:7474):
//   1. Drag this file onto the Favorites sidebar (star icon), or paste one
//      statement at a time. Each statement starts at its `// @name` line.
//   2. Set the parameters first; they persist for the session:
//        :param name => 'Ry Cooder';
//        :param limit => 40;
//        :param from => 'Frank Sinatra';
//        :param to => 'Snoop Dogg';
//        :param community => 1;
//   3. Statements that RETURN nodes and relationships draw a graph (ego
//      network, path); the others show a table.
// The demo script (order, talking points, timings) is in
// docs/features/F6.2-demo.md. `musin.demo_check` runs every statement with the
// demo parameters, so a broken query is caught before the presentation.
// Names are not unique: each lookup takes the artist with most recordings.

// @name demo_overview
// 1. What is in the graph: artists, genres and each collaboration type. One
//    count per type reads Neo4j's count store, so this answers instantly.
MATCH (a:Artist)
WITH count(a) AS artists
MATCH (g:Genre)
WITH artists, count(g) AS genres
CALL () {
  MATCH ()-[r:CO_CREDITED]->() RETURN 'CO_CREDITED' AS type, count(r) AS edges
  UNION ALL
  MATCH ()-[r:FEATURED]->() RETURN 'FEATURED' AS type, count(r) AS edges
  UNION ALL
  MATCH ()-[r:PRODUCED_FOR]->() RETURN 'PRODUCED_FOR' AS type, count(r) AS edges
  UNION ALL
  MATCH ()-[r:PERFORMED_FOR]->() RETURN 'PERFORMED_FOR' AS type, count(r) AS edges
}
RETURN artists, genres, type, edges
ORDER BY edges DESC;

// @name demo_top_bridges
// 2. RQ1: the top genre bridges (bridge score = genre diversity × betweenness percentile).
MATCH (a:Artist)
WHERE a.bridgeScore IS NOT NULL
RETURN a.name AS artist, a.type AS type, a.primaryGenre AS primaryGenre,
       round(a.genreDiversity, 3) AS genreDiversity, round(a.bridgeScore, 3) AS bridgeScore
ORDER BY a.bridgeScore DESC, a.artistId
LIMIT 10;

// @name demo_artist
// 3. One artist with every computed score ($name).
MATCH (a:Artist {name: $name})
WITH a ORDER BY a.recordingCount DESC LIMIT 1
RETURN a.name AS artist, a.type AS type, a.primaryGenre AS primaryGenre,
       a.recordingCount AS recordings, a.louvain AS community,
       round(a.genreDiversity, 3) AS genreDiversity, round(a.bridgeScore, 3) AS bridgeScore,
       a.degreeWeighted AS weightedDegree, round(a.pagerank, 2) AS pagerank,
       round(a.betweenness) AS betweenness;

// @name demo_artist_genres
// 4. Why the artist is a bridge: collaboration weight by the partner's primary genre.
MATCH (a:Artist {name: $name})
WITH a ORDER BY a.recordingCount DESC LIMIT 1
MATCH (a)-[r:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR]-(b:Artist)
RETURN coalesce(b.primaryGenre, '(no genre)') AS partnerGenre,
       count(DISTINCT b) AS partners, sum(r.weight) AS sharedRecordings
ORDER BY sharedRecordings DESC;

// @name demo_ego
// 5. Graph view: the artist's $limit strongest collaborations, with the
//    partners' genres (IN_GENRE is not drawn; colour nodes by primaryGenre in the Browser).
MATCH (a:Artist {name: $name})
WITH a ORDER BY a.recordingCount DESC LIMIT 1
MATCH (a)-[r:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR]-(b:Artist)
RETURN a, r, b
ORDER BY r.weight DESC, b.artistId
LIMIT $limit;

// @name demo_path
// 6. Degrees of separation ($from → $to): the canonical shortest path of F4.5
//    (D-036: unweighted hops over the four collaboration types, smallest artist-id sequence).
MATCH (x:Artist {name: $from})
WITH x ORDER BY x.recordingCount DESC LIMIT 1
MATCH (y:Artist {name: $to})
WITH x, y ORDER BY y.recordingCount DESC LIMIT 1
MATCH p = allShortestPaths((x)-[:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR*]-(y))
WITH p, [n IN nodes(p) | n.artistId] AS ids
ORDER BY ids
LIMIT 1
RETURN p;

// @name demo_centrality
// 7. RQ4: the top 10 by each centrality measure on COLLAB. They barely overlap.
CALL () {
  MATCH (a:Artist) RETURN 'weighted degree' AS measure, a ORDER BY a.degreeWeighted DESC, a.artistId LIMIT 10
  UNION ALL
  MATCH (a:Artist) RETURN 'PageRank' AS measure, a ORDER BY a.pagerank DESC, a.artistId LIMIT 10
  UNION ALL
  MATCH (a:Artist) RETURN 'betweenness' AS measure, a ORDER BY a.betweenness DESC, a.artistId LIMIT 10
}
RETURN measure, collect(a.name) AS top10;

// @name demo_community
// 8. RQ2: genre mix of community $community (1 = largest, F4.2 size ranks).
MATCH (a:Artist {louvain: $community})
WITH count(a) AS size, collect(a) AS members
UNWIND members AS a
WITH size, a WHERE a.primaryGenre IS NOT NULL
WITH size, count(a) AS labelled, collect(a.primaryGenre) AS genres
UNWIND genres AS genre
RETURN size, labelled, genre, count(*) AS artists, round(100.0 * count(*) / labelled, 1) AS percent
ORDER BY artists DESC
LIMIT 5;
