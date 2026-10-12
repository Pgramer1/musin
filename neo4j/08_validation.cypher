// F4.6: validation (FR-20, PRD §5.6). Run by `python -m musin.analytics.validation`
// after F4.1 (centrality) and F4.3 (bridges) have written their scores back.

// @name scores
// Every artist with every P4 score; null where a score is undefined
// (genreDiversity and bridgeScore need enough labelled neighbours, F4.3).
MATCH (a:Artist)
RETURN a.artistId AS artistId, a.mbid AS mbid, a.name AS name, a.type AS type,
       a.primaryGenre AS primaryGenre, a.recordingCount AS recordingCount,
       a.degreeWeighted AS degreeWeighted, a.pagerank AS pagerank,
       a.betweenness AS betweenness, a.genreDiversity AS genreDiversity,
       a.bridgeScore AS bridgeScore, a.louvain AS louvain;
