// F5.2: unweighted PageRank in GDS for the engine comparison (FR-22, D-042).
// Run by `python -m musin.benchmark.pagerank --engine gds` on collab_undirected
// (F3.5). GraphFrames' PageRank has no edge weights, so both engines run
// unweighted on the same artist pairs; damping, tolerance, iteration cap and
// concurrency come from config/pipeline.yaml. Mutate mode keeps the scores in
// the projection only: nothing is written to the database.

// @name pagerank_unweighted
CALL gds.pageRank.mutate($graph, {
  dampingFactor: $damping, maxIterations: $maxIterations, tolerance: $tolerance,
  mutateProperty: 'pagerankUnweighted', concurrency: $concurrency
})
YIELD nodePropertiesWritten, computeMillis, ranIterations, didConverge
RETURN nodePropertiesWritten, computeMillis, ranIterations, didConverge;

// @name stream_scores
// One score per projected artist, by Neo4j node id (mapped with artist_ids).
CALL gds.graph.nodeProperty.stream($graph, 'pagerankUnweighted')
YIELD nodeId, propertyValue
RETURN nodeId, propertyValue AS score;

// @name artist_ids
// Node-id mapping, names for the top lists, and F4.1's weighted PageRank
// (written back by F4.1, PRD §8.3) as the reference.
MATCH (a:Artist)
RETURN id(a) AS nodeId, a.artistId AS artistId, a.mbid AS mbid, a.name AS name,
       a.pagerank AS pagerankWeighted;
