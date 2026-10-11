// F4.1: centrality measures on a projection (RQ4, PRD §5.5).
// Run by `python -m musin.analytics.centrality`. Each algorithm runs in mutate
// mode, so its score is added to the in-memory projection under the §8.3
// property name; the scores are then streamed out and, for collab_undirected
// only, written back to :Artist. $graph is the projection name; the other
// parameters come from config/pipeline.yaml `analysis.*`.

// @name estimate_degree
CALL gds.degree.mutate.estimate($graph, {
  relationshipWeightProperty: 'weight', mutateProperty: 'degreeWeighted', concurrency: $concurrency
})
YIELD requiredMemory, bytesMax
RETURN requiredMemory, bytesMax;

// @name estimate_pagerank
CALL gds.pageRank.mutate.estimate($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping, maxIterations: $maxIterations,
  mutateProperty: 'pagerank', concurrency: $concurrency
})
YIELD requiredMemory, bytesMax
RETURN requiredMemory, bytesMax;

// @name estimate_betweenness
CALL gds.betweenness.mutate.estimate($graph, {
  samplingSize: $samplingSize, samplingSeed: $seed, mutateProperty: 'betweenness',
  concurrency: $betweennessConcurrency
})
YIELD requiredMemory, bytesMax
RETURN requiredMemory, bytesMax;

// @name degree
// Weighted degree: the sum of `weight` (shared recordings, D-028) over the
// artist's pairs. The projection is undirected, so each pair counts once.
CALL gds.degree.mutate($graph, {
  relationshipWeightProperty: 'weight', mutateProperty: 'degreeWeighted', concurrency: $concurrency
})
YIELD nodePropertiesWritten, computeMillis
RETURN nodePropertiesWritten, computeMillis, null AS ranIterations, null AS didConverge;

// @name pagerank
CALL gds.pageRank.mutate($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping, maxIterations: $maxIterations,
  mutateProperty: 'pagerank', concurrency: $concurrency
})
YIELD nodePropertiesWritten, computeMillis, ranIterations, didConverge
RETURN nodePropertiesWritten, computeMillis, ranIterations, didConverge;

// @name betweenness
// Unweighted on purpose (PRD §5.5): GDS reads weights as distances, so a pair
// with many shared recordings would look far apart. Sampled from $samplingSize
// source nodes chosen with $seed; the scores are not rescaled to the full graph.
// Single-threaded (D-033): with more threads the sampled sources depend on
// thread scheduling, and two runs with the same seed give different scores.
CALL gds.betweenness.mutate($graph, {
  samplingSize: $samplingSize, samplingSeed: $seed, mutateProperty: 'betweenness',
  concurrency: $betweennessConcurrency
})
YIELD nodePropertiesWritten, computeMillis
RETURN nodePropertiesWritten, computeMillis, null AS ranIterations, null AS didConverge;

// @name stream_property
// One score per projected artist. nodeId is the Neo4j node id, mapped to
// artistId in Python with `artists` (cheaper than gds.util.asNode per row).
CALL gds.graph.nodeProperty.stream($graph, $property)
YIELD nodeId, propertyValue
RETURN nodeId, propertyValue AS value;

// @name artists
// The node-id mapping and the descriptive columns of the top lists.
MATCH (a:Artist)
RETURN id(a) AS nodeId, a.artistId AS artistId, a.mbid AS mbid, a.name AS name,
       a.type AS type, a.primaryGenre AS primaryGenre;

// @name write_back
// PRD §8.3: COLLAB scores become :Artist properties (collab_undirected only).
CALL gds.graph.nodeProperties.write($graph, $properties, ['*'], {writeConcurrency: $concurrency})
YIELD propertiesWritten, writeMillis
RETURN propertiesWritten, writeMillis;

// @name count_written
// Every artist is in COLLAB (§8.3), so every artist must now carry all scores.
MATCH (a:Artist)
RETURN count(a) AS artists,
       count(a.degreeWeighted) AS degreeWeighted,
       count(a.pagerank) AS pagerank,
       count(a.betweenness) AS betweenness;
