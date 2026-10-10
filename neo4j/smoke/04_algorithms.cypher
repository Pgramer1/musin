// F1.5 smoke test: the PRD §5.5 algorithms on the toy projection.
// $graph is the projection name; other parameters come from config/pipeline.yaml.

// @name degree_weighted
CALL gds.degree.stream($graph, {relationshipWeightProperty: 'weight'})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).artistId AS artistId, score
ORDER BY artistId;

// @name pagerank_estimate
CALL gds.pageRank.stream.estimate($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping, maxIterations: $maxIterations
})
YIELD requiredMemory, bytesMin, bytesMax, nodeCount, relationshipCount
RETURN requiredMemory, bytesMin, bytesMax, nodeCount, relationshipCount;

// @name pagerank
CALL gds.pageRank.stream($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping,
  maxIterations: $maxIterations, tolerance: $tolerance
})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).artistId AS artistId, score
ORDER BY artistId;

// @name pagerank_stats
CALL gds.pageRank.stats($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping,
  maxIterations: $maxIterations, tolerance: $tolerance
})
YIELD ranIterations, didConverge
RETURN ranIterations, didConverge;

// @name betweenness_estimate
CALL gds.betweenness.stream.estimate($graph, {samplingSize: $samplingSize, samplingSeed: $seed})
YIELD requiredMemory, bytesMin, bytesMax
RETURN requiredMemory, bytesMin, bytesMax;

// @name betweenness
// Unweighted on purpose (PRD §5.5): GDS reads weights as distances.
CALL gds.betweenness.stream($graph, {samplingSize: $samplingSize, samplingSeed: $seed})
YIELD nodeId, score
RETURN gds.util.asNode(nodeId).artistId AS artistId, score
ORDER BY artistId;

// @name louvain
CALL gds.louvain.stream($graph, {
  relationshipWeightProperty: 'weight', concurrency: $concurrency
})
YIELD nodeId, communityId
RETURN gds.util.asNode(nodeId).artistId AS artistId, communityId
ORDER BY artistId;

// @name louvain_stats
CALL gds.louvain.stats($graph, {
  relationshipWeightProperty: 'weight', concurrency: $concurrency
})
YIELD communityCount, modularity, ranLevels
RETURN communityCount, modularity, ranLevels;

// @name pagerank_write
// Write-back path (RULES §7): results become Artist properties.
CALL gds.pageRank.write($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping,
  maxIterations: $maxIterations, tolerance: $tolerance, writeProperty: 'pagerank'
})
YIELD nodePropertiesWritten
RETURN nodePropertiesWritten;

// @name pagerank_read_back
MATCH (a:Artist) RETURN a.artistId AS artistId, a.pagerank AS pagerank ORDER BY artistId;

// @name shortest_path
// Degrees of separation (PRD §5.5, FR-19): unweighted hops over every edge type.
MATCH (a:Artist {artistId: $from}), (b:Artist {artistId: $to})
MATCH p = shortestPath((a)-[:CO_CREDITED|FEATURED|PRODUCED_FOR|PERFORMED_FOR*..10]-(b))
RETURN length(p) AS hops, [n IN nodes(p) | n.artistId] AS path;
