// F3.5: memory estimates of the PRD §5.5 algorithms on a projection (RULES §7:
// run .estimate before running an algorithm on the full graph). Parameters
// come from config/pipeline.yaml `analysis.*`; $graph is the projection name.
// requiredMemory excludes the projection itself (see gds.graph.list memoryUsage).

// @name degree
CALL gds.degree.stream.estimate($graph, {relationshipWeightProperty: 'weight'})
YIELD requiredMemory, bytesMin, bytesMax
RETURN requiredMemory, bytesMin, bytesMax;

// @name pagerank
CALL gds.pageRank.stream.estimate($graph, {
  relationshipWeightProperty: 'weight', dampingFactor: $damping, maxIterations: $maxIterations
})
YIELD requiredMemory, bytesMin, bytesMax
RETURN requiredMemory, bytesMin, bytesMax;

// @name betweenness
// Unweighted on purpose (PRD §5.5): GDS reads weights as distances.
CALL gds.betweenness.stream.estimate($graph, {samplingSize: $samplingSize, samplingSeed: $seed})
YIELD requiredMemory, bytesMin, bytesMax
RETURN requiredMemory, bytesMin, bytesMax;

// @name louvain
CALL gds.louvain.stream.estimate($graph, {relationshipWeightProperty: 'weight'})
YIELD requiredMemory, bytesMin, bytesMax
RETURN requiredMemory, bytesMin, bytesMax;
