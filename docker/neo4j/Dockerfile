# syntax=docker/dockerfile:1.7
# Neo4j 5.26 LTS Community + Graph Data Science, both pinned (F1.1, NFR-01).
#
# Why not NEO4J_PLUGINS='["graph-data-science"]'? The stock entrypoint then
# fetches versions.json and downloads the newest compatible GDS on EVERY
# start, so the GDS version could change between runs and startup needs the
# internet. Baking the jar in at build time, checksum-verified, pins it.
FROM neo4j:5.26.30-community

# GDS 2.13.12 is the release the official versions.json maps to Neo4j 5.26.30
# (checked 2026-10-10). Change both lines together when upgrading.
ADD --checksum=sha256:246e3fbbbf733b4def1e7b0a9740a2309f6605ee8a7b46b29fe1de56d0a4b47c \
    https://graphdatascience.ninja/neo4j-graph-data-science-2.13.12.jar \
    /var/lib/neo4j/plugins/graph-data-science.jar
RUN chmod 644 /var/lib/neo4j/plugins/graph-data-science.jar
