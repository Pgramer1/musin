# RAPIDS image for the F5.3 cuGraph PageRank benchmark (D-043).
# rapidsai/base 26.08 (CUDA 12, Python 3.11, pushed 2026-08-06) bundles cuDF and
# cuGraph; the pipeline's config loader needs PyYAML on top (same pin as docker/spark).
# Using the image means accepting the NVIDIA Deep Learning Container License
# (accepted by the team on 2026-10-20, D-043).
FROM rapidsai/base:26.08-cuda12-py3.11

RUN pip install --no-cache-dir pyyaml==6.0.3
