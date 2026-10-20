# Spark + Jupyter image for Musin (F1.1).
# Python 3.11 (RULES.md §5) + Java 17 + PySpark 3.5 in local mode.
# The pip pyspark wheel bundles the Spark jars, so no separate Spark install.
FROM python:3.11.16-slim-bookworm

# procps: Spark's launcher scripts call `ps`. gnupg: dump signature check (F1.2).
# lbzip2: multi-threaded bzip2 for listing/extracting the dumps (F1.3).
# fonts-noto-cjk: Chinese/Japanese/Korean artist names in report figures (F4.3).
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless procps gnupg lbzip2 \
       fonts-noto-cjk \
    && rm -rf /var/lib/apt/lists/*

ENV JAVA_HOME=/usr/lib/jvm/java-17-openjdk-amd64 \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt

# GraphFrames jars for the F5.2 PageRank comparison (D-042), pinned by SHA-1.
COPY jars.txt fetch_jars.py /tmp/
RUN python /tmp/fetch_jars.py /tmp/jars.txt

# Run as a non-root user whose UID matches the WSL user, so files written to
# $MUSIN_DATA and the repo are owned by you on the host. The first WSL user
# is UID 1000; if `id -u` says otherwise, rebuild with
#   docker compose build --build-arg HOST_UID=$(id -u) spark
ARG HOST_UID=1000
RUN useradd --create-home --uid ${HOST_UID} --shell /bin/bash musin
USER musin

# The repo is bind-mounted here; src/ is importable without an install step.
WORKDIR /workspace
ENV PYTHONPATH=/workspace/src

# Jupyter Lab with no token (team choice, see docs/features/F1.1-dev-environment.md).
# Safe only because docker-compose.yml publishes the port on 127.0.0.1.
CMD ["jupyter", "lab", "--ip=0.0.0.0", "--port=8888", "--no-browser", \
     "--ServerApp.root_dir=/workspace", \
     "--IdentityProvider.token=", "--ServerApp.password="]
