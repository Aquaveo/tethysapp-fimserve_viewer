# syntax=docker/dockerfile:1
# Build FIMserve Viewer as a standalone app image on the CIROH Tethys base, so
# the portal OIDC login backend, the S3 static/default storage, and the Dask
# offload code ship together. Build context is this app repo.
ARG UVX_BUILDER=awiciroh/ciroh-tethys:builder_9aa1fb30f333da7bead57939b0ac0651b0ea3090
ARG UVX_RUNTIME=ghcr.io/aquaveo/tethys-uvx:runtime-base

FROM ${UVX_BUILDER} AS builder

ENV UV_CACHE_DIR=/cache/uv

WORKDIR ${TETHYS_HOME}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends libgdal-dev gdal-bin g++ \
    && rm -rf /var/lib/apt/lists/*

COPY requirements/overrides.txt ${TETHYS_HOME}/overrides.txt
COPY . ${TETHYS_HOME}/apps/tethysapp-fimserve_viewer

RUN --mount=type=cache,target=/cache/uv \
    cd ${TETHYS_HOME}/apps/tethysapp-fimserve_viewer \
    && uv pip install . --overrides ${TETHYS_HOME}/overrides.txt \
    && uv pip uninstall nodejs-bin \
    && uv pip install --overrides ${TETHYS_HOME}/overrides.txt "gdal==$(gdal-config --version)" \
    && /opt/conda/envs/tethys/bin/python -c "import storages, boto3, dask.distributed" \
    && /opt/conda/envs/tethys/bin/python -c "import fimserve.datadownload, fimserve.runFIM" \
    && /opt/conda/envs/tethys/bin/python -c "import tethysapp.fimserve_viewer.dask_worker"

FROM ${UVX_RUNTIME}

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends gdal-bin git \
    && rm -rf /var/lib/apt/lists/*

COPY --chown=1000:1000 --from=builder /opt/python /opt/python
COPY --chown=1000:1000 --from=builder /opt/conda /opt/conda

USER 1000:1000
