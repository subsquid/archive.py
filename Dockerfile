FROM node:18 AS writer
RUN npm i -g @subsquid/eth-ingest


RUN set -x && \
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh" -O miniconda.sh -q && \
    echo "78f39f9bae971ec1ae7969f0516017f2413f17796670f7040725dd83fcff5689 miniconda.sh" > shasum && \
    sha256sum --check --status shasum && \
    mkdir -p /opt && \
    sh miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh shasum && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete && \
    /opt/conda/bin/conda clean -afy

WORKDIR /etha-writer
ADD environments/writer.yml environment.yml
RUN /opt/conda/bin/conda env create --prefix env
ENV PATH="/etha-writer/env/bin:${PATH}"
ADD etha etha/
ENTRYPOINT ["python3", "-m", "etha.writer.main"]


FROM debian:bullseye-slim AS worker

RUN set -x && \
    apt-get update && apt-get install -y --no-install-recommends wget ca-certificates && rm -rf /var/lib/apt/lists/* && \
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py39_4.12.0-Linux-x86_64.sh" -O miniconda.sh -q && \
    echo "78f39f9bae971ec1ae7969f0516017f2413f17796670f7040725dd83fcff5689 miniconda.sh" > shasum && \
    sha256sum --check --status shasum && \
    mkdir -p /opt && \
    sh miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh shasum && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete && \
    /opt/conda/bin/conda clean -afy


WORKDIR /etha-worker
ADD environments/worker.yml environment.yml
RUN /opt/conda/bin/conda env create --prefix env
ENV PATH="/etha-worker/env/bin:${PATH}"
ADD etha etha/
ENTRYPOINT ["python3", "-m", "etha.worker.server"]
