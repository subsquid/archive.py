FROM debian:bullseye-slim AS conda
RUN set -x && \
    apt-get update && apt-get install -y --no-install-recommends wget ca-certificates && rm -rf /var/lib/apt/lists/* && \
    wget "https://repo.anaconda.com/miniconda/Miniconda3-py39_23.1.0-1-Linux-x86_64.sh" -O miniconda.sh -q && \
    echo "5dc619babc1d19d6688617966251a38d245cb93d69066ccde9a013e1ebb5bf18 miniconda.sh" > shasum && \
    sha256sum --check --status shasum && \
    mkdir -p /opt && \
    sh miniconda.sh -b -p /opt/conda && \
    rm miniconda.sh shasum && \
    find /opt/conda/ -follow -type f -name '*.a' -delete && \
    find /opt/conda/ -follow -type f -name '*.js.map' -delete && \
    /opt/conda/bin/conda clean -afy


FROM conda AS ingest
WORKDIR /etha-ingest
ADD environments/ingest.yml environment.yml
RUN /opt/conda/bin/conda env create --prefix env
ENV PATH="/etha-ingest/env/bin:${PATH}"
ADD etha etha/
ENTRYPOINT ["python3", "-m", "etha.ingest"]


FROM conda AS worker
WORKDIR /etha-worker
ADD environments/worker.yml environment.yml
RUN /opt/conda/bin/conda env create --prefix env
ENV PATH="/etha-worker/env/bin:${PATH}"
ADD etha etha/
ENTRYPOINT ["python3", "-m", "etha.worker"]
EXPOSE 8000


FROM conda AS migrate
WORKDIR /etha-migrate
ADD environments/ingest.yml environment.yml
RUN /opt/conda/bin/conda env create --prefix env
ENV PATH="/etha-migrate/env/bin:${PATH}"
ADD etha etha/
ADD migrate.py .
ENTRYPOINT ["python3", "-m", "migrate"]
