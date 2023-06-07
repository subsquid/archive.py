FROM python:3.11-slim-bullseye AS base


FROM base AS builder
RUN pip install pdm
WORKDIR /project
RUN pdm venv create --with venv
ADD pyproject.toml .
ADD pdm.lock .
RUN pdm sync --no-self --prod
ADD etha etha/
ADD README.md .


FROM builder AS ingest-builder
RUN pdm sync -G ingest --no-editable --prod


FROM base AS ingest
COPY --from=ingest-builder /project/.venv /app/env/
COPY --from=ingest-builder /project/etha /app/etha/
RUN /app/env/bin/python -m etha.ingest --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "etha.ingest"]


FROM builder AS worker-builder
RUN pdm sync -G http-worker --no-editable --prod


FROM base AS worker
COPY --from=worker-builder /project/.venv /app/env/
COPY --from=worker-builder /project/etha /app/etha/
RUN /app/env/bin/python -m etha.worker --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "etha.worker"]
