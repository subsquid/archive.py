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

FROM builder as p2p-worker-builder
RUN pdm sync -G p2p-worker --no-editable --prod

FROM base as p2p-worker
COPY --from=p2p-worker-builder /project/.venv /app/env/
COPY --from=p2p-worker-builder /project/etha /app/etha/
VOLUME /app/data
RUN echo "#!/bin/bash \n exec /app/env/bin/python -m etha.worker.p2p --data-dir /app/data --proxy \${PROXY_ADDR} --scheduler-id \${SCHEDULER_ID}" > ./entrypoint.sh
RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
