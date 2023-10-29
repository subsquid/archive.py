FROM python:3.11-slim-bullseye AS base


FROM base AS builder
RUN pip install pdm
WORKDIR /project
RUN pdm venv create --with venv
ADD pyproject.toml .
ADD pdm.lock .
RUN pdm sync --no-self --prod


FROM builder AS writer-builder
RUN pdm sync -G writer --no-editable --prod


FROM base AS writer-base
COPY --from=writer-builder /project/.venv /app/env/
ADD sqa /app/sqa/
ADD rewrite_archive.py /app/rewrite_archive.py
ADD README.md .


FROM writer-base AS eth-ingest
RUN /app/env/bin/python -m sqa.eth.ingest --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.eth.ingest"]


FROM writer-base AS substrate-writer
RUN /app/env/bin/python -m sqa.substrate.writer --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.substrate.writer"]


FROM builder AS worker-builder
RUN pdm sync -G http-worker --no-editable --prod


FROM base AS worker
COPY --from=worker-builder /project/.venv /app/env/
ADD sqa /app/sqa/
ADD README.md .
RUN /app/env/bin/python -m sqa.worker --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.worker"]


FROM builder as p2p-worker-builder
RUN pdm sync -G p2p-worker --no-editable --prod


FROM base as p2p-worker
COPY --from=p2p-worker-builder /project/.venv /app/env/
ADD sqa /app/sqa/
ADD README.md .
VOLUME /app/data
ENV DATA_DIR=/app/data/worker
ENV PING_INTERVAL_SEC=20
RUN echo "#!/bin/bash \n exec /app/env/bin/python -m sqa.worker.p2p --data-dir \${DATA_DIR} --proxy \${PROXY_ADDR} --scheduler-id \${SCHEDULER_ID}" > ./entrypoint.sh
RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
