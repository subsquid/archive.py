FROM python:3.11-slim-bullseye AS base


FROM base AS builder
RUN apt update
RUN apt install -y gcc
RUN pip install pdm
WORKDIR /project
RUN pdm venv create --with venv
ADD pyproject.toml .
ADD pdm.lock .
RUN pdm sync --no-self --prod
ADD sqa sqa/
ADD README.md .


FROM builder AS writer-builder
RUN pdm sync -G writer --no-editable --prod


FROM base AS writer-base
COPY --from=writer-builder /project/.venv /app/env/
COPY --from=writer-builder /project/sqa /app/sqa/


FROM writer-base AS eth-ingest
RUN /app/env/bin/python -m sqa.eth.ingest --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.eth.ingest"]


FROM writer-base AS solana-writer
RUN /app/env/bin/python -m sqa.solana.writer --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.solana.writer"]


FROM writer-base AS substrate-writer
RUN /app/env/bin/python -m sqa.substrate.writer --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.substrate.writer"]


FROM builder AS worker-builder
RUN pdm sync -G http-worker --no-editable --prod


FROM base AS worker
COPY --from=worker-builder /project/.venv /app/env/
COPY --from=worker-builder /project/sqa /app/sqa/
RUN /app/env/bin/python -m sqa.worker --help > /dev/null # win a little bit of startup time
ENTRYPOINT ["/app/env/bin/python", "-m", "sqa.worker"]


FROM builder as p2p-worker-builder
RUN pdm sync -G p2p-worker --no-editable --prod


FROM base as p2p-worker
COPY --from=p2p-worker-builder /project/.venv /app/env/
COPY --from=p2p-worker-builder /project/sqa /app/sqa/
VOLUME /app/data
ENV DATA_DIR=/app/data
ENV PING_INTERVAL_SEC=20
ENV LOGS_SEND_INTERVAL_SEC=600
ENV PROMETHEUS_PORT=9090
ENV NUM_WORKER_PROC=16
RUN echo "#!/bin/bash \n exec /app/env/bin/python -m sqa.worker.p2p  \
    --data-dir \${DATA_DIR}  \
    --procs \${NUM_WORKER_PROC} \
    --proxy \${PROXY_ADDR}  \
    --prometheus-port \${PROMETHEUS_PORT} \
    --scheduler-id \${SCHEDULER_ID}  \
    --rpc-url \${RPC_URL} \
    --logs-collector-id \${LOGS_COLLECTOR_ID}" > ./entrypoint.sh
RUN chmod +x ./entrypoint.sh
ENTRYPOINT ["./entrypoint.sh"]
