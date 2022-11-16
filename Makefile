PY = env/bin/python3


init:
	@if [ -e env ]; then \
  		conda env update -f environments/dev.yml --prefix env --prune; \
	else \
  		conda env create -f environments/dev.yml --prefix env; \
	fi


build-writer:
	docker buildx build --target writer --platform linux/amd64 . --load


query:
	@$(PY) -m etha.gateway.main


write:
	@rm -rf data/parquet
	@cat data/blocks.jsonl | $(PY) -m etha.writer.main --dest data/parquet


ingest:
	@$(PY) -m etha.writer.main --dest data/mainnet --src-node ${ETH_NODE}


router:
	@$(PY) -m etha.worker.fake_router


worker:
	@$(PY) -m etha.worker.server \
		--router http://localhost:5555 \
		--worker-id 1 \
		--worker-url http://localhost:8000 \
		--data-dir data/worker


.PHONY: init dbuild-writer query write ingest
