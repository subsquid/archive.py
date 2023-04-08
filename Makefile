PY = env/bin/python3


init:
	@if [ -e env ]; then \
  		conda env update -f environments/dev.yml --prefix env --prune; \
	else \
  		conda env create -f environments/dev.yml --prefix env; \
	fi


build-ingest:
	docker buildx build --target ingest --platform linux/amd64 . --load


write:
	@rm -rf data/parquet
	@cat data/blocks.jsonl | $(PY) -m etha.ingest.main --dest data/parquet


ingest-eth:
	@$(PY) -m etha.ingest.main --dest data/mainnet \
		-e ${ETH_NODE} \
		-c 10 \
		-m trace_replayBlockTransactions \
		-e ${ETH_BLAST} \
		-c 10 \
		-r 500 \
		--batch-limit 100 \
		--with-receipts \
		--with-traces \
		--write-chunk-size 2048 \
		--first-block 15000000


ingest-poly:
	@$(PY) -m etha.ingest.main --dest data/poly \
		-e ${POLY_POKT} \
		-c 10 \
		-m eth_getTransactionReceipt \
		-e ${POLY_BLAST} \
		-c 10 \
		-r 1000 \
		--write-chunk-size 1024 \
		--batch-limit 100 \
		--first-block 40000000


router:
	@$(PY) -m etha.worker.fake_router


worker:
	@$(PY) -m etha.worker.server \
		--router http://localhost:5555 \
		--worker-id 1 \
		--worker-url http://localhost:8000 \
		--data-dir data/worker \
		--procs 2


.PHONY: init dbuild-ingest write ingest-eth ingest-poly router worker
