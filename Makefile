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
	@$(PY) -m etha.ingest --dest data/mainnet \
		-e ${ETH_ALCHEMY} \
		-c 20 \
		-r 400 \
		-e ${ETH_BLAST} \
		-c 20 \
		-r 400 \
		--batch-limit 100 \
		--with-receipts \
		--with-traces \
		--with-statediffs \
		--write-chunk-size 10 \
		--first-block 15000000


ingest-poly:
	@$(PY) -m etha.ingest --dest data/poly \
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
	@$(PY) -m etha.worker \
		--router http://localhost:5555 \
		--worker-id 1 \
		--worker-url http://localhost:8000 \
		--data-dir data/worker \
		--procs 2


.PHONY: init build-ingest write ingest-eth ingest-poly router worker
