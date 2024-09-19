deps:
	pdm sync -G:all


image-ingest:
	docker buildx build --target ingest --platform linux/amd64 . --load


image-worker:
	docker buildx build --target worker --platform linux/amd64 . --load


ingest-eth:
	@python3 -m sqa.eth.ingest --dest data/mainnet \
		-e ${ETH_NODE} \
		-c 30 \
		-r 600 \
		--batch-limit 100 \
		--with-traces \
		--with-statediffs \
		--write-chunk-size 10 \
		--first-block 16000000


ingest-arb:
	@python3 -m sqa.eth.ingest --dest data/arb-one \
		-e ${ARB_NODE} \
		-c 20 \
		-r 400 \
		--batch-limit 100 \
		--with-receipts \
		--with-traces \
		--with-statediffs \
		--use-debug-api-for-statediffs \
		--write-chunk-size 10 \
		--first-block 900 \
		--arbitrum


ingest-poly:
	@python3 -m sqa.eth.ingest --dest data/poly \
		-e ${POLY_NODE} \
		-c 10 \
		-r 1000 \
		--write-chunk-size 1024 \
		--batch-limit 100 \
		--first-block 40000000


ingest-kusama:
	@python3 -m sqa.substrate.writer data/kusama \
		--src http://localhost:7373 \
		--first-block 18961964 \
		--chunk-size 256


ingest-tron:
	@python3 -m sqa.tron.writer data/tron \
		--src http://localhost:7373 \
		--first-block 50078149 \
		--chunk-size 256


ingest-starknet:
	@python3 -m sqa.starknet.writer data/starknet \
		-e ${STARKNET_NODE} \
		--first-block 600000 \
		--chunk-size 256


ingest-fuel:
	@python3 -m sqa.fuel.writer data/fuel \
		--src http://localhost:7373 \
		--first-block 9000000 \
		--last-block 9025549 \
		--chunk-size 512


router:
	@python3 tests/fake_router.py


worker:
	@python3 -m sqa.worker \
		--router http://127.0.0.1:5555 \
		--worker-id 1 \
		--worker-url http://localhost:8000 \
		--data-dir data/worker \
		--procs 2


test:
	@python3 -m tests.run


.PHONY: deps image-ingest image-worker ingest-eth ingest-arb ingest-poly ingest-kusama ingest-tron router worker test
