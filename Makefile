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


ingest-starknet:
	@python3 -m sqa.starknet.writer data/starknet \
		-e ${STARKNET_NODE} \
		--first-block 600000 \
		--chunk-size 256


ingest-fuel:
	@python3 -m sqa.fuel.writer data/fuel \
		--src http://localhost:7373 \
		--first-block 1000050 \
		--chunk-size 512


ingest-solana:
	@python3 -m sqa.solana.writer data/solana \
		--src http://localhost:3000 \
		--first-block 245012900 \
		--last-block 245013399 \
		--chunk-size 8192


ingest-fantom:
	@python3 -m sqa.eth.ingest --dest data/fantom-mainnet \
		--raw \
		--endpoint https://fantom-testnet.blastapi.io/1a1c6dd2-8039-4c61-bb73-8f392295e154 \
		--first-block 25842238 \
		--with-receipts \
		--validate-tx-root \
		--validate-tx-type \
		--validate-logs-bloom \
		--write-chunk-size 386


ingest-optimism:
	@python3 -m sqa.eth.ingest --dest data/optimism \
		--raw \
		--endpoint https://optimism-mainnet.blastapi.io/a9b69b53-4921-4137-8abb-37425c1e8968 \
		--first-block 37762360 \
		--last-block 42042599 \
		--with-receipts \
		--with-traces \
		--write-chunk-size 2048


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


.PHONY: deps image-ingest image-worker ingest-eth ingest-arb ingest-poly ingest-kusama router worker test
