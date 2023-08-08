import json

from benchmark.queries import with_range
from tools import WorkerUser


W2_RANGE = (9138200, 9293379)
W3_RANGE = (9708320, 9971779)


QUERY = json.loads('''
{
  "fromBlock": 10419700,
  "toBlock": 17869558,
  "includeAllBlocks": true,
  "transactions": [
    {}
  ],
  "logs": [
    {}
  ],
  "fields": {
    "log": {
      "address": true,
      "topics": true,
      "transactionHash": true,
      "data": true
    },
    "transaction": {
      "nonce": true,
      "to": true,
      "hash": true,
      "contractAddress": true,
      "gas": true,
      "cumulativeGasUsed": true,
      "status": true,
      "effectiveGasPrice": true,
      "value": true,
      "input": true,
      "gasPrice": true,
      "type": true,
      "from": true,
      "gasUsed": true,
      "maxFeePerGas": true,
      "maxPriorityFeePerGas": true
    },
    "stateDiff": {
      "kind": true,
      "next": true,
      "prev": true
    },
    "trace": {
      "error": true
    },
    "block": {
      "sha3Uncles": true,
      "difficulty": true,
      "baseFeePerGas": true,
      "receiptsRoot": true,
      "extraData": true,
      "miner": true,
      "timestamp": true,
      "totalDifficulty": true,
      "nonce": true,
      "logsBloom": true,
      "stateRoot": true,
      "mixHash": true,
      "size": true,
      "gasLimit": true,
      "transactionsRoot": true,
      "gasUsed": true
    }
  }
}
''')


FetchAll = WorkerUser(
    'FetchAll',
    dataset='s3://ethereum-mainnet',
    query=with_range(QUERY, *W3_RANGE)
)
