import hashlib
from datetime import datetime
from typing import Optional

from sqa.worker.p2p import messages_pb2 as msg_pb
from sqa.worker.state.controller import State
from sqa.worker.state.intervals import to_range_set


class QueryInfo:
    def __init__(self, client_id: str):
        self.client_id = client_id
        self.start_time = datetime.now()
        self.end_time: 'Optional[datetime]' = None

    def finished(self) -> None:
        self.end_time = datetime.now()

    @property
    def exec_time_ms(self) -> int:
        return int((self.end_time - self.start_time).total_seconds() * 1000)


def state_to_proto(state: State) -> msg_pb.WorkerState:
    return msg_pb.WorkerState(datasets={
        ds: msg_pb.RangeSet(ranges=[
            msg_pb.Range(begin=begin, end=end)
            for begin, end in range_set
        ])
        for ds, range_set in state.items()
    })


def state_from_proto(state: msg_pb.WorkerState) -> State:
    return {
        ds: to_range_set((r.begin, r.end) for r in range_set.ranges)
        for ds, range_set in state.datasets.items()
    }


def sha3_256(data: bytes) -> bytes:
    hasher = hashlib.sha3_256()
    hasher.update(data)
    return hasher.digest()


def size_and_hash(data: bytes) -> msg_pb.SizeAndHash:
    return msg_pb.SizeAndHash(
        size=len(data),
        sha3_256=sha3_256(data),
    )