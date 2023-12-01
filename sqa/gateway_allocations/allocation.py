from dataclasses import dataclass


@dataclass
class Allocation:
    gateway: str
    computation_units: int
    block_number: int
