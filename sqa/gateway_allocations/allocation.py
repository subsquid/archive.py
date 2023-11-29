from dataclasses import dataclass


@dataclass
class Allocation:
    def __init__(self, gateway: str, computation_units: int, block_number: int):
        self.gateway = gateway
        self.computation_units = computation_units
        self.block_number = block_number

    def __repr__(self):
        return f'Allocation("gateway": {self.gateway}, "computation_units": {self.computation_units}, "block_number": {self.block_number})'
