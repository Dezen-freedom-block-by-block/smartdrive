#  MIT License
#
#  Copyright (c) 2024 Dezen | freedom block by block
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#  The above copyright notice and this permission notice shall be included in all
#  copies or substantial portions of the Software.
#
#  THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
#  SOFTWARE.

import json
from enum import Enum
from typing import List, Optional

from communex.types import Ss58Address


class Action(Enum):
    STORE = 0
    REMOVE = 1
    RETRIEVE = 2
    VALIDATION = 3


class MinerProcess:
    def __init__(self, chunk_uuid: Optional[str], miner_ss58_address: Ss58Address, succeed: Optional[bool] = None, processing_time: Optional[float] = None):
        self.chunk_uuid = chunk_uuid
        self.miner_ss58_address = miner_ss58_address
        self.succeed = succeed
        self.processing_time = processing_time

    def __repr__(self):
        return f"MinerProcess(chunk_uuid={self.chunk_uuid}, miner_ss58_address={self.miner_ss58_address}, succeed={self.processing_time}, processing_time={self.processing_time})"


class EventParams:
    def __init__(self, file_uuid: Optional[str], miners_processes: List[MinerProcess]):
        self.file_uuid = file_uuid
        self.miners_processes = miners_processes

    def __repr__(self):
        return f"EventParams(file_uuid={self.file_uuid}, miners_processes={self.miners_processes})"


class Event:
    def __init__(self, params: EventParams, signed_params: str, validator_ss58_address: Ss58Address):
        self.params = params
        self.signed_params = signed_params
        self.validator_ss58_address = validator_ss58_address

    def __repr__(self):
        return f"Event(params={self.params}, signed_params={self.signed_params}, validator_ss58_address={self.validator_ss58_address})"


class StoreEvent(Event):
    def __init__(self, params: EventParams, signed_params: str, validator_ss58_address: Ss58Address):
        super().__init__(params, signed_params, validator_ss58_address)

    def __repr__(self):
        return f"StoreEvent(params={self.params}, signed_params={self.signed_params}, validator_ss58_address={self.validator_ss58_address})"


class RemoveEvent(Event):
    def __init__(self, params: EventParams, signed_params: str, validator_ss58_address: Ss58Address):
        super().__init__(params, signed_params, validator_ss58_address)

    def __repr__(self):
        return f"RemoveEvent(params={self.params}, signed_params={self.signed_params}, validator_ss58_address={self.validator_ss58_address})"


class RetrieveEvent(Event):
    def __init__(self, params: EventParams, signed_params: str, validator_ss58_address: Ss58Address):
        super().__init__(params, signed_params, validator_ss58_address)

    def __repr__(self):
        return f"RetrieveEvent(params={self.params}, signed_params={self.signed_params}, validator_ss58_address={self.validator_ss58_address})"


class ValidateEvent(Event):
    def __init__(self, params: EventParams, signed_params: str, validator_ss58_address: Ss58Address):
        super().__init__(params, signed_params, validator_ss58_address)

    def __repr__(self):
        return f"ValidationEvent(params={self.params}, signed_params={self.signed_params}, validator_ss58_address={self.validator_ss58_address})"


def parse_event(json_data: str) -> Event:
    data = json.loads(json_data)
    action = Action(data['action'])
    params = data['params']
    signed_params = data['signed_params']
    validator_ss58_address = data['validator_ss58_address']

    if action == Action.STORE:
        return StoreEvent(params, signed_params, validator_ss58_address)
    elif action == Action.REMOVE:
        return RemoveEvent(params, signed_params, validator_ss58_address)
    elif action == Action.RETRIEVE:
        return RetrieveEvent(params, signed_params, validator_ss58_address)
    elif action == Action.VALIDATION:
        return ValidateEvent(params, signed_params, validator_ss58_address)
    else:
        raise ValueError(f"Unknown action: {action}")


class Block:
    def __init__(self, block_number: int, events: list[Event], proposer_signature: Ss58Address):
        self.block_number = block_number
        self.events = events
        self.proposer_signature = proposer_signature

    def __repr__(self):
        return f"Block(block_number={self.block_number}, events={self.events.__repr__}, proposer_signature={self.proposer_signature})"
