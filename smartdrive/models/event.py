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
from typing import List, Optional, Dict, Any
from pydantic.dataclasses import dataclass
from pydantic import BaseModel

from communex.types import Ss58Address


class Action(Enum):
    STORE = 0
    REMOVE = 1
    RETRIEVE = 2
    VALIDATION = 3


class MinerProcess(BaseModel):
    chunk_uuid: Optional[str]
    miner_ss58_address: Ss58Address
    succeed: Optional[bool] = None
    processing_time: Optional[float] = None


class EventParams(BaseModel):
    file_uuid: Optional[str]
    miners_processes: List[MinerProcess]


class StoreParams(EventParams):
    file_uuid: Optional[str]
    miners_processes: List[MinerProcess]
    sub_chunk_start: int
    sub_chunk_end: int
    sub_chunk_encoded: str


class Event(BaseModel):
    uuid: str
    validator_ss58_address: Ss58Address
    event_params: EventParams
    event_signed_params: str


class UserEvent(Event):
    user_ss58_address: Ss58Address
    input_params: Dict[str, Any]
    input_signed_params: str


class StoreEvent(UserEvent):
    pass


class RemoveEvent(UserEvent):
    pass


class RetrieveEvent(UserEvent):
    pass


class ValidateEvent(Event):
    pass


def parse_event(action: Action, json_data: str) -> Event:
    data = json.loads(json_data)
    uuid = data['uuid']
    validator_ss58_address = Ss58Address(data['validator_ss58_address'])
    event_params = data['event_params']
    event_signed_params = data['event_signed_params']

    if action == Action.STORE:
        return StoreEvent(
            uuid=uuid,
            user_ss58_address=Ss58Address(data['user_ss58_address']),
            input_params=data['input_params'],
            input_signed_params=data['input_signed_params'],
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    elif action == Action.REMOVE:
        return RemoveEvent(
            uuid=uuid,
            user_ss58_address=Ss58Address(data['user_ss58_address']),
            input_params=data['input_params'],
            input_signed_params=data['input_signed_params'],
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    elif action == Action.RETRIEVE:
        return RetrieveEvent(
            uuid=uuid,
            user_ss58_address=Ss58Address(data['user_ss58_address']),
            input_params=data['input_params'],
            input_signed_params=data['input_signed_params'],
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    elif action == Action.VALIDATION:
        return ValidateEvent(
            uuid=uuid,
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    else:
        raise ValueError(f"Unknown action: {action}")
