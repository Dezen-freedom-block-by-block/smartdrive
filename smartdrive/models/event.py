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

from enum import Enum
from typing import List, Optional, Union
from pydantic import BaseModel, root_validator, model_validator

from communex.types import Ss58Address


class Action(Enum):
    STORE = 0
    REMOVE = 1
    RETRIEVE = 2
    VALIDATION = 3


class InputParams(BaseModel):
    pass


class RemoveInputParams(InputParams):
    file_uuid: str


class RetrieveInputParams(InputParams):
    file_uuid: str


class StoreInputParams(InputParams):
    file: str


class MinerProcess(BaseModel):
    chunk_uuid: Optional[str]
    miner_ss58_address: Ss58Address
    succeed: Optional[bool] = None
    processing_time: Optional[float] = None


class EventParams(BaseModel):
    file_uuid: str
    miners_processes: Optional[List[MinerProcess]]


class StoreParams(EventParams):
    file_uuid: str
    miners_processes: List[MinerProcess]
    sub_chunk_start: int
    sub_chunk_end: int
    sub_chunk_encoded: str


class RemoveParams(EventParams):
    miners_processes: Optional[List[MinerProcess]] = None


class Event(BaseModel):
    uuid: str
    validator_ss58_address: Ss58Address
    event_params: EventParams
    event_signed_params: str

    def get_event_action(self) -> Action:
        if isinstance(self, StoreEvent):
            return Action.STORE
        elif isinstance(self, RemoveEvent):
            return Action.REMOVE
        elif isinstance(self, RetrieveEvent):
            return Action.RETRIEVE
        elif isinstance(self, ValidateEvent):
            return Action.VALIDATION
        else:
            raise ValueError("Unknown event type")


class UserEvent(Event):
    user_ss58_address: Ss58Address
    input_params: InputParams
    input_signed_params: str


class StoreEvent(UserEvent):
    event_params: StoreParams
    input_params: StoreInputParams


class RemoveEvent(UserEvent):
    event_params: RemoveParams
    input_params: RemoveInputParams


class RetrieveEvent(UserEvent):
    input_params: RetrieveInputParams


class ValidateEvent(Event):
    pass


class MessageEvent(BaseModel):
    event_action: Action
    event: Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]

    class Config:
        use_enum_values = True


def parse_event(message_event: MessageEvent) -> Event:
    uuid = message_event.event.uuid
    user_ss58_address = Ss58Address(message_event.event.user_ss58_address)
    validator_ss58_address = Ss58Address(message_event.event.validator_ss58_address)
    event_params = message_event.event.event_params
    event_signed_params = message_event.event.event_signed_params
    input_params = message_event.event.input_params
    input_signed_params = message_event.event.input_signed_params

    print(f"EVENT ACTIONNNN ES -> {message_event.event_action}")

    if message_event.event_action == Action.STORE.value:
        return StoreEvent(
            uuid=uuid,
            user_ss58_address=user_ss58_address,
            input_params=input_params,
            input_signed_params=input_signed_params,
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    elif message_event.event_action == Action.REMOVE.value:
        return RemoveEvent(
            uuid=uuid,
            user_ss58_address=user_ss58_address,
            input_params=input_params,
            input_signed_params=input_signed_params,
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    elif message_event.event_action == Action.RETRIEVE.value:
        return RetrieveEvent(
            uuid=uuid,
            user_ss58_address=user_ss58_address,
            input_params=input_params,
            input_signed_params=input_signed_params,
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    elif message_event.event_action == Action.VALIDATION.value:
        return ValidateEvent(
            uuid=uuid,
            validator_ss58_address=validator_ss58_address,
            event_params=event_params,
            event_signed_params=event_signed_params
        )
    else:
        raise ValueError(f"Unknown action: {message_event.event_action}")
