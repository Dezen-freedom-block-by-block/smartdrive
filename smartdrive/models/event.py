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
from typing import List, Optional, Union, Dict
from pydantic import BaseModel

from communex.types import Ss58Address


class Action(Enum):
    STORE = 0
    REMOVE = 1
    RETRIEVE = 2
    STORE_REQUEST = 3


class InputParams(BaseModel):
    pass


class RemoveInputParams(InputParams):
    file_uuid: str


class RetrieveInputParams(InputParams):
    file_uuid: str


class StoreRequestInputParams(InputParams):
    file_hash: str
    file_size_bytes: int
    chunks: List[Dict[str, Union[int, str]]]


class ChunkParams(BaseModel):
    uuid: Optional[str]
    miner_ss58_address: Optional[str]
    chunk_index: Optional[int] = None
    miner_connection: Optional[dict] = None
    chunk_hash: Optional[str] = None
    chunk_size: Optional[int] = None


class ValidationEvent(BaseModel):
    uuid: str
    miner_ss58_address: str
    sub_chunk_start: int
    sub_chunk_end: int
    sub_chunk_encoded: str
    expiration_ms: Optional[int] = None
    created_at: Optional[int] = None
    file_uuid: str
    user_owner_ss58_address: str


class EventParams(BaseModel):
    file_uuid: str


class StoreParams(EventParams):
    chunks_params: List[ChunkParams]
    file_hash: str
    file_size: int
    file_uuid: str


class StoreRequestParams(EventParams):
    expiration_at: int
    approved: Optional[bool] = None


class Event(BaseModel):
    uuid: str
    validator_ss58_address: Ss58Address
    event_params: EventParams
    event_signed_params: str

    def get_event_action(self) -> Action:
        """
        Determine the type of event and return the corresponding Action enum value.

        Returns:
            Action: The corresponding Action enum value for the event.

        Raises:
            ValueError: If the event type is unknown.
        """
        if isinstance(self, StoreEvent):
            return Action.STORE
        elif isinstance(self, RemoveEvent):
            return Action.REMOVE
        elif isinstance(self, StoreRequestEvent):
            return Action.STORE_REQUEST
        else:
            raise ValueError("Unknown event type")


class UserEvent(Event):
    user_ss58_address: Ss58Address
    input_params: InputParams
    input_signed_params: str


class StoreEvent(Event):
    user_ss58_address: Ss58Address
    event_params: StoreParams


class RemoveEvent(UserEvent):
    event_params: EventParams
    input_params: RemoveInputParams


class StoreRequestEvent(UserEvent):
    event_params: StoreRequestParams
    input_params: StoreRequestInputParams


class MessageEvent(BaseModel):
    event_action: Action
    event: Union[StoreEvent, RemoveEvent, StoreRequestEvent]

    class Config:
        use_enum_values = True

    @classmethod
    def from_json(cls, data: dict, event_action: Action):
        if event_action == Action.STORE:
            event = StoreEvent(**data)
        elif event_action == Action.REMOVE:
            event = RemoveEvent(**data)
        elif event_action == Action.STORE_REQUEST:
            event = StoreRequestEvent(**data)
        else:
            raise ValueError(f"Unknown action: {event_action}")

        return cls(event_action=event_action, event=event)


def parse_event(message_event: MessageEvent) -> Union[StoreEvent, RemoveEvent, StoreRequestEvent]:
    """
    Parses a MessageEvent object into a specific Event object based on the event action.

    Params:
        message_event (MessageEvent): The MessageEvent object to be parsed.

    Returns:
        Union[StoreEvent, RemoveEvent, StoreRequestEvent]: The specific Event object (StoreEvent, RemoveEvent, StoreRequestEvent).

    Raises:
        ValueError: If the event action is unknown.
    """
    uuid = message_event.event.uuid
    validator_ss58_address = Ss58Address(message_event.event.validator_ss58_address)
    event_params = message_event.event.event_params
    event_signed_params = message_event.event.event_signed_params

    common_params = {
        "uuid": uuid,
        "validator_ss58_address": validator_ss58_address,
        "event_params": event_params,
        "event_signed_params": event_signed_params,
    }

    if message_event.event_action == Action.STORE.value:
        return StoreEvent(
            **common_params,
            user_ss58_address=Ss58Address(message_event.event.user_ss58_address),
        )
    elif message_event.event_action == Action.REMOVE.value:
        return RemoveEvent(
            **common_params,
            user_ss58_address=Ss58Address(message_event.event.user_ss58_address),
            input_params=RemoveInputParams(file_uuid=message_event.event.input_params.file_uuid),
            input_signed_params=message_event.event.input_signed_params
        )
    elif message_event.event_action == Action.STORE_REQUEST.value:
        return StoreRequestEvent(
            **common_params,
            user_ss58_address=Ss58Address(message_event.event.user_ss58_address),
            input_params=StoreRequestInputParams(file_hash=message_event.event.input_params.file_hash, file_size_bytes=message_event.event.input_params.file_size_bytes, chunks=message_event.event.input_params.chunks),
            input_signed_params=message_event.event.input_signed_params
        )
    else:
        raise ValueError(f"Unknown action: {message_event.event_action}")
