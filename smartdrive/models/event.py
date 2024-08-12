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
from pydantic import BaseModel

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


class ChunkEvent(BaseModel):
    uuid: Optional[str]
    miner_ss58_address: Optional[str]
    chunk_index: Optional[int] = None
    sub_chunk_start: Optional[int] = None
    sub_chunk_end: Optional[int] = None
    sub_chunk_encoded: Optional[str] = None
    expiration_ms: Optional[int] = None
    created_at: Optional[int] = None
    file_uuid: Optional[str] = None
    user_owner_ss58_address: Optional[str] = None


class EventParams(BaseModel):
    file_uuid: str


class StoreParams(EventParams):
    file_uuid: str
    created_at: Optional[int]
    expiration_ms: Optional[int]
    chunks: List[ChunkEvent]


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
    event_params: EventParams
    input_params: RemoveInputParams


class MessageEvent(BaseModel):
    event_action: Action
    event: Union[StoreEvent, RemoveEvent]

    class Config:
        use_enum_values = True

    @classmethod
    def from_json(cls, data: dict, event_action: Action):
        if event_action == Action.STORE:
            event = StoreEvent(**data)
        elif event_action == Action.REMOVE:
            event = RemoveEvent(**data)
        else:
            raise ValueError(f"Unknown action: {event_action}")

        return cls(event_action=event_action, event=event)


def parse_event(message_event: MessageEvent) -> Union[StoreEvent, RemoveEvent]:
    """
    Parses a MessageEvent object into a specific Event object based on the event action.

    Params:
        message_event (MessageEvent): The MessageEvent object to be parsed.

    Returns:
        Union[StoreEvent, RemoveEvent]: The specific Event object (StoreEvent, RemoveEvent).

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
        "event_signed_params": event_signed_params
    }

    if message_event.event_action == Action.STORE.value:
        return StoreEvent(
            **common_params,
            user_ss58_address=Ss58Address(message_event.event.user_ss58_address),
            input_params=StoreInputParams(file=message_event.event.input_params.file),
            input_signed_params=message_event.event.input_signed_params
        )
    elif message_event.event_action == Action.REMOVE.value:
        return RemoveEvent(
            **common_params,
            user_ss58_address=Ss58Address(message_event.event.user_ss58_address),
            input_params=RemoveInputParams(file_uuid=message_event.event.input_params.file_uuid),
            input_signed_params=message_event.event.input_signed_params
        )
    else:
        raise ValueError(f"Unknown action: {message_event.event_action}")
