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

from typing import Union

from smartdrive.models.block import Block
from smartdrive.models.event import UserEvent, StoreEvent, RemoveEvent
from smartdrive.sign import verify_data_signature


def _verify_event_signatures(event: Union[StoreEvent, RemoveEvent]) -> bool:
    """
    Verifies the signatures of an individual event.

    Parameters:
        event Union[StoreEvent, RemoveEvent]: The specific Event object (StoreEvent, RemoveEvent).

    Returns:
        bool: True if both the input parameters and event parameters signatures are verified, False otherwise.
    """
    input_params_verified = True
    if isinstance(event, UserEvent):
        input_params_verified = verify_data_signature(event.input_params.dict(), event.input_signed_params, event.user_ss58_address)

    event_params_verified = verify_data_signature(event.event_params.dict(), event.event_signed_params, event.validator_ss58_address)

    return input_params_verified and event_params_verified


def are_all_block_events_valid(block: Block) -> bool:
    """
    Checks if all events in the block have valid signatures.

    Parameters:
        block (Block): The block containing events to be verified.

    Returns:
        bool: True if all events in the block have valid signatures, False otherwise.
    """
    for event in block.events:
        if not _verify_event_signatures(event):
            return False
    return True


def remove_invalid_block_events(block: Block):
    """
    Removes events with invalid signatures from the block.

    Parameters:
        block (Block): The block containing events to be verified and cleaned.
    """
    verified_events = [event for event in block.events if _verify_event_signatures(event)]
    block.events = verified_events
