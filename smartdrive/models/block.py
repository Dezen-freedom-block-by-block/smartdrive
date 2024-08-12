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

from communex.types import Ss58Address
from pydantic import BaseModel

from smartdrive.models.event import MessageEvent, parse_event, StoreEvent, RemoveEvent


class Block(BaseModel):
    block_number: int
    events: list[Union[StoreEvent, RemoveEvent]]
    proposer_signature: str
    proposer_ss58_address: Ss58Address


class BlockEvent(Block):
    events: list[MessageEvent]


def block_to_block_event(block: Block) -> BlockEvent:
    """
    Converts a Block object into a BlockEvent object.

    Params:
        block (Block): The Block object to be converted.

    Returns:
        BlockEvent: The converted BlockEvent object.
    """
    return BlockEvent(
        block_number=block.block_number,
        events=list(map(lambda event: MessageEvent.from_json(event.dict(), event.get_event_action()), block.events)),
        proposer_signature=block.proposer_signature,
        proposer_ss58_address=block.proposer_ss58_address
    )


def block_event_to_block(block_event: BlockEvent) -> Block:
    """
    Converts a BlockEvent object into a Block object.

    Params:
        block_event (BlockEvent): The BlockEvent object to be converted.

    Returns:
        Block: The converted Block object.
    """
    return Block(
        block_number=block_event.block_number,
        events=list(map(lambda message_event: parse_event(message_event), block_event.events)),
        proposer_signature=block_event.proposer_signature,
        proposer_ss58_address=block_event.proposer_ss58_address
    )
