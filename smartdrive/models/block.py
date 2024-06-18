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

from communex.types import Ss58Address
from pydantic import BaseModel

from smartdrive.models.event import Event, MessageEvent, parse_event


class Block(BaseModel):
    block_number: int
    events: list[Event]
    proposer_signature: Ss58Address


class BlockEvent(Block):
    events: list[MessageEvent]


def block_to_block_event(block: Block) -> BlockEvent:
    return BlockEvent(
        block_number=block.block_number,
        events=list(map(lambda event: MessageEvent(event_action=event.get_event_action(), event=event), block.events)),
        proposer_signature=block.proposer_signature
    )


def block_event_to_block(block_event: BlockEvent) -> Block:
    return Block(
        block_number=block_event.block_number,
        events=list(map(lambda event: parse_event(event.event), block_event.events)),
        proposer_signature=block_event.proposer_signature
    )
