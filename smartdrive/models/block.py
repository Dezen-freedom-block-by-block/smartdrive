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

import hashlib
from typing import Union, List
from pydantic import BaseModel

from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.models.event import MessageEvent, parse_event, StoreEvent, RemoveEvent, StoreRequestEvent
from smartdrive.sign import sign_data

# TODO: REPLACE THIS WITH bytes
MAX_EVENTS_PER_BLOCK = 100


class Block(BaseModel):
    block_number: int
    events: List[Union[StoreEvent, RemoveEvent, StoreRequestEvent]]
    signed_block: str = None
    proposer_ss58_address: Ss58Address
    previous_hash: str = ""
    hash: str = None

    def __init__(self, keypair: Keypair = None, **data):
        super().__init__(**data)
        if not self.signed_block and keypair:
            self.signed_block = sign_data({
                "block_number": self.block_number,
                "events": [event.dict() for event in self.events]
            }, keypair).hex()

        if not self.hash:
            self.hash = self.create_hash()

    @classmethod
    def from_json(cls, json_data):
        return cls(
            block_number=json_data["block_number"],
            events=json_data["events"],
            signed_block=json_data["signed_block"],
            proposer_ss58_address=json_data["proposer_ss58_address"],
            previous_hash=json_data["previous_hash"],
            hash=json_data["hash"]
        )

    def create_hash(self):
        block_contents = f"{self.block_number}{[event.dict() for event in self.events]}{self.signed_block}{self.proposer_ss58_address}{self.previous_hash}"
        return hashlib.sha256(block_contents.encode()).hexdigest()


class BlockEvent(Block):
    events: List[MessageEvent]


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
        signed_block=block.signed_block,
        proposer_ss58_address=block.proposer_ss58_address,
        hash=block.hash,
        previous_hash=block.previous_hash
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
        signed_block=block_event.signed_block,
        proposer_ss58_address=block_event.proposer_ss58_address,
        hash=block_event.hash,
        previous_hash=block_event.previous_hash
    )
