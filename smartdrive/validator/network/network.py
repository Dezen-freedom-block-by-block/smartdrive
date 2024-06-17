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

import asyncio
import time

from communex.client import CommuneClient
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.request import get_truthful_validators, get_filtered_modules, ping_proposer_validator
from smartdrive.models.event import Event
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.utils import process_events
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.block import Block
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.node import Node
from smartdrive.validator.network.node.util.message_code import MessageCode
from smartdrive.validator.network.node.utils import send_json


class Network:
    MAX_EVENTS_PER_BLOCK = 25
    BLOCK_INTERVAL = 12

    _node: Node = None
    _keypair: Keypair = None
    _comx_client: CommuneClient = None
    _netuid: int = None
    _database: Database = None

    def __init__(self, keypair: Keypair, ip: str, netuid: int, comx_client: CommuneClient, database: Database):
        self._keypair = keypair
        self._comx_client = comx_client
        self._database = database
        self._node = Node(keypair=keypair, ip=ip, netuid=netuid)
        asyncio.run(self.start_creation_block())

    async def create_blocks(self):
        # TODO: retrieve last block from other leader validator

        block_number = self._database.get_database_block()
        block_number = -1 if block_number is None else block_number

        while True:
            start_time = time.time()

            truthful_validators = await get_truthful_validators(self._keypair, self._comx_client, self._netuid)
            all_validators = get_filtered_modules(self._comx_client, self._netuid, ModuleType.VALIDATOR)

            proposer_active_validator = max(truthful_validators, key=lambda v: v.stake or 0)
            proposer_validator = max(all_validators, key=lambda v: v.stake or 0)

            if proposer_validator.ss58_address != proposer_active_validator.ss58_address:
                ping_validator = await ping_proposer_validator(self._keypair, proposer_validator)
                if not ping_validator:
                    proposer_validator = proposer_active_validator

            if proposer_validator.ss58_address == self._keypair.ss58_address:
                block_number += 1

                # Create and process block
                block_events = self._node.get_mempool_items(max_items=self.MAX_EVENTS_PER_BLOCK)
                self._database.create_block(Block(block_number=block_number, events=block_events,
                                                  proposer_signature=Ss58Address(self._keypair.ss58_address)))
                await process_events(events=block_events, is_proposer_validator=True)

                # Propagate block to other validators
                block = Block(block_number=block_number, events=block_events,
                              proposer_signature=Ss58Address(self._keypair.ss58_address))
                await asyncio.to_thread(self.send_block_to_validators(block=block))

            elapsed = time.time() - start_time
            if elapsed < self.BLOCK_INTERVAL:
                sleep_time = self.BLOCK_INTERVAL - elapsed
                print(f"Sleeping for {sleep_time} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)


    async def send_block_to_validators(self, block: Block):
        connections = self._node.get_all_connections()
        body = {
            "code": MessageCode.MESSAGE_CODE_BLOCK,
            "data": block.__dict__
        }
        body_sign = sign_data(body, self._keypair)
        message = {
            "body": body,
            "signature_hex": body_sign,
            "public_key_hex": self._keypair.public_key.hex()
        }

        for c in connections:
            send_json(c[ConnectionPool.CONNECTION], message)

    async def start_creation_block(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.create_blocks())
        await asyncio.Event().wait()

    def emit_event(self, event: Event):
        identifiers_connections = self._node.get_identifiers_connections()
        # TODO: Emit event
