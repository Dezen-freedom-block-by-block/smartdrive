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
import multiprocessing
import time

from communex.client import CommuneClient
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.request import get_filtered_modules, ping_proposer_validator, get_truthful_validators
from smartdrive.models.event import Event
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.utils import process_events
from smartdrive.validator.database.database import Database
from smartdrive.models.block import Block, block_to_block_event
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.node import Node
from smartdrive.models.event import MessageEvent
from smartdrive.validator.network.node.util.message_code import MessageCode
from smartdrive.validator.network.utils import send_json


class Network:
    MAX_EVENTS_PER_BLOCK = 25
    BLOCK_INTERVAL = 12

    _node: Node = None
    _keypair: Keypair = None
    _comx_client: CommuneClient = None
    _netuid: int = None
    _database: Database = None

    def __init__(self, keypair: Keypair, ip: str, netuid: int, comx_client: CommuneClient, database: Database, testnet: bool):
        self._keypair = keypair
        self._netuid = netuid
        self._comx_client = comx_client
        self._database = database
        multiprocessing.set_start_method("fork")
        self._node = Node(keypair=keypair, ip=ip, netuid=netuid, database=database, testnet=testnet)

    async def create_blocks(self):
        # TODO: retrieve last block from other leader validator

        block_number = self._database.get_database_block()
        block_number = -1 if block_number is None else block_number

        while True:
            start_time = time.time()

            truthful_validators = await get_truthful_validators(self._keypair, self._comx_client, self._netuid)
            all_validators = get_filtered_modules(self._comx_client, self._netuid, ModuleType.VALIDATOR)

            proposer_active_validator = max(truthful_validators if truthful_validators else all_validators, key=lambda v: v.stake or 0)
            proposer_validator = max(all_validators, key=lambda v: v.stake or 0)

            if proposer_validator.ss58_address != proposer_active_validator.ss58_address:
                ping_validator = await ping_proposer_validator(self._keypair, proposer_validator)
                if not ping_validator:
                    proposer_validator = proposer_active_validator

            if proposer_validator.ss58_address == self._keypair.ss58_address:
                block_number += 1

                # Create and process block
                block_events = self._node.consume_mempool_items(count=self.MAX_EVENTS_PER_BLOCK)
                print(f"BLOCK EVENTS -> {block_events} ")
                block = Block(block_number=block_number, events=block_events, proposer_signature=Ss58Address(self._keypair.ss58_address))
                print(f"Creating block - {block}")
                await process_events(events=block_events, is_proposer_validator=True, keypair=self._keypair, comx_client=self._comx_client, netuid=self._netuid, database=self._database)
                self._database.create_block(block=block)

                # Propagate block to other validators
                asyncio.create_task(self.send_block_to_validators(block=block))

            elapsed = time.time() - start_time
            if elapsed < self.BLOCK_INTERVAL:
                sleep_time = self.BLOCK_INTERVAL - elapsed
                print(f"Sleeping for {sleep_time} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)

    async def send_block_to_validators(self, block: Block):
        connections = self._node.get_all_connections()
        if connections:
            block_event = block_to_block_event(block)

            body = {
                "code": MessageCode.MESSAGE_CODE_BLOCK.value,
                "data": block_event.dict()
            }

            body_sign = sign_data(body, self._keypair)
            message = {
                "body": body,
                "signature_hex": body_sign.hex(),
                "public_key_hex": self._keypair.public_key.hex()
            }

            print(f"Emit block to validators: {message}")

            for c in connections:
                send_json(c[ConnectionPool.CONNECTION], message)

    def emit_event(self, event: Event):
        connections = self._node.get_all_connections()
        print("START EMIT EVENT")
        print(event)

        print("--------------")
        print("--------------")
        print(event.dict())
        print("++++++++++++++")
        print("++++++++++++++")
        body = {
            "code": MessageCode.MESSAGE_CODE_EVENT.value,
            "data": MessageEvent(
                event_action=event.get_event_action(),
                event=event.dict()
            ).dict()
        }

        print(body)
        body_sign = sign_data(body, self._keypair)
        message = {
            "body": body,
            "signature_hex": body_sign.hex(),
            "public_key_hex": self._keypair.public_key.hex()
        }

        print("END EMIT EVENT")
        print(message)

        self._node.insert_event(event)

        for c in connections:
            try:
                send_json(c[ConnectionPool.CONNECTION], message)
            except Exception as e:
                print(e)