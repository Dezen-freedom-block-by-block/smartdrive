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
from substrateinterface import Keypair

from smartdrive.commune.request import get_truthful_validators, get_filtered_modules, ping_proposer_validator, ModuleInfo
from smartdrive.validator.api.middleware.sign import verify_block, verify_json_signature, sign_json
from smartdrive.validator.database.database import Database
from smartdrive.validator.models import ModuleType, Event, Chunk, File, Block
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

                # Process events
                block_events = []
                while not self._node.mempool_queue.empty() and len(block_events) < self.MAX_EVENTS_PER_BLOCK:
                    block_events.append(await self._node.get_all_mempool_items())
                await self.process_events(events=block_events)

                # Create and send block
                self._database.create_block(block_number)
                await self.send_block_to_validators(block_number)

            elapsed = time.time() - start_time
            if elapsed < self.BLOCK_INTERVAL:
                sleep_time = self.BLOCK_INTERVAL - elapsed
                print(f"Sleeping for {sleep_time} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)

    async def process_events(self, events: list[Event]):
        # TODO: check if returns ok and remove from mempool, otherwise keep events
        for e in events:
            if e.action == "store":
                chunks = []
                for miner_chunk in e.params.get("miner_chunk"):
                    chunks.append(Chunk(
                        miner_owner_ss58address=miner_chunk["ss58_address"],
                        chunk_uuid=miner_chunk["chunk_uuid"],
                        file_uuid=None,
                        sub_chunk=None,
                    ))
                file = File(
                    user_owner_ss58address=e.params.get("user_ss58_address"),
                    file_uuid=None,
                    chunks=chunks,
                    created_at=None,
                    expiration_ms=None
                )
                result = self._database.insert_file(file)
                print(f"PROCESS EVENT STORE: {result}")
            elif e.action == "remove":
                result = await self.api.remove_api.remove_endpoint(user_ss58_address=e.params.get("user_ss58_address"),
                                                                   file_uuid=e.params.get("file_uuid"))
                print(f"PROCESS EVENT REMOVE: {result}")
            elif e.action == "miner_response":
                self._database.insert_miner_response(e.params.get("user_ss58_address"), e.params.get("action"),
                                                     e.params.get("succeed"), e.params.get("final_time"))
                print(f"PROCESS EVENT MINER RESPONSE")

    async def handle_received_block(self, block: Block, leader_validator: ModuleInfo):
        # TODO: check handle received block
        processed_events = []
        if verify_block(block, leader_validator.ss58_address, block.signature):
            for event in block.events:
                if verify_json_signature(event.params, event.signature, event.params.get("user_ss58_address")):
                    processed_events.append(event)

            await self.process_events(processed_events)

    async def send_block_to_validators(self, block_number: int):
        connections = self._node._connection_pool.get_all_connections()

        for c in connections:
            body = {
                "code": MessageCode.MESSAGE_CODE_IDENTIFIER,
                "data": {"block_number": block_number}
            }
            body_sign = sign_json(body, self._keypair)
            message = {
                "body": body,
                "signature_hex": body_sign.hex(),
                "public_key_hex": self._keypair.public_key.hex()
            }

            send_json(c[ConnectionPool.CONNECTION], message)

    async def start_creation_block(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.create_blocks())
        await asyncio.Event().wait()
