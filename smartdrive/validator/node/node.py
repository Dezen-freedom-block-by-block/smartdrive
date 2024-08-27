# MIT License
#
# Copyright (c) 2024 Dezen | freedom block by block
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
# of this software and associated documentation files (the "Software"), to deal
# in the Software without restriction, including without limitation the rights
# to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
# copies of the Software, and to permit persons to whom the Software is
# furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in all
# copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
# IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
# FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
# AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
# LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
# OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
# SOFTWARE.

import asyncio
import multiprocessing
from typing import List, Union

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

from smartdrive.models.block import Block, block_to_block_event
from smartdrive.models.event import MessageEvent, StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.config import config_manager
from smartdrive.validator.node.connection_pool import ConnectionPool
from smartdrive.validator.node.server import Server
from smartdrive.validator.node.active_validator_manager import ActiveValidatorsManager
from smartdrive.validator.node.util.message_code import MessageCode
from smartdrive.validator.node.util.utils import send_json


class Node:
    _keypair: Keypair
    _server_process = None
    _event_pool = None
    _connection_pool = None
    _event_pool_lock = None
    _active_validator_manager = None
    initial_sync_completed = None

    def __init__(self):
        self._keypair = classic_load_key(config_manager.config.key)

        manager = multiprocessing.Manager()
        self._event_pool = manager.list()
        self._event_pool_lock = manager.Lock()
        self._active_validators_manager = ActiveValidatorsManager()
        self.initial_sync_completed = multiprocessing.Value('b', False)
        self._connection_pool = ConnectionPool(cache_size=Server.MAX_N_CONNECTIONS)

        # Although these variables are managed by multiprocessing.Manager(),
        # we explicitly pass them as parameters to make it clear that they are dependencies of the server process.
        self._server_process = multiprocessing.Process(target=self.run_server, args=(self._event_pool, self._event_pool_lock, self._active_validators_manager, self.initial_sync_completed, self._connection_pool,))
        self._server_process.start()

    def run_server(self, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed, connection_pool: ConnectionPool):
        server = Server(
            event_pool=event_pool,
            event_pool_lock=event_pool_lock,
            connection_pool=connection_pool,
            active_validators_manager=active_validators_manager,
            initial_sync_completed=initial_sync_completed
        )
        server.run()

    def get_active_validators(self):
        return self._active_validators_manager.get_active_validators()

    def get_active_validators_connections(self):
        return self._active_validators_manager.get_active_validators_connections()

    def get_connections(self):
        return self._connection_pool.get_all_connections()

    def consume_pool_events(self, count: int):
        items = []
        with self._event_pool_lock:
            for _ in range(min(count, len(self._event_pool))):
                items.append(self._event_pool.pop(0))
        return items

    def insert_pool_event(self, event: Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]):
        with self._event_pool_lock:
            self._event_pool.append(event)

    def insert_pool_events(self, events: List[Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]]):
        with self._event_pool_lock:
            self._event_pool.extend(events)

    def send_event_to_validators(self, event: Union[StoreEvent, RemoveEvent, RetrieveEvent, ValidateEvent]):
        connections = self.get_connections()

        message_event = MessageEvent.from_json(event.dict(), event.get_event_action())
        body = {
            "code": MessageCode.MESSAGE_CODE_EVENT.value,
            "data": message_event.dict()
        }

        body_sign = sign_data(body, self._keypair)
        message = {
            "body": body,
            "signature_hex": body_sign.hex(),
            "public_key_hex": self._keypair.public_key.hex()
        }

        self.insert_pool_event(event)

        for c in connections:
            try:
                send_json(c[ConnectionPool.CONNECTION], message)
            except Exception as e:
                print(e)

    async def send_block_to_validators(self, block: Block):
        connections = self.get_connections()
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

            for c in connections:
                try:
                    send_json(c[ConnectionPool.CONNECTION], message)
                except Exception as e:
                    print(e)

    async def ping_validators(self):
        connections = self.get_connections()

        async def _ping_validator(c):
            try:
                body = {"code": MessageCode.MESSAGE_CODE_PING.value}
                body_sign = sign_data(body, self._keypair)
                message = {
                    "body": body,
                    "signature_hex": body_sign.hex(),
                    "public_key_hex": self._keypair.public_key.hex()
                }
                send_json(c[ConnectionPool.CONNECTION], message)
            except Exception as e:
                print(f"Error pinging validator: {e}")

        tasks = [_ping_validator(c) for c in connections]
        await asyncio.gather(*tasks)

        inactive_validators = self._active_validators_manager.remove_inactive_validators()
        for identifier in inactive_validators:
            removed_connection = self._connection_pool.remove_connection(identifier)
            if removed_connection:
                removed_connection.close()
