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
from smartdrive.models.event import MessageEvent, StoreEvent, RemoveEvent, ValidationEvent
from smartdrive.sign import sign_data
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

    def insert_pool_event(self, event: Union[StoreEvent, RemoveEvent]):
        with self._event_pool_lock:
            self._event_pool.append(event)

    def insert_pool_events(self, events: List[Union[StoreEvent, RemoveEvent]]):
        with self._event_pool_lock:
            self._event_pool.extend(events)

    def _send_message_to_validators(self, code: MessageCode, data: dict):
        connections = self.get_connections()
        body = {
            "code": code.value,
            "data": data
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

    def send_validation_events_to_validators(self, connections, validations_events_per_validator: list[list[ValidationEvent]]):
        for index, c in enumerate(connections):
            data_list = [validations_events.dict() for validations_events in validations_events_per_validator[index]]

            body = {
                "code": MessageCode.MESSAGE_CODE_VALIDATION_EVENTS.value,
                "data": data_list
            }

            body_sign = sign_data(body, self._keypair)
            message = {
                "body": body,
                "signature_hex": body_sign.hex(),
                "public_key_hex": self._keypair.public_key.hex()
            }
            try:
                send_json(c, message)
            except Exception as e:
                print(f"Error send chunk event to validators {e}")

    def send_event_to_validators(self, event: Union[StoreEvent, RemoveEvent]):
        message_event = MessageEvent.from_json(event.dict(), event.get_event_action())
        self.insert_pool_event(event)
        self._send_message_to_validators(MessageCode.MESSAGE_CODE_EVENT, message_event.dict())

    async def send_block_to_validators(self, block: Block):
        block_event = block_to_block_event(block)
        self._send_message_to_validators(MessageCode.MESSAGE_CODE_BLOCK, block_event.dict())

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
