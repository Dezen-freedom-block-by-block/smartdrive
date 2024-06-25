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

import multiprocessing
from typing import List

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

from smartdrive.models.block import Block, block_to_block_event
from smartdrive.models.event import Event, MessageEvent
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.config import config_manager
from smartdrive.validator.node.connection_pool import ConnectionPool
from smartdrive.validator.node.server import Server
from smartdrive.validator.node.util.message_code import MessageCode
from smartdrive.validator.node.util.utils import send_json


class Node:

    _keypair: Keypair

    _server_process = None
    _event_pool = None
    _connection_pool = None

    def __init__(self):
        self._keypair = classic_load_key(config_manager.config.key)

        self._event_pool = multiprocessing.Manager().list()
        self._connection_pool = ConnectionPool(cache_size=Server.MAX_N_CONNECTIONS)

        # Although these variables are managed by multiprocessing.Manager(),
        # we explicitly pass them as parameters to make it clear that they are dependencies of the server process.
        self._server_process = multiprocessing.Process(target=self.run_server, args=(self._event_pool, self._connection_pool,))
        self._server_process.start()

    def run_server(self, event_pool, connection_pool: ConnectionPool):
        server = Server(
            event_pool=event_pool,
            connection_pool=connection_pool,
        )
        server.run()

    def get_connections(self):
        return self._connection_pool.get_all_connections()

    def consume_pool_events(self, count: int):
        items = []
        with multiprocessing.Lock():
            for _ in range(min(count, len(self._event_pool))):
                items.append(self._event_pool.pop(0))
        return items

    def insert_pool_event(self, event: Event):
        return self._event_pool.append(event)

    def insert_pool_events(self, events: List[Event]):
        return self._event_pool.extend(events)

    def send_event_to_validators(self, event: Event):
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
                send_json(c[ConnectionPool.CONNECTION], message)
