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
import traceback
from time import sleep
from typing import Union

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

from smartdrive.models.block import Block, block_to_block_event
from smartdrive.models.event import MessageEvent, StoreEvent, RemoveEvent
from smartdrive.sign import sign_data
from smartdrive.validator.config import config_manager
from smartdrive.validator.node.connection_pool import ConnectionPool, Connection
from smartdrive.validator.node.server import Server
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.node.util.utils import send_json


class Node:
    _keypair: Keypair
    _server_process = None
    _ping_process = None
    _event_pool = None
    _connection_pool = None
    _event_pool_lock = None
    initial_sync_completed = None

    def __init__(self):
        self._keypair = classic_load_key(config_manager.config.key)

        manager = multiprocessing.Manager()
        self._event_pool = manager.list()
        self._event_pool_lock = manager.Lock()
        self.initial_sync_completed = multiprocessing.Value('b', False)
        self._connection_pool = ConnectionPool(cache_size=Server.MAX_N_CONNECTIONS)

        # TODO: Currently this process is not removed from memory when main app is finished. It also prevents the main process from being freed from memory.
        # Although these variables are managed by multiprocessing.Manager(),
        # we explicitly pass them as parameters to make it clear that they are dependencies of the server process.
        self._server_process = multiprocessing.Process(target=self._run_server, args=(self._event_pool, self._event_pool_lock, self.initial_sync_completed, self._connection_pool,))
        self._server_process.start()

        self._ping_process = multiprocessing.Process(target=self.periodically_ping_nodes, daemon=True)
        self._ping_process.start()

    def _run_server(self, event_pool, event_pool_lock, initial_sync_completed, connection_pool: ConnectionPool):
        server = Server(
            event_pool=event_pool,
            event_pool_lock=event_pool_lock,
            connection_pool=connection_pool,
            initial_sync_completed=initial_sync_completed
        )
        server.run()

    def get_connections(self):
        return self._connection_pool.get_all()

    def get_connected_modules(self):
        return [connection.module for connection in self._connection_pool.get_all()]

    def add_event(self, event: Union[StoreEvent, RemoveEvent]):
        with self._event_pool_lock:
            self._event_pool.append(event)

        message_event = MessageEvent.from_json(event.dict(), event.get_event_action())

        connections = self.get_connections()
        for index, connection in enumerate(connections):

            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_EVENT,
                data=message_event.dict()
            )

            body_sign = sign_data(body.dict(), self._keypair)

            message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=self._keypair.public_key.hex()
            )
            self.send_message(connection, message)

    def consume_events(self, count: int):
        items = []
        with self._event_pool_lock:
            for _ in range(min(count, len(self._event_pool))):
                items.append(self._event_pool.pop(0))
        return items

    def send_message(self, connection: Connection, message: Message):
        try:
            send_json(connection.socket, message.dict())
        except Exception as e:
            traceback.print_exc()
            print(e)

    def send_block(self, block: Block):
        block_event = block_to_block_event(block)

        body = MessageBody(
            code=MessageCode.MESSAGE_CODE_BLOCK,
            data=block_event.dict()
        )

        body_sign = sign_data(body.dict(), self._keypair)

        message = Message(
            body=body,
            signature_hex=body_sign.hex(),
            public_key_hex=self._keypair.public_key.hex()
        )

        connections = self.get_connections()
        for c in connections:
            self.send_message(c, message)

    def periodically_ping_nodes(self):
        while True:
            connections = self.get_connections()

            for c in connections:
                try:
                    body = MessageBody(
                        code=MessageCode.MESSAGE_CODE_PING
                    )

                    body_sign = sign_data(body.dict(), self._keypair)

                    message = Message(
                        body=body,
                        signature_hex=body_sign.hex(),
                        public_key_hex=self._keypair.public_key.hex()
                    )

                    self.send_message(c, message)
                except Exception as e:
                    traceback.print_exc()
                    print(f"Error pinging validator: {e}")

            inactive_connections = self._connection_pool.remove_and_return_inactive_sockets()
            for inactive_connection in inactive_connections:
                inactive_connection.close()

            sleep(5)
