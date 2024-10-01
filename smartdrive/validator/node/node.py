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

import multiprocessing
from multiprocessing import Value
from typing import Union, List

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

from smartdrive.commune.models import ModuleInfo
from smartdrive.models.event import MessageEvent, StoreEvent, RemoveEvent
from smartdrive.sign import sign_data
from smartdrive.validator.config import config_manager
from smartdrive.validator.node.connection.connection_pool import ConnectionPool, Connection
from smartdrive.validator.node.connection.connection_manager import ConnectionManager
from smartdrive.validator.node.event.event_pool import EventPool
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.node.connection.utils.utils import send_message


class Node:
    _keypair: Keypair
    _event_pool: EventPool = None
    _connection_pool: ConnectionPool = None
    initial_sync_completed: Value = None

    def __init__(self):
        self._keypair = classic_load_key(config_manager.config.key)

        manager = multiprocessing.Manager()
        self._event_pool = EventPool(manager)
        self._connection_pool = ConnectionPool(manager=manager, cache_size=ConnectionManager.MAX_N_CONNECTIONS)
        self.initial_sync_completed = Value('b', False)

        connection_manager = ConnectionManager(
            event_pool=self._event_pool,
            connection_pool=self._connection_pool,
            initial_sync_completed=self.initial_sync_completed
        )
        connection_manager.daemon = True
        connection_manager.start()

    def get_connections(self) -> List[Connection]:
        return self._connection_pool.get_all()

    def get_connected_modules(self) -> List[ModuleInfo]:
        return [connection.module for connection in self._connection_pool.get_all()]

    def distribute_event(self, event: Union[StoreEvent, RemoveEvent]):
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

            send_message(connection, message)

    def consume_events(self, count: int) -> List[Union[StoreEvent, RemoveEvent]]:
        return self._event_pool.consume_events(count)
