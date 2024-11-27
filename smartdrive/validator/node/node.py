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
from typing import Union, List

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

from smartdrive.commune.models import ModuleInfo
from smartdrive.models.event import MessageEvent, StoreEvent, RemoveEvent, StoreRequestEvent
from smartdrive.sign import sign_data
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.validator.node.connection.connection_pool import ConnectionPool, Connection
from smartdrive.validator.node.connection.peer_manager import PeerManager
from smartdrive.validator.node.event.event_pool import EventPool
from smartdrive.validator.node.sync_service import SyncService
from smartdrive.validator.node.block.integrity import verify_event_signatures
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.node.connection.utils.utils import send_message


class Node:
    _keypair: Keypair
    _event_pool: EventPool = None
    connection_pool: ConnectionPool = None
    sync_service: SyncService = None
    _database: Database = None

    def __init__(self):
        self._keypair = classic_load_key(config_manager.config.key)

        manager = multiprocessing.Manager()
        self._event_pool = EventPool(manager)
        self.connection_pool = ConnectionPool(manager=manager, cache_size=PeerManager.MAX_N_CONNECTIONS)
        self.sync_service = SyncService()
        self._database = Database()

        connection_manager = PeerManager(
            event_pool=self._event_pool,
            connection_pool=self.connection_pool,
            sync_service=self.sync_service
        )
        connection_manager.daemon = True
        connection_manager.start()

    def get_connections(self) -> List[Connection]:
        return self.connection_pool.get_all()

    def get_connected_modules(self) -> List[ModuleInfo]:
        return [connection.module for connection in self.connection_pool.get_all()]

    def distribute_event(self, event: Union[StoreEvent, RemoveEvent, StoreRequestEvent]):
        """
        Add an event to the node's event pool and distribute it.

        Params:
            event (Union[StoreEvent, RemoveEvent]): The event to distribute.

        Raises:
            InvalidSignatureException: If the event signature is not valid.
        """
        verify_event_signatures(event)

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
