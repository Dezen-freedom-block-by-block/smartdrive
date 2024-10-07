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
import socket
import select
import threading
from multiprocessing import Value
from time import sleep

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

import smartdrive.validator.node.connection.utils.utils
from smartdrive.commune.models import ModuleInfo
from smartdrive.logging_config import logger
from smartdrive.commune.request import get_filtered_modules
from smartdrive.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.evaluation.evaluation import MAX_ALLOWED_UIDS
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.connection.peer import Peer
from smartdrive.validator.node.connection.connection_pool import ConnectionPool, PING_INTERVAL_SECONDS
from smartdrive.validator.node.connection.utils.utils import connect_to_peer
from smartdrive.validator.node.event.event_pool import EventPool
from smartdrive.validator.node.util.exceptions import ConnectionPoolMaxSizeReached
from smartdrive.validator.node.util.message import MessageBody, Message, MessageCode
from smartdrive.validator.node.connection.utils.utils import send_message


class PeerManager(multiprocessing.Process):
    MAX_N_CONNECTIONS = MAX_ALLOWED_UIDS - 1
    IDENTIFIER_TIMEOUT_SECONDS = 5
    CONNECTION_PROCESS_TIMEOUT_SECONDS = 10

    _event_pool: EventPool = None
    _connection_pool: ConnectionPool = None
    _initial_sync_completed: Value = None
    _keypair: Keypair = None

    def __init__(self, event_pool: EventPool, initial_sync_completed: Value, connection_pool: ConnectionPool):
        multiprocessing.Process.__init__(self)
        self._event_pool = event_pool
        self._connection_pool = connection_pool
        self._initial_sync_completed = initial_sync_completed
        self._keypair = classic_load_key(config_manager.config.key)

    def run(self):
        listening_socket = None

        try:
            threading.Thread(target=self._discovery).start()
            threading.Thread(target=self._periodically_ping_nodes).start()

            listening_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            listening_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            listening_socket.bind(("0.0.0.0", config_manager.config.port + 1))
            listening_socket.listen(self.MAX_N_CONNECTIONS)

            while True:
                peer_socket, address = listening_socket.accept()
                threading.Thread(target=self._handle_connection, args=(peer_socket, address,)).start()

        except Exception:
            logger.error(f"Peer manager stopped unexpectedly - PID: {self.pid}", exc_info=True)

        finally:
            if listening_socket:
                listening_socket.close()

    def _handle_connection(self, peer_socket, peer_address):
        async def handle_connection():
            validator_connection = None
            try:
                # Wait self.IDENTIFIER_TIMEOUT_SECONDS as maximum time to get the identifier message
                ready = select.select([peer_socket], [], [], self.IDENTIFIER_TIMEOUT_SECONDS)
                if not ready[0]:
                    logger.debug(f"Timeout: No identification from {peer_address}")
                    peer_socket.close()
                    return

                identification_message = smartdrive.validator.node.connection.utils.utils.receive_msg(peer_socket)

                signature_hex = identification_message["signature_hex"]
                public_key_hex = identification_message["public_key_hex"]
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                logger.debug(f"Identification message received {ss58_address}")

                is_verified_signature = verify_data_signature(identification_message["body"], signature_hex, ss58_address)
                if not is_verified_signature:
                    logger.debug(f"Invalid signature for {ss58_address}")
                    peer_socket.close()
                    return

                active_connection = self._connection_pool.get_actives(ss58_address)
                if active_connection:
                    logger.debug(f"Peer {ss58_address} is already active")
                    peer_socket.close()
                    return

                validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
                if not validators:
                    logger.debug("No active validators found")
                    peer_socket.close()
                    return

                validator_connection = next((validator for validator in validators if validator.ss58_address == ss58_address), None)
                if not validator_connection:
                    logger.info(f"Validator {ss58_address} is not valid")
                    peer_socket.close()
                    return

                # Check that the connection related to the validator modules is the same as address
                if peer_address[0] != validator_connection.connection.ip:
                    logger.info(f"Validator {ss58_address} connected from wrong address {peer_address}")
                    peer_socket.close()
                    return

                try:
                    self._connection_pool.update_or_append(validator_connection.ss58_address, validator_connection, peer_socket)
                    Peer(peer_socket, ss58_address, self._connection_pool, self._event_pool, self._initial_sync_completed).start()
                    logger.debug(f"Peer {ss58_address} connected from {peer_address}")
                except ConnectionPoolMaxSizeReached:
                    logger.debug(f"Connection pool full for {ss58_address}", exc_info=True)
                    peer_socket.close()

            except Exception:
                logger.error("Error handling connection", exc_info=True)

                if validator_connection:
                    self._connection_pool.remove(validator_connection.ss58_address)

                if peer_socket:
                    peer_socket.close()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle_connection())

    def _discovery(self):
        async def discovery():
            while True:
                try:
                    validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
                    validators_ss58_addresses = {validator.ss58_address for validator in validators}

                    unregistered_validators_ss8_addresses = [ss58_address for ss58_address in self._connection_pool.get_identifiers() if ss58_address not in validators_ss58_addresses]
                    removed_connections = self._connection_pool.remove_multiple(unregistered_validators_ss8_addresses)
                    for removed_connection in removed_connections:
                        removed_connection.close()

                    connected_ss58_addresses = self._connection_pool.get_identifiers()
                    new_registered_validators = [
                        validator for validator in validators
                        if validator.ss58_address not in connected_ss58_addresses and validator.ss58_address != self._keypair.ss58_address
                    ]
                    for validator in new_registered_validators:
                        threading.Thread(target=self._connect_to_peer, args=(validator,)).start()

                except Exception:
                    logger.error("Error discovering new validators", exc_info=True)

                finally:
                    await asyncio.sleep(self.CONNECTION_PROCESS_TIMEOUT_SECONDS)

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(discovery())

    def _periodically_ping_nodes(self):
        while True:
            connections = self._connection_pool.get_all()

            for connection in connections:
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

                    send_message(connection.socket, message)

                except Exception:
                    logger.debug("Error pinging node")

            inactive_connections = self._connection_pool.remove_inactive()
            for inactive_connection in inactive_connections:
                inactive_connection.close()

            sleep(PING_INTERVAL_SECONDS)

    def _connect_to_peer(self, validator: ModuleInfo):
        peer_socket = None

        try:
            peer_socket = connect_to_peer(self._keypair, validator)
            self._connection_pool.update_or_append(validator.ss58_address, validator, peer_socket)
            Peer(peer_socket, validator.ss58_address, self._connection_pool, self._event_pool, self._initial_sync_completed).start()
            logger.debug(f"Peer {validator.ss58_address} connected and added to the pool")

        except Exception:
            logger.debug(f"Error connecting to peer {validator.ss58_address}")

            self._connection_pool.remove(validator.ss58_address)

            if peer_socket:
                peer_socket.close()
