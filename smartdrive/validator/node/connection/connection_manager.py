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
from smartdrive.validator.node.client.client import Client
from smartdrive.validator.node.connection.connection_pool import ConnectionPool
from smartdrive.validator.node.connection.utils.utils import connect_to_module
from smartdrive.validator.node.event.event_pool import EventPool
from smartdrive.validator.node.util.exceptions import ConnectionPoolMaxSizeReached
from smartdrive.validator.node.util.message import MessageBody, Message, MessageCode
from smartdrive.validator.node.connection.utils.utils import send_message


class ConnectionManager(multiprocessing.Process):
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
        server_socket = None

        try:
            threading.Thread(target=self._discovery).start()
            threading.Thread(target=self._periodically_ping_nodes).start()

            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("0.0.0.0", config_manager.config.port + 1))
            server_socket.listen(self.MAX_N_CONNECTIONS)

            while True:
                client_socket, address = server_socket.accept()
                threading.Thread(target=self._handle_connection, args=(client_socket, address,)).start()

        except Exception:
            logger.error(f"Server stopped unexpectedly - PID: {self.pid}", exc_info=True)

        finally:
            if server_socket:
                server_socket.close()

    def _handle_connection(self, client_socket, address):
        async def handle_connection():
            validator_connection = None
            try:
                # Wait self.IDENTIFIER_TIMEOUT_SECONDS as maximum time to get the identifier message
                ready = select.select([client_socket], [], [], self.IDENTIFIER_TIMEOUT_SECONDS)
                if ready[0]:

                    identification_message = smartdrive.validator.node.connection.utils.utils.receive_msg(client_socket)
                    logger.debug(f"Identification message received: {identification_message}")

                    signature_hex = identification_message["signature_hex"]
                    public_key_hex = identification_message["public_key_hex"]
                    ss58_address = get_ss58_address_from_public_key(public_key_hex)

                    is_verified_signature = verify_data_signature(identification_message["body"], signature_hex, ss58_address)
                    if not is_verified_signature:
                        logger.debug("Connection signature is not valid.")
                        client_socket.close()
                        return

                    inactive_connection = self._connection_pool.remove_if_inactive(ss58_address)
                    if not inactive_connection:
                        logger.debug(f"Current connection with {ss58_address} is active.")
                        client_socket.close()
                        return

                    # When the connection is inactive, we terminate it even though it is not the method's responsibility
                    inactive_connection.close()

                    validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
                    if validators:

                        validator_connection = next((validator for validator in validators if validator.ss58_address == ss58_address), None)
                        if validator_connection:

                            # TODO: Check that the connection related to the validator modules is the same as address

                            try:
                                self._connection_pool.update_or_append(validator_connection.ss58_address, validator_connection, client_socket)
                                logger.debug(f"Connection {ss58_address} upserted.")
                                client_receiver = Client(client_socket, ss58_address, self._connection_pool, self._event_pool, self._initial_sync_completed)
                                client_receiver.start()
                            except ConnectionPoolMaxSizeReached:
                                logger.debug(f"No space available in the connection pool for connection {ss58_address}.", exc_info=True)
                                client_socket.close()

                        else:
                            logger.info(f"Looks like connection {ss58_address} is not a valid validator.")
                            client_socket.close()
                    else:
                        logger.debug("No active validators in subnet.")
                        client_socket.close()
                else:
                    logger.debug(f"No identification received from {address} within timeout.")
                    client_socket.close()

            except Exception:
                logger.error("Error handling connection", exc_info=True)

                if validator_connection:
                    self._connection_pool.remove(validator_connection.ss58_address)

                if client_socket:
                    client_socket.close()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(handle_connection())

    def _discovery(self):
        async def discovery():
            while True:
                try:
                    validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
                    validators_ss58_addresses = {validator.ss58_address for validator in validators}

                    unregistered_validators_ss8_addresses = [ss58_address for ss58_address in self._connection_pool.get_module_ss58_addresses() if ss58_address not in validators_ss58_addresses]
                    removed_connections = self._connection_pool.remove_multiple(unregistered_validators_ss8_addresses)
                    for removed_connection in removed_connections:
                        removed_connection.close()

                    connected_ss58_addresses = self._connection_pool.get_module_ss58_addresses()
                    new_registered_validators = [
                        validator for validator in validators
                        if validator.ss58_address not in connected_ss58_addresses and validator.ss58_address != self._keypair.ss58_address
                    ]
                    for validator in new_registered_validators:
                        threading.Thread(target=self._connect_to_validator, args=(validator,)).start()

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

                    send_message(connection, message)

                except Exception:
                    logger.debug("Error pinging validator", exc_info=True)

            inactive_connections = self._connection_pool.remove_inactive()
            for inactive_connection in inactive_connections:
                inactive_connection.close()

            sleep(5)

    def _connect_to_validator(self, validator: ModuleInfo):
        validator_socket = None

        try:
            validator_socket = connect_to_module(self._keypair, validator)
            self._connection_pool.update_or_append(validator.ss58_address, validator, validator_socket)

            client_receiver = Client(validator_socket, validator.ss58_address, self._connection_pool, self._event_pool, self._initial_sync_completed)
            client_receiver.start()

            logger.debug(f"Validator {validator.ss58_address} connected and added to the pool.")

        except Exception:
            logger.debug(f"Error connecting to validator {validator.ss58_address}", exc_info=True)

            self._connection_pool.remove(validator.ss58_address)

            if validator_socket:
                validator_socket.close()
