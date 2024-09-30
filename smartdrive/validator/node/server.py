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

from communex.compat.key import classic_load_key

from smartdrive.logging_config import logger
from smartdrive.commune.request import get_filtered_modules
from smartdrive.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.evaluation.evaluation import MAX_ALLOWED_UIDS
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.client import Client
from smartdrive.validator.node.connection_pool import ConnectionPool
from smartdrive.validator.node.util import packing
from smartdrive.validator.node.util.message import MessageCode, MessageBody, Message
from smartdrive.validator.node.util.utils import send_json


class Server(multiprocessing.Process):
    MAX_N_CONNECTIONS = MAX_ALLOWED_UIDS - 1
    CONNECTION_TIMEOUT_SECONDS = 5
    IDENTIFIER_TIMEOUT_SECONDS = 5
    CONNECTION_PROCESS_TIMEOUT_SECONDS = 10

    _event_pool = None
    _initial_sync_completed = None
    _connection_pool = None
    _keypair = None

    def __init__(self, event_pool, event_pool_lock, initial_sync_completed, connection_pool: ConnectionPool):
        multiprocessing.Process.__init__(self)
        self._event_pool = event_pool
        self._event_pool_lock = event_pool_lock
        self._connection_pool = connection_pool
        self._keypair = classic_load_key(config_manager.config.key)
        self._initial_sync_completed = initial_sync_completed

    def run(self):
        server_socket = None

        try:
            threading.Thread(target=self._run_check_connections).start()
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("0.0.0.0", config_manager.config.port + 1))
            server_socket.listen(self.MAX_N_CONNECTIONS)

            while True:
                client_socket, address = server_socket.accept()
                threading.Thread(target=self._run_handle_connection, args=(client_socket, address,)).start()

        except Exception:
            logger.error(f"Server stopped unexpectedly - PID: {self.pid}", exc_info=True)
        finally:
            if server_socket:
                server_socket.close()

    def _run_handle_connection(self, client_socket, address):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._handle_connection(client_socket, address))

    def _run_check_connections(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._check_connections())

    async def _check_connections(self):
        while True:
            try:
                validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

                active_ss58_addresses = {validator.ss58_address for validator in validators}
                to_remove = [ss58_address for ss58_address in self._connection_pool.get_module_identifiers() if ss58_address not in active_ss58_addresses]
                for ss58_address in to_remove:
                    removed_connection = self._connection_pool.remove_if_exists(ss58_address)
                    if removed_connection:
                        removed_connection.close()
                identifiers = self._connection_pool.get_module_identifiers()
                new_validators = [validator for validator in validators if validator.ss58_address not in identifiers and validator.ss58_address != self._keypair.ss58_address]

                await self._initialize_validators(new_validators)

            except Exception:
                logger.error("Error check connections process", exc_info=True)

            finally:
                await asyncio.sleep(self.CONNECTION_PROCESS_TIMEOUT_SECONDS)

    def _connect_to_validator(self, validator):
        validator_socket = None

        try:
            validator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            validator_socket.settimeout(self.CONNECTION_TIMEOUT_SECONDS)
            validator_socket.connect((validator.connection.ip, validator.connection.port + 1))
            # When you call settimeout(), the timeout applies to all subsequent blocking operations on the socket, such as connect(), recv(), send(), and others.
            # Since we only want to set the timeout at the connection level, it should be reset.
            validator_socket.settimeout(None)

            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_IDENTIFIER,
                data={"ss58_address": self._keypair.ss58_address}
            )

            body_sign = sign_data(body.dict(), self._keypair)

            message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=self._keypair.public_key.hex()
            )
            send_json(validator_socket, message.dict())
            self._connection_pool.upsert_connection(validator.ss58_address, validator, validator_socket)
            client_receiver = Client(validator_socket, validator.ss58_address, self._connection_pool, self._event_pool, self._event_pool_lock, self._initial_sync_completed)
            client_receiver.start()
            logger.debug(f"Validator {validator.ss58_address} connected and added to the pool.")
        except Exception as e:
            logger.debug(f"Error connecting to validator {validator.ss58_address}: {e}")
            self._connection_pool.remove_if_exists(validator.ss58_address)
            if validator_socket:
                validator_socket.close()

    async def _initialize_validators(self, validators):
        try:
            if validators is None:
                validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

            validators = [validator for validator in validators if validator.ss58_address != self._keypair.ss58_address]

            for validator in validators:
                threading.Thread(
                    target=self._connect_to_validator,
                    args=(
                        validator,
                    )
                ).start()

        except Exception:
            logger.error("Error discovering", exc_info=True)

    async def _handle_connection(self, client_socket, address):
        validator_connection = None
        try:
            # Wait self.IDENTIFIER_TIMEOUT_SECONDS as maximum time to get the identifier message
            ready = select.select([client_socket], [], [], self.IDENTIFIER_TIMEOUT_SECONDS)
            if ready[0]:
                identification_message = packing.receive_msg(client_socket)
                logger.debug(f"Identification message received: {identification_message}")

                signature_hex = identification_message["signature_hex"]
                public_key_hex = identification_message["public_key_hex"]
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                is_verified_signature = verify_data_signature(identification_message["body"], signature_hex, ss58_address)

                if not is_verified_signature:
                    logger.debug("Connection signature is not valid.")
                    client_socket.close()

                connection_identifier = identification_message["body"]["data"]["ss58_address"]

                # Replacing the incoming connection because the one currently in place maybe is outdated.
                # TODO: Study if it is possible check if a connection is not outdated. It we can check that it won't be necessary replace the actual connection.
                removed_connection = self._connection_pool.remove_if_exists(connection_identifier)
                if removed_connection:
                    removed_connection.close()

                if self._connection_pool.get_remaining_capacity() == 0:
                    logger.debug("Connection pool is full.")
                    client_socket.close()
                    return

                validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

                if validators:
                    validator_connection = next((validator for validator in validators if validator.ss58_address == connection_identifier), None)

                    if validator_connection:
                        if self._connection_pool.get_remaining_capacity() > 0:
                            self._connection_pool.upsert_connection(validator_connection.ss58_address, validator_connection, client_socket)
                            logger.debug(f"Connection added {validator_connection}")
                            client_receiver = Client(client_socket, connection_identifier, self._connection_pool, self._event_pool, self._event_pool_lock, self._initial_sync_completed)
                            client_receiver.start()
                        else:
                            logger.debug(f"No space available in the connection pool for connection {connection_identifier}.")
                            client_socket.close()
                    else:
                        logger.info(f"Looks like connection {connection_identifier} is not a valid validator.")
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
                self._connection_pool.remove_if_exists(validator_connection.ss58_address)

            if client_socket:
                client_socket.close()
