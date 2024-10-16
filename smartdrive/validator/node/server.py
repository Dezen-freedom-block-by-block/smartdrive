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
import socket
import select
import threading
import time
import traceback

from communex.compat.key import classic_load_key

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
            self._start_check_connections_thread()
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("0.0.0.0", config_manager.config.port + 1))
            server_socket.listen(self.MAX_N_CONNECTIONS)

            while True:
                client_socket, address = server_socket.accept()
                threading.Thread(
                    target=self._handle_connection,
                    args=(self._connection_pool, self._event_pool, self._event_pool_lock, self._initial_sync_completed, client_socket, address,)
                ).start()

        except Exception as e:
            print(f"Server stopped unexpectedly - PID: {self.pid} - {e} - {traceback.print_exc()}")
        finally:
            if server_socket:
                server_socket.close()

    def _start_check_connections_thread(self):
        threading.Thread(
            target=self._check_connections,
            args=(self._connection_pool, self._event_pool, self._event_pool_lock, self._initial_sync_completed,)
        ).start()

    def _check_connections(self, connection_pool: ConnectionPool, event_pool, event_pool_lock, initial_sync_completed):
        while True:
            try:
                validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

                active_ss58_addresses = {validator.ss58_address for validator in validators}
                to_remove = [ss58_address for ss58_address in connection_pool.get_module_identifiers() if ss58_address not in active_ss58_addresses]
                for ss58_address in to_remove:
                    removed_connection = connection_pool.remove_if_exists(ss58_address)
                    if removed_connection:
                        removed_connection.close()
                identifiers = connection_pool.get_module_identifiers()
                new_validators = [validator for validator in validators if validator.ss58_address not in identifiers and validator.ss58_address != self._keypair.ss58_address]

                self._initialize_validators(connection_pool, event_pool, event_pool_lock, initial_sync_completed, new_validators)

            except Exception as e:
                print(f"Error check connections process - {e}")

            finally:
                time.sleep(self.CONNECTION_PROCESS_TIMEOUT_SECONDS)

    def _connect_to_validator(self, validator, keypair, connection_pool, event_pool, event_pool_lock, initial_sync_completed):
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
                data={"ss58_address": keypair.ss58_address}
            )

            body_sign = sign_data(body.dict(), self._keypair)

            message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=keypair.public_key.hex()
            )
            send_json(validator_socket, message.dict())
            connection_pool.upsert_connection(validator.ss58_address, validator, validator_socket)
            client_receiver = Client(validator_socket, validator.ss58_address, connection_pool, event_pool, event_pool_lock, initial_sync_completed)
            client_receiver.start()
            print(f"Validator {validator.ss58_address} connected and added to the pool.")
        except Exception as e:
            print(f"Error connecting to validator {validator.ss58_address}: {e}")
            connection_pool.remove_if_exists(validator.ss58_address)
            if validator_socket:
                validator_socket.close()

    def _initialize_validators(self, connection_pool: ConnectionPool, event_pool, event_pool_lock, initial_sync_completed, validators):
        try:
            if validators is None:
                validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

            validators = [validator for validator in validators if validator.ss58_address != self._keypair.ss58_address]

            for validator in validators:
                threading.Thread(
                    target=self._connect_to_validator,
                    args=(
                        validator,
                        self._keypair,
                        connection_pool,
                        event_pool,
                        event_pool_lock,
                        initial_sync_completed,
                    )
                ).start()

        except Exception as e:
            print(f"Error initializing validators: {e}")

    def _handle_connection(self, connection_pool: ConnectionPool, event_pool, event_pool_lock, initial_sync_completed, client_socket, address):
        validator_connection = None
        try:
            # Wait self.IDENTIFIER_TIMEOUT_SECONDS as maximum time to get the identifier message
            ready = select.select([client_socket], [], [], self.IDENTIFIER_TIMEOUT_SECONDS)
            if ready[0]:
                identification_message = packing.receive_msg(client_socket)
                print(f"Identification message received: {identification_message}")

                signature_hex = identification_message["signature_hex"]
                public_key_hex = identification_message["public_key_hex"]
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                is_verified_signature = verify_data_signature(identification_message["body"], signature_hex, ss58_address)

                if not is_verified_signature:
                    print(f"Connection signature is not valid.")
                    client_socket.close()

                connection_identifier = identification_message["body"]["data"]["ss58_address"]
                print(f"Connection reached {connection_identifier}")

                # Replacing the incoming connection because the one currently in place maybe is outdated.
                # TODO: Study if it is possible check if a connection is not outdated. It we can check that it won't be necessary replace the actual connection.
                removed_connection = connection_pool.remove_if_exists(connection_identifier)
                if removed_connection:
                    removed_connection.close()

                if connection_pool.get_remaining_capacity() == 0:
                    print(f"Connection pool is full.")
                    client_socket.close()
                    return

                validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

                if validators:
                    validator_connection = next((validator for validator in validators if validator.ss58_address == connection_identifier), None)

                    if validator_connection:
                        if connection_pool.get_remaining_capacity() > 0:
                            connection_pool.upsert_connection(validator_connection.ss58_address, validator_connection, client_socket)
                            print(f"Connection added {validator_connection}")
                            client_receiver = Client(client_socket, connection_identifier, connection_pool, event_pool, event_pool_lock, initial_sync_completed)
                            client_receiver.start()
                        else:
                            print(f"No space available in the connection pool for connection {connection_identifier}.")
                            client_socket.close()
                    else:
                        print(f"Looks like connection {connection_identifier} is not a valid validator.")
                        client_socket.close()
                else:
                    print("No active validators in subnet.")
                    client_socket.close()
            else:
                print(f"No identification received from {address} within timeout.")
                client_socket.close()

        except Exception as e:
            traceback.print_exc()
            print(f"Error handling connection: {e}")
            if validator_connection:
                connection_pool.remove_if_exists(validator_connection.ss58_address)

            if client_socket:
                client_socket.close()