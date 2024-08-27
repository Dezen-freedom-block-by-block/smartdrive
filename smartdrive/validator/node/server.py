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
import time
import traceback

from communex.compat.key import classic_load_key

from smartdrive.commune.request import get_filtered_modules
from smartdrive.validator.api.middleware.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.evaluation.evaluation import MAX_ALLOWED_UIDS
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.client import Client
from smartdrive.validator.node.connection_pool import ConnectionPool
from smartdrive.validator.node.util import packing
from smartdrive.validator.node.util.message_code import MessageCode
from smartdrive.validator.node.util.utils import send_json


class Server(multiprocessing.Process):
    MAX_N_CONNECTIONS = MAX_ALLOWED_UIDS - 1
    IDENTIFIER_TIMEOUT_SECONDS = 5
    CONNECTION_PROCESS_TIMEOUT_SECONDS = 10

    _event_pool = None
    _active_validators_manager = None
    _initial_sync_completed = None
    _connection_pool = None
    _keypair = None

    def __init__(self, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed, connection_pool: ConnectionPool):
        multiprocessing.Process.__init__(self)
        self._event_pool = event_pool
        self._event_pool_lock = event_pool_lock
        self._active_validators_manager = active_validators_manager
        self._connection_pool = connection_pool
        self._keypair = classic_load_key(config_manager.config.key)
        self._initial_sync_completed = initial_sync_completed

    def run(self):
        server_socket = None

        try:
            self._start_check_connections_process()
            server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            server_socket.bind(("0.0.0.0", config_manager.config.port + 1))
            server_socket.listen(self.MAX_N_CONNECTIONS)

            while True:
                client_socket, address = server_socket.accept()
                process = multiprocessing.Process(
                    target=self._handle_connection,
                    args=(self._connection_pool, self._event_pool, self._event_pool_lock, self._active_validators_manager, self._initial_sync_completed, client_socket, address,)
                )
                process.start()
                client_socket.close()  # Close the socket in the parent process

        except Exception as e:
            print(f"Server stopped unexpectedly - PID: {self.pid} - {e} - {traceback.print_exc()}")
        finally:
            if server_socket:
                server_socket.close()

    def _start_check_connections_process(self):
        # Although these variables are managed by multiprocessing.Manager(),
        # we explicitly pass them as parameters to make it clear that they are dependencies of the server process.
        process = multiprocessing.Process(
            target=self._check_connections_process,
            args=(self._connection_pool, self._event_pool, self._event_pool_lock, self._active_validators_manager, self._initial_sync_completed,)
        )
        process.start()

    def _check_connections_process(self, connection_pool: ConnectionPool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed):
        while True:
            try:
                validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

                active_ss58_addresses = {validator.ss58_address for validator in validators}
                to_remove = [ss58_address for ss58_address in connection_pool.get_identifiers() if ss58_address not in active_ss58_addresses]
                for ss58_address in to_remove:
                    removed_connection = connection_pool.remove_connection(ss58_address)
                    if removed_connection:
                        removed_connection.close()
                identifiers = connection_pool.get_identifiers()
                new_validators = [validator for validator in validators if validator.ss58_address not in identifiers and validator.ss58_address != self._keypair.ss58_address]
                asyncio.run(self._initialize_validators(connection_pool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed, new_validators))

            except Exception as e:
                print(f"Error check connections process - {e}")

            finally:
                time.sleep(self.CONNECTION_PROCESS_TIMEOUT_SECONDS)

    async def _connect_to_validator(self, validator, keypair, connection_pool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed):
        validator_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            await asyncio.get_event_loop().sock_connect(validator_socket, (validator.connection.ip, validator.connection.port + 1))
            body = {
                "code": MessageCode.MESSAGE_CODE_IDENTIFIER.value,
                "data": {"ss58_address": keypair.ss58_address}
            }
            body_sign = sign_data(body, keypair)
            message = {
                "body": body,
                "signature_hex": body_sign.hex(),
                "public_key_hex": keypair.public_key.hex()
            }
            send_json(validator_socket, message)
            connection_pool.add_connection(validator.ss58_address, validator, validator_socket)
            client_receiver = Client(validator_socket, validator.ss58_address, connection_pool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed)
            client_receiver.start()
            active_validators_manager.update_validator(validator, validator_socket)
            print(f"Validator {validator.ss58_address} connected and added to the pool.")
        except Exception as e:
            connection_pool.remove_connection(validator.ss58_address)
            validator_socket.close()
            print(f"Error connecting to validator {validator.ss58_address}: {e}")

    async def _initialize_validators(self, connection_pool: ConnectionPool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed, validators):
        try:
            if validators is None:
                validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)

            validators = [validator for validator in validators if validator.ss58_address != self._keypair.ss58_address]

            tasks = [
                self._connect_to_validator(
                    validator,
                    self._keypair,
                    connection_pool,
                    event_pool,
                    event_pool_lock,
                    active_validators_manager,
                    initial_sync_completed
                )
                for validator in validators
            ]

            await asyncio.gather(*tasks)
        except Exception as e:
            print(f"Error initializing validators: {e}")

    def _handle_connection(self, connection_pool: ConnectionPool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed, client_socket, address):
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

                # Replacing the incoming connection because the one currently in place is outdated.
                if connection_identifier in connection_pool.get_identifiers():
                    print(f"Connection {connection_identifier} is already in the connection pool.")
                    removed_connection = connection_pool.remove_connection(connection_identifier)
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
                            connection_pool.add_connection(validator_connection.ss58_address, validator_connection, client_socket)
                            print(f"Connection added {validator_connection}")
                            client_receiver = Client(client_socket, connection_identifier, connection_pool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed)
                            client_receiver.start()
                            active_validators_manager.update_validator(validator_connection, client_socket)
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
            print(f"Error handling connection: {e}")
            traceback.print_exc()
            client_socket.close()
