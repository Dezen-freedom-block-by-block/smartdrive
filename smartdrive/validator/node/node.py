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
import itertools
import multiprocessing
import os
import threading
import time
import uuid
from time import sleep
from typing import Union, List, Tuple

from communex.compat.key import classic_load_key
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.request import get_filtered_modules
from smartdrive.commune.utils import filter_truthful_validators
from smartdrive.logging_config import logger
from smartdrive.commune.models import ModuleInfo
from smartdrive.models.block import Block, block_to_block_event
from smartdrive.models.event import MessageEvent, StoreEvent, RemoveEvent, StoreRequestEvent, EventParams, \
    RemoveInputParams
from smartdrive.sign import sign_data
from smartdrive.utils import get_stake_from_user, calculate_storage_capacity
from smartdrive.validator.config import config_manager
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.connection_pool import ConnectionPool, Connection
from smartdrive.validator.node.server import Server
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.node.util.utils import send_json
from smartdrive.validator.node.connection_pool import INACTIVITY_TIMEOUT_SECONDS as VALIDATOR_INACTIVITY_TIMEOUT_SECONDS


SLEEP_TIME_CHECK_STAKE_SECONDS = 1 * 60 * 60  # 1 hour
VALIDATION_VOTE_INTERVAL_SECONDS = 10 * 60  # 10 minutes


class Node:
    _keypair: Keypair
    _ping_process = None
    _event_pool = None
    _connection_pool = None
    _event_pool_lock = None
    initial_sync_completed = None

    def __init__(self, database: Database):
        self._keypair = classic_load_key(config_manager.config.key)

        manager = multiprocessing.Manager()
        self._event_pool = manager.list()
        self._event_pool_lock = manager.Lock()
        self.initial_sync_completed = multiprocessing.Value('b', False)
        self._connection_pool = ConnectionPool(cache_size=Server.MAX_N_CONNECTIONS)
        self._database = database

        server = Server(
            event_pool=self._event_pool,
            event_pool_lock=self._event_pool_lock,
            connection_pool=self._connection_pool,
            initial_sync_completed=self.initial_sync_completed
        )
        server.daemon = True
        server.start()

        threading.Thread(target=self.periodically_ping_nodes, daemon=True).start()

        multiprocessing.Process(target=self.periodically_check_stake, daemon=True).start()

    def get_connections(self) -> List[Connection]:
        return self._connection_pool.get_all()

    def get_connected_modules(self) -> List[ModuleInfo]:
        return [connection.module for connection in self._connection_pool.get_all()]

    def add_event(self, event: Union[StoreEvent, RemoveEvent, StoreRequestEvent]):
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
        threading.Thread(target=send_json, args=(connection.socket, message.dict(),)).start()

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
                except Exception:
                    logger.debug("Error pinging validator", exc_info=True)

            inactive_connections = self._connection_pool.remove_and_return_inactive_sockets()
            for inactive_connection in inactive_connections:
                inactive_connection.close()

            sleep(5)

    async def get_proposer_validator(self) -> Tuple[bool, List[ModuleInfo], List[ModuleInfo]]:
        """
        Determines the proposer validator based on the validators' stake.

        Returns:
            is_current_validator_proposer (bool): True if the current validator is the proposer, False otherwise.
            active_validators (List[ModuleInfo]): List of currently active validators.
            all_validators (List[ModuleInfo]): List of all validators in the network.
        """
        # Retrieving all active validators is crucial, so we attempt it an optimal number of times.
        # Between each attempt, we wait VALIDATOR_INACTIVITY_TIMEOUT_SECONDS / 2,
        # as new validators might be activated in the background.
        active_validators = []
        for _ in range(4):
            active_validators = self.get_connected_modules()
            if active_validators:
                break
            await asyncio.sleep(VALIDATOR_INACTIVITY_TIMEOUT_SECONDS / 2)

        truthful_validators = filter_truthful_validators(active_validators)

        # Since the list of active validators never includes the current validator, we need to locate our own
        # validator within the complete list.
        all_validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
        own_validator = next((v for v in all_validators if v.ss58_address == self._keypair.ss58_address), None)

        is_own_validator_truthful = own_validator and own_validator.stake >= TRUTHFUL_STAKE_AMOUNT
        if is_own_validator_truthful:
            truthful_validators.append(own_validator)

        proposer_validator = max(truthful_validators or all_validators, key=lambda v: v.stake or 0)

        is_current_validator_proposer = proposer_validator.ss58_address == self._keypair.ss58_address

        return is_current_validator_proposer, active_validators, all_validators

    def periodically_check_stake(self):
        asyncio.run(self.periodically_check_stake_async())

    async def periodically_check_stake_async(self):
        """
        Periodically checks each user's stake and adjusts their stored data if they exceed their storage capacity.

        This function runs in an infinite loop, checking if any user exceeds their permitted storage capacity
        based on their stake in the network. If a user exceeds the permitted storage, the function will attempt
        to remove the necessary amount of data, prioritizing the removal of the fewest number of files to resolve the issue.

        The function performs the following steps:
        1. Waits for the configured sleep interval (SLEEP_TIME_CHECK_STAKE_SECONDS).
        2. Fetches a list of validators and users' SS58 addresses.
        3. For each user, it calculates the total stake and the total size of the data they have stored.
        4. Compares the total stored size with the user's allowed storage capacity based on their stake.
        5. If the user exceeds their storage capacity, the function:
           a. Attempts to find a single file large enough to resolve the excess storage.
           b. If no single file is sufficient, it searches for a combination of files whose combined size
              is enough to reduce the excess storage.
        6. The selected file(s) are removed, and the function creates and sends events to remove the files from the network.
        7. Continues to the next user and repeats the process.
        8. After processing all users, the function sleeps for the configured time before starting the process again.
        """
        # Set the process priority to a low value
        os.nice(19)

        while True:
            try:
                await asyncio.sleep(SLEEP_TIME_CHECK_STAKE_SECONDS)
                is_current_validator_proposer, _, validators = self.get_proposer_validator()

                if is_current_validator_proposer:
                    user_ss58_addresses = self._database.get_unique_user_ss58_addresses()

                    for user_ss58_address in user_ss58_addresses:
                        total_stake = await get_stake_from_user(user_ss58_address=Ss58Address(user_ss58_address), validators=validators)
                        total_size_stored_by_user = self._database.get_total_file_size_by_user(user_ss58_address=user_ss58_address, only_files=True)
                        available_storage_of_user = calculate_storage_capacity(total_stake)

                        if total_size_stored_by_user > available_storage_of_user:
                            excess_storage = total_size_stored_by_user - available_storage_of_user
                            user_files = self._database.get_files_by_user(user_ss58_address=user_ss58_address)
                            user_files_sorted = sorted(user_files, key=lambda x: x.file_size_bytes)

                            files_to_remove = None
                            for file in user_files_sorted:
                                if file.file_size_bytes >= excess_storage:
                                    files_to_remove = [file]
                                    break

                            if not files_to_remove:
                                for r in range(1, len(user_files_sorted) + 1):
                                    for combo in itertools.combinations(user_files_sorted, r):
                                        if sum(f.file_size_bytes for f in combo) >= excess_storage:
                                            files_to_remove = list(combo)
                                            break
                                    if files_to_remove:
                                        break

                            for file in files_to_remove:
                                event_params = EventParams(file_uuid=file.file_uuid)
                                signed_params = sign_data(event_params.dict(), self._keypair)
                                input_params = RemoveInputParams(file_uuid=file.file_uuid)
                                signed_input_params = sign_data(input_params.dict(), self._keypair)

                                event = RemoveEvent(
                                    uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
                                    validator_ss58_address=Ss58Address(self._keypair.ss58_address),
                                    event_params=event_params,
                                    event_signed_params=signed_params.hex(),
                                    user_ss58_address=user_ss58_address,
                                    input_params=input_params,
                                    input_signed_params=signed_input_params.hex()
                                )

                                self.add_event(event)
            except Exception:
                continue
