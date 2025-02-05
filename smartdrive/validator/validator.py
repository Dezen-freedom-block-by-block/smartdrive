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

import itertools
import os
import argparse
import time
import asyncio
import uuid

from communex.module.module import Module
from communex.compat.key import classic_load_key
from communex.types import Ss58Address
from substrateinterface import Keypair

import smartdrive
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.logging_config import logger
from smartdrive.models.block import Block, block_to_block_event
from smartdrive.models.event import RemoveEvent, EventParams, RemoveInputParams, StoreRequestEvent
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.config import DEFAULT_VALIDATOR_PATH, SLEEP_TIME_CHECK_STAKE_SECONDS, VALIDATION_VOTE_INTERVAL_SECONDS, \
    BLOCK_INTERVAL_SECONDS, VALIDATOR_DELAY_SECONDS, MAX_EVENTS_PER_BLOCK
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.validator.config import Config, config_manager
from smartdrive.validator.database.database import Database
from smartdrive.validator.node.block.fork import handle_consensus_and_resolve_fork, ChainState, evaluate_local_chain
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.node import Node
from smartdrive.validator.api.api import API
from smartdrive.validator.evaluation.evaluation import score_miners, set_weights
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.block.integrity import get_invalid_events
from smartdrive.validator.node.util.exceptions import InvalidSignatureException, InvalidStorageRequestException
from smartdrive.validator.node.util.message import MessageBody, MessageCode, Message
from smartdrive.validator.node.util.utils import get_proposer_validator
from smartdrive.validator.validation import validate
from smartdrive.validator.utils import prepare_sync_blocks, get_stake_from_user, calculate_storage_capacity, \
    request_last_block
from smartdrive.sign import sign_data
from smartdrive.commune.request import get_filtered_modules, get_modules


def get_config() -> Config:
    """
    Parse params and prepare config object.

    Returns:
        Config: Config object created from parser arguments.
    """
    # Create parser and add all params.
    parser = argparse.ArgumentParser(description="Configure the validator.")
    parser.add_argument("--key-name", required=True, help="Name of key.")
    parser.add_argument("--database-path", default=DEFAULT_VALIDATOR_PATH, required=False, help="Path to the database.")
    parser.add_argument("--port", type=int, default=8001, required=False, help="Default remote API port.")
    parser.add_argument("--testnet", action='store_true', help="Use testnet or not.")

    args = parser.parse_args()
    args.netuid = smartdrive.TESTNET_NETUID if args.testnet else smartdrive.NETUID
    args.database_path = os.path.expanduser(args.database_path)

    if args.database_path:
        os.makedirs(args.database_path, exist_ok=True)

    _config = Config(
        key=args.key_name,
        database_path=args.database_path,
        port=args.port,
        testnet=args.testnet,
        netuid=args.netuid
    )

    return _config


class Validator(Module):
    _config = None
    _key: Keypair = None
    _database: Database = None
    api: API = None
    node: Node = None
    _has_proposed_block: bool = None

    def __init__(self):
        super().__init__()
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()
        self.node = Node()
        self.api = API(self.node)
        self._has_proposed_block = False

    async def run_steps(self):
        """
        This method runs in an infinite loop, following these steps:
            1. Validates and votes on users within the SmartDrive subnet.
            2. Ensures that user stakes remain active as long as storage is being requested.
            3. Proposes a new block if the current node is designated as the proposer.
        """
        last_validation_vote_time = time.monotonic() - VALIDATION_VOTE_INTERVAL_SECONDS
        last_check_stake_time = time.monotonic() - SLEEP_TIME_CHECK_STAKE_SECONDS

        while True:
            start_step_time = time.monotonic()

            try:
                if start_step_time - last_validation_vote_time >= VALIDATION_VOTE_INTERVAL_SECONDS:
                    logger.info("Starting validation and voting task")
                    asyncio.create_task(self.validate_vote_task())
                    last_validation_vote_time = start_step_time
            except Exception:
                logger.error("Error validating and voting", exc_info=True)

            try:
                if start_step_time - last_check_stake_time >= SLEEP_TIME_CHECK_STAKE_SECONDS:
                    logger.info("Starting checking stake")
                    asyncio.create_task(self.check_stake_task())
                    last_check_stake_time = start_step_time
            except Exception:
                logger.error("Error checking stake", exc_info=True)

            try:
                is_current_validator_proposer, active_validators = await get_proposer_validator(
                    keypair=self._key,
                    connected_modules=self.node.connection_pool.get_modules(),
                    sync_service=self.node.sync_service
                )
                if is_current_validator_proposer:
                    get_blocks = self._database.get_blocks(last_block_only=True, include_events=False)
                    last_block = get_blocks[0] if get_blocks else None
                    new_block_number = (last_block.block_number if last_block is not None else 0) + 1

                    # The has_proposed_block variable will help to always enter the update condition the first time
                    # the user is a proposer, because the ideal is to still ask the other validators in case there is
                    # a block that has not been synchronized
                    if not self.node.sync_service.is_synced() or not self._has_proposed_block:
                        if self.node.sync_service.is_syncing() or self.node.sync_service.is_fork_syncing():
                            logger.debug("Synchronization already in progress, skipping new request.")
                            await asyncio.sleep(BLOCK_INTERVAL_SECONDS)
                            continue

                        self._has_proposed_block = True

                        if active_validators:
                            request_last_block(key=self._key, connections=self.node.get_connections())
                            await asyncio.sleep(BLOCK_INTERVAL_SECONDS / 2)

                            sync_status = evaluate_local_chain(
                                local_block=last_block,
                                sync_service=self.node.sync_service,
                                active_connections=self.node.get_connections(),
                                database=self._database,
                                local_validator_ss58_address=self._key.ss58_address
                            )

                            if sync_status == ChainState.OUT_OF_SYNC:
                                logger.info("Out of sync. Starting synchronization process.")
                                self.node.sync_service.start_sync()
                                try:
                                    prepare_sync_blocks(
                                        start=last_block.block_number + 1,
                                        active_connections=self.node.get_connections(),
                                        sync_service=self.node.sync_service,
                                        keypair=self._key,
                                        request_last_block_to_validators=False
                                    )
                                except Exception:
                                    logger.error("Error starting synchronization process", exc_info=True)

                                await asyncio.sleep(BLOCK_INTERVAL_SECONDS / 2)
                                continue

                            elif sync_status == ChainState.FORK_DETECTED:
                                logger.info("Potential fork detected. Requesting additional data to resolve.")
                                handle_consensus_and_resolve_fork(
                                    sync_service=self.node .sync_service,
                                    database=self._database,
                                    local_block=last_block,
                                    active_connections=self.node.get_connections(),
                                    keypair=self._key,
                                )
                                await asyncio.sleep(BLOCK_INTERVAL_SECONDS / 2)
                                continue
                            elif sync_status == ChainState.SYNCHRONIZED:
                                logger.info("Local chain is synchronized.")
                                self.node.sync_service.complete_sync(is_synced=True)

                            continue
                        else:
                            self._has_proposed_block = True
                            self.node.sync_service.complete_sync(is_synced=True)

                    block_events = self.node.consume_events(count=MAX_EVENTS_PER_BLOCK)

                    invalid_events = await get_invalid_events(block_events, self._database)
                    for invalid_event, exception in invalid_events:
                        if isinstance(invalid_event, StoreRequestEvent) and isinstance(exception, InvalidStorageRequestException):
                            invalid_event.event_params.approved = False

                    block = Block(
                        block_number=new_block_number,
                        events=block_events,
                        proposer_ss58_address=Ss58Address(self._key.ss58_address),
                        keypair=self._key,
                        previous_hash=last_block.hash
                    )
                    self._database.create_block(previous_hash=last_block.hash, block=block)

                    block_event = block_to_block_event(block)
                    body = MessageBody(
                        code=MessageCode.MESSAGE_CODE_BLOCK,
                        data=block_event.dict()
                    )
                    body_sign = sign_data(body.dict(), self._key)
                    block_message = Message(
                        body=body,
                        signature_hex=body_sign.hex(),
                        public_key_hex=self._key.public_key.hex()
                    )
                    for connection in self.node.get_connections():
                        send_message(connection, block_message)

                    for event in block_events:
                        if isinstance(event, RemoveEvent):
                            # Only deletions are chosen, since as the block is processed before, the deletion is already marked for these events.
                            chunks = self._database.get_chunks(file_uuid=event.event_params.file_uuid, only_not_removed=False)
                            miners = await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER, config_manager.config.testnet)
                            miners_info_with_chunk = compile_miners_info_and_chunks(miners, chunks)

                            for miner in miners_info_with_chunk:
                                connection = ConnectionInfo(miner["connection"]["ip"], miner["connection"]["port"])
                                miner_info = ModuleInfo(miner["uid"], miner["ss58_address"], connection)
                                await remove_chunk_request(self._key, event.user_ss58_address, miner_info, miner["chunk_uuid"])
                else:
                    self._has_proposed_block = False

                elapsed = time.monotonic() - start_step_time
                sleep_time = max(0.0, BLOCK_INTERVAL_SECONDS - elapsed)
                logger.info(f"Sleeping for {sleep_time:.2f} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)

            except Exception:
                logger.error("Error creating block", exc_info=True)
                await asyncio.sleep(BLOCK_INTERVAL_SECONDS)

    async def validate_vote_task(self):
        miners = [
            miner for miner in await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER, config_manager.config.testnet)
            if miner.ss58_address != self._key.ss58_address
        ]

        result_miners = await validate(
            miners=miners,
            database=self._database,
            key=self._key
        )

        if result_miners:
            score_dict = score_miners(result_miners=result_miners)
            if score_dict:
                await set_weights(score_dict, self._key)
        else:
            score_dict = {}
            for miner in miners:
                score_dict[int(miner.uid)] = 0.0
            await set_weights(score_dict, self._key)

    async def check_stake_task(self):
        """
        Periodically checks each user's stake and adjusts their stored data if they exceed their storage capacity.

        This function checks if any user exceeds their permitted storage capacity based on their stake in the network.
        If a user exceeds the permitted storage, the function will attempt to remove the necessary amount of data, prioritizing
        the removal of the fewest number of files to resolve the issue.

        The function performs the following steps:
            1. Fetches a list of validators and users' SS58 addresses.
            2. For each user, it calculates the total stake and the total size of the data they have stored.
            3. Compares the total stored size with the user's allowed storage capacity based on their stake.
            4. If the user exceeds their storage capacity, the function:
               a. Attempts to find a single file large enough to resolve the excess storage.
               b. If no single file is sufficient, it searches for a combination of files whose combined size is enough to reduce the excess storage.
            5. The selected file(s) are removed, and the function creates and sends events to remove the files from the network.
            6. Continues to the next user and repeats the process.
            7. After processing all users, the function sleeps for the configured time before starting the process again.
        """
        all_validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR, config_manager.config.testnet)
        is_current_validator_proposer = self.node.sync_service.get_current_proposer() == self._key.ss58_address

        if is_current_validator_proposer:
            user_ss58_addresses = self._database.get_unique_user_ss58_addresses()

            for user_ss58_address in user_ss58_addresses:
                total_stake = await get_stake_from_user(user_ss58_address=Ss58Address(user_ss58_address), validators=all_validators)
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
                        signed_params = sign_data(event_params.dict(), self._key)
                        input_params = RemoveInputParams(file_uuid=file.file_uuid)
                        signed_input_params = sign_data(input_params.dict(), self._key)

                        event = RemoveEvent(
                            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
                            validator_ss58_address=Ss58Address(self._key.ss58_address),
                            event_params=event_params,
                            event_signed_params=signed_params.hex(),
                            user_ss58_address=user_ss58_address,
                            input_params=input_params,
                            input_signed_params=signed_input_params.hex()
                        )

                        try:
                            self.node.distribute_event(event)
                        except InvalidSignatureException:
                            logger.debug("Error sending remove event", exc_info=True)


if __name__ == "__main__":
    config = get_config()
    config_manager.initialize(config)

    key = classic_load_key(config_manager.config.key)

    async def main():
        registered_modules = await get_modules(config_manager.config.netuid, config_manager.config.testnet)

        if key.ss58_address not in [module.ss58_address for module in registered_modules]:
            raise Exception(f"Your key: {key.ss58_address} is not registered.")

        validator = Validator()

        async def run_tasks():
            # Initial delay to allow active validators to load before request them
            await asyncio.sleep(VALIDATOR_DELAY_SECONDS)

            await asyncio.gather(
                validator.api.run_server(),
                validator.run_steps()
            )

        await run_tasks()

    asyncio.run(main())
