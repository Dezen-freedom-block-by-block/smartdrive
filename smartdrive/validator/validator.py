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

import os
import argparse
import time
import asyncio

from substrateinterface import Keypair

from communex.module.module import Module
from communex.compat.key import classic_load_key
from communex.types import Ss58Address

import smartdrive
from smartdrive.commune.connection_pool import initialize_commune_connection_pool
from smartdrive.models.block import Block
from smartdrive.validator.config import Config, config_manager
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT
from smartdrive.validator.database.database import Database
from smartdrive.validator.api.api import API
from smartdrive.validator.errors import InitialSyncError
from smartdrive.validator.evaluation.evaluation import score_miners, set_weights
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.active_validator_manager import INACTIVITY_TIMEOUT_SECONDS
from smartdrive.validator.node.node import Node
from smartdrive.validator.step import validate_step
from smartdrive.validator.utils import process_events, prepare_sync_blocks
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.commune.request import get_filtered_modules, get_modules
from smartdrive.commune.utils import filter_truthful_validators


def get_config() -> Config:
    """
    Parse params and prepare config object.

    Returns:
        Config: Config object created from parser arguments.
    """
    path = os.path.abspath(__file__)
    db_path = os.path.join(os.path.dirname(path), "database")

    # Create parser and add all params.
    parser = argparse.ArgumentParser(description="Configure the validator.")
    parser.add_argument("--key", required=True, help="Name of key.")
    parser.add_argument("--database_path", default=db_path, required=False, help="Path to the database.")
    parser.add_argument("--port", type=int, default=8001, required=False, help="Default remote API port.")
    parser.add_argument("--testnet", action='store_true', help="Use testnet or not.")

    args = parser.parse_args()
    args.netuid = smartdrive.TESTNET_NETUID if args.testnet else smartdrive.NETUID

    if args.database_path:
        os.makedirs(args.database_path, exist_ok=True)

    args.database_path = os.path.expanduser(args.database_path)

    _config = Config(
        key=args.key,
        database_path=args.database_path,
        port=args.port,
        testnet=args.testnet,
        netuid=args.netuid
    )

    return _config


class Validator(Module):
    # TODO: REPLACE THIS WITH bytes
    MAX_EVENTS_PER_BLOCK = 100
    BLOCK_INTERVAL_SECONDS = 30
    PING_INTERVAL_SECONDS = 5
    MAX_RETRIES = 2
    RETRY_DELAY_CREATION_BLOCK = 5
    VALIDATION_VOTE_INTERVAL_SECONDS = 2 * 60

    _config = None
    _key: Keypair = None
    _database: Database = None
    api: API = None
    node: Node = None
    _initial_sync_block = False

    def __init__(self):
        super().__init__()
        self.node = Node()
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def create_blocks(self):
        last_validation_time = time.time()

        # We wait the same number of seconds as indicated to mark a validator as inactive
        await asyncio.sleep(INACTIVITY_TIMEOUT_SECONDS)

        while True:
            try:
                current_time = time.time()

                # Start validation and voting task
                if current_time - last_validation_time >= self.VALIDATION_VOTE_INTERVAL_SECONDS:
                    print("Starting validation and voting task")
                    asyncio.create_task(self.validation_task())
                    last_validation_time = current_time

                start_time = current_time
                active_validators = self.node.get_active_validators()

                block_number = self._database.get_last_block() or 0
                truthful_validators = filter_truthful_validators(active_validators)

                # Retry mechanism to get truthful validators
                for _ in range(self.MAX_RETRIES):
                    if truthful_validators:
                        break
                    await asyncio.sleep(self.RETRY_DELAY_CREATION_BLOCK)
                    active_validators = self.node.get_active_validators()
                    truthful_validators = filter_truthful_validators(active_validators)

                # Get all validators and find self
                all_validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
                own_validator = next((v for v in all_validators if v.ss58_address == self._key.ss58_address), None)

                # Add self to truthful validators if criteria are met
                if own_validator and own_validator.stake >= TRUTHFUL_STAKE_AMOUNT:
                    truthful_validators.append(own_validator)

                # Determine proposer validator
                proposer_validator = max(truthful_validators or all_validators, key=lambda v: v.stake or 0)

                if proposer_validator.ss58_address == self._key.ss58_address:
                    if not self._initial_sync_block:
                        self._initial_sync_block = True
                        if active_validators:
                            prepare_sync_blocks(
                                start=block_number + 1,
                                active_connections=self.node.get_active_validators_connections(),
                                keypair=self._key
                            )
                            await asyncio.sleep(self.BLOCK_INTERVAL_SECONDS)
                            continue
                    block_number += 1
                    block_events = self.node.consume_pool_events(count=self.MAX_EVENTS_PER_BLOCK)
                    await process_events(
                        events=block_events,
                        is_proposer_validator=True,
                        keypair=self._key,
                        netuid=config_manager.config.netuid,
                        database=self._database
                    )
                    proposer_signature = sign_data({"block_number": block_number, "events": [event.dict() for event in block_events]}, self._key)
                    block = Block(
                        block_number=block_number,
                        events=block_events,
                        proposer_signature=proposer_signature.hex(),
                        proposer_ss58_address=Ss58Address(self._key.ss58_address)
                    )
                    self._database.create_block(block=block)

                    asyncio.create_task(self.node.send_block_to_validators(block=block))

                    if not _validator.node.initial_sync_completed.value:
                        _validator.node.initial_sync_completed.value = True

                # Calculate sleep time to maintain block interval
                elapsed = time.time() - start_time
                sleep_time = max(0, self.BLOCK_INTERVAL_SECONDS - elapsed)
                print(f"Sleeping for {sleep_time:.2f} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)

            except Exception as e:
                print(f"Error creating blocks - {e}")
                await asyncio.sleep(self.BLOCK_INTERVAL_SECONDS)

    async def validation_task(self):
        miners = [
            miner for miner in get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
            if miner.ss58_address != self._key.ss58_address
        ]

        remove_events, validation_events, result_miners = await validate_step(
                miners=miners,
                database=self._database,
                key=self._key,
                validators_len=len(self.node.get_active_validators_connections()) + 1  # To include myself
        )

        if remove_events:
            await process_events(
                events=remove_events,
                is_proposer_validator=True,
                keypair=self._key,
                netuid=config_manager.config.netuid,
                database=self._database
            )

        if validation_events:
            self._database.insert_validation_events(validation_events=validation_events)

        if result_miners:
            # Voting
            score_dict = score_miners(result_miners=result_miners)
            if _validator.node.initial_sync_completed.value and score_dict:
                await set_weights(score_dict, config_manager.config.netuid, self._key)

    async def periodically_ping_validators(self):
        while True:
            await self.node.ping_validators()
            await asyncio.sleep(self.PING_INTERVAL_SECONDS)


if __name__ == "__main__":
    smartdrive.check_version()

    config = get_config()
    config_manager.initialize(config)

    initialize_commune_connection_pool(config_manager.config.testnet)

    key = classic_load_key(config_manager.config.key)
    registered_modules = get_modules(config_manager.config.netuid)

    if key.ss58_address not in [module.ss58_address for module in registered_modules]:
        raise Exception(f"Your key: {key.ss58_address} is not registered.")

    # Using an underscore to prevent naming conflicts with other variables later used named 'validator'
    _validator = Validator()

    async def run_tasks():
        try:
            asyncio.create_task(_validator.periodically_ping_validators())
            _validator.api = API(_validator.node)
            await asyncio.gather(
                _validator.api.run_server(),
                _validator.create_blocks()
            )
        except InitialSyncError as e:
            print(f"Error - {e}")

    asyncio.run(run_tasks())
