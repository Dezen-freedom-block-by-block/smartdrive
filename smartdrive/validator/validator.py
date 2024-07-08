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
import tempfile
import zipfile
from substrateinterface import Keypair

from communex.module.module import Module
from communex.compat.key import classic_load_key
from communex.types import Ss58Address

import smartdrive
from smartdrive.commune.module._protocol import create_headers
from smartdrive.models.block import Block
from smartdrive.validator.config import Config, config_manager
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT
from smartdrive.validator.database.database import Database
from smartdrive.validator.api.api import API
from smartdrive.validator.evaluation.evaluation import score_miner, set_weights
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.node import Node
from smartdrive.validator.step import validate_step
from smartdrive.validator.utils import extract_sql_file, fetch_with_retries, process_events
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.commune.request import get_filtered_modules, get_truthful_validators, ping_proposer_validator, get_modules
from smartdrive.commune.models import ConnectionInfo, ModuleInfo


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
    BLOCK_INTERVAL_SECONDS = 12
    VALIDATION_INTERVAL_SECONDS = 3 * 60
    # TODO: CHECK INTERVAL DAYS FOR SCORE MINER
    DAYS_INTERVAL = 14

    _config = None
    _key: Keypair = None
    _database: Database = None
    api: API = None
    node: Node = None

    def __init__(self):
        super().__init__()
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def initial_sync(self):
        """
        Performs the initial synchronization by fetching database versions from truthful active validators,
        selecting the validator with the highest version, downloading the database, and importing it.
        """
        try:
            # get_truthful_validators could raise CommuneNetworkUnreachable
            truthful_validators = await get_truthful_validators(self._key, config_manager.config.netuid)

            if not truthful_validators:
                return

            headers = create_headers(sign_data({}, self._key), self._key)
            truthful_database = []

            for validator in truthful_validators:
                response = await fetch_with_retries("block-number", validator.connection, params=None, headers=headers, timeout=30)
                if response and response.status_code == 200:
                    try:
                        truthful_database.append({
                            "uid": validator.uid,
                            "ss58_address": validator.ss58_address,
                            "connection": {
                                "ip": validator.connection.ip,
                                "port": validator.connection.port
                            },
                            "database_block": int(response.json()["block"] or 0)
                        })
                    except Exception as e:
                        print(e)
                        continue

            if not truthful_database:
                return

            block_number = self._database.get_last_block() or 0

            while truthful_database:
                validator = max(truthful_database, key=lambda obj: obj["database_block"])

                if block_number > validator["database_block"]:
                    # If local block is greater than truthful validator we just erase the database and fetch
                    self._database.clear_database()

                connection = ConnectionInfo(validator["connection"]["ip"], validator["connection"]["port"])
                headers = create_headers(sign_data({}, self._key), self._key)
                answer = await fetch_with_retries("database", connection, headers=headers, params=None, timeout=30)

                if answer and answer.status_code == 200:
                    with tempfile.NamedTemporaryFile(delete=False, suffix='.zip') as temp_zip:
                        temp_zip.write(answer.content)
                        temp_zip_path = temp_zip.name

                    if zipfile.is_zipfile(temp_zip_path):
                        sql_file_path = extract_sql_file(temp_zip_path)
                        if sql_file_path:
                            self._database.import_database(sql_file_path)
                            os.remove(temp_zip_path)
                            os.remove(sql_file_path)
                            return
                        else:
                            print("Failed to extract SQL file.")
                            os.remove(temp_zip_path)
                    else:
                        print("The downloaded file is not a valid ZIP file.")
                        os.remove(temp_zip_path)
                else:
                    print("Failed to fetch the database, trying the next validator.")
                    truthful_database.remove(validator)

            print("No more validators available.")

        except Exception as e:
            print(f"Error initializing - {e}")

    async def create_blocks(self):
        last_validation_time = time.time()

        while True:
            try:
                start_time = time.time()

                block_number = self._database.get_last_block() or 0

                # get_truthful_validators or get_filtered_modules could raise CommuneNetworkUnreachable
                truthful_validators = await get_truthful_validators(self._key, config_manager.config.netuid)
                all_validators = get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR)
                own_validator = next((item for item in all_validators if item.ss58_address == self._key.ss58_address), None)

                # Since I can't ping myself, I do this check to add it to the truthful array
                if own_validator.stake >= TRUTHFUL_STAKE_AMOUNT:
                    truthful_validators.append(own_validator)

                proposer_active_validator = max(truthful_validators, key=lambda v: v.stake or 0)
                proposer_validator = max(all_validators, key=lambda v: v.stake or 0)

                if proposer_validator.ss58_address != proposer_active_validator.ss58_address:
                    ping_validator = await ping_proposer_validator(self._key, proposer_validator)
                    if not ping_validator:
                        proposer_validator = proposer_active_validator

                if proposer_validator.ss58_address == self._key.ss58_address:
                    block_number += 1

                    block_events = self.node.consume_pool_events(count=self.MAX_EVENTS_PER_BLOCK)
                    await process_events(events=block_events, is_proposer_validator=True, keypair=self._key, netuid=config_manager.config.netuid, database=self._database)
                    proposer_signature = sign_data({"block_number": block_number, "events": [event.dict() for event in block_events]}, self._key)
                    block = Block(
                        block_number=block_number,
                        events=block_events,
                        proposer_signature=proposer_signature.hex(),
                        proposer_ss58_address=Ss58Address(self._key.ss58_address)
                    )
                    self._database.create_block(block=block)

                    asyncio.create_task(self.node.send_block_to_validators(block=block))

                if time.time() - last_validation_time >= self.VALIDATION_INTERVAL_SECONDS:
                    if proposer_validator.ss58_address == self._key.ss58_address:
                        print("Starting validation task")
                        asyncio.create_task(self.validation_task())

                    asyncio.create_task(self.vote_miners())
                    last_validation_time = time.time()

                elapsed = time.time() - start_time
                if elapsed < self.BLOCK_INTERVAL_SECONDS:
                    sleep_time = self.BLOCK_INTERVAL_SECONDS - elapsed
                    print(f"Sleeping for {sleep_time} seconds before trying to create the next block.")
                    await asyncio.sleep(sleep_time)

            except Exception as e:
                print(f"Error create blocks - {e}")
                await asyncio.sleep(self.BLOCK_INTERVAL_SECONDS)

    async def validation_task(self):
        result = await validate_step(
            database=self._database,
            key=self._key,
            netuid=config_manager.config.netuid
        )

        if result is not None:
            remove_events, validate_events, store_event = result

            if remove_events:
                self.node.insert_pool_events(remove_events)

            if validate_events:
                self.node.insert_pool_events(validate_events)

            if store_event:
                self.node.insert_pool_event(store_event)

    async def vote_miners(self):
        """
        Calculates the weights of the miners and sets them in the network.

        Collects performance data of miners, calculates a score based on their successful and failed calls,
        and then sets these weights in the network.

        Raises:
            CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
        """
        score_dict = {}
        # get_filtered_modules could raise CommuneNetworkUnreachable
        for miner in get_filtered_modules(config_manager.config.netuid, ModuleType.MINER):
            if miner.ss58_address == self._key.ss58_address:
                continue
            total_calls, failed_calls = self._database.get_miner_processes(
                miner_ss58_address=miner.ss58_address,
                days_interval=self.DAYS_INTERVAL
            )
            score_dict[int(miner.uid)] = score_miner(total_calls=total_calls, failed_calls=failed_calls)

        if score_dict:
            await set_weights(score_dict, config_manager.config.netuid, self._key)


if __name__ == "__main__":
    smartdrive.check_version()

    config = get_config()
    config_manager.initialize(config)

    key = classic_load_key(config_manager.config.key)
    registered_modules = get_modules(config_manager.config.netuid, config_manager.config.testnet)

    if key.ss58_address not in [module.ss58_address for module in registered_modules]:
        raise Exception(f"Your key: {key.ss58_address} is not registered.")

    # Using an underscore to prevent naming conflicts with other variables later used named 'validator'
    _validator = Validator()

    async def run_tasks():
        await _validator.initial_sync()
        _validator.node = Node()
        _validator.api = API(_validator.node)
        await asyncio.gather(
            _validator.api.run_server(),
            _validator.create_blocks()
        )

    asyncio.run(run_tasks())
