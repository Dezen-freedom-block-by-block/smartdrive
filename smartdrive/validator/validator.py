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
import stun
import argparse
import time
import asyncio
import tempfile
import concurrent.futures
import multiprocessing
import zipfile
import random

from substrateinterface import Keypair

from communex._common import get_node_url
from communex.client import CommuneClient
from communex.module.module import Module
from communex.compat.key import classic_load_key
from communex.types import Ss58Address

import smartdrive
from smartdrive.commune.module._protocol import create_headers
from smartdrive.validator.api.middleware.sign import sign_data, verify_data_signature, verify_block
from smartdrive.validator.api.remove_api import _remove_chunk_request, remove_files
from smartdrive.validator.api.utils import get_miner_info_with_chunk
from smartdrive.validator.api.validate_api import validate_miners
from smartdrive.validator.database.database import Database
from smartdrive.validator.evaluation.evaluation import score_miner, set_weights
from smartdrive.validator.evaluation.utils import generate_data
from smartdrive.validator.api.api import API
from smartdrive.validator.models.block import Event, Block
from smartdrive.validator.models.models import SubChunk, Chunk, File, MinerWithSubChunk, ModuleType
from smartdrive.validator.network.network import Network
from smartdrive.validator.utils import extract_sql_file, fetch_validator, encode_bytes_to_b64
from smartdrive.commune.request import (get_modules, get_active_validators, get_active_miners, ConnectionInfo, ModuleInfo,
                                        execute_miner_request, get_truthful_validators, ping_leader_validator, get_filtered_modules)


def get_config():
    """
    Parse params and prepare config object.

    Returns:
        dict: Nested config object created from parser arguments.
    """
    path = os.path.abspath(__file__)
    db_path = os.path.join(os.path.dirname(path), "database")

    # Create parser and add all params.
    parser = argparse.ArgumentParser(description="Configure the validator.")
    parser.add_argument("--key", required=True, help="Name of key.")
    parser.add_argument("--name", required=True, help="Name of validator.")
    parser.add_argument("--database_path", default=db_path, required=False, help="Path to the database.")
    parser.add_argument("--port", type=int, default=8001, required=False, help="Default remote api port.")
    parser.add_argument("--testnet", action='store_true', help="Use testnet or not.")

    config = parser.parse_args()
    config.netuid = smartdrive.TESTNET_NETUID if config.testnet else smartdrive.NETUID

    if config.database_path:
        os.makedirs(config.database_path, exist_ok=True)

    config.database_path = os.path.expanduser(config.database_path)
    config.database_file = os.path.join(config.database_path, "smartdrive.db")
    config.database_export_file = os.path.join(config.database_path, "export.zip")

    return config


class Validator(Module):
    ITERATION_INTERVAL = 60
    MAX_EVENTS_PER_BLOCK = 25
    BLOCK_INTERVAL = 12

    _config = None
    _key: Keypair = None
    _database: Database = None
    api: API = None
    _comx_client: CommuneClient = None
    _network: Network = None

    def __init__(self, config):
        super().__init__()

        self._config = config
        self._key = classic_load_key(config.key)
        self._database = Database(config.database_file, config.database_export_file)
        self.api = API(self._config, self._key, self._database, self._comx_client)
        self._comx_client = CommuneClient(url=get_node_url(use_testnet=self._config.testnet), num_connections=5)
        self._network = Network(keypair=self._key, ip=self._config.ip, netuid=self._config.netuid)

    async def validation_loop(self):
        """
        Continuously runs the validation process in a loop.

        This method enters an infinite loop where it continuously checks the software version,
        performs a validation step, and ensures that each iteration runs at a consistent interval
        defined by `self.ITERATION_INTERVAL`. If the validation step completes faster than the
        interval, it sleeps for the remaining time.
        """
        while True:
            smartdrive.check_version()
            start_time = time.time()
            await self.validate_step()

            elapsed = time.time() - start_time
            if elapsed < self.ITERATION_INTERVAL:
                sleep_time = self.ITERATION_INTERVAL - elapsed
                print(f"Sleeping for {sleep_time}")
                await asyncio.sleep(sleep_time)

    async def validate_step(self):
        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)
        if not active_miners:
            print("Skipping validation step, there is not any miner active")
            return

        files = self._database.get_files_with_expiration(Ss58Address(self._key.ss58_address))
        expired_files = []
        non_expired_files = []
        current_timestamp = int(time.time() * 1000)
        for file in files:
            expired_files.append(file) if file.has_expired(current_timestamp) else non_expired_files.append(file)

        # Remove expired files
        if expired_files:
            remove_events = await remove_files(
                files=expired_files,
                keypair=self._key,
                comx_client=self._comx_client,
                netuid=self._config.netuid
            )

        # Validate non expired files
        if non_expired_files:
            validate_events = await validate_miners(
                files=non_expired_files,
                keypair=self._key,
                comx_client=self._comx_client,
                netuid=self._config.netuid
            )

        # TODO: Move store before validate to check the new files
        # Store new files
        miners_to_store = self._determine_miners_to_store(files, expired_files, active_miners)
        if miners_to_store:
            await self._store_new_file(miners_to_store)

        # Set weights to miners
        score_dict = {}
        for miner in get_filtered_modules(self._comx_client, self._config.netuid, ModuleType.MINER):
            if miner.ss58_address == key.ss58_address:
                continue
            avg_miner_response_time = self._database.get_avg_miner_response_time(miner.ss58_address)
            successful_store_responses, total_store_responses = self._database.get_successful_responses_and_total(miner.ss58_address, "store")
            successful_responses, total_responses = self._database.get_successful_responses_and_total(miner.ss58_address)

            score_dict[int(miner.uid)] = score_miner(successful_store_responses, total_store_responses, avg_miner_response_time, successful_responses, total_responses)

        if not score_dict:
            print("Skipping set weights")
            return

        await set_weights(score_dict, self._config.netuid, self._comx_client, self._key, self._config.testnet)

    def _determine_miners_to_store(self, files: list[File], expired_files_dict: list[File], active_miners: list[ModuleInfo]):
        """
        Determines which miners should store new files.

        This method decides which miners should be assigned to store new files based on the
        list of current files, expired files, and active miners. It ensures that active miners
        that were previously storing expired files and active miners not currently storing any
        files are selected.

        Params:
            files (list[File]): The list of current files.
            expired_files_dict (list[File]): The list of expired files.
            active_miners (list[ModuleInfo]): The list of active miners.

        Returns:
            list[ModuleInfo]: The list of miners that should store new files.
        """
        miners_to_store = []

        if not files:
            miners_to_store = active_miners

        else:
            # Collect miners from expired files
            expired_miners = {
                file.chunks[0].miner_owner_ss58address
                for file in expired_files_dict
            }

            # Add miners with matching ss58_address
            for miner in active_miners:
                if miner.ss58_address in expired_miners:
                    miners_to_store.append(miner)

            # Add miners not present in expired_miners
            users_ss58addresses = [
                chunk.miner_owner_ss58address
                for file in files
                for chunk in file.chunks
            ]
            for miner in active_miners:
                if miner.ss58_address not in users_ss58addresses:
                    miners_to_store.append(miner)

        return miners_to_store

    async def _store_new_file(self, miners: list[ModuleInfo]):
        """
        Stores a new file across a list of miners.

        This method generates file data, encodes it, and sends it to a list of miners to be stored.
        It handles the storage requests asynchronously, logs the responses, and stores information
        about successfully stored chunks in the database.

        Params:
            miners (list[ModuleInfo]): A list of miner objects where the file data will be stored.
        """
        # TODO: Set max value for chunk
        file_data = generate_data(5)
        data_encoded = encode_bytes_to_b64(file_data)
        # TODO: Fix this, currently an early stage of what it should be
        end = random.randint(51, len(data_encoded) - 1)
        start = end - 50
        sub_chunk_encoded = data_encoded[start:end]

        miners_answers = []
        async def handle_store_request(miner):
            miner_dict = miner.__dict__
            start_time = time.time()
            miner_answer = await self.api.store_api.store_request(miner, Ss58Address(self._key.ss58_address), data_encoded)
            final_time = time.time() - start_time
            miner_dict["stored"] = True if miner_answer else False

            if miner_answer:
                miner_dict["chunk_uuid"] = miner_answer.chunk_uuid

            self._database.insert_miner_response(miner_dict["ss58_address"], "store", True if miner_answer else False, final_time)

            miners_answers.append(miner_dict)

        futures = [
            handle_store_request(miner)
            for miner in miners
        ]
        await asyncio.gather(*futures)

        miners_answers_saved = list(filter(lambda miner_answer: miner_answer["stored"], miners_answers))
        if miners_answers_saved:
            sub_chunk = SubChunk(
                id=None,
                start=start,
                end=end,
                chunk_uuid=None,
                data=sub_chunk_encoded
            )

            chunks = []
            for miner_answer in miners_answers_saved:
                chunk = Chunk(
                    miner_owner_ss58address=miner_answer["ss58_address"],
                    chunk_uuid=miner_answer["chunk_uuid"],
                    file_uuid=None,
                    sub_chunk=sub_chunk,
                )
                chunks.append(chunk)

            file = File(
                user_owner_ss58address=Ss58Address(self._key.ss58_address),
                file_uuid=None,
                chunks=chunks,
                created_at=None,
                expiration_ms=None
            )

            self._database.insert_file(file)

    async def initial_sync(self):
        """
        Performs the initial synchronization by fetching database versions from active validators,
        selecting the validator with the highest version, downloading the database, and importing it.
        """
        active_validators = await get_active_validators(self._key, self._comx_client, self._config.netuid)
        active_validators = [validator for validator in active_validators if validator.ss58_address != self._key.ss58_address]

        with concurrent.futures.ThreadPoolExecutor(max_workers=multiprocessing.cpu_count()) as executor:
            headers = create_headers(sign_data({}, self._key), self._key)
            futures = [
                executor.submit(fetch_validator, "database-block", validator.connection, 10, headers)
                for validator in active_validators
            ]
            answers = [future.result() for future in concurrent.futures.as_completed(futures)]

        active_validators_database = []
        for response, validator in zip(answers, active_validators):
            if response and response.status_code == 200:
                try:
                    active_validators_database.append({
                        "uid": validator.uid,
                        "ss58_address": validator.miner_ss58_address,
                        "connection": {
                            "ip": validator.connection.ip,
                            "port": validator.connection.port
                        },
                        "database_block": int(response.json()["block"] or 0)
                    })
                except Exception as e:
                    print(e)
                    pass

        if not active_validators_database:
            return

        max_database_block = max(obj["database_block"] for obj in active_validators_database)
        max_block_validators = [
            obj for obj in active_validators_database
            if obj["database_block"] == max_database_block
        ]
        validator = max_block_validators[0]

        connection = ConnectionInfo(validator["connection"]["ip"], validator["connection"]["port"])
        headers = create_headers(sign_data({}, self._key), self._key)
        answer = fetch_validator("database", connection, headers=headers)

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
                else:
                    print("Failed to extract SQL file.")
                    os.remove(temp_zip_path)
            else:
                print("The downloaded file is not a valid ZIP file.")
                os.remove(temp_zip_path)
        else:
            print("Failed to fetch the database.")
            return None

    async def create_blocks(self):
        # TODO: retrieve real block events
        # TODO: permit only MAX_EVENTS_PER_BLOCK
        # TODO: retrieve last block from other leader validator
        # TODO: propagate blocks to validators

        block_number = self._database.get_database_block()
        block_number = -1 if block_number is None else block_number

        while True:
            start_time = time.time()

            truthful_validators = await get_truthful_validators(self._key, self._comx_client, self._config.netuid)
            all_validators = get_filtered_modules(self._comx_client, self._config.netuid, ModuleType.VALIDATOR)

            leader_active_validator = max(truthful_validators, key=lambda v: v.stake or 0)
            leader_validator = max(all_validators, key=lambda v: v.stake or 0)

            if leader_validator.ss58_address != leader_active_validator.ss58_address:
                ping_validator = await ping_leader_validator(self._key, leader_validator)
                if not ping_validator:
                    leader_validator = leader_active_validator

            if leader_validator.ss58_address == self._key.ss58_address:
                block_number += 1
                block_events = []
                await self.process_events(events=block_events)
                self._database.create_block(block_number)

            elapsed = time.time() - start_time
            if elapsed < self.BLOCK_INTERVAL:
                sleep_time = self.BLOCK_INTERVAL - elapsed
                print(f"Sleeping for {sleep_time} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)

    async def process_events(self, events: list[Event]):
        # TODO: check if returns ok and remove from mempool, otherwise keep events
        for e in events:
            if e.action == "store":
                # TODO: data is inserted, now we have to insert where is located data (miner info)
                result = await self.api.store_api.store_event()
                print(f"STORE: {result}")
            elif e.action == "remove":
                result = await self.api.remove_api.remove_endpoint(user_ss58_address=e.params.get("user_ss58_address"), file_uuid=e.params.get("file_uuid"))
                print(f"REMOVE: {result}")

    async def handle_received_block(self, block: Block, leader_validator: ModuleInfo):
        # TODO: check handle received block
        processed_events = []
        if verify_block(block, leader_validator.ss58_address, block.proposer_signature):
            for event in block.events:
                if verify_data_signature(event.params, event.signature, event.params.get("user_ss58_address")):
                    processed_events.append(event)

            await self.process_events(processed_events)

if __name__ == "__main__":
    config = get_config()
    _comx_client = CommuneClient(get_node_url(use_testnet=config.testnet))
    key = classic_load_key(config.key)
    registered_modules = get_modules(_comx_client, config.netuid)

    if key.ss58_address in [module.ss58_address for module in registered_modules]:
        nat_type, external_ip, external_port = stun.get_ip_info()
        # _comx_client.update_module(
        #     key=key,
        #     name=config.name,
        #     address=f"127.0.0.1:{config.port}",
        #     netuid=config.netuid
        # )
        config.ip = "127.0.0.1"
    else:
        raise Exception(f"Your key: {key.ss58_address} is not registered.")

    # Using an underscore to prevent naming conflicts with other variables later used named 'validator'
    _validator = Validator(config)

    async def run_tasks():
        await asyncio.gather(
            _validator.api.run_server(),
            _validator.initial_sync(),
            _validator.validation_loop(),
            # _validator.create_blocks()
        )

    asyncio.run(run_tasks())
