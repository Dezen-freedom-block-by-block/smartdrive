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
import zipfile
from substrateinterface import Keypair

from communex._common import get_node_url
from communex.client import CommuneClient
from communex.module.module import Module
from communex.compat.key import classic_load_key

import smartdrive
from smartdrive.commune.module._protocol import create_headers
from smartdrive.validator.database.database import Database
from smartdrive.validator.api.api import API
from smartdrive.validator.evaluation.evaluation import score_miner, set_weights
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.network.network import Network
from smartdrive.validator.step import validate_step
from smartdrive.validator.utils import extract_sql_file, fetch_with_retries
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.commune.request import (get_modules, ConnectionInfo, get_filtered_modules, get_truthful_validators)


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

    MAX_RETRIES = 3
    RETRY_DELAY = 5

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
        self._comx_client = CommuneClient(url=get_node_url(use_testnet=self._config.testnet), num_connections=5)
        self._network = Network(keypair=self._key, ip=self._config.ip, netuid=self._config.netuid, comx_client=self._comx_client, database=self._database, testnet=self._config.testnet)
        self.api = API(self._config, self._key, self._database, self._comx_client, self._network)

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

            result = await validate_step(
                database=self._database,
                key=self._key,
                comx_client=self._comx_client,
                netuid=self._config.netuid
            )

            if result is not None:
                remove_events, validate_events, store_event = result
                # TODO: Sent to block creation

            # Set weights to miners
            score_dict = {}
            for miner in get_filtered_modules(self._comx_client, self._config.netuid, ModuleType.MINER):
                if miner.ss58_address == self._key.ss58_address:
                    continue
                avg_miner_response_time = self._database.get_avg_miner_response_time(miner.ss58_address)
                successful_store_responses, total_store_responses = self._database.get_successful_responses_and_total(miner.ss58_address, "store")
                successful_responses, total_responses = self._database.get_successful_responses_and_total(miner.ss58_address)

                score_dict[int(miner.uid)] = score_miner(successful_store_responses, total_store_responses, avg_miner_response_time, successful_responses, total_responses)

            if not score_dict:
                print("Skipping set weights")
                return

            await set_weights(score_dict, self._config.netuid, self._comx_client, self._key, self._config.testnet)

            elapsed = time.time() - start_time

            if elapsed < self.ITERATION_INTERVAL:
                sleep_time = self.ITERATION_INTERVAL - elapsed
                print(f"Sleeping for {sleep_time}")
                await asyncio.sleep(sleep_time)

    async def initial_sync(self):
        """
        Performs the initial synchronization by fetching database versions from truthful active validators,
        selecting the validator with the highest version, downloading the database, and importing it.
        """
        active_validators = await get_truthful_validators(self._key, self._comx_client, self._config.netuid)
        block_number = self._database.get_database_block() or -1

        if not active_validators:
            # Retry once more if no active validators are found initially
            active_validators = await get_truthful_validators(self._key, self._comx_client, self._config.netuid)

        if not active_validators:
            return

        headers = create_headers(sign_data({}, self._key), self._key)
        active_validators_database = []

        for validator in active_validators:
            response = await fetch_with_retries("database-block", validator.connection, headers=headers, timeout=30, retries=self.MAX_RETRIES, delay=self.RETRY_DELAY)
            if response and response.status_code == 200:
                try:
                    active_validators_database.append({
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

        if not active_validators_database:
            return

        while active_validators_database:
            validator = max(active_validators_database, key=lambda obj: obj["database_block"])

            if block_number > validator["database_block"]:
                print("Skipping database import... you have a larger version")
                return

            connection = ConnectionInfo(validator["connection"]["ip"], validator["connection"]["port"])
            headers = create_headers(sign_data({}, self._key), self._key)
            answer = await fetch_with_retries("database", connection, headers=headers, timeout=30, retries=self.MAX_RETRIES, delay=self.RETRY_DELAY)

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
                active_validators_database.remove(validator)

        print("No more validators available.")
        return


if __name__ == "__main__":
    config = get_config()
    _comx_client = CommuneClient(get_node_url(use_testnet=config.testnet))
    key = classic_load_key(config.key)
    registered_modules = get_modules(_comx_client, config.netuid)

    if key.ss58_address in [module.ss58_address for module in registered_modules]:
        nat_type, external_ip, external_port = stun.get_ip_info()
        _comx_client.update_module(
            key=key,
            name=config.name,
            address=f"127.0.0.1:{config.port}",
            netuid=config.netuid
        )
        config.ip = "127.0.0.1"
    else:
        raise Exception(f"Your key: {key.ss58_address} is not registered.")

    # Using an underscore to prevent naming conflicts with other variables later used named 'validator'
    _validator = Validator(config)

    async def run_tasks():
        await _validator.initial_sync()
        await asyncio.gather(
            _validator.api.run_server(),
            _validator.validation_loop(),
            _validator._network.create_blocks()
        )

    asyncio.run(run_tasks())
