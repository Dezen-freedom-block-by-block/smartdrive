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

import os
import argparse
import time
import asyncio

from communex.module.module import Module
from communex.compat.key import classic_load_key
from communex.types import Ss58Address
from substrateinterface import Keypair

import smartdrive
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.logging_config import logger
from smartdrive.commune.connection_pool import initialize_commune_connection_pool
from smartdrive.models.block import Block, MAX_EVENTS_PER_BLOCK, block_to_block_event
from smartdrive.models.event import RemoveEvent
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.utils import DEFAULT_VALIDATOR_PATH
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.validator.config import Config, config_manager
from smartdrive.validator.database.database import Database
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.node import Node, VALIDATION_VOTE_INTERVAL_SECONDS
from smartdrive.validator.api.api import API
from smartdrive.validator.evaluation.evaluation import score_miners, set_weights
from smartdrive.validator.node.connection.connection_pool import INACTIVITY_TIMEOUT_SECONDS as VALIDATOR_INACTIVITY_TIMEOUT_SECONDS
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.util.message import MessageBody, MessageCode, Message
from smartdrive.validator.validation import validate
from smartdrive.validator.utils import prepare_sync_blocks
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
    BLOCK_INTERVAL_SECONDS = 30

    _config = None
    _key: Keypair = None
    _database: Database = None
    api: API = None
    node: Node = None

    def __init__(self):
        super().__init__()
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()
        self.node = Node()
        self.api = API(self.node)

    async def create_blocks(self):
        """
        Periodically attempts to create new blocks by proposing them to the network if the current node is the
        proposer.

        This method operates in an infinite loop, regularly checking whether it's time to vote, validate, or create
        a new block. The process includes validating the current validator's status, handling the initial sync,
        processing events, and ensuring that the block creation and validation intervals are respected.
        """
        last_validation_time = time.monotonic()
        first_validation_vote_launched = False

        while True:
            start_time = time.monotonic()

            try:
                if not first_validation_vote_launched or start_time - last_validation_time >= VALIDATION_VOTE_INTERVAL_SECONDS:
                    logger.info("Starting validation and voting task")
                    asyncio.create_task(self.validate_vote_task())
                    first_validation_vote_launched = True
                    last_validation_time = start_time
            except Exception:
                logger.error("Error validating", exc_info=True)

            try:
                is_current_validator_proposer, active_validators, all_validators = await self.node.get_proposer_validator()
                if is_current_validator_proposer:
                    new_block_number = (self._database.get_last_block_number() or 0) + 1

                    # Trigger the initial sync and reiterate the loop after BLOCK_INTERVAL_SECONDS to verify if
                    # initial_sync_completed has been set to True. This is needed since the response to the
                    # prepare_sync_blocks will be in the background via TCP.
                    # TODO: Improve initial sync
                    if not self.node.initial_sync_completed.value:
                        self.node.initial_sync_completed.value = True
                        if active_validators:
                            prepare_sync_blocks(
                                start=new_block_number,
                                active_connections=self.node.get_connections(),
                                keypair=self._key
                            )
                            await asyncio.sleep(self.BLOCK_INTERVAL_SECONDS)
                            continue

                    block_events = self.node.consume_events(count=MAX_EVENTS_PER_BLOCK)

                    signed_block = sign_data({"block_number": new_block_number, "events": [event.dict() for event in block_events]}, self._key)
                    block = Block(
                        block_number=new_block_number,
                        events=block_events,
                        signed_block=signed_block.hex(),
                        proposer_ss58_address=Ss58Address(self._key.ss58_address)
                    )
                    await self._database.create_block(block=block, is_proposer=is_current_validator_proposer, validators=all_validators)

                    block_event = block_to_block_event(block)
                    body = MessageBody(
                        code=MessageCode.MESSAGE_CODE_BLOCK,
                        data=block_event.dict()
                    )
                    body_sign = sign_data(body.dict(), self._key)
                    message = Message(
                        body=body,
                        signature_hex=body_sign.hex(),
                        public_key_hex=self._key.public_key.hex()
                    )
                    for connection in self.node.get_connections():
                        send_message(connection.socket, message)

                    for event in block_events:
                        if isinstance(event, RemoveEvent):
                            chunks = self._database.get_chunks(file_uuid=event.event_params.file_uuid)
                            miners = await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
                            miners_info_with_chunk = compile_miners_info_and_chunks(miners, chunks)

                            for miner in miners_info_with_chunk:
                                connection = ConnectionInfo(miner["connection"]["ip"], miner["connection"]["port"])
                                miner_info = ModuleInfo(miner["uid"], miner["ss58_address"], connection)
                                await remove_chunk_request(self._key, event.user_ss58_address, miner_info, miner["chunk_uuid"])

                elapsed = time.monotonic() - start_time
                sleep_time = max(0.0, self.BLOCK_INTERVAL_SECONDS - elapsed)
                logger.info(f"Sleeping for {sleep_time:.2f} seconds before trying to create the next block.")
                await asyncio.sleep(sleep_time)

            except Exception:
                logger.error("Error creating blocks", exc_info=True)
                await asyncio.sleep(self.BLOCK_INTERVAL_SECONDS)

    async def validate_vote_task(self):
        miners = [
            miner for miner in await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
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
                await set_weights(score_dict, config_manager.config.netuid, self._key)


if __name__ == "__main__":
    smartdrive.check_version()

    config = get_config()
    config_manager.initialize(config)

    initialize_commune_connection_pool(config_manager.config.testnet)

    key = classic_load_key(config_manager.config.key)

    async def main():
        registered_modules = await get_modules(config_manager.config.netuid)

        if key.ss58_address not in [module.ss58_address for module in registered_modules]:
            raise Exception(f"Your key: {key.ss58_address} is not registered.")

        validator = Validator()

        async def run_tasks():
            # Initial delay to allow active validators to load before request them
            await asyncio.sleep(VALIDATOR_INACTIVITY_TIMEOUT_SECONDS)

            await asyncio.gather(
                validator.api.run_server(),
                validator.create_blocks()
            )

        await run_tasks()

    asyncio.run(main())
