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
import random
import time
from typing import List

from communex.balance import from_nano
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.models import ModuleInfo
from smartdrive.commune.request import get_staketo

from smartdrive.logging_config import logger
from smartdrive.sign import sign_data
from smartdrive.utils import MINIMUM_STAKE, INITIAL_STORAGE, ADDITIONAL_STORAGE_PER_COMAI, MAXIMUM_STORAGE
from smartdrive.validator.config import config_manager
from smartdrive.validator.node.connection.connection_pool import Connection
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.sync_service import SyncService
from smartdrive.validator.node.util.message import MessageCode, MessageBody, Message


def prepare_sync_blocks(
        start: int,
        keypair: Keypair,
        sync_service: SyncService,
        active_connections,
        end: int = None,
        validator: str = None,
        request_last_block_to_validators: bool = True
):
    if request_last_block_to_validators:
        request_last_block(key=keypair, connections=active_connections)
        time.sleep(10)

    highest_validator, highest_block_number = sync_service.get_highest_block_validator()
    expected_end = end if end else highest_block_number
    highest_block_validator = validator if validator else highest_validator
    connection = next((connection for connection in active_connections if connection.module.ss58_address == highest_block_validator), None)

    if not connection:
        connection = random.choice(active_connections)

    async def _prepare_sync_blocks():
        if not connection:
            return
        await get_synced_blocks(start, connection, keypair, expected_end)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        asyncio.create_task(_prepare_sync_blocks())
    else:
        asyncio.run(_prepare_sync_blocks())


async def get_synced_blocks(start: int, connection, keypair, end: int = None):
    async def _get_synced_blocks(connection: Connection):
        try:
            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_SYNC,
                data={"start": str(start)}
            )
            if end:
                body.data["end"] = str(end)

            body_sign = sign_data(body.dict(), keypair)

            message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=keypair.public_key.hex()
            )

            send_message(connection, message)
        except Exception:
            logger.error("Error getting synced blocks", exc_info=True)

    await _get_synced_blocks(connection)


def get_file_expiration() -> int:
    """
    Generate a random expiration time in milliseconds within a range.

    Returns:
        int: A random expiration time between 30 minutes (min_ms) and 1 hour (max_ms) in milliseconds.
    """
    min_ms = 30 * 60 * 1000
    max_ms = 1 * 60 * 60 * 1000
    return random.randint(min_ms, max_ms)


async def get_stake_from_user(user_ss58_address: Ss58Address, validators: [ModuleInfo]):
    staketo_modules = await get_staketo(user_ss58_address, config_manager.config.testnet)
    validator_addresses = {validator.ss58_address for validator in validators}
    active_stakes = {address: from_nano(stake) for address, stake in staketo_modules.items() if address in validator_addresses and address != str(user_ss58_address)}

    return sum(active_stakes.values())


def calculate_storage_capacity(stake: float) -> int:
    """
    Calculates the storage capacity based on the user's stake,
    with a maximum limit of MAXIMUM_STORAGE.

    Params:
        stake (float): The current user's stake in COMAI.

    Returns:
        int: The total storage capacity in bytes, capped at MAXIMUM_STORAGE.
    """
    if stake < MINIMUM_STAKE:
        return 0

    total_storage_bytes = INITIAL_STORAGE

    additional_comai = stake - MINIMUM_STAKE
    if additional_comai > 0:
        total_storage_bytes += additional_comai * ADDITIONAL_STORAGE_PER_COMAI

    # Limit the total storage to MAXIMUM_STORAGE in bytes
    return int(min(total_storage_bytes, MAXIMUM_STORAGE))


def request_last_block(key: Keypair, connections: List[Connection]):
    for connection in connections:
        body = MessageBody(code=MessageCode.MESSAGE_CODE_LAST_BLOCK)
        body_sign = sign_data(body.dict(), key)
        request_message = Message(
            body=body,
            signature_hex=body_sign.hex(),
            public_key_hex=key.public_key.hex()
        )
        send_message(connection, request_message)
