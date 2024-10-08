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
from typing import Union, List

from smartdrive.commune.models import ModuleInfo
from smartdrive.models.block import Block
from smartdrive.models.event import UserEvent, StoreEvent, RemoveEvent, StoreRequestEvent
from smartdrive.sign import verify_data_signature
from smartdrive.utils import get_stake_from_user, calculate_storage_capacity
from smartdrive.validator.database.database import Database
from smartdrive.validator.node.util.exceptions import BlockIntegrityException


def check_block_integrity(block: Block, database: Database, validators: List[ModuleInfo]):
    if not are_all_block_events_valid(block, database, validators):
        raise BlockIntegrityException(f"Invalid events in {block}")

    if not verify_data_signature(
            data={"block_number": block.block_number, "events": [event.dict() for event in block.events]},
            signature_hex=block.signed_block,
            ss58_address=block.proposer_ss58_address
    ):
        raise BlockIntegrityException(f"Block {block.block_number} data signature not verified")


def _verify_event_signatures(event: Union[StoreEvent, RemoveEvent]) -> bool:
    """
    Verifies the signatures of an individual event.

    Parameters:
        event Union[StoreEvent, RemoveEvent]: The specific Event object (StoreEvent, RemoveEvent).

    Returns:
        bool: True if both the input parameters and event parameters signatures are verified, False otherwise.
    """
    input_params_verified = True
    if isinstance(event, UserEvent):
        input_params_verified = verify_data_signature(event.input_params.dict(), event.input_signed_params, event.user_ss58_address)

    event_params_verified = verify_data_signature(event.event_params.dict(), event.event_signed_params, event.validator_ss58_address)

    return input_params_verified and event_params_verified


def are_all_block_events_valid(block: Block, database: Database, validators: List[ModuleInfo]) -> bool:
    """
    Checks if all events in the block are valid.

    Parameters:
        block (Block): The block containing events to be verified.

    Returns:
        bool: True if all events in the block are valid, False otherwise.
    """
    async def _are_all_block_events_valid(block: Block, database: Database, validators: List[ModuleInfo]) -> bool:
        users_to_check = set()
        for event in block.events:
            if isinstance(event, StoreRequestEvent):
                users_to_check.add(event.user_ss58_address)

        total_stakes = {}
        for user_ss58_address in users_to_check:
            total_stakes[user_ss58_address] = await get_stake_from_user(user_ss58_address=user_ss58_address, validators=validators)

        for event in block.events:
            if not _verify_event_signatures(event):
                return False

            if isinstance(event, StoreRequestEvent):
                total_stake = total_stakes.get(event.user_ss58_address)
                total_size_stored_by_user = database.get_total_file_size_by_user(user_ss58_address=event.user_ss58_address)
                available_storage_of_user = calculate_storage_capacity(total_stake)
                size_available = total_size_stored_by_user + event.input_params.file_size_bytes <= available_storage_of_user
                if not size_available:
                    return False

        return True

    try:
        loop = asyncio.get_running_loop()
        return loop.run_until_complete(_are_all_block_events_valid(block, database, validators))
    except RuntimeError:
        return asyncio.run(_are_all_block_events_valid(block, database, validators))
