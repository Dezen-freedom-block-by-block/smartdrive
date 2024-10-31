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
from typing import Union, List, Tuple

from smartdrive.commune.request import get_filtered_modules
from smartdrive.models.block import Block
from smartdrive.models.event import UserEvent, StoreEvent, RemoveEvent, StoreRequestEvent
from smartdrive.sign import verify_data_signature
from smartdrive.validator.utils import get_stake_from_user, calculate_storage_capacity
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.util.exceptions import BlockIntegrityException, InvalidSignatureException, InvalidStorageRequestException


def check_block_integrity(block: Block, database: Database):
    if get_invalid_events(block.events, database):
        raise BlockIntegrityException(f"Invalid events in {block}")

    if not verify_data_signature(
            data={"block_number": block.block_number, "events": [event.dict() for event in block.events]},
            signature_hex=block.signed_block,
            ss58_address=block.proposer_ss58_address
    ):
        raise BlockIntegrityException(f"Block {block.block_number} data signature not verified")


def get_invalid_events(events: List[Union[StoreEvent, RemoveEvent, StoreRequestEvent]], database: Database) -> Union[List[Tuple[Union[StoreEvent, RemoveEvent, StoreRequestEvent], Exception]], asyncio.Task]:
    """
    Returns a list with the invalid events in a block

    Parameters:
        events (List[Union[StoreEvent, RemoveEvent, StoreRequestEvent]]): The events to be verified.
        database (Database): The database instance to operate on.

    Returns:
        list or Task: A list of tuples where each tuple contains an invalid event and its associated exception, or a Task if the event loop is running.
    """

    async def _get_invalid_events():
        invalid_events = []
        storage_requests_users = set()

        for event in events:
            if isinstance(event, StoreRequestEvent):
                storage_requests_users.add(event.user_ss58_address)

        if storage_requests_users:
            validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR, config_manager.config.testnet, without_address=True)

            total_stakes = {}
            for user_ss58_address in storage_requests_users:
                total_stakes[user_ss58_address] = await get_stake_from_user(
                    user_ss58_address=user_ss58_address,
                    validators=validators
                )

        for event in events:
            try:
                verify_event_signatures(event)

                if isinstance(event, StoreRequestEvent) and event.event_params.approved:
                    total_stake = total_stakes.get(event.user_ss58_address)
                    total_size_stored_by_user = database.get_total_file_size_by_user(user_ss58_address=event.user_ss58_address)
                    available_storage_of_user = calculate_storage_capacity(total_stake)
                    size_available = total_size_stored_by_user + event.input_params.file_size_bytes <= available_storage_of_user
                    if not size_available:
                        raise InvalidStorageRequestException()

            except (InvalidSignatureException, InvalidStorageRequestException) as e:
                invalid_events.append((event, e))

        return invalid_events

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        return asyncio.create_task(_get_invalid_events())
    else:
        return asyncio.run(_get_invalid_events())


def verify_event_signatures(event: Union[StoreEvent, RemoveEvent, StoreRequestEvent]):
    """
    Verifies the signatures of an individual event.

    Parameters:
        event Union[StoreEvent, RemoveEvent]: The specific Event object (StoreEvent, RemoveEvent).

    Raises:
        InvalidSignatureException: If the signs are not valid.
    """
    input_params_verified = True
    if isinstance(event, UserEvent):
        input_params_verified = verify_data_signature(event.input_params.dict(), event.input_signed_params, event.user_ss58_address)

        # RemoveEvent created by validators (check_stake_task)
        if isinstance(event, RemoveEvent) and not input_params_verified:
            input_params_verified = verify_data_signature(event.input_params.dict(), event.input_signed_params, event.validator_ss58_address)

    event_params_verified = verify_data_signature(event.event_params.dict(), event.event_signed_params, event.validator_ss58_address)

    if not input_params_verified or not event_params_verified:
        raise InvalidSignatureException()
