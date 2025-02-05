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
import os.path
import time
from typing import List, Optional

from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.utils import calculate_hash
from smartdrive.logging_config import logger
from smartdrive.commune.models import ModuleInfo
from smartdrive.models.event import ValidationEvent
from smartdrive.config import DEFAULT_VALIDATOR_PATH
from smartdrive.validator.api.store_api import store_validation_file
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.validator.api.validate_api import validate_chunk_request
from smartdrive.validator.database.database import Database
from smartdrive.validator.evaluation.utils import generate_data


async def validate(miners: List[ModuleInfo], database: Database, key: Keypair) -> Optional[dict[int, bool]]:
    """
    This function retrieves the current validations from the network and performs the following processes:
    1. Separates the current validations into validations with expiration and validations without expiration.
    2. Divides the validations with expiration into expired or non-expired validations. It includes validations without
       expiration as non-expired and ensures that there is always a single validation per validator in the non-expired
       validations, which, whenever possible, will be a validation without expiration, but if not, it will be a validation
       with expiration.
    3. Stores a file with an expiration on the network and generates a validation event for each miner who stored the file.
    4. Checks if the miners who stored the file currently have a validation event in the `validation_events_not_expired` array;
       if not, it inserts the newly generated validation event.
    5. Finally, it removes the expired validations and validates the miners with the non-expired validations.

    Params:
        miners (list[ModuleInfo]): List of miners objects.
        database (Database): The database instance to operate on.
        key (Keypair): The keypair used for signing requests.

    Returns:
        Optional[dict[int, bool]: An optional dict containing the miner UUID and the validation result.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    if not miners:
        logger.info("Skipping validation, there is not any miner.")
        return

    validation_events_expired, validation_events_not_expired = [], []

    # These validations are not refreshed from the database because we only need to validate the current ones,
    # along with the new ones created by this process.
    validation_events_without_expiration = database.get_random_validation_events_without_expiration_per_miners(miners)
    validation_events_with_expiration = database.get_validation_events_with_expiration()

    # Include as not expired validations validation events without expiration, since later behaviour is the same
    validation_events_not_expired.extend(validation for validation in validation_events_without_expiration)

    current_timestamp = int(time.time() * 1000)
    miners_ss58_address_in_validation_events_not_expired = [validation_event.miner_ss58_address for validation_event in validation_events_not_expired]

    for validation_event in validation_events_with_expiration:
        if current_timestamp > (validation_event.created_at + validation_event.expiration_ms):
            validation_events_expired.append(validation_event)
        else:
            # This ensures we avoid adding duplicate validation events for the same miner, particularly when the miner
            # has a non-expired events.
            if validation_event.miner_ss58_address not in miners_ss58_address_in_validation_events_not_expired:
                miners_ss58_address_in_validation_events_not_expired.append(validation_event.miner_ss58_address)
                validation_events_not_expired.append(validation_event)

    # Check if any miner has both expired and non-expired validations
    miners_with_expired_and_non_expired_validations = set(
        validation_event.miner_ss58_address for validation_event in validation_events_expired
    ).intersection(
        miners_ss58_address_in_validation_events_not_expired
    )

    # Remove expired validations
    if validation_events_expired:
        expired_events_to_remove = [
            event for event in validation_events_expired
            if event.miner_ss58_address in miners_with_expired_and_non_expired_validations or not miners_with_expired_and_non_expired_validations
        ]
        await _remove_expired_validations(
            validation_events_expired=expired_events_to_remove,
            miners=miners,
            database=database,
            keypair=key
        )

    # Handle creating new validation events if there are no conflicts with expired validations
    miners_to_store = _determine_miners_to_store(validation_events_with_expiration, validation_events_expired, miners)
    if miners_to_store and not miners_with_expired_and_non_expired_validations:
        path = os.path.expanduser(DEFAULT_VALIDATOR_PATH)
        os.makedirs(path, exist_ok=True)
        validation_path = os.path.join(path, "validation.bin")
        file_path = generate_data(size_mb=5, file_path=validation_path)
        chunk_size = os.path.getsize(file_path)
        chunk_hash = await calculate_hash(file_path)

        validations_events = await store_validation_file(
            chunk_path=file_path,
            chunk_size=chunk_size,
            chunk_hash=chunk_hash,
            miners=miners_to_store,
            validator_keypair=key,
            user_ss58_address=Ss58Address(key.ss58_address)
        )

        if validations_events:
            database.insert_validation_events(validations_events)

            # Check if there is no validation for each of the miners who stored the previously generated file. If there
            # is no validation, insert the generated one.
            for validation_event in validations_events:
                if validation_event.miner_ss58_address not in miners_ss58_address_in_validation_events_not_expired:
                    miners_ss58_address_in_validation_events_not_expired.append(validation_event.miner_ss58_address)
                    validation_events_not_expired.append(validation_event)

    # Validate miners using the non-expired validations
    result_miners = {}
    if validation_events_not_expired:
        result_miners = await _validate_miners(
            validation_events_not_expired=validation_events_not_expired,
            miners=miners,
            keypair=key
        )

    return result_miners


async def _remove_expired_validations(validation_events_expired: List[ValidationEvent], miners: List[ModuleInfo], database: Database, keypair: Keypair):
    """
    Removes expired validations from the SmartDrive network. It also removes the expired validations in the current validator database.

    Params:
        validation_events_expired (List[ValidationEvent]): A list of ValidationEvent objects containing the files to be removed.
        miners (List[ModuleInfo]): A list of ModuleInfo objects representing all the miners used in the validation process.
        keypair (Keypair): The keypair used to authorize and sign the removal requests.
    """
    async def _remove_task(validation_event: ValidationEvent) -> Optional[bool]:
        for miner in miners:
            if miner.ss58_address == validation_event.miner_ss58_address:
                return await remove_chunk_request(keypair, Ss58Address(validation_event.user_owner_ss58_address), miner, validation_event.uuid)
        return None

    tasks = []
    for validation_event in validation_events_expired:
        tasks.append(_remove_task(validation_event))

        database.remove_file(validation_event.file_uuid)

    await asyncio.gather(*tasks, return_exceptions=True)


async def _validate_miners(validation_events_not_expired: List[ValidationEvent], miners: List[ModuleInfo], keypair: Keypair) -> dict[int, bool]:
    """
    Validates the stored chunks across miners.

    This method checks the integrity of chunks stored across various miners
    by comparing the stored data with the original data. It logs the response times and
    success status of each validation request.

    Params:
        validation_events_not_expired (List[ValidationEvent]): A list of ValidationEvent objects containing relative info for the validation.
        miners (List[ModuleInfo]): A list of ModuleInfo objects representing all the miners used in the validation process.
        keypair (Keypair): The validator key used to authorize the requests.

    Returns:
        dict[int, bool]: A dictionary of miners uid and his result.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    result_miners: dict[int, bool] = {}

    async def _validation_task(validation_event: ValidationEvent):
        validation_event_miner_module_info = next((miner for miner in miners if miner.ss58_address == validation_event.miner_ss58_address), None)
        if validation_event_miner_module_info:
            result = await validate_chunk_request(
                keypair=keypair,
                user_owner_ss58_address=Ss58Address(validation_event.user_owner_ss58_address),
                miner_module_info=validation_event_miner_module_info,
                validation_event=validation_event
            )
            result_miners[int(validation_event_miner_module_info.uid)] = result

    futures = [_validation_task(validation_event) for validation_event in validation_events_not_expired]
    await asyncio.gather(*futures, return_exceptions=True)

    return result_miners


def _determine_miners_to_store(validations_with_expiration: List[ValidationEvent], validation_events_expired: List[ValidationEvent], miners: List[ModuleInfo]):
    """
    Determines which miners should store new files.

    This method decides which miners should be assigned to store new files based on the
    list of current validation events with expiration, expired validation events, and active miners. It ensures that active miners
    that were previously storing expired files and active miners not currently storing any
    files are selected.

    Params:
        validations_with_expiration (list[ValidationEvent]): The list of current ValidationEvent with expiration.
        validation_events_expired (list[ValidationEvent]): The list of expired ValidationEvent.
        miners (list[ModuleInfo]): The list of miners.

    Returns:
        list[ModuleInfo]: The list of miners that should store new files.
    """
    miners_to_store = []

    if not validations_with_expiration:
        miners_to_store = miners

    else:
        expired_miners_ss58_address = {
            validation_event.miner_ss58_address
            for validation_event in validation_events_expired
        }

        for miner in miners:
            if miner.ss58_address in expired_miners_ss58_address:
                miners_to_store.append(miner)

        # Add miners without any file
        users_ss58_addresses_having_files = [
            validation_event.miner_ss58_address
            for validation_event in validations_with_expiration
        ]
        for miner in miners:
            if miner.ss58_address not in users_ss58_addresses_having_files:
                miners_to_store.append(miner)

    return miners_to_store
