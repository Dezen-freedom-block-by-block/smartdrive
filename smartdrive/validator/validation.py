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

import asyncio
import time
import uuid
from typing import List, Optional, Tuple

from substrateinterface import Keypair
from communex.types import Ss58Address

from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.store_api import store_new_file
from smartdrive.validator.api.validate_api import validate_chunk_request
from smartdrive.validator.database.database import Database
from smartdrive.validator.evaluation.utils import generate_data
from smartdrive.models.event import RemoveEvent, EventParams, RemoveInputParams, ValidationEvent
from smartdrive.commune.utils import calculate_hash


async def validate(miners: list[ModuleInfo], database: Database, key: Keypair) -> Optional[Tuple[List[RemoveEvent], dict[int, bool]]]:
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
        Optional[Tuple[List[RemoveEvent], dict[int, bool]]: An optional tuple containing a list of
        Events objects and miners and his result.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    if not miners:
        print("Skipping validation, there is not any miner.")
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

    miners_to_store = _determine_miners_to_store(validation_events_with_expiration, validation_events_expired, miners)
    if miners_to_store:
        file_data = generate_data(size_mb=5)
        input_params = {"file": calculate_hash(file_data)}
        input_signed_params = sign_data(input_params, key)

        _, validations_events_per_validator = await store_new_file(
            file_bytes=file_data,
            miners=miners_to_store,
            validator_keypair=key,
            user_ss58_address=Ss58Address(key.ss58_address),
            input_signed_params=input_signed_params.hex(),
            validating=True,
            validators_len=1 # To include myself
        )

        if validations_events_per_validator:
            current_validator_validation_events = validations_events_per_validator[0]
            database.insert_validation_events(current_validator_validation_events)

            # Check if there is no validation for each of the miners who stored the previously generated file. If there
            # is no validation, insert the generated one.
            for validation_event in current_validator_validation_events:
                if validation_event.miner_ss58_address not in miners_ss58_address_in_validation_events_not_expired:
                    miners_ss58_address_in_validation_events_not_expired.append(validation_event.miner_ss58_address)
                    validation_events_not_expired.append(validation_event)

    remove_events = []
    if validation_events_expired:
        remove_events = _generate_remove_events(
            validation_events=validation_events_expired,
            keypair=key
        )

    result_miners = {}
    if validation_events_not_expired:
        result_miners = await _validate_miners(
            miners=miners,
            validation_events=validation_events_not_expired,
            keypair=key
        )

    return remove_events, result_miners


def _generate_remove_events(validation_events: List[ValidationEvent], keypair: Keypair) -> List[RemoveEvent]:
    """
    Removes files from the SmartDrive network and generates removal events.

    This function generates `RemoveEvent` objects that contain details of the removal process. The remove process will
    be processed when the new block will be generated.

    Params:
        validation_events (List[ValidationEvent]): A list of ValidationEvent objects containing the files to be removed.
        keypair (Keypair): The keypair used to authorize and sign the removal requests.

    Returns:
        List[RemoveEvent]: A list of `RemoveEvent` objects, each representing the removal operation for a file.
    """
    events: List[RemoveEvent] = []

    for validation_event in validation_events:
        event_params = EventParams(
            file_uuid=validation_event.file_uuid
        )

        signed_params = sign_data(event_params.dict(), keypair)

        input_params = RemoveInputParams(file_uuid=validation_event.file_uuid)
        input_signed_params = sign_data(input_params.dict(), keypair)

        event = RemoveEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=Ss58Address(validation_event.user_owner_ss58_address),
            input_params=input_params,
            input_signed_params=input_signed_params.hex()
        )
        events.append(event)

    return events


async def _validate_miners(miners: list[ModuleInfo], validation_events: list[ValidationEvent], keypair: Keypair) -> dict[int, bool]:
    """
    Validates the stored chunks across miners.

    This method checks the integrity of chunks stored across various miners
    by comparing the stored data with the original data. It logs the response times and
    success status of each validation request.

    Params:
        miners (list[ModuleInfo]): List of miners objects.
        validation_events (list[ValidationEvent]): A list of ValidationEvent containing relative information for validation.
        keypair (Keypair): The validator key used to authorize the requests.

    Returns:
        dict[int, bool]: A dictionary of miners uid and his result.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    result_miners: dict[int, bool] = {}

    async def handle_validation_request(miner_info: ModuleInfo, validation_event: ValidationEvent) -> bool:
        return await validate_chunk_request(
            keypair=keypair,
            user_owner_ss58_address=Ss58Address(validation_event.user_owner_ss58_address),
            miner_module_info=miner_info,
            validation_event=validation_event
        )

    async def process_file(validation_event: ValidationEvent):
        validation_event_miner_module_info = next((miner for miner in miners if miner.ss58_address == validation_event.miner_ss58_address), None)
        if validation_event_miner_module_info:
            result = await handle_validation_request(validation_event_miner_module_info, validation_event)
            result_miners[int(validation_event_miner_module_info.uid)] = result

    futures = [process_file(validation_event) for validation_event in validation_events]
    await asyncio.gather(*futures)

    return result_miners


def _determine_miners_to_store(validations_with_expiration: list[ValidationEvent], expired_validations_dict: list[ValidationEvent], miners: list[ModuleInfo]):
    """
    Determines which miners should store new files.

    This method decides which miners should be assigned to store new files based on the
    list of current validation events with expiration, expired validation events, and active miners. It ensures that active miners
    that were previously storing expired files and active miners not currently storing any
    files are selected.

    Params:
        validations_with_expiration (list[ValidationEvent]): The list of current ValidationEvent with expiration.
        expired_validations_dict (list[ValidationEvent]): The list of expired ValidationEvent.
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
            for validation_event in expired_validations_dict
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
