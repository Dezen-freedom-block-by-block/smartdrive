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
from smartdrive.models.event import RemoveEvent, EventParams, RemoveInputParams, ChunkEvent
from smartdrive.commune.utils import calculate_hash


async def validate_step(miners: list[ModuleInfo], database: Database, key: Keypair, validators_len: int) -> Optional[Tuple[List[RemoveEvent], List[ChunkEvent], dict[int, bool]]]:
    """
    Performs a validation step in the process.

    This function retrieves potentially expired files, deletes them if necessary, and creates new files to replace
    the deleted ones. It also validates files that have not expired.

    Params:
        miners (list[ModuleInfo]): List of miners objects.
        database (Database): The database instance to operate on.
        key (Keypair): The keypair used for signing requests.
        netuid (int): The network UID used to filter the active miners.
        validators_len (int): Validators len.

    Returns:
        Optional[Tuple[List[RemoveEvent], List[ChunkEvent], dict[int, bool]]: An optional tuple containing a list of
        Events objects and miners and his result.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    if not miners:
        print("Skipping validation step, there is not any miner.")
        return

    expired_chunks, non_expired_chunks, remove_events, chunk_events, result_miners = [], [], [], [], {}
    current_timestamp = int(time.time() * 1000)

    chunks_events_without_expiration = database.get_validation_without_expiration(registered_miners=miners)
    chunks_events_with_expiration = database.get_chunk_events_with_expiration()

    # Split chunks_events in expired or not expired
    for chunk_event in chunks_events_with_expiration:
        if current_timestamp > (chunk_event.created_at + chunk_event.expiration_ms):
            expired_chunks.append(chunk_event)
        else:
            non_expired_chunks.append(chunk_event)

    existing_miners_non_expired_chunks = {chunk.miner_ss58_address: chunk for chunk in non_expired_chunks}
    non_expired_chunks.extend(
        chunk for chunk in chunks_events_without_expiration
        if chunk.miner_ss58_address not in existing_miners_non_expired_chunks
    )

    miners_to_store = _determine_miners_to_store(chunks_events_with_expiration, expired_chunks, miners)

    if miners_to_store:
        file_data = generate_data(5)
        input_params = {"file": calculate_hash(file_data)}
        input_signed_params = sign_data(input_params, key)

        _, chunk_events_per_validator = await store_new_file(
            file_bytes=file_data,
            miners=miners_to_store,
            validator_keypair=key,
            user_ss58_address=Ss58Address(key.ss58_address),
            input_signed_params=input_signed_params.hex(),
            validating=True,
            validators_len=validators_len
        )

        if chunk_events_per_validator:
            chunk_events.extend(chunk_events_per_validator[0])

    # Get remove events
    if expired_chunks:
        remove_events = _remove_files(
            chunk_events=expired_chunks,
            keypair=key
        )

    # Validate non expired files
    if non_expired_chunks:
        result_miners = await _validate_miners(
            miners=miners,
            chunk_events=non_expired_chunks,
            keypair=key,
        )

    return remove_events, chunk_events, result_miners


def _remove_files(chunk_events: List[ChunkEvent], keypair: Keypair) -> List[RemoveEvent]:
    """
    Removes files from the SmartDrive network and generates removal events.

    This function generates `RemoveEvent` objects that contain details of the removal process. The remove process will
    be processed when the new block will be generated.

    Params:
        files (List[File]): A list of ChunkEvent objects representing the files to be removed.
        keypair (Keypair): The keypair used to authorize and sign the removal requests.

    Returns:
        List[RemoveEvent]: A list of `RemoveEvent` objects, each representing the removal operation for a file.
    """
    events: List[RemoveEvent] = []

    for chunk in chunk_events:
        event_params = EventParams(
            file_uuid=chunk.file_uuid
        )

        signed_params = sign_data(event_params.dict(), keypair)

        input_params = RemoveInputParams(file_uuid=chunk.file_uuid)
        input_signed_params = sign_data(input_params.dict(), keypair)

        event = RemoveEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=Ss58Address(chunk.user_owner_ss58_address),
            input_params=input_params,
            input_signed_params=input_signed_params.hex()
        )
        events.append(event)

    return events


async def _validate_miners(miners: list[ModuleInfo], chunk_events: list[ChunkEvent], keypair: Keypair) -> dict[int, bool]:
    """
    Validates the stored chunks across miners.

    This method checks the integrity of chunks stored across various miners
    by comparing the stored data with the original data. It logs the response times and
    success status of each validation request.

    Params:
        miners (list[ModuleInfo]): List of miners objects.
        chunk_events (list[ChunkEvent]): A list of ChunkEvent containing chunks to be validated.
        keypair (Keypair): The validator key used to authorize the requests.
        netuid (int): The network UID used to filter the miners.

    Returns:
        dict[int, bool]: A dictionary of miners uid and his result.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    result_miners: dict[int, bool] = {}

    async def handle_validation_request(miner_info: ModuleInfo, chunk_event: ChunkEvent) -> bool:
        return await validate_chunk_request(
            keypair=keypair,
            user_owner_ss58_address=Ss58Address(chunk_event.user_owner_ss58_address),
            miner_module_info=miner_info,
            chunk_event=chunk_event
        )

    async def process_file(chunk_event: ChunkEvent):
        chunk_miner_module_info = next((miner for miner in miners if miner.ss58_address == chunk_event.miner_ss58_address), None)
        if chunk_miner_module_info:
            result = await handle_validation_request(chunk_miner_module_info, chunk_event)
            result_miners[int(chunk_miner_module_info.uid)] = result

    futures = [process_file(chunk) for chunk in chunk_events]
    await asyncio.gather(*futures)

    return result_miners


def _determine_miners_to_store(chunks_with_expiration: list[ChunkEvent], expired_chunks_dict: list[ChunkEvent], miners: list[ModuleInfo]):
    """
    Determines which miners should store new files.

    This method decides which miners should be assigned to store new files based on the
    list of current ChunkEvent, expired ChunkEvent, and active miners. It ensures that active miners
    that were previously storing expired files and active miners not currently storing any
    files are selected.

    Params:
        chunks_with_expiration (list[ChunkEvent]): The list of current ChunkEvent with expiration.
        expired_chunks_dict (list[ChunkEvent]): The list of expired ChunkEvent.
        miners (list[ModuleInfo]): The list of miners.

    Returns:
        list[ModuleInfo]: The list of miners that should store new files.
    """
    miners_to_store = []

    if not chunks_with_expiration:
        miners_to_store = miners

    else:
        # Map expired miner ss58_address
        expired_miners_ss58_address = {
            chunk_event.miner_ss58_address
            for chunk_event in expired_chunks_dict
        }

        # Add expired miner to list
        for miner in miners:
            if miner.ss58_address in expired_miners_ss58_address:
                miners_to_store.append(miner)

        # Add miners without any file
        users_ss58_addresses_having_files = [
            chunk_event.miner_ss58_address
            for chunk_event in chunks_with_expiration
        ]

        for miner in miners:
            if miner.ss58_address not in users_ss58_addresses_having_files:
                miners_to_store.append(miner)

    return miners_to_store
