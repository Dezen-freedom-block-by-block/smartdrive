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

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.commune.request import get_active_miners, ModuleInfo, get_filtered_modules
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.store_api import store_new_file
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.validator.api.validate_api import validate_chunk_request
from smartdrive.validator.database.database import Database
from smartdrive.validator.evaluation.utils import generate_data
from smartdrive.models.event import RemoveEvent, ValidateEvent, StoreEvent, MinerProcess, EventParams, RemoveParams, \
    RemoveInputParams
from smartdrive.validator.models.models import File, ModuleType, SubChunk
from smartdrive.validator.utils import calculate_hash


async def validate_step(database: Database, key: Keypair, comx_client: CommuneClient, netuid: int) -> Optional[Tuple[List[RemoveEvent], List[ValidateEvent], Optional[StoreEvent]]]:
    """
    Performs a validation step in the process.

    This function retrieves potentially expired files, deletes them if necessary, and creates new files to replace
    the deleted ones. It also validates files that have not expired.

    Params:
        database (Database): The database instance to operate on.
        key (Keypair): The keypair used for signing requests.
        comx_client (CommuneClient): The client used to interact with the commune network.
        netuid (int): The network UID used to filter the active miners.

    Returns:
        Optional[Tuple[List[RemoveEvent], List[ValidateEvent], StoreEvent]]: An optional tuple containing a list of Event objects.
    """
    active_miners = await get_active_miners(key, comx_client, netuid)
    if not active_miners:
        print("Skipping validation step, there is not any active miner.")
        return

    # Get files with expiration
    files = database.get_files_with_expiration(Ss58Address(key.ss58_address))

    # Split them in expired or not
    expired_files = []
    non_expired_files = []
    current_timestamp = int(time.time() * 1000)
    for file in files:
        expired_files.append(file) if file.has_expired(current_timestamp) else non_expired_files.append(file)

    remove_events, validate_events, store_event = [], [], None

    # Remove expired files
    if expired_files:
        remove_events = await _remove_files(
            files=expired_files,
            keypair=key,
            comx_client=comx_client,
            netuid=netuid
        )

    # Validate non expired files
    if non_expired_files:
        validate_events = await _validate_miners(
            files=non_expired_files,
            keypair=key,
            comx_client=comx_client,
            netuid=netuid
        )

    # TODO: Move store before validate to check the new files
    # Store new file
    miners_to_store = _determine_miners_to_store(files, expired_files, active_miners)
    if miners_to_store:
        file_data = generate_data(5)
        input_params = {"file": calculate_hash(file_data)}
        input_signed_params = sign_data(input_params, key)

        store_event = await store_new_file(
            file_bytes=file_data,
            miners=miners_to_store,
            validator_keypair=key,
            user_ss58_address=Ss58Address(key.ss58_address),
            input_signed_params=input_signed_params.hex()
        )

    return remove_events, validate_events, store_event


async def _remove_files(files: List[File], keypair: Keypair, comx_client: CommuneClient, netuid: int) -> List[RemoveEvent]:
    """
    Removes files from the SmartDrive network and generates removal events.

    This function processes a list of files to be removed by sending removal requests to the respective miners
    and generates `RemoveEvent` objects that contain details of the removal process.

    Params:
        files (List[File]): A list of `File` objects representing the files to be removed.
        keypair (Keypair): The keypair used to authorize and sign the removal requests.
        comx_client (CommuneClient): The client used to interact with the commune network.
        netuid (int): The network UID used to filter the miners.

    Returns:
        List[RemoveEvent]: A list of `RemoveEvent` objects, each representing the removal operation for a file.
    """
    miners = get_filtered_modules(comx_client, netuid, ModuleType.MINER)
    events: List[RemoveEvent] = []

    async def handle_remove_request(miner_info: ModuleInfo, chunk_uuid: str):
        start_time = time.time()
        remove_request_succeed = await remove_chunk_request(keypair, Ss58Address(keypair.ss58_address), miner_info, chunk_uuid)
        final_time = time.time() - start_time

        return MinerProcess(
            chunk_uuid=chunk_uuid,
            miner_ss58_address=miner_info.ss58_address,
            succeed=remove_request_succeed,
            processing_time=final_time
        )

    async def process_file(file: File):
        miner_processes = []
        for chunk in file.chunks:
            for miner in miners:
                if miner.ss58_address == chunk.miner_owner_ss58address:
                    miner_process = await handle_remove_request(miner, chunk.chunk_uuid)
                    miner_processes.append(miner_process)

        event_params = RemoveParams(
            file_uuid=file.file_uuid,
            miners_processes=miner_processes,
        )

        signed_params = sign_data(event_params.dict(), keypair)

        input_params = RemoveInputParams(file_uuid=file.file_uuid)
        input_signed_params = sign_data(input_params.dict(), keypair)

        event = RemoveEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=Ss58Address(file.user_owner_ss58address),
            input_params=input_params,
            input_signed_params=input_signed_params.hex()
        )
        events.append(event)

    futures = [process_file(file) for file in files]
    await asyncio.gather(*futures)

    return events


async def _validate_miners(files: list[File], keypair: Keypair, comx_client: CommuneClient, netuid: int) -> List[ValidateEvent]:
    """
    Validates the stored sub-chunks across active miners.

    This method checks the integrity of sub-chunks stored across various active miners
    by comparing the stored data with the original data. It logs the response times and
    success status of each validation request.

    Params:
        files (list[File]): A list of files containing chunks to be validated.
        keypair (Keypair): The validator key used to authorize the requests.
        comx_client (CommuneClient): The client used to interact with the commune network.
        netuid (int): The network UID used to filter the active miners.

    Returns:
        List[ValidateEvent]: A list of ValidateEvent objects, each representing the validation operation for a sub-chunk.
    """
    events: List[ValidateEvent] = []

    active_miners = await get_active_miners(keypair, comx_client, netuid)
    if not active_miners:
        return events

    sub_chunks = list(chunk.sub_chunk is not None for file in files for chunk in file.chunks)
    has_sub_chunks = any(sub_chunks)

    if not has_sub_chunks:
        return events

    async def handle_validation_request(miner_info: ModuleInfo, user_owner_ss58_address: Ss58Address, subchunk: SubChunk):
        start_time = time.time()
        validate_request_succeed = await validate_chunk_request(
            keypair=keypair,
            user_owner_ss58_address=user_owner_ss58_address,
            miner_module_info=miner_info,
            subchunk=subchunk
        )
        final_time = time.time() - start_time

        miner_process = MinerProcess(
            chunk_uuid=subchunk.chunk_uuid,
            miner_ss58_address=miner_info.ss58_address,
            succeed=validate_request_succeed,
            processing_time=final_time
        )
        event_params = EventParams(
            file_uuid=subchunk.chunk_uuid,
            miners_processes=[miner_process],
        )

        signed_params = sign_data(event_params.dict(), keypair)

        event = ValidateEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
        )
        events.append(event)

    async def process_file(file: File):
        for chunk in file.chunks:
            if chunk.sub_chunk is not None:
                chunk_miner_module_info = next((miner for miner in active_miners if miner.ss58_address == chunk.miner_owner_ss58address), None)
                if chunk_miner_module_info:
                    await handle_validation_request(chunk_miner_module_info, file.user_owner_ss58address, chunk.sub_chunk)

    futures = [process_file(file) for file in files]
    await asyncio.gather(*futures)

    return events


def _determine_miners_to_store(files: list[File], expired_files_dict: list[File], active_miners: list[ModuleInfo]):
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
