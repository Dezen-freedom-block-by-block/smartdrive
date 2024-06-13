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
from typing import List, Dict

from communex.client import CommuneClient
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.request import get_active_miners, execute_miner_request, ModuleInfo
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.models.block import ValidateEvent, MinerProcess, EventParams
from smartdrive.validator.models.models import File, SubChunk


async def validate_miners(files: list[File], keypair: Keypair, comx_client: CommuneClient, netuid: int) -> List[ValidateEvent]:
    """
    Validates the stored sub-chunks across active miners.

    This method checks the integrity of sub-chunks stored across various active miners
    by comparing the stored data with the original data. It logs the response times and
    success status of each validation request.

    Args:
        files (list[File]): A list of files containing chunks to be validated.
        keypair (Keypair): The validator key used to authorize the requests.
        comx_client (CommuneClient): The client used to interact with the commune network.
        netuid (int): The network UID used to filter the active miners.

    Returns:
        List[ValidateEvent]: A list of ValidateEvent objects, each representing the validation operation for a sub-chunk.
    """
    active_miners = await get_active_miners(keypair, comx_client, netuid)
    if not active_miners:
        return []

    sub_chunks = list(chunk.sub_chunk is not None for file in files for chunk in file.chunks)
    has_sub_chunks = any(sub_chunks)

    if not has_sub_chunks:
        return []

    events: List[ValidateEvent] = []

    async def handle_validation_request(miner_info: ModuleInfo, user_owner_ss58_address: Ss58Address, subchunk: SubChunk):
        start_time = time.time()
        validate_request_succeed = await _validate_chunk_request(
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

        signed_params = sign_data(event_params.__dict__, keypair)

        event = ValidateEvent(
            params=event_params,
            signed_params=signed_params.hex(),
            validator_ss58_address=Ss58Address(keypair.ss58_address)
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


async def _validate_chunk_request(keypair: Keypair, user_owner_ss58_address: Ss58Address, miner_module_info: ModuleInfo, subchunk: SubChunk) -> bool:
    """
    Sends a request to a miner to validate a specific sub-chunk.

    This method sends an asynchronous request to a specified miner to validate a sub-chunk
    identified by its UUID. The request is executed using the miner's connection and
    address information.

    Params:
        keypair (Keypair): The validator key used to authorize the request.
        user_owner_ss58_address (Ss58Address): The SS58 address of the user associated with the sub-chunk.
        miner_module_info (ModuleInfo): The miner's module information.
        subchunk (SubChunk): The sub-chunk to be validated.

    Returns:
        bool: Returns True if the miner confirms the validation request, otherwise False.
    """
    miner_answer = await execute_miner_request(
        keypair, miner_module_info.connection, miner_module_info.ss58_address, "validation",
        {
            "folder": user_owner_ss58_address,
            "chunk_uuid": subchunk.chunk_uuid,
            "start": subchunk.start,
            "end": subchunk.end
        }
    )
    return True if miner_answer else False