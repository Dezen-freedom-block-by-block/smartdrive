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
from typing import List
from fastapi import HTTPException, Form
from substrateinterface import Keypair

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.commune.request import get_active_miners, execute_miner_request, ModuleInfo, get_filtered_modules
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.utils import get_miner_info_with_chunk
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.block import RemoveEvent, EventParams, MinerProcess, Event
from smartdrive.validator.models.models import File, ModuleType
from smartdrive.validator.network.network import Network


class RemoveAPI:
    _config = None
    _key: Keypair = None
    _database: Database = None
    _comx_client: CommuneClient = None
    _network: Network = None

    def __init__(self, config, key, database, comx_client, network: Network):
        self._config = config
        self._key = key
        self._database = database
        self._comx_client = comx_client
        self._network = network

    async def remove_endpoint(self, user_ss58_address: Ss58Address = Form(), file_uuid: str = Form()) -> dict[str, bool]:
        """
        Send an event with the user's purpose to remove a specific file.

        Params:
            user_ss58_address: The address of the user who owns the file.
            file_uuid: The UUID of the file to be removed.

        Returns:
            dict[str, bool]: Dictionary containing if the removal operation was successful.

        Raises:
           HTTPException: If the file does not exist, there are no miners with the file, or some miners failed to delete the file.
        """
        # Check if the file exists
        file_exists = self._database.check_if_file_exists(user_ss58_address, file_uuid)
        if not file_exists:
            raise HTTPException(status_code=404, detail="The file does not exist")

        # Get miners and chunks for the file
        miner_chunks = self._database.get_miner_chunks(file_uuid)
        if not miner_chunks:
            raise HTTPException(status_code=404, detail="Currently there are no miners with this file name")

        # Get active miners
        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)
        if not active_miners:
            raise HTTPException(status_code=404, detail="Currently there are no active miners")

        miners_with_chunks = get_miner_info_with_chunk(active_miners, miner_chunks)

        # Create event
        miners_processes: List[MinerProcess] = []
        for miner_chunk in miners_with_chunks:
            miner_process = MinerProcess(
                chunk_uuid=miner_chunk["chunk_uuid"],
                miner_ss58_address=miner_chunk["ss58_address"]
            )
            miners_processes.append(miner_process)

        event_params = EventParams(
            file_uuid=file_uuid,
            miners_processes=miners_processes,
        )

        signed_params = sign_data(event_params.__dict__, self._key)

        event = RemoveEvent(
            params=event_params,
            signed_params=signed_params.hex(),
            validator_ss58_address=Ss58Address(self._key.ss58_address)
        )

        # Emit event
        self._network.emit_event(event)


async def remove_files(files: List[File], keypair: Keypair, comx_client: CommuneClient, netuid: int):
    """
    Handles the removal of multiple files.

    This function iterates over a list of files and handles the removal of each file's chunks from the associated miners.
    For each file, it gathers all related miner processes and creates a RemoveEvent that records the removal operation's details.

    Args:
        files (list[File]): A list of file objects to be removed. Each File object contains information about its chunks.
        keypair (Keypair): The validator key used to authorize the request.
        comx_client (CommuneClient): The client used to interact with the commune network.
        netuid (int): The network UID used to filter the miners.

    Returns:
        List[Event]: A list of RemoveEvent objects, each representing the removal operation for a file.
    """
    miners = get_filtered_modules(comx_client, netuid, ModuleType.MINER)
    events: List[Event] = []

    async def handle_remove_request(miner_info: ModuleInfo, chunk_uuid: str):
        start_time = time.time()
        remove_request_succeed = await _remove_chunk_request(keypair, Ss58Address(keypair.ss58_address), miner_info, chunk_uuid)
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

        event_params = EventParams(
            file_uuid=file.file_uuid,
            miners_processes=miner_processes,
        )

        signed_params = sign_data(event_params.__dict__, keypair)

        event = RemoveEvent(
            params=event_params,
            signed_params=signed_params.hex(),
            validator_ss58_address=Ss58Address(keypair.ss58_address)
        )
        events.append(event)

    futures = [process_file(file) for file in files]
    await asyncio.gather(*futures)

    return events


async def _remove_chunk_request(keypair: Keypair, user_ss58_address: Ss58Address, miner: ModuleInfo, chunk_uuid: str) -> bool:
    """
    Sends a request to a miner to remove a specific data chunk.

    This method sends an asynchronous request to a specified miner to remove a data chunk
    identified by its UUID. The request is executed using the miner's connection and
    address information.

    Params:
        keypair (Keypair): The validator key used to authorize the request.
        user_ss58_address (Ss58Address): The SS58 address of the user associated with the data chunk.
        miner (ModuleInfo): The miner's module information.
        chunk_uuid (str): The UUID of the data chunk to be removed.

    Returns:
        bool: Returns True if the miner confirms the removal request, otherwise False.
    """
    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "remove",
        {
            "folder": user_ss58_address,
            "chunk_uuid": chunk_uuid
        }
    )
    return True if miner_answer else False
