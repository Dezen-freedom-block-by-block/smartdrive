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
import os
import random
import shutil
import time
import uuid
from typing import Optional

import aiofiles
from fastapi import Request, BackgroundTasks

from communex.compat.key import classic_load_key
from starlette.responses import StreamingResponse
from substrateinterface import Keypair
from communex.types import Ss58Address

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.utils import DEFAULT_VALIDATOR_PATH
from smartdrive.validator.api.exceptions import FileDoesNotExistException, \
    CommuneNetworkUnreachable as HTTPCommuneNetworkUnreachable, NoMinersInNetworkException, FileNotAvailableException, \
    ChunkNotAvailableException
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.node import Node


MINER_RETRIEVE_TIMEOUT_SECONDS = 180
MAX_SIMULTANEOUS_DOWNLOADS = 4


class RetrieveAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def retrieve_endpoint(self, request: Request, file_uuid: str, background_tasks: BackgroundTasks):
        """
        Retrieves a file chunk from active miners.

        This method checks if a file exists for a given user SS58 address and file UUID.
        If the file exists, it proceeds to find and retrieve the corresponding chunks
        from active miners.

        Params:
            file_uuid (str): The UUID of the file to be retrieved.

        Returns:
            chunk: The retrieved file chunk if available, None otherwise.

        Raises:
            FileDoesNotExistException: If the file does not exist or no miner has the chunk.
            CommuneNetworkUnreachable: If the commune network is unreachable.
            NoMinersInNetworkException: If there are no active miners in the SmartDrive network.
            FileNotAvailableException: If the file is not fully available.
        """
        user_public_key = request.headers.get("X-Key")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)

        file = self._database.get_file(user_ss58_address, file_uuid)
        if not file:
            raise FileDoesNotExistException

        chunks = self._database.get_chunks(file_uuid)
        if not chunks:
            # Using the same error detail as above as the end-user experience is essentially the same
            raise FileDoesNotExistException

        try:
            miners = await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
        except CommuneNetworkUnreachable:
            raise HTTPCommuneNetworkUnreachable

        if not miners:
            raise NoMinersInNetworkException

        miners_info_with_chunk = compile_miners_info_and_chunks(miners, chunks)

        miners_info_with_chunk_ordered_by_chunk_index = {}
        for miner_info_with_chunk in miners_info_with_chunk:
            chunk_index = miner_info_with_chunk["chunk_index"]

            if chunk_index not in miners_info_with_chunk_ordered_by_chunk_index:
                miners_info_with_chunk_ordered_by_chunk_index[chunk_index] = []

            miners_info_with_chunk_ordered_by_chunk_index[chunk_index].append(miner_info_with_chunk)

        # Randomly shuffle the list of miners for each chunk to ensure that requests are not always made to the same miner
        for chunk_index in miners_info_with_chunk_ordered_by_chunk_index:
            random.shuffle(miners_info_with_chunk_ordered_by_chunk_index[chunk_index])

        path = os.path.expanduser(DEFAULT_VALIDATOR_PATH)
        user_path = os.path.join(path, f"{int(time.time())}_{str(uuid.uuid4())}")
        os.makedirs(user_path, exist_ok=True)

        async def cleanup(user_path: str):
            if user_path and os.path.exists(user_path):
                shutil.rmtree(user_path)

        background_tasks.add_task(cleanup, user_path)

        async def _retrieve_request_task(chunk_index, miners_info_with_chunk, semaphore):
            async with semaphore:
                for miner_info_with_chunk in miners_info_with_chunk:
                    connection = ConnectionInfo(
                        miner_info_with_chunk["connection"]["ip"],
                        miner_info_with_chunk["connection"]["port"]
                    )
                    miner_info = ModuleInfo(
                        miner_info_with_chunk["uid"],
                        miner_info_with_chunk["ss58_address"],
                        connection
                    )
                    chunk = await _retrieve_request(self._key, user_ss58_address, miner_info, miner_info_with_chunk["chunk_uuid"], chunk_index, user_path)
                    if chunk:
                        return chunk_index, chunk
                raise ChunkNotAvailableException

        semaphore = asyncio.Semaphore(MAX_SIMULTANEOUS_DOWNLOADS)

        retrieve_request_tasks = [
            _retrieve_request_task(chunk_index, miners_info_with_chunk, semaphore)
            for chunk_index, miners_info_with_chunk in miners_info_with_chunk_ordered_by_chunk_index.items()
        ]

        try:
            retrieve_requests = await asyncio.gather(*retrieve_request_tasks)
        except ChunkNotAvailableException:
            # If any chunk fails to be retrieved, we stop the process and raise the exception
            shutil.rmtree(user_path)
            raise FileNotAvailableException

        received_chunks = {chunk_index: chunk_path for chunk_index, chunk_path in retrieve_requests if chunk_path is not None}

        received_same_as_required_chunks = len(received_chunks) == file.total_chunks
        if received_same_as_required_chunks:

            sorted_chunks = [received_chunks[i] for i in range(file.total_chunks)]
            async def iter_combined_chunks():
                for chunk_path in sorted_chunks:
                    async with aiofiles.open(chunk_path, 'rb') as f:
                        while True:
                            chunk = await f.read(16384)
                            if not chunk:
                                break
                            yield chunk

            return StreamingResponse(iter_combined_chunks(), media_type='application/octet-stream')

        else:
            raise FileNotAvailableException


async def _retrieve_request(keypair: Keypair, user_ss58address: Ss58Address, miner: ModuleInfo, chunk_uuid: str, chunk_index: str, user_path: str) -> Optional[str]:
    """
    Sends a request to a miner to retrieve a specific data chunk.

    This method sends an asynchronous request to a specified miner to retrieve a data chunk
    identified by its UUID. The request is executed using the miner's connection and
    address information.

    Params:
        keypair (Keypair): The validator key used to authorize the request.
        user_ss58_address (Ss58Address): The SS58 address of the user associated with the data chunk.
        miner (ModuleInfo): The miner's module information.
        chunk_uuid (str): The UUID of the data chunk to be retrieved.
        chunk_index (str): The index of the data chunk to be retrieved.
        user_path (str): The path of the user associated with the data chunk.

    Returns:
        Optional[str]: Returns the chunk UUID, or None if something went wrong.
    """
    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "retrieve",
        {
            "folder": user_ss58address,
            "chunk_uuid": chunk_uuid,
            "chunk_index": chunk_index,
            "user_path": user_path
        },
        timeout=MINER_RETRIEVE_TIMEOUT_SECONDS
    )

    return miner_answer if miner_answer else None
