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
import io
import random
from typing import Optional
from fastapi import Request

from communex.compat.key import classic_load_key
from starlette.responses import StreamingResponse
from substrateinterface import Keypair
from communex.types import Ss58Address

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.validator.api.exceptions import FileDoesNotExistException, \
    CommuneNetworkUnreachable as HTTPCommuneNetworkUnreachable, NoMinersInNetworkException, FileNotAvailableException
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.node import Node


class RetrieveAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def retrieve_endpoint(self, request: Request, file_uuid: str):
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
            miners = get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
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

        async def _retrieve_request_task(chunk_index, miners_info_with_chunk):
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
                chunk = await _retrieve_request(self._key, user_ss58_address, miner_info, miner_info_with_chunk["chunk_uuid"])
                if chunk:
                    return (chunk_index, chunk)

            return (chunk_index, None)

        retrieve_request_tasks = [
            _retrieve_request_task(chunk_index, miners_info_with_chunk)
            for chunk_index, miners_info_with_chunk in miners_info_with_chunk_ordered_by_chunk_index.items()
        ]
        retrieve_requests = await asyncio.gather(*retrieve_request_tasks)

        chunks = []
        for chunk_index, chunk in retrieve_requests:
            if chunk is not None:
                chunks.append((chunk_index, chunk))

        if len(chunks) == file.total_chunks:
            # Sort chunks by chunk_index
            chunks.sort(key=lambda x: x[0])

            # Combine all chunks into a single file
            final_file = b''.join(chunk for _, chunk in chunks)
            return StreamingResponse(io.BytesIO(final_file), media_type='application/octet-stream')

        else:
            raise FileNotAvailableException


async def _retrieve_request(keypair: Keypair, user_ss58address: Ss58Address, miner: ModuleInfo, chunk_uuid: str) -> Optional[str]:
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

    Returns:
        Optional[str]: Returns the chunk UUID, or None if something went wrong.
    """
    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "retrieve",
        {
            "folder": user_ss58address,
            "chunk_uuid": chunk_uuid
        }
    )

    return miner_answer if miner_answer else None
