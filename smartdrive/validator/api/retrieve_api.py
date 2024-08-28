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
import time
import uuid
from typing import Optional, List
from fastapi import HTTPException, Request

from communex.compat.key import classic_load_key
from starlette.responses import StreamingResponse
from substrateinterface import Keypair
from communex.types import Ss58Address

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import get_miner_info_with_chunk
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.models.event import RetrieveEvent, MinerProcess, EventParams, RetrieveInputParams
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
            user_ss58_address (Ss58Address): The SS58 address of the user associated with the file.
            file_uuid (str): The UUID of the file to be retrieved.

        Returns:
            chunk: The retrieved file chunk if available, None otherwise.

        Raises:
            HTTPException: If the file does not exist, no miner has the chunk, or no active miners are available.
        """
        user_public_key = request.headers.get("X-Key")
        input_signed_params = request.headers.get("X-Signature")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)

        file = self._database.get_file(user_ss58_address, file_uuid)
        if not file:
            print("The file not exists")
            raise HTTPException(status_code=404, detail="File does not exist")

        chunks = self._database.get_chunks(file_uuid)
        if not chunks:
            print("Currently no miner has any chunk")
            raise HTTPException(status_code=404, detail="Currently no miner has any chunk")

        try:
            miners = get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
        except CommuneNetworkUnreachable:
            raise HTTPException(status_code=404, detail="Commune network is unreachable")

        if not miners:
            print("Currently there are no miners")
            raise HTTPException(status_code=404, detail="Currently there are no miners")

        # Group miner_chunks by chunk_index
        miners_with_chunks = get_miner_info_with_chunk(miners, chunks)
        miner_chunks_by_index = {}
        for miner_chunk in miners_with_chunks:
            chunk_index = miner_chunk["chunk_index"]
            if chunk_index not in miner_chunks_by_index:
                miner_chunks_by_index[chunk_index] = []
            miner_chunks_by_index[chunk_index].append(miner_chunk)

        # Shuffle the items in each list within the dictionary
        for chunk_index in miner_chunks_by_index:
            random.shuffle(miner_chunks_by_index[chunk_index])

        # Create event
        miners_processes: List[MinerProcess] = []
        chunks = []

        async def retrieve_chunk_from_miners(user_ss58_address, miners_with_chunks, miner_processes):
            for miner_chunk in miners_with_chunks:
                start_time = time.monotonic()
                connection = ConnectionInfo(miner_chunk["connection"]["ip"], miner_chunk["connection"]["port"])
                miner_info = ModuleInfo(
                    miner_chunk["uid"],
                    miner_chunk["ss58_address"],
                    connection
                )
                chunk = await retrieve_request(self._key, user_ss58_address, miner_info, miner_chunk["chunk_uuid"])
                final_time = time.monotonic() - start_time
                succeed = chunk is not None

                miner_process = MinerProcess(
                    chunk_uuid=miner_chunk["chunk_uuid"],
                    miner_ss58_address=miner_chunk["ss58_address"],
                    succeed=succeed,
                    processing_time=final_time
                )
                miner_processes.append(miner_process)

                if succeed:
                    return chunk
            return None

        async def fetch_chunk(chunk_index, miner_chunks_for_index):
            chunk = await retrieve_chunk_from_miners(user_ss58_address, miner_chunks_for_index, miners_processes)
            return (chunk_index, chunk)

        # Use asyncio.gather to fetch all chunks concurrently
        fetch_tasks = [
            fetch_chunk(chunk_index, miner_chunks_for_index)
            for chunk_index, miner_chunks_for_index in miner_chunks_by_index.items()
        ]

        fetched_chunks = await asyncio.gather(*fetch_tasks)

        for chunk_index, chunk in fetched_chunks:
            if chunk is not None:
                chunks.append((chunk_index, chunk))

        # Sort chunks by chunk_index
        chunks.sort(key=lambda x: x[0])

        # Combine all chunks into a single file
        final_file = b''.join(chunk for _, chunk in chunks)

        event_params = EventParams(
            file_uuid=file_uuid,
            miners_processes=miners_processes,
        )

        signed_params = sign_data(event_params.dict(), self._key)

        event = RetrieveEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(self._key.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params=RetrieveInputParams(file_uuid=file_uuid),
            input_signed_params=input_signed_params
        )

        # Emit event
        self._node.send_event_to_validators(event)

        if len(chunks) == file.total_chunks:
            return StreamingResponse(io.BytesIO(final_file), media_type='application/octet-stream')
        else:
            print("The file currently is not available")
            raise HTTPException(status_code=404, detail="The file currently is not available")


async def retrieve_request(keypair: Keypair, user_ss58address: Ss58Address, miner: ModuleInfo, chunk_uuid: str) -> Optional[str]:
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
