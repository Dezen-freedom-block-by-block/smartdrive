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

import time
import base64
from substrateinterface import Keypair
from typing import Optional
from fastapi import Form, UploadFile, HTTPException

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.validator.database.database import Database
from smartdrive.validator.models import MinerWithChunk, File, Chunk
from smartdrive.commune.request import get_active_miners, execute_miner_request, ModuleInfo


class StoreAPI:
    _config = None
    _key: Keypair = None
    _database: Database = None
    _comx_client: CommuneClient = None

    def __init__(self, config, key, database, comx_client):
        self._config = config
        self._key = key
        self._database = database
        self._comx_client = comx_client

    async def store_endpoint(self, user_ss58_address: Ss58Address = Form(), file: UploadFile = Form(...)) -> dict[str, str | None]:
        """
        Stores a file across multiple active miners.

        This method reads a file uploaded by a user and distributes it among
        active miners available in the system. It ensures the miners are active
        before storing the file chunks and records the storage details in the database.

        Params:
            user_ss58_address (Ss58Address): The user's SS58 address.
            file (UploadFile): The file to be uploaded.

        Returns:
            dict[str, str | None]: Dict containing the UUID of the file if it was stores successfully.

        Raises:
            HTTPException: If no active miners are available or if no miner responds with a valid response.
        """
        file_bytes = await file.read()

        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)

        if not active_miners:
            raise HTTPException(status_code=404, detail="Currently there are no active miners")

        # TODO: Split in chunks

        # TODO: Don't use base64, file need be trasnfered directly.
        base64_bytes = base64.b64encode(file_bytes).decode("utf-8")

        miner_chunks = []
        for miner in active_miners:
            start_time = time.time()
            miner_with_chunk = await self.store_request(miner, user_ss58_address, base64_bytes)
            final_time = time.time() - start_time
            if miner_with_chunk:
                miner_chunks.append(miner_with_chunk)

            self._database.insert_miner_response(miner.ss58_address, "store", True if miner_with_chunk else False, final_time)

        if not miner_chunks:
            raise HTTPException(status_code=404, detail="No miner answered with a valid response")

        chunks = []
        for miner_chunk in miner_chunks:
            chunks.append(Chunk(
                miner_owner_ss58address=miner_chunk.ss58_address,
                chunk_uuid=miner_chunk.chunk_uuid,
                file_uuid=None,
                sub_chunk=None,
            ))

        file = File(
            user_owner_ss58address=user_ss58_address,
            file_uuid=None,
            chunks=chunks,
            created_at=None,
            expiration_ms=None
        )

        # Store file information in database and return the file UUID
        return {"uuid": self._database.insert_file(file)}

    async def store_request(self, miner: ModuleInfo, user_ss58_address: Ss58Address, base64_bytes: str) -> Optional[MinerWithChunk]:
        """
         Sends a request to a miner to store a file chunk.

         This method sends an asynchronous request to a specified miner to store a file chunk
         encoded in base64 format. The request includes the user's SS58 address as the folder
         and the base64-encoded chunk.

         Params:
            miner (ModuleInfo): The miner's module information containing connection details and SS58 address.
            user_ss58_address (Ss58Address): The SS58 address of the user associated with the file chunk.
            base64_bytes (str): The base64-encoded file chunk to be stored.

         Returns:
            Optional[MinerWithChunk]: An object containing a MinerWithChunk if the storage request is successful, otherwise None.
         """
        miner_answer = await execute_miner_request(
            self._key, miner.connection, miner.ss58_address, "store",
            {
                "folder": user_ss58_address,
                "chunk": base64_bytes
            }
        )

        if miner_answer:
            return MinerWithChunk(miner.ss58_address, miner_answer["id"])

    async def store_event(self):
        print("")
        # TODO: store in database where is located the store event