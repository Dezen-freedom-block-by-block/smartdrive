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
from typing import Optional
from fastapi import HTTPException
from substrateinterface import Keypair

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.validator.api.utils import get_miner_info_with_chunk
from smartdrive.validator.database.database import Database
from smartdrive.commune.request import get_active_miners, execute_miner_request, ConnectionInfo, ModuleInfo


class RetrieveAPI:
    _config = None
    _key: Keypair = None
    _database: Database = None
    _comx_client: CommuneClient = None

    def __init__(self, config, key, database, comx_client):
        self._config = config
        self._key = key
        self._database = database
        self._comx_client = comx_client

    async def retrieve_endpoint(self, user_ss58_address: Ss58Address, file_uuid: str):
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
        # TODO: This method currently is assuming that chunks are final files. This is an early stage.
        file_exists = self._database.check_if_file_exists(user_ss58_address, file_uuid)

        if not file_exists:
            print("The file not exists")
            raise HTTPException(status_code=404, detail="File does not exist")

        miner_chunks = self._database.get_miner_chunks(file_uuid)
        if not miner_chunks:
            print("Currently no miner has any chunk")
            raise HTTPException(status_code=404, detail="Currently no miner has any chunk")

        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)
        if not active_miners:
            print("Currently there are no active miners")
            raise HTTPException(status_code=404, detail="Currently there are no active miners")

        miner_info_with_chunk = get_miner_info_with_chunk(active_miners, miner_chunks)

        for miner in miner_info_with_chunk:
            start_time = time.time()
            connection = ConnectionInfo(miner["connection"]["ip"], miner["connection"]["port"])
            miner_info = ModuleInfo(
                miner["uid"],
                miner["ss58_address"],
                connection
            )
            chunk = await self.retrieve_request(user_ss58_address, miner_info, miner["chunk_uuid"])
            final_time = time.time() - start_time
            miner["chunk"] = chunk

            self._database.insert_miner_response(miner["ss58_address"], "retrieve", True if chunk else False, final_time)

        if miner_info_with_chunk[0]["chunk"]:
            return miner_info_with_chunk[0]["chunk"]

        else:
            print("The chunk does not exists")
            raise HTTPException(status_code=404, detail="The chunk does not exist")

    async def retrieve_request(self, user_ss58address: Ss58Address, miner: ModuleInfo, chunk_uuid: str) -> Optional[str]:
        miner_answer = await execute_miner_request(
            self._key, miner.connection, miner.ss58_address, "retrieve",
            {
                "folder": user_ss58address,
                "chunk_uuid": chunk_uuid
            }
        )

        return miner_answer["chunk"] if miner_answer else None
