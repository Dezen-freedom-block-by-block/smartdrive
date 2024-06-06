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
from typing import Dict

from fastapi import HTTPException, Form
from substrateinterface import Keypair

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.commune.request import get_active_miners, execute_miner_request, ConnectionInfo, ModuleInfo
from smartdrive.validator.api.utils import get_miner_info_with_chunk
from smartdrive.validator.database.database import Database


class RemoveAPI:
    _config = None
    _key: Keypair = None
    _database: Database = None
    _comx_client: CommuneClient = None

    def __init__(self, config, key, database, comx_client):
        self._config = config
        self._key = key
        self._database = database
        self._comx_client = comx_client

    async def remove_endpoint(self, user_ss58_address: Ss58Address = Form(), file_uuid: str = Form()) -> dict[str, bool]:
        """
        Remove a file and its chunks from the database and notify active miners to delete their copies.

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

        miners = get_miner_info_with_chunk(active_miners, miner_chunks)

        # Notify miners to delete their chunks
        miner_answers = []
        for miner in miners:
            start_time = time.time()
            connection = ConnectionInfo(miner["connection"]["ip"], miner["connection"]["port"])
            miner_info = ModuleInfo(
                miner["uid"],
                miner["ss58_address"],
                connection
            )
            remove_request_succeed = await self.remove_request(user_ss58_address, miner_info, miner["chunk_uuid"])
            final_time = time.time() - start_time
            if remove_request_succeed:
                miner_answers.append(miner["ss58_address"])
                
            self._database.insert_miner_response(miner["ss58_address"], "remove", remove_request_succeed, final_time)

        # Check if all miners successfully deleted the file
        if len(miner_answers) == len(miners):
            return {"removed": self._database.remove_file(file_uuid)}

        else:
            raise HTTPException(status_code=404, detail="Some miners have not deleted the file, please try again.")

            # TODO: sync with other validators

    async def remove_request(self, user_ss58_address: Ss58Address, miner: ModuleInfo, chunk_uuid: str) -> bool:
        """
        Sends a request to a miner to remove a specific data chunk.

        This method sends an asynchronous request to a specified miner to remove a data chunk
        identified by its UUID. The request is executed using the miner's connection and
        address information.

        Params:
            user_ss58_address (Ss58Address): The SS58 address of the user associated with the data chunk.
            miner (ModuleInfo): The miner's module information.
            chunk_uuid (str): The UUID of the data chunk to be removed.

        Returns:
            bool: Returns True if the miner confirms the removal request, otherwise False.
        """
        miner_answer = await execute_miner_request(
            self._key, miner.connection, miner.ss58_address, "remove",
            {
                "folder": user_ss58_address,
                "chunk_uuid": chunk_uuid
            }
        )
        return True if miner_answer else False
