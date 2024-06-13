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
from typing import Optional, List, Dict
from fastapi import Form, UploadFile, HTTPException

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.block import MinerProcess, EventParams, StoreEvent
from smartdrive.validator.models.models import MinerWithChunk
from smartdrive.commune.request import get_active_miners, execute_miner_request, ModuleInfo
from smartdrive.validator.network.network import Network


class StoreAPI:
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

    async def store_endpoint(self, user_ss58_address: Ss58Address = Form(), file: UploadFile = Form(...)):
        """
        Stores a file across multiple active miners.

        This method reads a file uploaded by a user and distributes it among active miners available in the system.
         Once it is distributed sends an event with the related info.

        Params:
            user_ss58_address (Ss58Address): The user's SS58 address.
            file (UploadFile): The file to be uploaded.

        Raises:
            HTTPException: If no active miners are available or if no miner responds with a valid response.
        """
        file_bytes = await file.read()

        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)

        if not active_miners:
            raise HTTPException(status_code=404, detail="Currently there are no active miners")

        # TODO: Split in chunks

        # TODO: Don't use base64, file need be transferred directly.
        base64_bytes = base64.b64encode(file_bytes).decode("utf-8")

        miners_processes: List[MinerProcess] = []
        for miner in active_miners:
            start_time = time.time()
            miner_with_chunk = await store_request(self._key, miner, user_ss58_address, base64_bytes)
            final_time = time.time() - start_time

            succeeded = miner_with_chunk is not None
            miner_process = MinerProcess(
                chunk_uuid=miner_with_chunk.chunk_uuid if succeeded else None,
                miner_ss58_address=miner.ss58_address,
                succeed=succeeded,
                processing_time=final_time
            )

            miners_processes.append(miner_process)

        event_params = EventParams(
            file_uuid=None,
            miners_processes=miners_processes,
        )

        signed_params = sign_data(event_params.__dict__, self._key)

        event = StoreEvent(
            params=event_params,
            signed_params=signed_params.hex(),
            validator_ss58_address=Ss58Address(self._key.ss58_address)
        )

        # Emit event
        self._network.emit_event(event)

        succeeded_responses = list(filter(lambda miner_process: miner_process.succeed, miners_processes))
        if not succeeded_responses:
            raise HTTPException(status_code=404, detail="No miner answered with a valid response")


async def store_request(keypair: Keypair, miner: ModuleInfo, user_ss58_address: Ss58Address, base64_bytes: str) -> Optional[MinerWithChunk]:
    """
     Sends a request to a miner to store a file chunk.

     This method sends an asynchronous request to a specified miner to store a file chunk
     encoded in base64 format. The request includes the user's SS58 address as the folder
     and the base64-encoded chunk.

     Params:
         keypair (Keypair): The validator key used to authorize the request.
         miner (ModuleInfo): The miner's module information containing connection details and SS58 address.
         user_ss58_address (Ss58Address): The SS58 address of the user associated with the file chunk.
         base64_bytes (str): The base64-encoded file chunk to be stored.

     Returns:
         Optional[MinerWithChunk]: An object containing a MinerWithChunk if the storage request is successful, otherwise None.
     """
    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "store",
        {
            "folder": user_ss58_address,
            "chunk": base64_bytes
        }
    )

    if miner_answer:
        return MinerWithChunk(miner.ss58_address, miner_answer["id"])
