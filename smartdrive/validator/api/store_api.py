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
import random
import time
import base64
import uuid

from substrateinterface import Keypair
from typing import Optional
from fastapi import Form, UploadFile, HTTPException, Request

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.database.database import Database
from smartdrive.models.event import MinerProcess, StoreEvent, StoreParams
from smartdrive.validator.models.models import MinerWithChunk
from smartdrive.commune.request import get_active_miners, execute_miner_request, ModuleInfo
from smartdrive.validator.network.network import Network
from smartdrive.validator.utils import calculate_hash


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

    async def store_endpoint(self, request: Request, file: UploadFile = Form(...)):
        """
        Stores a file across multiple active miners.

        This method reads a file uploaded by a user and distributes it among active miners available in the system.
         Once it is distributed sends an event with the related info.

        Params:
            file (UploadFile): The file to be uploaded.

        Raises:
            HTTPException: If no active miners are available or if no miner responds with a valid response.
        """
        user_public_key = request.headers.get("X-Key")
        input_signed_params = request.headers.get("X-Signature")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)
        file_bytes = await file.read()

        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)

        if not active_miners:
            raise HTTPException(status_code=404, detail="Currently there are no active miners")

        store_event = await store_new_file(
            file_bytes=file_bytes,
            miners=active_miners,
            validator_keypair=self._key,
            user_ss58_address=user_ss58_address,
            input_signed_params=input_signed_params
        )

        # Emit event
        self._network.emit_event(store_event)

        # Return response
        succeeded_responses = list(filter(lambda miner_process: miner_process.succeed, store_event.event_params.miners_processes))
        if not succeeded_responses:
            raise HTTPException(status_code=404, detail="No miner answered with a valid response")

        return {"uuid": store_event.event_params.file_uuid}


async def store_new_file(
        file_bytes: bytes,
        miners: list[ModuleInfo],
        validator_keypair: Keypair,
        user_ss58_address: Ss58Address,
        input_signed_params
) -> StoreEvent:
    """
    Stores a new file across a list of miners.

    This method generates file data, encodes it, and sends it to a list of miners to be stored.
    It handles the storage requests asynchronously, logs the responses, and stores information
    about successfully stored chunks in the database.

    Params:
        file_encoded (str): The encoded file data to be stored.
        miners (list[ModuleInfo]): A list of miner objects where the file data will be stored.
        keypair (Keypair): The keypair used for signing requests and creating events.
        user_ss58_address (Ss58Address): The SS58 address of the user storing the file.

    Returns:
        StoreEvent: An StoreEvent representing the storage operation for the file.
    """
    # TODO: Split in chunks
    # TODO: Don't use base64, file need be transferred directly.
    # TODO: Set max length for sub_chunk
    # TODO: Don't store sub_chunk info in params must be store in MineProcess one the split system it's complete
    miners_processes = []

    file_encoded = base64.b64encode(file_bytes).decode("utf-8")

    async def handle_store_request(miner: ModuleInfo):
        start_time = time.time()
        miner_answer = await _store_request(
            keypair=validator_keypair,
            miner=miner,
            user_ss58_address=user_ss58_address,
            base64_bytes=file_encoded
        )
        final_time = time.time() - start_time

        miner_process = MinerProcess(
            chunk_uuid=miner_answer.chunk_uuid,
            miner_ss58_address=miner.ss58_address,
            succeed=True,
            processing_time=final_time
        )
        miners_processes.append(miner_process)

    futures = [
        handle_store_request(miner)
        for miner in miners
    ]
    await asyncio.gather(*futures)

    sub_chunk_end = random.randint(51, len(file_encoded) - 1)
    sub_chunk_start = sub_chunk_end - 50
    sub_chunk_encoded = file_encoded[sub_chunk_start:sub_chunk_end]

    event_params = StoreParams(
        file_uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
        miners_processes=miners_processes,
        sub_chunk_start=sub_chunk_start,
        sub_chunk_end=sub_chunk_end,
        sub_chunk_encoded=sub_chunk_encoded
    )

    signed_params = sign_data(event_params.dict(), validator_keypair)

    event = StoreEvent(
        validator_ss58_address=Ss58Address(validator_keypair.ss58_address),
        event_params=event_params,
        event_signed_params=signed_params.hex(),
        user_ss58_address=user_ss58_address,
        input_params={"file": calculate_hash(file_bytes)},
        input_signed_params=input_signed_params
    )

    return event


async def _store_request(keypair: Keypair, miner: ModuleInfo, user_ss58_address: Ss58Address, base64_bytes: str) -> Optional[MinerWithChunk]:
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
