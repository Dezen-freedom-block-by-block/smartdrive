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
from typing import Optional
from substrateinterface import Keypair
from fastapi import Form, UploadFile, HTTPException, Request

from communex.compat.key import classic_load_key
from communex.types import Ss58Address

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.event import MinerProcess, StoreEvent, StoreParams, StoreInputParams
from smartdrive.validator.models.models import MinerWithChunk, ModuleType
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.node.node import Node
from smartdrive.validator.utils import calculate_hash


class StoreAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

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

        try:
            miners = get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
        except CommuneNetworkUnreachable:
            raise HTTPException(status_code=404, detail="Commune network is unreachable")

        if not miners:
            raise HTTPException(status_code=404, detail="Currently there are no miners")

        # TODO: right now, we just save the file in one miner, we have to change it in the future when they are split
        #  into chunks
        store_event = await store_new_file(
            file_bytes=file_bytes,
            miners=miners,
            validator_keypair=self._key,
            user_ss58_address=user_ss58_address,
            input_signed_params=input_signed_params,
            save_in_one_miner=True
        )

        if not store_event:
            raise HTTPException(status_code=404, detail="No miner answered with a valid response")

        # Emit event
        self._node.send_event_to_validators(store_event)

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
        input_signed_params: str,
        save_in_one_miner: bool = False
) -> StoreEvent | None:
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
        StoreEvent | None: An StoreEvent representing the storage operation for the file or None.
    """
    # TODO: Split in chunks
    # TODO: Don't use base64, file need be transferred directly.
    # TODO: Set max length for sub_chunk
    # TODO: Don't store sub_chunk info in params must be stored in MineProcess once the split system it's completed
    miners_processes = []

    file_encoded = base64.b64encode(file_bytes).decode("utf-8")

    async def handle_store_request(miner: ModuleInfo) -> bool:
        start_time = time.time()
        miner_answer = await _store_request(
            keypair=validator_keypair,
            miner=miner,
            user_ss58_address=user_ss58_address,
            base64_bytes=file_encoded
        )
        final_time = time.time() - start_time

        if miner_answer:
            miners_processes.append(MinerProcess(
                chunk_uuid=miner_answer.chunk_uuid,
                miner_ss58_address=miner.ss58_address,
                succeed=True,
                processing_time=final_time
            ))
            return True
        else:
            return False

    if save_in_one_miner:
        random.shuffle(miners)
        for miner in miners:
            if await handle_store_request(miner):
                break
    else:
        await asyncio.gather(*[handle_store_request(miner) for miner in miners])

    if miners_processes:
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

        return StoreEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(validator_keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params=StoreInputParams(file=calculate_hash(file_bytes)),
            input_signed_params=input_signed_params
        )

    return None


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
