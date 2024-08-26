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
import traceback
import uuid
from typing import Optional, Tuple, List

from substrateinterface import Keypair
from fastapi import Form, UploadFile, HTTPException, Request

from communex.compat.key import classic_load_key
from communex.types import Ss58Address

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.event import StoreEvent, StoreParams, StoreInputParams, ChunkEvent
from smartdrive.validator.models.models import MinerWithChunk, ModuleType
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.node.node import Node
from smartdrive.validator.utils import get_file_expiration
from smartdrive.commune.utils import calculate_hash

# TODO: CHANGE VALUES IN PRODUCTION IF IT IS NECESSARY
MIN_MINERS_FOR_FILE = 2
MIN_REPLICATION_FOR_FILE = 2
MAX_MINERS_FOR_FILE = 10
MAX_ENCODED_RANGE = 50


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

        active_validators = self._node.get_active_validators_connections()
        validators_len = len(active_validators) + 1  # To include myself
        store_event, chunk_events = await store_new_file(
            file_bytes=file_bytes,
            miners=miners,
            validator_keypair=self._key,
            user_ss58_address=user_ss58_address,
            input_signed_params=input_signed_params,
            validators_len=validators_len
        )

        if not store_event:
            raise HTTPException(status_code=404, detail="No miner answered with a valid response")

        if chunk_events:
            self._database.insert_validation(chunk_events=chunk_events.pop(0))
            self._node.send_chunk_event_to_validators(connections=active_validators, event=chunk_events)

        self._node.send_event_to_validators(store_event)

        return {"uuid": store_event.event_params.file_uuid}

async def store_new_file(
        file_bytes: bytes,
        miners: List[ModuleInfo],
        validator_keypair: Keypair,
        user_ss58_address: Ss58Address,
        input_signed_params: str,
        validators_len: int,
        validating: bool = False,
) -> Tuple[Optional[StoreEvent], List[List[ChunkEvent]]]:
    if not validating and len(miners) < MIN_MINERS_FOR_FILE:
        raise HTTPException(status_code=400, detail=f"Not enough miners available to meet the minimum requirement of {MIN_MINERS_FOR_FILE} miners for file storage.")

    chunk_event_store: List[ChunkEvent] = []
    chunks_events: List[List[ChunkEvent]] = []
    stored_chunks_result = []
    stored_miners: List[Tuple[ModuleInfo, str]] = []
    created_at = int(time.time() * 1000) if validating else None
    expiration_ms = get_file_expiration() if validating else None
    file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"

    async def handle_store_request(miner: ModuleInfo, chunk_data: bytes, chunk_index: int) -> bool:
        miner_answer = await _store_request(
            keypair=validator_keypair,
            miner=miner,
            user_ss58_address=user_ss58_address,
            file_bytes=chunk_data
        )
        if miner_answer:
            stored_chunks_result.append((miner_answer.chunk_uuid, chunk_index, miner.ss58_address, chunk_data))
            stored_miners.append((miner, miner_answer.chunk_uuid))
            return True
        return False

    async def store_chunk_with_redundancy(chunk_data: bytes, chunk_index: int):
        available_miners = miners.copy()
        random.shuffle(available_miners)
        tasks = []
        replication_count = 0

        while replication_count < MIN_REPLICATION_FOR_FILE and available_miners:
            miner = available_miners.pop()
            tasks.append(asyncio.create_task(handle_store_request(miner, chunk_data, chunk_index)))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            replication_count += sum(1 for result in results if result is True)

            tasks = [task for task, result in zip(tasks, results) if result is not True]

    async def remove_stored_chunks():
        if stored_miners:
            remove_tasks = [
                asyncio.create_task(
                    remove_chunk_request(
                        keypair=validator_keypair,
                        user_ss58_address=user_ss58_address,
                        miner=miner,
                        chunk_uuid=chunk_uuid
                    )
                )
                for miner, chunk_uuid in stored_miners
            ]
            await asyncio.gather(*remove_tasks)

    try:
        if validating:
            await asyncio.gather(*[handle_store_request(miner, file_bytes, 0) for miner in miners])
            if not stored_chunks_result:
                return None, []
        else:
            num_chunks = min(len(miners), MAX_MINERS_FOR_FILE)
            chunk_size = max(1, len(file_bytes) // num_chunks)
            remainder = len(file_bytes) % num_chunks
            chunks = [file_bytes[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

            if remainder:
                chunks[-1] += file_bytes[-remainder:]

            await asyncio.gather(*[store_chunk_with_redundancy(chunk, index) for index, chunk in enumerate(chunks)])

            if len(stored_chunks_result) != len(chunks) * MIN_REPLICATION_FOR_FILE:
                raise HTTPException(status_code=500, detail="Failed to store all chunks in the required number of miners.")

        for chunk_uuid, chunk_index, miner_ss58_address, file in stored_chunks_result:
            chunk_event_store.append(ChunkEvent(
                uuid=chunk_uuid,
                chunk_index=chunk_index,
                miner_ss58_address=miner_ss58_address
            ))

        for _ in range(validators_len):
            chunk_event = []
            for chunk_uuid, chunk_index, miner_ss58_address, file in stored_chunks_result:
                sub_chunk_start = random.randint(0, max(0, len(file) - MAX_ENCODED_RANGE))
                sub_chunk_end = min(sub_chunk_start + MAX_ENCODED_RANGE, len(file))
                sub_chunk_encoded = file[sub_chunk_start:sub_chunk_end].hex()

                chunk = ChunkEvent(
                    uuid=chunk_uuid,
                    chunk_index=chunk_index,
                    miner_ss58_address=miner_ss58_address,
                    sub_chunk_start=sub_chunk_start,
                    sub_chunk_end=sub_chunk_end,
                    sub_chunk_encoded=sub_chunk_encoded,
                    file_uuid=file_uuid,
                    user_owner_ss58_address=user_ss58_address
                )

                if validating:
                    chunk.expiration_ms = expiration_ms
                    chunk.created_at = created_at

                chunk_event.append(chunk)
            if chunk_event:
                chunks_events.append(chunk_event)

        chunk_event_store.sort(key=lambda c: c.uuid)
        event_params = StoreParams(
            file_uuid=file_uuid,
            created_at=created_at,
            expiration_ms=expiration_ms,
            chunks=chunk_event_store
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
        ), chunks_events

    except Exception:
        await remove_stored_chunks()
        raise

async def _store_request(keypair: Keypair, miner: ModuleInfo, user_ss58_address: Ss58Address, file_bytes: bytes) -> Optional[MinerWithChunk]:
    """
     Sends a request to a miner to store a file chunk.

     This method sends an asynchronous request to a specified miner to store a file chunk
     in bytes format. The request includes the user's SS58 address as the folder
     and the bytes chunk.

     Params:
         keypair (Keypair): The validator key used to authorize the request.
         miner (ModuleInfo): The miner's module information containing connection details and SS58 address.
         user_ss58_address (Ss58Address): The SS58 address of the user associated with the file chunk.
         file_bytes (bytes): The chunk in bytes.

     Returns:
         Optional[MinerWithChunk]: An object containing a MinerWithChunk if the storage request is successful, otherwise None.
     """

    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "store",
        file={
           'folder': user_ss58_address,
           'chunk': file_bytes
        }
    )

    if miner_answer:
        return MinerWithChunk(miner.ss58_address, miner_answer["id"])
