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
import random
import time
import uuid
from typing import Optional, Tuple, List
from fastapi import Form, UploadFile, Request
from substrateinterface import Keypair

from communex.compat.key import classic_load_key
from communex.types import Ss58Address

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.utils import MAX_FILE_SIZE, calculate_storage_capacity, format_size
from smartdrive.sign import sign_data
from smartdrive.validator.api.exceptions import RedundancyException, FileTooLargeException, NoMinersInNetworkException, \
    NoValidMinerResponseException, UnexpectedErrorException, HTTPRedundancyException, \
    CommuneNetworkUnreachable as HTTPCommuneNetworkUnreachable
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.event import StoreEvent, StoreParams, StoreInputParams, ChunkParams, ValidationEvent
from smartdrive.validator.models.models import MinerWithChunk, ModuleType
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.node.node import Node
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.utils import get_file_expiration
from smartdrive.commune.utils import calculate_hash

MIN_MINERS_FOR_FILE = 2
MIN_MINERS_REPLICATION_FOR_CHUNK = 2
MAX_MINERS_FOR_FILE = 10
MAX_ENCODED_RANGE = 50
MINER_STORE_TIMEOUT_SECONDS = 60


class StoreAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def store_request_endpoint(self, request: Request):
        # TODO: CREATE STORAGEREQUESTEVENT
        return {"uuid": ""}

    async def store_request_permission_endpoint(self, request: Request):
        # TODO: CHECK IF USER HAS PERMISSION TO STORE FROM DATABASE
        return {}

    async def store_endpoint(self, request: Request, file: UploadFile = Form(...)):
        """
        Stores a file across multiple active miners.

        This method reads a file uploaded by a user and distributes it among active miners available in the SmartDrive network.
        Once it is distributed sends an event with the related info.

        Params:
            request (Request): The incoming request containing necessary headers for validation.
            file (UploadFile): The file to be uploaded.

        Raises:
            FileDoesNotExistException: If the file is bigger than allowed.
            HTTPCommuneNetworkUnreachable: If the Commune network is unreachable.
            NoMinersInNetworkException: If there are no active miners in the SmartDrive network.
            HTTPRedundancyException: If the file redundancy is not guaranteed.
            UnexpectedErrorException: If an unexpected error happened.
            NoValidMinerResponseException: If any miner answered with a valid response.
        """
        user_public_key = request.headers.get("X-Key")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)
        input_signed_params = request.headers.get("X-Signature")
        file_bytes = await file.read()
        file_size = len(file_bytes)
        total_stake = request.state.total_stake

        # TODO: Change in the future
        if file_size > MAX_FILE_SIZE:
            raise FileTooLargeException

        total_size_stored_by_user = self._database.get_total_file_size_by_user(user_ss58_address=user_ss58_address)
        available_storage_of_user = calculate_storage_capacity(total_stake)
        if total_size_stored_by_user + file_size > available_storage_of_user:
            raise FileTooLargeException(
                f"Storage limit exceeded. You have used {format_size(total_size_stored_by_user)} out of {format_size(available_storage_of_user)}. "
                f"The file you are trying to upload is {format_size(file_size)}."
            )

        try:
            miners = await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER)
        except CommuneNetworkUnreachable:
            raise HTTPCommuneNetworkUnreachable

        if not miners:
            raise NoMinersInNetworkException

        active_connections = self._node.get_connections()
        try:
            store_event, validations_events_per_validator = await store_new_file(
                file_bytes=file_bytes,
                miners=miners,
                validator_keypair=self._key,
                user_ss58_address=user_ss58_address,
                input_signed_params=input_signed_params,
                validators_len=len(active_connections) + 1  # To include myself
            )
        except RedundancyException as redundancy_exception:
            raise HTTPRedundancyException(redundancy_exception.message)
        except Exception:  # TODO: Do not capture bare exceptions
            raise UnexpectedErrorException

        if not store_event:
            raise NoValidMinerResponseException

        if validations_events_per_validator:
            self._database.insert_validation_events(validation_events=validations_events_per_validator.pop(0))

            for index, active_connection in enumerate(active_connections):
                data_list = [validations_events.dict() for validations_events in validations_events_per_validator[index]]

                body = MessageBody(
                    code=MessageCode.MESSAGE_CODE_VALIDATION_EVENTS,
                    data={"list": data_list}
                )

                body_sign = sign_data(body.dict(), self._key)

                message = Message(
                    body=body,
                    signature_hex=body_sign.hex(),
                    public_key_hex=self._key.public_key.hex()
                )
                self._node.send_message(active_connection, message)

        self._node.add_event(store_event)

        return {"uuid": store_event.event_params.file_uuid}


async def store_new_file(
        file_bytes: bytes,
        miners: List[ModuleInfo],
        validator_keypair: Keypair,
        user_ss58_address: Ss58Address,
        input_signed_params: str,
        validators_len: int,
        validating: bool = False,
) -> Tuple[Optional[StoreEvent], List[List[ValidationEvent]]]:
    if not validating and len(miners) < MIN_MINERS_FOR_FILE:
        raise RedundancyException

    stored_chunks_results = []
    stored_miner_with_chunk_uuid: List[Tuple[ModuleInfo, str]] = []

    validations_events_per_validator: List[List[ValidationEvent]] = []
    chunks_params: List[ChunkParams] = []

    file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"

    async def handle_store_request(miner: ModuleInfo, chunk_bytes: bytes, chunk_index: int) -> bool:
        miner_answer = await _store_request(
            keypair=validator_keypair,
            miner=miner,
            user_ss58_address=user_ss58_address,
            chunk_bytes=chunk_bytes
        )
        if miner_answer:
            stored_chunks_results.append((miner_answer.chunk_uuid, chunk_index, miner.ss58_address, chunk_bytes))
            stored_miner_with_chunk_uuid.append((miner, miner_answer.chunk_uuid))
            return True
        return False

    async def store_chunk_with_redundancy(chunk: bytes, chunk_index: int):
        available_miners = miners.copy()
        random.shuffle(available_miners)
        tasks = []
        replication_count = 0

        while replication_count < MIN_MINERS_REPLICATION_FOR_CHUNK and available_miners:
            miner = available_miners.pop()
            tasks.append(asyncio.create_task(handle_store_request(miner, chunk, chunk_index)))

            results = await asyncio.gather(*tasks, return_exceptions=True)
            replication_count += sum(1 for result in results if result is True)

            tasks = [task for task, result in zip(tasks, results) if result is not True]

    async def remove_stored_chunks():
        if stored_miner_with_chunk_uuid:
            remove_tasks = [
                asyncio.create_task(
                    remove_chunk_request(
                        keypair=validator_keypair,
                        user_ss58_address=user_ss58_address,
                        miner=miner,
                        chunk_uuid=chunk_uuid
                    )
                )
                for miner, chunk_uuid in stored_miner_with_chunk_uuid
            ]
            await asyncio.gather(*remove_tasks, return_exceptions=True)

    try:
        if validating:
            await asyncio.gather(*[handle_store_request(miner, file_bytes, 0) for miner in miners],
                                 return_exceptions=True)
            if not stored_chunks_results:
                return None, []

        else:
            num_chunks = min(len(miners), MAX_MINERS_FOR_FILE)
            chunk_size = max(1, len(file_bytes) // num_chunks)
            remainder = len(file_bytes) % num_chunks
            chunks_bytes = [file_bytes[i * chunk_size:(i + 1) * chunk_size] for i in range(num_chunks)]

            if remainder:
                chunks_bytes[-1] += file_bytes[-remainder:]

            await asyncio.gather(
                *[store_chunk_with_redundancy(chunk_bytes, index) for index, chunk_bytes in enumerate(chunks_bytes)],
                return_exceptions=True)

            if len(stored_chunks_results) != len(chunks_bytes) * MIN_MINERS_REPLICATION_FOR_CHUNK:
                raise RedundancyException

        # A ChunkParam object is generated per chunk stored
        for chunk_uuid, chunk_index, miner_ss58_address, file in stored_chunks_results:
            chunks_params.append(ChunkParams(
                uuid=chunk_uuid,
                chunk_index=chunk_index,
                miner_ss58_address=miner_ss58_address
            ))

        # A ValidationEvent object is generated for each chunk stored * each validator
        for _ in range(validators_len):
            validator_events_validations = []

            for chunk_uuid, chunk_index, miner_ss58_address, file in stored_chunks_results:
                sub_chunk_start = random.randint(0, max(0, len(file) - MAX_ENCODED_RANGE))
                sub_chunk_end = min(sub_chunk_start + MAX_ENCODED_RANGE, len(file))
                sub_chunk_encoded = file[sub_chunk_start:sub_chunk_end].hex()

                validation_event = ValidationEvent(
                    uuid=chunk_uuid,
                    miner_ss58_address=miner_ss58_address,
                    sub_chunk_start=sub_chunk_start,
                    sub_chunk_end=sub_chunk_end,
                    sub_chunk_encoded=sub_chunk_encoded,
                    file_uuid=f"{int(time.time())}_{str(uuid.uuid4())}" if validating else file_uuid,
                    user_owner_ss58_address=user_ss58_address
                )

                if validating:
                    validation_event.expiration_ms = get_file_expiration()
                    validation_event.created_at = int(time.time() * 1000)

                validator_events_validations.append(validation_event)

            if validator_events_validations:
                validations_events_per_validator.append(validator_events_validations)

        # When converting the TCP StoreEvent message to its object, the chunk parameters are being sorted by their UUID.
        # To ensure the parameter signatures match, we sorted them beforehand.
        # TODO: Ideally, the sorting of chunk parameters should not be produced when converting the TCP StoreEvent message to its object.
        chunks_params.sort(key=lambda c: c.uuid)

        event_params = StoreParams(
            file_uuid=file_uuid,
            chunks_params=chunks_params
        )

        signed_params = sign_data(event_params.dict(), validator_keypair)

        store_event = StoreEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(validator_keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params=StoreInputParams(file=calculate_hash(file_bytes), file_size_bytes=len(file_bytes)),
            input_signed_params=input_signed_params
        )

        return store_event, validations_events_per_validator

    except Exception:
        await remove_stored_chunks()
        raise


async def _store_request(keypair: Keypair, miner: ModuleInfo, user_ss58_address: Ss58Address, chunk_bytes: bytes) -> Optional[MinerWithChunk]:
    """
     Sends a request to a miner to store a file chunk.

     This method sends an asynchronous request to a specified miner to store a file chunk
     in bytes format. The request includes the user's SS58 address as the folder
     and the bytes chunk.

     Params:
         keypair (Keypair): The validator key used to authorize the request.
         miner (ModuleInfo): The miner's module information containing connection details and SS58 address.
         user_ss58_address (Ss58Address): The SS58 address of the user associated with the file chunk.
         chunk_bytes (bytes): The chunk in bytes.

     Returns:
         Optional[MinerWithChunk]: An object containing a MinerWithChunk if the storage request is successful, otherwise None.
     """

    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "store",
        file={
            'folder': user_ss58_address,
            'chunk': chunk_bytes
        },
        timeout=MINER_STORE_TIMEOUT_SECONDS
    )

    return MinerWithChunk(miner.ss58_address, miner_answer["id"]) if miner_answer else None
