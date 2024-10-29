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
import hashlib
import os
import random
import shutil
import time
import uuid
from typing import Optional, Tuple, List, Union, AsyncGenerator

import aiofiles
from fastapi import Request
from starlette.responses import JSONResponse
from substrateinterface import Keypair

from communex.compat.key import classic_load_key
from communex.types import Ss58Address

from smartdrive.check_file import check_file
from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.utils import DEFAULT_VALIDATOR_PATH
from smartdrive.sign import sign_data
from smartdrive.validator.api.exceptions import RedundancyException, NoMinersInNetworkException, \
    NoValidMinerResponseException, UnexpectedErrorException, HTTPRedundancyException, \
    CommuneNetworkUnreachable as HTTPCommuneNetworkUnreachable, StoreRequestNotApprovedException, \
    InvalidFileEventAssociationException
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import remove_chunk_request, validate_storage_capacity
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.event import StoreEvent, StoreParams, StoreInputParams, ChunkParams, ValidationEvent, \
    StoreRequestEvent, StoreRequestInputParams, StoreRequestParams
from smartdrive.validator.models.models import MinerWithChunk, ModuleType
from smartdrive.commune.request import execute_miner_request, get_filtered_modules
from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.node import Node
from smartdrive.validator.node.util.exceptions import InvalidSignatureException
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.utils import get_file_expiration

MIN_MINERS_FOR_FILE = 2
MIN_MINERS_REPLICATION_FOR_CHUNK = 2
MAX_MINERS_FOR_FILE = 10
MAX_ENCODED_RANGE = 50
MINER_STORE_TIMEOUT_SECONDS = 2 * 60
TIME_EXPIRATION_STORE_REQUEST_EVENT_SECONDS = 20 * 60
MAX_CHUNK_SIZE = 100 * 1024 * 1024  # Max chunk size to store, 100 MB
MAX_SIMULTANEOUS_UPLOADS = 4
MAX_SIMULTANEOUS_VALIDATIONS = 15


class StoreAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def store_request_endpoint(self, request: Request):
        """
        Handle a storage request event and register it in the system.

        This endpoint handles a request to store a file on the SmartDrive network. It creates and registers a
        `StoreRequestEvent` with an expiration time (typically 5 minutes). This event will be later processed to
        decide if the file can be stored based on factors such as available storage space.

        Args:
            request (Request): The incoming request object containing the necessary headers.

        Returns:
            dict:
                - A dictionary containing the UUID of the created store request event (`store_request_event_uuid`).

        Raises:
            Exception: If any unexpected error occurs during the process.

        """
        body = await request.json()
        user_public_key = request.headers.get("X-Key")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)
        input_signed_params = request.headers.get("X-Signature")
        file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"
        total_stake = request.state.total_stake

        validate_storage_capacity(
            database=self._database,
            user_ss58_address=user_ss58_address,
            file_size_bytes=body["file_size_bytes"],
            total_stake=total_stake,
        )

        event_params = StoreRequestParams(file_uuid=file_uuid, expiration_at=int(time.time()) + TIME_EXPIRATION_STORE_REQUEST_EVENT_SECONDS, approved=True)
        signed_params = sign_data(event_params.dict(), self._key)

        event = StoreRequestEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(self._key.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params=StoreRequestInputParams(file_hash=body["file_hash"], file_size_bytes=body["file_size_bytes"]),
            input_signed_params=input_signed_params
        )

        try:
            self._node.distribute_event(event)
            return {"store_request_event_uuid": event.uuid, "file_uuid": file_uuid}

        except InvalidSignatureException:
            raise UnexpectedErrorException

    async def store_request_permission_endpoint(self, store_request_event_uuid: str):
        """
        Check if the store request event has been approved by the validator and return the appropriate response.

        This endpoint validates whether a specific storage request event (identified by `store_request_event_uuid`)
        has been approved or not by the validator. The check is done by querying the database for the approval status
        and responding accordingly.

        Args:
            store_request_event_uuid (str): The UUID of the storage request event that needs to be checked.

        Returns:
            JSONResponse:
                - Returns a 200 status code if the event is approved.
                - Raises a `StoreRequestNotApprovedException` if the event is not approved or approval is pending.

        Raises:
            StoreRequestNotApprovedException:
                - Raised when the event is either not yet approved or has been explicitly denied.
        """
        approved = self._database.get_store_request_event_approvement(store_request_event_uuid)

        if approved:
            return JSONResponse(content=None, status_code=200)

        elif approved is None:
            raise StoreRequestNotApprovedException("Store request is still pending approval", status_code=202)

        else:
            raise StoreRequestNotApprovedException

    async def store_endpoint(self, request: Request):
        """
        Stores a file across multiple active miners.

        This method reads a file uploaded by a user and distributes it among active miners available in the SmartDrive network.
        Once it is distributed sends an event with the related info.

        Params:
            request (Request): The incoming request containing necessary headers for validation.

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
        file_size = int(request.headers.get("X-File-Size"))
        file_hash = request.headers.get("X-File-Hash")
        event_uuid = request.headers.get("X-Event-UUID")
        file_uuid = request.headers.get("X-File-UUID")
        total_stake = request.state.total_stake

        if not self._database.verify_file_uuid_for_event(file_uuid=file_uuid, event_uuid=event_uuid):
            raise InvalidFileEventAssociationException

        validate_storage_capacity(
            database=self._database,
            user_ss58_address=user_ss58_address,
            file_size_bytes=file_size,
            total_stake=total_stake,
            only_files=True
        )

        try:
            miners = await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER, self._key.ss58_address)
        except CommuneNetworkUnreachable:
            raise HTTPCommuneNetworkUnreachable

        if not miners:
            raise NoMinersInNetworkException

        active_connections = self._node.get_connections()
        try:
            store_event, validations_events_per_validator = await store_new_file(
                file=request.stream(),
                miners=miners,
                validator_keypair=self._key,
                user_ss58_address=user_ss58_address,
                input_signed_params=input_signed_params,
                file_size_bytes=file_size,
                file_hash=file_hash,
                file_uuid=file_uuid,
                validators_len=len(active_connections) + 1  # To include myself
            )
        except RedundancyException as redundancy_exception:
            raise HTTPRedundancyException(redundancy_exception.message)
        except Exception:
            raise UnexpectedErrorException

        if not store_event:
            raise NoValidMinerResponseException

        try:
            self._node.distribute_event(store_event)

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
                    send_message(active_connection.socket, message)

            return JSONResponse(content=None, status_code=200)  # status_code 204 throw errors

        except InvalidSignatureException:
            raise UnexpectedErrorException()


async def store_new_file(
        file: Union[str, AsyncGenerator[bytes, None]],
        miners: List[ModuleInfo],
        validator_keypair: Keypair,
        user_ss58_address: Ss58Address,
        input_signed_params: str,
        validators_len: int,
        file_hash: str,
        file_uuid: str = None,
        file_size_bytes: int = None,
) -> Tuple[Optional[StoreEvent], List[List[ValidationEvent]]]:
    validating = isinstance(file, str)
    store_event_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"

    if not validating and len(miners) < MIN_MINERS_FOR_FILE:
        raise RedundancyException

    stored_chunks_results = []
    stored_miner_with_chunk_uuid: List[Tuple[ModuleInfo, str]] = []

    semaphore = asyncio.Semaphore(MAX_SIMULTANEOUS_VALIDATIONS if validating else MAX_SIMULTANEOUS_UPLOADS)

    validations_events_per_validator: List[List[ValidationEvent]] = []
    chunks_params: List[ChunkParams] = []

    if not file_uuid:
        file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"

    async def handle_store_request(miner: ModuleInfo, chunk_path: str, chunk_index: int) -> bool:
        miner_answer = await _store_request(
            keypair=validator_keypair,
            miner=miner,
            user_ss58_address=user_ss58_address,
            chunk_path=chunk_path
        )
        if miner_answer:
            stored_chunks_results.append((miner_answer.chunk_uuid, chunk_index, miner.ss58_address, chunk_path))
            stored_miner_with_chunk_uuid.append((miner, miner_answer.chunk_uuid))
            return True
        return False

    async def store_chunk_with_redundancy(chunk_path: str, chunk_index: int):
        available_miners = miners.copy()
        random.shuffle(available_miners)
        replication_count = 0

        while replication_count < MIN_MINERS_REPLICATION_FOR_CHUNK and available_miners:
            miner = available_miners.pop()
            async with semaphore:
                success = await handle_store_request(miner, chunk_path, chunk_index)
                if success:
                    replication_count += 1

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
            async def gather_with_semaphore(miners, file):
                async def handle_with_limit(miner):
                    async with semaphore:
                        return await handle_store_request(miner, file, 0)

                await asyncio.gather(*[handle_with_limit(miner) for miner in miners], return_exceptions=True)

            await gather_with_semaphore(miners, file)
            if not stored_chunks_results:
                return None, []

        else:
            sha256 = hashlib.sha256()
            current_chunk_index = 0
            current_chunk_size = 0
            total_size = 0
            chunk_paths = []

            path = os.path.expanduser(DEFAULT_VALIDATOR_PATH)
            user_path = os.path.join(path, store_event_uuid)
            os.makedirs(user_path, exist_ok=True)
            chunk_path = os.path.join(user_path, f"chunk_{current_chunk_index}.part")
            async with aiofiles.open(chunk_path, "wb") as f:
                async for chunk in file:
                    total_size += len(chunk)
                    sha256.update(chunk)
                    await f.write(chunk)
                    current_chunk_size += len(chunk)

                    if current_chunk_size >= MAX_CHUNK_SIZE:
                        chunk_paths.append((chunk_path, current_chunk_index))
                        current_chunk_index += 1
                        current_chunk_size = 0
                        chunk_path = os.path.join(user_path, f"chunk_{current_chunk_index}.part")
                        f = await aiofiles.open(chunk_path, 'wb')

            await check_file(file_hash=sha256.hexdigest(), file_size=total_size, original_file_size=file_size_bytes, original_file_hash=file_hash)

            if current_chunk_size > 0:
                chunk_paths.append((chunk_path, current_chunk_index))

            tasks = [asyncio.create_task(store_chunk_with_redundancy(chunk_path, chunk_index)) for chunk_path, chunk_index in chunk_paths]
            await asyncio.gather(*tasks)

            if len(stored_chunks_results) != (current_chunk_index + 1) * MIN_MINERS_REPLICATION_FOR_CHUNK:
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

            for chunk_uuid, chunk_index, miner_ss58_address, chunk_path in stored_chunks_results:
                with open(chunk_path, 'rb') as chunk:
                    file_size = os.path.getsize(chunk_path)
                    sub_chunk_start = random.randint(0, max(0, file_size - MAX_ENCODED_RANGE))
                    sub_chunk_end = min(sub_chunk_start + MAX_ENCODED_RANGE, file_size)

                    chunk.seek(sub_chunk_start)
                    sub_chunk = chunk.read(sub_chunk_end - sub_chunk_start)
                    sub_chunk_encoded = sub_chunk.hex()

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
            uuid=store_event_uuid,
            validator_ss58_address=Ss58Address(validator_keypair.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params=StoreInputParams(file_hash=file_hash, file_size_bytes=file_size_bytes),
            input_signed_params=input_signed_params
        )

        return store_event, validations_events_per_validator

    except Exception:
        await remove_stored_chunks()
        raise
    finally:
        if validating:
            os.remove(file)
        else:
            shutil.rmtree(user_path)


async def _store_request(keypair: Keypair, miner: ModuleInfo, user_ss58_address: Ss58Address, chunk_path: str) -> Optional[MinerWithChunk]:
    """
     Sends a request to a miner to store a file chunk.

     This method sends an asynchronous request to a specified miner to store a file chunk
     in bytes format. The request includes the user's SS58 address as the folder
     and the bytes chunk.

     Params:
         keypair (Keypair): The validator key used to authorize the request.
         miner (ModuleInfo): The miner's module information containing connection details and SS58 address.
         user_ss58_address (Ss58Address): The SS58 address of the user associated with the file chunk.
         chunk_path (str): The chunk path.

     Returns:
         Optional[MinerWithChunk]: An object containing a MinerWithChunk if the storage request is successful, otherwise None.
     """

    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "store",
        file={
            'folder': user_ss58_address,
            'chunk': chunk_path
        },
        timeout=MINER_STORE_TIMEOUT_SECONDS
    )

    return MinerWithChunk(miner.ss58_address, miner_answer["id"]) if miner_answer else None
