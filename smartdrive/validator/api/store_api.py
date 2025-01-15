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
import os
import random
import time
import uuid
from typing import Tuple, List

from fastapi import Request
from starlette.responses import JSONResponse
from substrateinterface import Keypair

from communex.compat.key import classic_load_key
from communex.types import Ss58Address

from smartdrive.config import MAX_SIMULTANEOUS_VALIDATIONS, TIME_EXPIRATION_STORE_REQUEST_EVENT_SECONDS
from smartdrive.sign import sign_data
from smartdrive.validator.api.exceptions import (RedundancyException, UnexpectedErrorException, HTTPRedundancyException,
                                                 StoreRequestNotApprovedException)
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import remove_chunk_request, validate_storage_capacity
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.event import StoreEvent, StoreParams, ValidationEvent, \
    StoreRequestEvent, StoreRequestInputParams, StoreRequestParams, ChunkParams
from smartdrive.validator.models.models import MinerWithChunk
from smartdrive.commune.request import execute_miner_request
from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.node import Node
from smartdrive.validator.node.util.exceptions import InvalidSignatureException
from smartdrive.validator.node.util.message import MessageBody, MessageCode, Message
from smartdrive.validator.utils import get_file_expiration

MAX_ENCODED_RANGE = 50
MINER_STORE_TIMEOUT_SECONDS = 2 * 60


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
        `StoreRequestEvent` with an expiration time. This event will be later processed to
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
            input_params=StoreRequestInputParams(file_hash=body["file_hash"], file_size_bytes=body["file_size_bytes"], chunks=body["chunks"]),
            input_signed_params=input_signed_params
        )

        try:
            self._node.distribute_event(event)
            return {"store_request_event_uuid": event.uuid, "file_uuid": file_uuid}

        except InvalidSignatureException:
            raise UnexpectedErrorException

    async def store_request_approval_endpoint(self, request: Request):
        """
        Check if the store request event has been approved by the validator and return the appropriate response.

        This endpoint validates whether a specific storage request event (identified by `store_request_event_uuid`)
        has been approved or not by the validator. The check is done by querying the database for the approval status
        and responding accordingly.

        Args:
            request (Request): The incoming request object containing the necessary headers.

        Returns:
            JSONResponse:
                - Returns a 200 status code if the event is approved.
                - Raises a `StoreRequestNotApprovedException` if the event is not approved or approval is pending.

        Raises:
            StoreRequestNotApprovedException:
                - Raised when the event is either not yet approved or has been explicitly denied.
        """
        body = await request.json()
        user_ss58_address = body["user_ss58_address"]
        chunks = []

        if body.get("chunk_hash") and body.get("chunk_size"):
            chunks = [
                ChunkParams(
                    uuid="",
                    miner_ss58_address="",
                    chunk_hash=body["chunk_hash"],
                    chunk_size=body["chunk_size"]
                )
            ]

        approved = self._database.get_store_request_event_approval(
            store_request_event_uuid=body["store_request_event_uuid"],
            user_ss58_address=user_ss58_address,
            chunks=chunks
        )

        if approved:
            return JSONResponse(content=None, status_code=200)

        elif approved is None:
            raise StoreRequestNotApprovedException("Store request is still pending creation", status_code=404)

        else:
            raise StoreRequestNotApprovedException

    async def store_approval_endpoint(self, request: Request):
        body = await request.json()
        user_public_key = request.headers.get("X-Key")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)
        event_uuid = request.headers.get("X-Event-UUID")
        chunk_params = [ChunkParams(**chunk_param) for chunk_param in body["event_params"]["chunks_params"]]

        approved = self._database.get_store_request_event_approval(
            store_request_event_uuid=event_uuid,
            user_ss58_address=user_ss58_address,
            chunks=chunk_params,
            final_file_hash=body["event_params"]["file_hash"],
            final_file_size=body["event_params"]["file_size"]
        )

        if not approved:
            raise StoreRequestNotApprovedException(status_code=404, detail="Store request not approved or expired")

        try:
            store_event = StoreEvent(
                uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
                validator_ss58_address=Ss58Address(self._key.ss58_address),
                event_params=StoreParams(
                    chunks_params=[
                        ChunkParams(
                            uuid=chunk_param.uuid,
                            miner_ss58_address=chunk_param.miner_ss58_address,
                            chunk_index=chunk_param.chunk_index
                        )
                        for chunk_param in chunk_params
                    ],
                    file_hash=body["event_params"]["file_hash"],
                    file_size=int(body["event_params"]["file_size"]),
                    file_uuid=body["event_params"]["file_uuid"]
                ),
                event_signed_params=body["event_signed_params"],
                user_ss58_address=user_ss58_address
            )

            self._node.distribute_event(store_event)

            validation_events_raw = body.get("validation_events", [])
            if validation_events_raw:
                validation_events = [
                    [ValidationEvent(**validation_event) for validation_event in validator_events]
                    for validator_events in validation_events_raw
                ]
                self._database.insert_validation_events(validation_events=validation_events.pop(0))

                active_connections = self._node.get_connections()
                if active_connections:
                    for index, active_connection in enumerate(active_connections):
                        if index < len(validation_events):
                            data_list = [event.dict() for event in validation_events[index]]

                            body = MessageBody(
                                code=MessageCode.MESSAGE_CODE_VALIDATION_EVENTS,
                                data={"validation_events": data_list}
                            )

                            body_sign = sign_data(body.dict(), self._key)

                            message = Message(
                                body=body,
                                signature_hex=body_sign.hex(),
                                public_key_hex=self._key.public_key.hex()
                            )

                            send_message(active_connection, message)

            return JSONResponse(content=None, status_code=200)

        except RedundancyException as redundancy_exception:
            raise HTTPRedundancyException(redundancy_exception.message)
        except Exception:
            raise UnexpectedErrorException


async def store_validation_file(
        chunk_path: str,
        chunk_hash: str,
        chunk_size: int,
        miners: List[ModuleInfo],
        validator_keypair: Keypair,
        user_ss58_address: Ss58Address
) -> List[ValidationEvent]:
    stored_chunks_results = []
    stored_miner_with_chunk_uuid: List[Tuple[ModuleInfo, str]] = []

    semaphore = asyncio.Semaphore(MAX_SIMULTANEOUS_VALIDATIONS)

    async def handle_store_request(miner: ModuleInfo, chunk_path: str, chunk_index: int) -> bool:
        miner_answer = await execute_miner_request(
            validator_keypair, miner.connection, miner.ss58_address, "store",
            file={
                'folder': user_ss58_address,
                'chunk': chunk_path,
                'chunk_size': chunk_size,
                'chunk_hash': chunk_hash
            },
            timeout=MINER_STORE_TIMEOUT_SECONDS
        )
        if miner_answer:
            miner_answer = MinerWithChunk(miner.ss58_address, miner_answer["id"])
            stored_chunks_results.append((miner_answer.chunk_uuid, chunk_index, miner.ss58_address, chunk_path))
            stored_miner_with_chunk_uuid.append((miner, miner_answer.chunk_uuid))
            return True
        return False

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

    async def gather_with_semaphore(miners, file):
        async def handle_with_limit(miner):
            async with semaphore:
                return await handle_store_request(miner, file, 0)

        await asyncio.gather(*[handle_with_limit(miner) for miner in miners], return_exceptions=True)

    try:
        await gather_with_semaphore(miners, chunk_path)

        if not stored_chunks_results:
            return []

        # A ValidationEvent object is generated for each chunk stored
        validator_events_validations = []
        for chunk_uuid, chunk_index, miner_ss58_address, chunk_path in stored_chunks_results:
            with open(chunk_path, 'rb') as chunk:
                chunk_size = os.path.getsize(chunk_path)
                sub_chunk_start = random.randint(0, max(0, chunk_size - MAX_ENCODED_RANGE))
                sub_chunk_end = min(sub_chunk_start + MAX_ENCODED_RANGE, chunk_size)

                chunk.seek(sub_chunk_start)
                sub_chunk = chunk.read(sub_chunk_end - sub_chunk_start)
                sub_chunk_encoded = sub_chunk.hex()

                validation_event = ValidationEvent(
                    uuid=chunk_uuid,
                    miner_ss58_address=miner_ss58_address,
                    sub_chunk_start=sub_chunk_start,
                    sub_chunk_end=sub_chunk_end,
                    sub_chunk_encoded=sub_chunk_encoded,
                    file_uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
                    user_owner_ss58_address=user_ss58_address,
                    expiration_ms=get_file_expiration(),
                    created_at=int(time.time() * 1000)
                )
                validator_events_validations.append(validation_event)

        return validator_events_validations

    except Exception:
        await remove_stored_chunks()
        raise
    finally:
        os.remove(chunk_path)
