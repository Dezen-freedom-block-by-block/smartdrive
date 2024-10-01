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
from typing import Optional, Union
import requests

from communex.types import Ss58Address
from starlette.datastructures import Headers
from substrateinterface import Keypair

from smartdrive.logging_config import logger
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.commune.request import get_filtered_modules
from smartdrive.models.event import StoreEvent, RemoveEvent
from smartdrive.sign import sign_data
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.models import Chunk, File, ModuleType
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.util.message import MessageCode, MessageBody, Message

MAX_RETRIES = 3
RETRY_DELAY = 5


def fetch_validator(action: str, connection: ConnectionInfo, params=None, timeout=60, headers: Headers = None) -> Optional[requests.Response]:
    """
    Sends a request to a specified validator action endpoint.

    This function sends a request to a specified action endpoint of a validator
    using the provided connection information. It handles any exceptions that may occur
    during the request and logs an error message if the request fails.

    Params:
        action (str): The action to be performed at the validator's endpoint.
        connection (ConnectionInfo): The connection information containing the IP address and port of the validator.
        timeout (int): The timeout for the request in seconds. Default is 60 seconds.

    Returns:
        Optional[requests.Response]: The response object if the request is successful, otherwise None.
    """
    try:
        response = requests.get(f"https://{connection.ip}:{connection.port}/{action}", params=params, headers=headers,
                                timeout=timeout, verify=False)
        response.raise_for_status()
        return response
    except Exception:
        logger.error(f"Error fetching action {action} with connection {connection.ip}:{connection.port}", exc_info=True)
        return None


async def process_events(events: list[Union[StoreEvent, RemoveEvent]], is_proposer_validator: bool, keypair: Keypair, netuid: int, database: Database):
    """
    Process a list of events. Depending on the type of event, it either stores a file or removes it.

    Params:
        events (list[Union[StoreEvent, RemoveEvent]]): A list of events to process.
        is_proposer_validator (bool): Flag indicating if the current node is the proposer validator.
        keypair (Keypair): The keypair used for signing data.
        netuid (int): The network UID.
        database (Database): The database instance for storing or removing files.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    for event in events:
        if isinstance(event, StoreEvent):
            total_chunks_index = set()
            chunks = []
            for chunk in event.event_params.chunks_params:
                total_chunks_index.add(chunk.chunk_index)
                chunks.append(
                    Chunk(
                        miner_ss58_address=Ss58Address(chunk.miner_ss58_address),
                        chunk_uuid=chunk.uuid,
                        file_uuid=event.event_params.file_uuid,
                        chunk_index=chunk.chunk_index
                    )
                )
            file = File(
                user_owner_ss58address=event.user_ss58_address,
                total_chunks=len(total_chunks_index),
                file_uuid=event.event_params.file_uuid,
                chunks=chunks,
                file_size_bytes=event.input_params.file_size_bytes
            )
            database.insert_file(file=file, event_uuid=event.uuid)

        elif isinstance(event, RemoveEvent):
            if is_proposer_validator:
                chunks = database.get_chunks(file_uuid=event.event_params.file_uuid)

                # If it is the events being processed by the validator when it is creating a block it should raise the
                # exception and cancel the block creation.
                miners = await get_filtered_modules(netuid, ModuleType.MINER)

                miners_info_with_chunk = compile_miners_info_and_chunks(miners, chunks)

                for miner in miners_info_with_chunk:
                    connection = ConnectionInfo(miner["connection"]["ip"], miner["connection"]["port"])
                    miner_info = ModuleInfo(miner["uid"], miner["ss58_address"], connection)
                    await remove_chunk_request(keypair, event.user_ss58_address, miner_info, miner["chunk_uuid"])

            database.remove_file(event.event_params.file_uuid)


def prepare_sync_blocks(start, keypair, end=None, active_connections=None):
    async def _prepare_sync_blocks():
        if not active_connections:
            return
        await get_synced_blocks(start, active_connections, keypair, end)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        asyncio.create_task(_prepare_sync_blocks())
    else:
        asyncio.run(_prepare_sync_blocks())


async def get_synced_blocks(start: int, connections, keypair, end: int = None):
    async def _get_synced_blocks(c):
        try:
            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_SYNC_BLOCK,
                data={"start": str(start)}
            )
            if end:
                body.data["end"] = str(end)

            body_sign = sign_data(body.dict(), keypair)

            message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=keypair.public_key.hex()
            )

            send_message(c.socket, message)
        except Exception:
            logger.error("Error getting synced blocks", exc_info=True)

    connection = random.choice(connections)
    await _get_synced_blocks(connection)


def get_file_expiration() -> int:
    """
    Generate a random expiration time in milliseconds within a range.

    Returns:
        int: A random expiration time between 30 minutes (min_ms) and 1 hour (max_ms) in milliseconds.
    """
    min_ms = 30 * 60 * 1000
    max_ms = 1 * 60 * 60 * 1000
    return random.randint(min_ms, max_ms)
