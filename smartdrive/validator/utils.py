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
import os
import random
import tempfile
import zipfile
from typing import Optional
import requests

from communex.types import Ss58Address
from starlette.datastructures import Headers
from substrateinterface import Keypair

from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.commune.request import get_filtered_modules
from smartdrive.models.event import Event, StoreEvent, RemoveEvent
from smartdrive.validator.api.utils import remove_chunk_request
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.models import Chunk, File, ModuleType
from smartdrive.validator.node.util.message import MessageCode
from smartdrive.validator.node.util.utils import prepare_body_tcp, send_json

MAX_RETRIES = 3
RETRY_DELAY = 5


def extract_sql_file(zip_filename: str) -> Optional[str]:
    """
    Extracts the SQL file from the given ZIP archive and stores it in a temporary file.

    Params:
        zip_filename (str): The path to the ZIP file that contains the SQL file.

    Returns:
        Optional[str]: The path to the temporary SQL file if extraction is successful, or None if an error occurs.
    """
    try:
        with tempfile.TemporaryDirectory() as temp_dir:
            with zipfile.ZipFile(zip_filename, 'r') as zip_ref:
                zip_ref.extractall(temp_dir)

            sql_files = [f for f in os.listdir(temp_dir) if f.endswith('.sql')]
            if not sql_files:
                print("No SQL files found in the ZIP archive.")
                return None

            sql_file_path = os.path.join(temp_dir, sql_files[0])
            temp_sql_file = tempfile.NamedTemporaryFile(delete=False, suffix='.sql')
            temp_sql_file.close()
            os.rename(sql_file_path, temp_sql_file.name)
            return temp_sql_file.name

    except Exception as e:
        print(f"Error during database import - {e}")
        return None


def fetch_validator(action: str, connection: ConnectionInfo, params=None, timeout=60, headers: Headers = None) -> \
Optional[requests.Response]:
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
    except Exception as e:
        print(f"Error fetching action {action} with connection {connection.ip}:{connection.port} - {e}")
        return None


async def fetch_with_retries(action: str, connection: ConnectionInfo, params, timeout: int, headers: Headers,
                             retries: int = MAX_RETRIES, delay: int = RETRY_DELAY) -> Optional[requests.Response]:
    for attempt in range(retries):
        response = fetch_validator(action, connection, params=params, headers=headers, timeout=timeout)
        if response and response.status_code == 200:
            return response
        print(f"Failed to fetch {action} on attempt {attempt + 1}/{retries}. Retrying...")
        await asyncio.sleep(delay)
    return None


async def process_events(events: list[Event], is_proposer_validator: bool, keypair: Keypair, netuid: int,
                         database: Database):
    """
    Process a list of events. Depending on the type of event, it either stores a file or removes it.

    Params:
        events (list[Event]): A list of events to process.
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
                chunks=chunks
            )
            database.insert_file(file=file, event_uuid=event.uuid)

        elif isinstance(event, RemoveEvent):
            if is_proposer_validator:
                chunks = database.get_chunks(file_uuid=event.event_params.file_uuid)

                # If it is the events being processed by the validator when it is creating a block it should raise the
                # exception and cancel the block creation.
                miners = get_filtered_modules(netuid, ModuleType.MINER)

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
            body = {"code": MessageCode.MESSAGE_CODE_SYNC_BLOCK.value, "start": str(start)}
            if end:
                body["end"] = str(end)
            message = prepare_body_tcp(body, keypair)
            send_json(c.socket, message)
        except Exception as e:
            print(f"Error getting synced blocks: {e}")

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
