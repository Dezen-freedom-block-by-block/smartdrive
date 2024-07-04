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
import base64
import os
import random
import tempfile
import time
import zipfile
from typing import Optional
import requests
import hashlib

from communex.types import Ss58Address
from starlette.datastructures import Headers
from substrateinterface import Keypair

from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.commune.request import get_filtered_modules
from smartdrive.models.event import Event, StoreEvent, RemoveEvent, MinerProcess
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.utils import get_miner_info_with_chunk, remove_chunk_request
from smartdrive.validator.database.database import Database
from smartdrive.validator.models.models import Chunk, SubChunk, File, ModuleType

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
        response = requests.get(f"https://{connection.ip}:{connection.port}/{action}", params=params, headers=headers, timeout=timeout, verify=False)
        response.raise_for_status()
        return response
    except Exception as e:
        print(f"Error fetching action {action} with connection {connection.ip}:{connection.port} - {e}")
        return None


async def fetch_with_retries(action: str, connection: ConnectionInfo, params, timeout: int, headers: Headers, retries: int = MAX_RETRIES, delay: int = RETRY_DELAY) -> Optional[requests.Response]:
    for attempt in range(retries):
        response = fetch_validator(action, connection, params=params, headers=headers, timeout=timeout)
        if response and response.status_code == 200:
            return response
        print(f"Failed to fetch {action} on attempt {attempt + 1}/{retries}. Retrying...")
        await asyncio.sleep(delay)
    return None


async def process_events(events: list[Event], is_proposer_validator: bool, keypair: Keypair, netuid: int, database: Database):
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
            chunks = []
            for miner_chunk in event.event_params.miners_processes:
                chunks.append(Chunk(
                    miner_owner_ss58address=miner_chunk.miner_ss58_address,
                    chunk_uuid=miner_chunk.chunk_uuid,
                    file_uuid=event.event_params.file_uuid,
                    sub_chunk=SubChunk(id=None, start=event.event_params.sub_chunk_start, end=event.event_params.sub_chunk_end,
                                       data=event.event_params.sub_chunk_encoded, chunk_uuid=miner_chunk.chunk_uuid)
                ))
            file = File(
                user_owner_ss58address=event.user_ss58_address,
                file_uuid=event.event_params.file_uuid,
                chunks=chunks,
                created_at=None,
                expiration_ms=_get_file_expiration()
            )
            database.insert_file(file)
        elif isinstance(event, RemoveEvent):
            if is_proposer_validator:
                miner_chunks = database.get_miner_chunks(event.event_params.file_uuid)

                # get_filtered_modules could raise CommuneNetworkUnreachable
                # If it is the events being processed by the validator when it is creating a block it should raise the
                # exception and cancel the block creation. This method can also be launched in clint.py but in that case
                # is not a proposer validator.
                miners = get_filtered_modules(netuid, ModuleType.MINER)

                miner_with_chunks = get_miner_info_with_chunk(miners, miner_chunks)
                miner_processes = []

                for miner in miner_with_chunks:
                    start_time = time.time()

                    connection = ConnectionInfo(miner["connection"]["ip"], miner["connection"]["port"])
                    miner_info = ModuleInfo(miner["uid"], miner["ss58_address"], connection)
                    result = await remove_chunk_request(keypair, event.user_ss58_address, miner_info, miner["chunk_uuid"])

                    final_time = time.time() - start_time
                    miner_process = MinerProcess(chunk_uuid=miner["chunk_uuid"], miner_ss58_address=miner["ss58_address"],
                                                 succeed=True if result else False, processing_time=final_time)
                    miner_processes.append(miner_process)

                # Since in the remove call processed by a validator it cannot finish completing the event_params
                # (since it does not fill in the miners processes), the already signed event must be replaced with a new
                # event in which the miner processes are added to the file_uuid parameter that already existed.
                # Once the parameters of this event have been replaced, they must be signed again, thus replacing
                # the validator's signature with that of the proposer.
                event.event_params.miners_processes = miner_processes
                event.event_signed_params = sign_data(event.event_params.dict(), keypair).hex()
                event.validator_ss58_address = Ss58Address(keypair.ss58_address)

            database.remove_file(event.event_params.file_uuid)
            
            
def _get_file_expiration() -> int:
    """
    Generate a random expiration time in milliseconds within a range.

    Returns:
        int: A random expiration time between 3 hours (min_ms) and 24 hours (max_ms) in milliseconds.
    """
    min_ms = 3 * 60 * 60 * 1000  # 3 hours
    max_ms = 24 * 60 * 60 * 1000  # 24 hours
    return random.randint(min_ms, max_ms)


def encode_bytes_to_b64(data: bytes) -> str:
    """
    Encodes bytes into a base64 string.

    Params:
        data (bytes): The data to be encoded.

    Returns:
        str: The base64 encoded string.
    """
    return base64.b64encode(data).decode("utf-8")


def decode_b64_to_bytes(data: str) -> bytes:
    """
    Decodes a base64 string into bytes.

    Params:
        data (str): The base64 encoded string.

    Returns:
        bytes: The decoded bytes.
    """
    return base64.b64decode(data)


def calculate_hash(data: bytes) -> str:
    """
    Calculates the SHA-256 hash of the given data.

    Params:
        data (bytes): The data to hash, provided as a byte string.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash of the input data.
    """
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()
