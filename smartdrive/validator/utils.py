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
import tempfile
import zipfile
from typing import Optional
import requests
import hashlib

from starlette.datastructures import Headers

from smartdrive.commune.models import ConnectionInfo

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
