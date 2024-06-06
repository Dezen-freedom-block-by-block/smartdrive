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

import io
import sys
import os
import lzma
import random
import base64
import asyncio
from pathlib import Path
from getpass import getpass

import py7zr
import urllib3
import requests
from substrateinterface import Keypair
from nacl.exceptions import CryptoError

from communex._common import get_node_url
from communex.client import CommuneClient
from communex.compat.key import is_encrypted, classic_load_key

import smartdrive
from smartdrive.commune.request import get_active_validators

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


def store_handler(file_path: str, key_name: str = None, testnet: bool = False):
    """
    Encrypt, compress, and store a file on the SmartDrive network.

    Params:
        file_path (str): The path to the file to be stored.
        key_name (str, optional): An optional key for encryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used.

    Example:
        store_handler("path/to/file.txt", key_name="my-key", testnet=True)
    """
    smartdrive.check_version(sys.argv)

    file_path = os.path.expanduser(file_path)

    if not Path(file_path).is_file():
        print(f"ERROR: File {file_path} not exist or is a directory.")

    else:
        total_size = os.path.getsize(file_path)
        key = _get_key(key_name)

        print(f"Encrypting, compressing and storing data: {file_path}")

        data = io.BytesIO()
        with py7zr.SevenZipFile(data, 'w', password=key.private_key.hex()) as archive:
            archive.write(file_path, arcname=os.path.basename(file_path))

        data = data.getvalue()

        print(f"Compression ratio: {round((1 - (len(data) / total_size)) * 100, 2)}%")

        validator_url = _get_validator_url(key, testnet)
        response = requests.post(f"{validator_url}/store", data={"user_ss58_address": key.ss58_address}, files={"file": data}, verify=False)

        if response.status_code != 200:
            try:
                error_message = response.json()
                print(f"ERROR: {error_message.get('detail')}")

            except ValueError:
                print(f"ERROR: {response.text}")
        else:
            message = response.json()
            print(f"Data stored successfully. Your identifier is: {message.get('uuid')}")


def retrieve_handler(file_uuid: str, file_path: str, key_name: str = None, testnet: bool = False):
    """
    Retrieve, decompress, and decrypt a file from the SmartDrive network.

    Params:
        file_uuid (str): The ID of the file to be retrieved.
        file_path (str): The path where the retrieved file will be saved.
        key_name (str, optional): An optional key for decryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used.

    Example:
        retrieve_handler("file-id-123", "path/to/save/file.txt", key_name="my-key", testnet=True)
    """
    smartdrive.check_version(sys.argv)

    key = _get_key(key_name)
    print(f"Retrieving, decompressing and decrypting data: {file_uuid}")

    validator_url = _get_validator_url(key, testnet)
    response = requests.get(f"{validator_url}/retrieve", {"user_ss58_address": key.ss58_address, "file_uuid": file_uuid}, verify=False)

    if response.status_code != 200:
        try:
            error_message = response.json()
            print(f"ERROR: {error_message.get('detail')}")

        except ValueError:
            print(f"ERROR: {response.text}")

    else:
        # TODO: Receive bytes directly, not base64 data.
        data_bytes = base64.b64decode(response.content)
        data = io.BytesIO(data_bytes)
        try:
            with py7zr.SevenZipFile(data, 'r', password=key.private_key.hex()) as archive:
                archive.extractall(path=file_path)
                filename = archive.getnames()

            print(f"Data downloaded successfully in {file_path}{filename[0]}")

        except lzma.LZMAError:
            print("ERROR: Decompression failed. The data is corrupted or the key is incorrect.")
            exit(1)


def remove_handler(file_uuid: str, key_name: str = None, testnet: bool = False):
    """
    Remove a file from the SmartDrive network.

    Params:
        file_uuid (str): The ID of the file to be removed.
        key_name (str, optional): An optional key for decryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used.

    Example:
        remove_handler("file-id-123", key_name="my-key", testnet=True)
    """
    smartdrive.check_version(sys.argv)

    # TODO: Improve security.
    key = _get_key(key_name)
    print(f"Removing file with id: {file_uuid}")

    validator_url = _get_validator_url(key, testnet)
    response = requests.post(f"{validator_url}/remove", {"user_ss58_address": key.ss58_address, "file_uuid": file_uuid}, verify=False)

    if response.status_code != 200:
        try:
            error_message = response.json()
            print(f"ERROR: {error_message.get('detail')}")

        except ValueError:
            print(f"ERROR: {response.text}")
    else:
        message = response.json()
        if message.get("removed"):
            print(f"File {file_uuid} deleted successfully.")
        else:
            print(f"Error: File {file_uuid} could not be removed.")


def _get_key(key_name: str) -> Keypair:
    """
    Retrieve the encryption key.

    Params:
        key_name (str): The name of the key.

    Returns:
        Keypair: The retrieved key.

    Example:
        key = _get_key("my-key")
    """
    if not key_name:
        key_name = input("Key: ")

    password = getpass("Password: ") if is_encrypted(key_name) else ""

    try:
        key = classic_load_key(key_name, password)
    except CryptoError:
        print("ERROR: Decryption failed. Ciphertext failed verification.")
        exit(1)
    except FileNotFoundError:
        print("ERROR: Key not found.")
        exit(1)

    return key


def _get_validator_url(key: Keypair, testnet: bool = False) -> str:
    """
    Get the URL of an active validator.

    Params:
        key (Keypair): The keypair object.
        testnet (bool, optional): Flag to indicate if the testnet should be used.

    Returns:
        - str: The URL of an active validator.

    Example:
        url = _get_validator_url(key, testnet=True)
    """
    loop = asyncio.get_event_loop()
    comx_client = CommuneClient(get_node_url())
    netuid = smartdrive.TESTNET_NETUID if testnet else smartdrive.NETUID
    validators = loop.run_until_complete(get_active_validators(key, comx_client, netuid))

    if not validators:
        print("ERROR: No validators available.")
        exit(1)

    validator = random.choice(validators)
    return f"https://{validator.connection.ip}:{validator.connection.port}"
