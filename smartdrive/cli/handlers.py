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
from smartdrive.cli.errros import NoValidatorsAvailableException
from smartdrive.cli.spinner import Spinner
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_active_validators
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.utils import decode_b64_to_bytes, calculate_hash

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# TODO: Modify all responses since actions are not anymore instant actions, are actions being processed


def store_handler(file_path: str, key_name: str = None, testnet: bool = False):
    """
    Encrypts, compresses, and sends a file storage request to the SmartDrive network.

    This function performs the following steps:
    1. Compresses the file.
    2. Signs the request data.
    3. Sends the request to the SmartDrive network and waits for the transaction UUID.

    Args:
        file_path (str): The path to the file to be stored.
        key_name (str, optional): An optional key for encryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used. Default is False.

    Raises:
        NoValidatorsAvailableException: If no validators are available.
        requests.RequestException: If there is a network error during the request.
        Exception: For any other unexpected errors.

    """
    smartdrive.check_version(sys.argv)

    file_path = os.path.expanduser(file_path)

    if not Path(file_path).is_file():
        print(f"ERROR: File {file_path} does not exist or is a directory.")
        return

    total_size = os.path.getsize(file_path)
    key = _get_key(key_name)

    # Step 1: Compress the file
    spinner = Spinner("Compressing")
    spinner.start()

    try:
        data = io.BytesIO()
        with py7zr.SevenZipFile(data, 'w', password=key.private_key.hex()) as archive:
            archive.writeall(file_path)

        data.seek(0)
        compressed_data = data.getvalue()

        spinner.stop_with_message("¡Done!")
        print(f"Compression ratio: {round((1 - (len(compressed_data) / total_size)) * 100, 2)}%")

        # Step 2: Sign the request
        spinner = Spinner("Signing request")
        spinner.start()

        signed_data = sign_data({"file": calculate_hash(compressed_data)}, key)

        spinner.stop_with_message("¡Done!")

        # Step 3: Send the request
        spinner = Spinner("Sending request")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        headers = create_headers(signed_data, key, show_content_type=False)

        response = requests.post(
            url=f"{validator_url}/store",
            headers=headers,
            files={"file": data},
            verify=False
        )

        response.raise_for_status()
        spinner.stop_with_message("¡Done!")

        try:
            message = response.json()
        except ValueError:
            print(f"ERROR: Unable to parse response JSON - {response.text}")
            return

        uuid = message.get('uuid')
        if uuid:
            print(f"Data stored successfully. Your transaction identifier is: {uuid}")
        else:
            print("Data stored successfully, but no transaction identifier was returned.")

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: Network error - {error_message}.")
        except ValueError:
            spinner.stop_with_message(f"Error: Network error.")
    except Exception:
        spinner.stop_with_message(f"Unexpected error.")


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
    body = {"user_ss58_address": key.ss58_address, "file_uuid": file_uuid}
    headers = create_headers(sign_data(body, key), key)
    response = requests.get(f"{validator_url}/retrieve", body, headers=headers, verify=False)

    if response.status_code != 200:
        try:
            error_message = response.json()
            print(f"ERROR: {error_message.get('detail')}")

        except ValueError:
            print(f"ERROR: {response.text}")

    else:
        # TODO: Receive bytes directly, not base64 data.
        data_bytes = decode_b64_to_bytes(response.content)
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
    body = {"user_ss58_address": key.ss58_address, "file_uuid": file_uuid}
    headers = create_headers(sign_data(body, key), key, "application/x-www-form-urlencoded")
    response = requests.post(f"{validator_url}/remove", body, headers=headers, verify=False)

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
    comx_client = CommuneClient(get_node_url(use_testnet=testnet))
    netuid = smartdrive.TESTNET_NETUID if testnet else smartdrive.NETUID
    validators = loop.run_until_complete(get_active_validators(key, comx_client, netuid))

    if not validators:
        raise NoValidatorsAvailableException

    validator = random.choice(validators)
    return f"https://{validator.connection.ip}:{validator.connection.port}"
