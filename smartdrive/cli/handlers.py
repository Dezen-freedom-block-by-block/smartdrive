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
import random
import asyncio
import traceback
import zipfile
from pathlib import Path
from getpass import getpass
import urllib3
import requests
import zstd
from substrateinterface import Keypair
from nacl.exceptions import CryptoError

from communex.compat.key import is_encrypted, classic_load_key

import smartdrive
from smartdrive.cli.errors import NoValidatorsAvailableException
from smartdrive.cli.spinner import Spinner
from smartdrive.cli.utils import decrypt_and_decompress, compress_and_encrypt
from smartdrive.commune.connection_pool import initialize_commune_connection_pool
from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_active_validators, EXTENDED_PING_TIMEOUT
from smartdrive.models.event import StoreInputParams, RetrieveInputParams, RemoveInputParams
from smartdrive.utils import MAX_FILE_SIZE, format_size
from smartdrive.sign import sign_data
from smartdrive.commune.utils import calculate_hash

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

initialize_commune_connection_pool(testnet=False, max_pool_size=1, num_connections=1)


def store_handler(file_path: str, key_name: str = None, testnet: bool = False):
    """
    Encrypts, compresses, and sends a file storage request to the SmartDrive network.

    This function performs the following steps:
    1. Compresses the file.
    2. Signs the request data.
    3. Sends the request to the SmartDrive network and waits for the transaction UUID.

    Params:
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
        print(f"Error: File {file_path} does not exist or is a directory.")
        return

    # TODO: Change in the future
    if os.path.getsize(file_path) > MAX_FILE_SIZE:
        print(f"Error: File size exceeds the maximum limit of {format_size(MAX_FILE_SIZE)}")
        return

    key = _get_key(key_name)

    # Step 1: Compress the file
    spinner = Spinner("Compressing")
    spinner.start()

    try:
        compressed_data = compress_and_encrypt(file_path, key.private_key)

        spinner.stop_with_message("¡Done!")

        # Step 2: Sign the request
        spinner = Spinner("Signing request")
        spinner.start()

        input_params = StoreInputParams(file=calculate_hash(compressed_data), file_size_bytes=len(compressed_data))
        signed_data = sign_data(input_params.dict(), key)

        spinner.stop_with_message("¡Done!")

        # Step 3: Send the request
        spinner = Spinner("Sending request")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        headers = create_headers(signed_data, key, show_content_type=False)

        response = requests.post(
            url=f"{validator_url}/store",
            headers=headers,
            files={"file": io.BytesIO(compressed_data)},
            verify=False
        )

        response.raise_for_status()
        spinner.stop_with_message("¡Done!")

        try:
            message = response.json()
        except ValueError:
            print(f"Error: Unable to parse response JSON - {response.text}")
            return

        uuid = message.get('uuid')
        if uuid:
            print(f"Your data will be stored soon. Your file UUID is: {uuid}")
        else:
            print("Your data will be stored soon, but no file UUID was returned.")

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message(f"Error: Network error.")
    except Exception:
        spinner.stop_with_message(f"Unexpected error.")


def retrieve_handler(file_uuid: str, file_path: str, key_name: str = None, testnet: bool = False):
    """
    Retrieve, decompress, and decrypt a file from the SmartDrive network.

    This function performs the following steps:
    1. Retrieves the file from the SmartDrive network.
    2. Decompresses the file.
    3. Decrypts the file and saves it to the specified path.

    Params:
        file_uuid (str): The UUID of the file to be retrieved.
        file_path (str): The path where the retrieved file will be saved.
        key_name (str, optional): An optional key for decryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used. Default is False.

    Raises:
        NoValidatorsAvailableException: If no validators are available.
        requests.RequestException: If there is a network error during the request.
        lzma.LZMAError: If decompression fails due to corrupted data or incorrect key.
        Exception: For any other unexpected errors.
    """
    smartdrive.check_version(sys.argv)

    key = _get_key(key_name)

    try:
        # Step 1: Retrieve the file
        spinner = Spinner(f"Retrieving file with UUID: {file_uuid}")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        input_params = RetrieveInputParams(file_uuid=file_uuid)
        headers = create_headers(sign_data(input_params.dict(), key), key)

        response = requests.get(f"{validator_url}/retrieve", params=input_params.dict(), headers=headers, verify=False)

        response.raise_for_status()
        spinner.stop_with_message("¡Done!")

        # Step 2: Decompress and storing the file
        spinner = Spinner("Decompressing")
        spinner.start()

        try:
            filename = decrypt_and_decompress(response.content, key.private_key[:32], file_path)

            spinner.stop_with_message("¡Done!")
            print(f"Data downloaded and decompressed successfully in {file_path}{filename}")

        except zstd.Error:
            spinner.stop_with_message("Error: Decompression failed.")
            print("Error: Decompression failed. The data is corrupted or the format is incorrect.")
            return

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message(f"Error: Network error.")
    except Exception as e:
        spinner.stop_with_message(f"Unexpected error: {e}")


def remove_handler(file_uuid: str, key_name: str = None, testnet: bool = False):
    """
    Remove a file from the SmartDrive network.

    This function performs the following steps:
    1. Sends a request to remove the file from the SmartDrive network.
    2. Checks the response to confirm removal.

    Params:
        file_uuid (str): The UUID of the file to be removed.
        key_name (str, optional): An optional key for decryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used. Default is False.

    Raises:
        NoValidatorsAvailableException: If no validators are available.
        requests.RequestException: If there is a network error during the request.
        Exception: For any other unexpected errors.
    """
    smartdrive.check_version(sys.argv)

    # Retrieve the key
    key = _get_key(key_name)

    try:
        # Step 1: Send remove request
        spinner = Spinner(f"Removing file with UUID: {file_uuid}")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        input_params = RemoveInputParams(file_uuid=file_uuid)
        headers = create_headers(sign_data(input_params.dict(), key), key)
        response = requests.delete(f"{validator_url}/remove", params=input_params.dict(), headers=headers, verify=False)

        response.raise_for_status()
        spinner.stop_with_message("¡Done!")

        print(f"File {file_uuid} will be removed.")

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message(f"Error: Network error.")
    except Exception as e:
        spinner.stop_with_message(f"Unexpected error: {e}")


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
        print("Error: Decryption failed. Ciphertext failed verification.")
        exit(1)
    except FileNotFoundError:
        print("Error: Key not found.")
        exit(1)

    return key


def _get_validator_url(key: Keypair, testnet: bool = False) -> str:
    """
    Get the URL of an active validator.

    Params:
        key (Keypair): The keypair object.
        testnet (bool, optional): Flag to indicate if the testnet should be used.

    Returns:
        str: The URL of an active validator.
    """
    loop = asyncio.get_event_loop()
    netuid = smartdrive.TESTNET_NETUID if testnet else smartdrive.NETUID

    try:
        validators = loop.run_until_complete(get_active_validators(key, netuid, EXTENDED_PING_TIMEOUT))
    except CommuneNetworkUnreachable:
        raise NoValidatorsAvailableException

    if not validators:
        raise NoValidatorsAvailableException

    validator = random.choice(validators)
    return f"https://{validator.connection.ip}:{validator.connection.port}"
