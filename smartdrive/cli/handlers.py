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
import subprocess
import sys
import os
import random
import asyncio
from pathlib import Path
from getpass import getpass
import urllib3
import requests
import zstandard as zstd
from substrateinterface import Keypair
from nacl.exceptions import CryptoError

from communex.compat.key import is_encrypted, classic_load_key

import smartdrive
from smartdrive.commune.utils import calculate_hash_sync
from smartdrive.logging_config import logger
from smartdrive.cli.errors import NoValidatorsAvailableException
from smartdrive.cli.spinner import Spinner
from smartdrive.cli.utils import compress_encrypt_and_save, decompress_decrypt_and_save
from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_active_validators, EXTENDED_PING_TIMEOUT
from smartdrive.models.event import StoreInputParams, RetrieveInputParams, RemoveInputParams
from smartdrive.sign import sign_data
from smartdrive.utils import MAXIMUM_STORAGE, format_size

urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)


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
    smartdrive.check_version()

    file_path = os.path.expanduser(file_path)

    if not Path(file_path).is_file():
        logger.error(f"File {file_path} does not exist or is a directory.")
        return

    if os.path.getsize(file_path) > MAXIMUM_STORAGE:
        logger.error(f"Error: File size exceeds the maximum limit of {format_size(MAXIMUM_STORAGE)}")
        return

    key = _get_key(key_name)

    # Step 1: Compress the file
    spinner = Spinner("Compressing and encrypting file")
    spinner.start()

    temp_file_path = ""
    try:
        temp_file_path = compress_encrypt_and_save(file_path, key.private_key[:32])
        file_hash = calculate_hash_sync(temp_file_path)
        spinner.stop_with_message("Done!")

        file_size_bytes = os.path.getsize(temp_file_path)
        input_params = StoreInputParams(file_hash=file_hash, file_size_bytes=file_size_bytes)
        signed_data = sign_data(input_params.dict(), key)

        # Step 3: Send the request
        spinner = Spinner("Sending request")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        headers = create_headers(signed_data, key, show_content_type=False)

        response = requests.post(
            url=f"{validator_url}/store/request",
            headers=headers,
            json=input_params.dict(),
            verify=False
        )

        response.raise_for_status()
        message = response.json()

        file_uuid = message.get("file_uuid")
        store_request_event_uuid = message.get("store_request_event_uuid")
        spinner.stop_with_message("Done!")

        if file_uuid and store_request_event_uuid:
            logger.info(f"Your data will be stored soon in background. Your file UUID is: {file_uuid}")
            subprocess.Popen(
                [sys.executable, "smartdrive/cli/scripts/async_upload_file.py", temp_file_path, file_hash, str(file_size_bytes), store_request_event_uuid, key_name, str(testnet), file_uuid],
                close_fds=True,
                start_new_session=True
            )

    except NoValidatorsAvailableException:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            if temp_file_path and os.path.exists(temp_file_path):
                os.remove(temp_file_path)
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message("Error: Network error.")
    except Exception as e:
        if temp_file_path and os.path.exists(temp_file_path):
            os.remove(temp_file_path)
        spinner.stop_with_message(f"Unexpected error. {e}")


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
    smartdrive.check_version()

    key = _get_key(key_name)

    try:
        # Step 1: Retrieve the file
        spinner = Spinner(f"Retrieving file with UUID: {file_uuid}")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        input_params = RetrieveInputParams(file_uuid=file_uuid)
        headers = create_headers(sign_data(input_params.dict(), key), key)

        response = requests.get(f"{validator_url}/retrieve", params=input_params.dict(), headers=headers, verify=False, stream=True)

        response.raise_for_status()
        spinner.stop_with_message("Done!")

        # Step 2: Decompress and storing the file
        spinner = Spinner("Decompressing and decrypting")
        spinner.start()

        try:
            final_file_path = decompress_decrypt_and_save(response.raw, key.private_key[:32], file_path)

            spinner.stop_with_message("Done!")
            logger.info(f"Data downloaded and decompressed successfully in {final_file_path}")

        except zstd.ZstdError:
            spinner.stop_with_message("Error: Decompression failed.")
            logger.error("Error: Decompression failed. The data is corrupted or the format is incorrect.")
            return

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message("Error: Network error.")
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
    smartdrive.check_version()

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
        spinner.stop_with_message("Done!")

        logger.info(f"File {file_uuid} will be removed.")

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message("Error: Network error.")
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
        logger.error("Error: Decryption failed. Ciphertext failed verification.")
        exit(1)
    except FileNotFoundError:
        logger.error("Error: Key not found.")
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
        validators = loop.run_until_complete(get_active_validators(key, netuid, testnet, EXTENDED_PING_TIMEOUT))
    except CommuneNetworkUnreachable:
        raise NoValidatorsAvailableException

    if not validators:
        raise NoValidatorsAvailableException

    validator = random.choice(validators)
    return f"https://{validator.connection.ip}:{validator.connection.port}"
