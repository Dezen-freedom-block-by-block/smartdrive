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

import sys
import os
import random
import asyncio
import time
from pathlib import Path
from getpass import getpass
import urllib3
import requests
import zstandard as zstd
from substrateinterface import Keypair
from nacl.exceptions import CryptoError

from communex.compat.key import is_encrypted, classic_load_key

import smartdrive
from smartdrive.logging_config import logger
from smartdrive.cli.errors import NoValidatorsAvailableException
from smartdrive.cli.spinner import Spinner
from smartdrive.cli.utils import decrypt_and_decompress, compress_encrypt_and_save, stream_file_with_signature
from smartdrive.commune.connection_pool import initialize_commune_connection_pool
from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_active_validators, EXTENDED_PING_TIMEOUT
from smartdrive.models.event import StoreInputParams, RetrieveInputParams, RemoveInputParams
from smartdrive.sign import sign_data
from smartdrive.commune.utils import calculate_hash_stream

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
        logger.error(f"File {file_path} does not exist or is a directory.")
        return

    key = _get_key(key_name)

    # Step 1: Compress the file
    spinner = Spinner("Compressing file")
    spinner.start()

    try:
        temp_file_path = compress_encrypt_and_save(file_path, key.private_key)
        spinner.stop_with_message("Done!")

        with open(temp_file_path, 'rb') as temp_file:
            file_hash = calculate_hash_stream(temp_file)
            file_size_bytes = os.path.getsize(temp_file_path)

        input_params = StoreInputParams(file=file_hash, file_size_bytes=file_size_bytes)
        signed_data = sign_data(input_params.dict(), key)

        # Step 3: Send the request
        spinner = Spinner("Sending store request")
        spinner.start()

        validator_url = _get_validator_url(key, testnet)
        headers = create_headers(signed_data, key, show_content_type=False)

        response = requests.post(
            url=f"{validator_url}/store/request",
            headers=headers,
            json={"file": file_hash, "file_size_bytes": str(file_size_bytes)},
            verify=False
        )

        response.raise_for_status()
        message = response.json()
        store_request_event_uuid = message.get("store_request_event_uuid")
        spinner.stop_with_message("Done!")

        if store_request_event_uuid:
            _check_permission_store(temp_file_path, file_hash, file_size_bytes, store_request_event_uuid, key, testnet)

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message("Error: Network error.")
    except Exception:
        spinner.stop_with_message("Unexpected error.")


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
        spinner.stop_with_message("Done!")

        # Step 2: Decompress and storing the file
        spinner = Spinner("Decompressing")
        spinner.start()

        try:
            filename = decrypt_and_decompress(response.content, key.private_key[:32], file_path)

            spinner.stop_with_message("Done!")
            logger.info(f"Data downloaded and decompressed successfully in {file_path}{filename}")

        except zstd.Error:
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
        validators = loop.run_until_complete(get_active_validators(key, netuid, EXTENDED_PING_TIMEOUT))
    except CommuneNetworkUnreachable:
        raise NoValidatorsAvailableException

    if not validators:
        raise NoValidatorsAvailableException

    validator = random.choice(validators)
    return f"https://{validator.connection.ip}:{validator.connection.port}"


def _check_permission_store(file_path: str, file_hash: str, file_size_bytes: int, store_request_event_uuid: str, key: Keypair, testnet: bool) -> bool:
    spinner = Spinner("Checking permission to store")
    spinner.start()

    for i in range(3):
        time.sleep(10)

        validator_url = _get_validator_url(key, testnet)
        headers = create_headers(b'', key, show_content_type=False)

        response = requests.get(
            url=f"{validator_url}/store/check-permission",
            headers=headers,
            params=store_request_event_uuid,
            verify=False,
        )

        response.raise_for_status()

        if response.status_code == requests.codes.ok:
            spinner.stop_with_message("Done!")
            return _store_file(file_path=file_path, file_hash=file_hash, file_size_bytes=file_size_bytes, keypair=key, testnet=testnet)


def _store_file(file_path: str, file_hash: str, file_size_bytes: int, keypair: Keypair, testnet: bool) -> bool:
    spinner = Spinner("Sending file")
    spinner.start()
    input_params = StoreInputParams(file=file_hash, file_size_bytes=file_size_bytes)
    signed_data = sign_data(input_params.dict(), keypair)

    validator_url = _get_validator_url(keypair, testnet)
    headers = create_headers(signed_data, keypair, show_content_type=False)
    headers["X-File-Hash"] = file_hash
    headers["X-File-Size"] = str(file_size_bytes)

    response = requests.post(
        url=f"{validator_url}/store",
        headers=headers,
        data=stream_file_with_signature(file_path=file_path, keypair=keypair),
        verify=False,
        stream=True
    )

    response.raise_for_status()

    if os.path.exists(file_path):
        os.remove(file_path)

    spinner.stop_with_message("Done!")

    try:
        message = response.json()
    except ValueError:
        logger.error(f"Error: Unable to parse response JSON - {response.text}")
        return False

    uuid = message.get("uuid")
    if uuid:
        logger.info(f"Your data will be stored soon. Your file UUID is: {uuid}")
    else:
        logger.info("Your data will be stored soon, but no file UUID was returned.")

    return response.status_code == requests.codes.ok
