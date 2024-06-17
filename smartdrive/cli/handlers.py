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
from smartdrive.cli.errors import NoValidatorsAvailableException
from smartdrive.cli.spinner import Spinner
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_active_validators
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.utils import decode_b64_to_bytes, calculate_hash

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
            archive.writeall(file_path, arcname=Path(file_path).name)  # Use the file name only

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
            print(f"Your data will be stored soon. Your file UUID is: {uuid}")
        else:
            print("Your data will be stored soon, but no file UUID was returned.")

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
        body = {"file_uuid": file_uuid}
        headers = create_headers(sign_data(body, key), key)

        response = requests.get(f"{validator_url}/retrieve", params=body, headers=headers, verify=False)

        response.raise_for_status()
        spinner.stop_with_message("¡Done!")

        # Step 2: Decompress and storing the file
        spinner = Spinner("Decompressing")
        spinner.start()

        data_bytes = decode_b64_to_bytes(response.content)
        data = io.BytesIO(data_bytes)

        try:
            with py7zr.SevenZipFile(data, 'r', password=key.private_key.hex()) as archive:
                archive.extractall(path=file_path)
                filename = archive.getnames()

            spinner.stop_with_message("¡Done!")
            print(f"Data downloaded successfully in {file_path}{filename[0]}")

        except lzma.LZMAError:
            spinner.stop_with_message("Error: Decompression failed.")
            print("ERROR: Decompression failed. The data is corrupted or the key is incorrect.")
            return

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: Network error - {error_message}.")
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
        body = {"file_uuid": file_uuid}
        headers = create_headers(sign_data(body, key), key, content_type="application/x-www-form-urlencoded")
        response = requests.post(f"{validator_url}/remove", body, headers=headers, verify=False)

        response.raise_for_status()
        spinner.stop_with_message("¡Done!")

        print(f"File {file_uuid} will be removed.")

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: Network error - {error_message}.")
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
