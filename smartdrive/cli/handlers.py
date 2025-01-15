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
import os
from pathlib import Path
from getpass import getpass
from typing import List, Dict

import urllib3
import requests
import zstandard as zstd
from communex.types import Ss58Address
from substrateinterface import Keypair
from nacl.exceptions import CryptoError

from communex.compat.key import is_encrypted, classic_load_key

import smartdrive
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.commune.request import get_filtered_modules, execute_miner_request
from smartdrive.logging_config import logger
from smartdrive.cli.errors import NoValidatorsAvailableException
from smartdrive.cli.spinner import Spinner
from smartdrive.cli.utils import compress_encrypt_and_split_file, determine_chunk_size_and_semaphore_limit, \
    decompress_decrypt_and_unify_file, determine_semaphore_limit
from smartdrive.commune.module._protocol import create_headers
from smartdrive.models.event import RetrieveInputParams, RemoveInputParams, StoreRequestInputParams, \
    ChunkParams, StoreParams, ValidationEvent
from smartdrive.sign import sign_data
from smartdrive.utils import format_size, _get_validator_url, _get_validator_url_async
from smartdrive.config import MAXIMUM_STORAGE_PER_USER_PER_FILE, REDUNDANCY_PER_CHUNK, MINER_STORE_TIMEOUT_SECONDS, \
    DEFAULT_CLIENT_PATH, MAX_ENCODED_RANGE_SUB_CHUNKS, MINER_RETRIEVE_TIMEOUT_SECONDS
from smartdrive.validator.api.exceptions import ChunkNotAvailableException, FileNotAvailableException, RedundancyException
from smartdrive.validator.models.models import ModuleType

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
    file_size = os.path.getsize(file_path)

    if not Path(file_path).is_file():
        logger.error(f"File {file_path} does not exist or is a directory.")
        return

    if file_size > MAXIMUM_STORAGE_PER_USER_PER_FILE:
        logger.error(f"Error: File size exceeds the maximum limit of {format_size(MAXIMUM_STORAGE_PER_USER_PER_FILE)}")
        return

    key = _get_key(key_name)
    chunks = []
    try:
        spinner = Spinner("Splitting file into chunks, compressing and encrypting")
        spinner.start()
        chunk_size, simultaneous_uploads = determine_chunk_size_and_semaphore_limit(file_size=file_size)
        output_dir = os.path.expanduser(DEFAULT_CLIENT_PATH)
        chunks, final_chunk_hash, final_chunk_size_bytes = compress_encrypt_and_split_file(file_path, key.private_key[:32], chunk_size, output_dir)

        spinner.stop_with_message("Done!")

        spinner = Spinner("Sending store request")
        spinner.start()

        filtered_chunks = [
            {key: value for key, value in chunk.items() if key != "chunk_path"}
            for chunk in chunks
        ]
        input_params = StoreRequestInputParams(file_hash=final_chunk_hash, file_size_bytes=final_chunk_size_bytes, chunks=filtered_chunks)
        signed_data = sign_data(input_params.dict(), key)

        validator_url = _get_validator_url(key, testnet)
        headers = create_headers(signed_data, key, show_content_type=False)

        response = requests.post(
            url=f"{validator_url}/store/request",
            headers=headers,
            json=input_params.dict(),
            verify=False,
            timeout=60
        )

        response.raise_for_status()
        message = response.json()

        file_uuid = message.get("file_uuid")
        store_request_event_uuid = message.get("store_request_event_uuid")

        if file_uuid and store_request_event_uuid:
            spinner.stop_with_message("Done!")

            asyncio.run(
                _check_store_permission(
                    key=key,
                    testnet=testnet,
                    store_request_event_uuid=store_request_event_uuid,
                    chunks=chunks,
                    file_uuid=file_uuid,
                    file_size=final_chunk_size_bytes,
                    file_hash=final_chunk_hash,
                    simultaneous_uploads=simultaneous_uploads
                )
            )
        else:
            spinner.stop_with_message("Error, try again")

    except NoValidatorsAvailableException:
        spinner.stop_with_message("Error: No validators available")
    except requests.RequestException as e:
        try:
            error_message = e.response.json().get('detail', 'Unknown error')
            spinner.stop_with_message(f"Error: {error_message}.")
        except ValueError:
            spinner.stop_with_message("Error: Network error.")
    except Exception as e:
        spinner.stop_with_message(f"Unexpected error. {e}")
    finally:
        if chunks:
            for chunk in chunks:
                os.remove(chunk["chunk_path"])


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

        response = requests.get(f"{validator_url}/retrieve", params=input_params.dict(), headers=headers, verify=False)

        response.raise_for_status()
        spinner.stop_with_message("Done!")

        message = response.json()

        # Step 2: Decompress and storing the file
        spinner = Spinner("Downloading, decrypting and decompressing chunks")
        spinner.start()

        try:
            output_file = asyncio.run(
                _download_decrypt_and_decompress_chunks(
                    miner_with_chunks=message["miners_info"],
                    file_size=message["file_size"],
                    aes_key=key.private_key[:32],
                    keypair=key,
                    file_path=file_path
                )
            )
            spinner.stop_with_message("Done!")
            logger.info(f"File decrypted and decompressed successfully in {output_file}")

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


async def _check_store_permission(key: Keypair, testnet: bool, store_request_event_uuid: str, chunks: List[Dict[str, str]], file_uuid: str, file_size: int, file_hash: str, simultaneous_uploads: int):
    spinner = Spinner("Checking permission to store")
    spinner.start()

    for i in range(3):
        try:
            await asyncio.sleep(20)
            input_params = {"store_request_event_uuid": store_request_event_uuid, "user_ss58_address": key.ss58_address}
            signed_data = sign_data(input_params, key)
            headers = create_headers(signed_data, key, show_content_type=False)
            validator_url = await _get_validator_url_async(key=key, testnet=testnet)

            response = requests.post(
                url=f"{validator_url}/store/check-approval",
                headers=headers,
                json=input_params,
                verify=False,
                timeout=30
            )

            if response.status_code == requests.codes.ok:
                spinner.stop_with_message("Done!")
                try:
                    await _store_chunks(
                        chunks=chunks,
                        keypair=key,
                        testnet=testnet,
                        file_uuid=file_uuid,
                        file_hash=file_hash,
                        file_size=file_size,
                        event_uuid=store_request_event_uuid,
                        simultaneous_uploads=simultaneous_uploads
                    )
                    return
                except Exception:
                    return
            elif response.status_code != requests.codes.not_found:
                spinner.stop_with_message(response.json().get("detail", "No permission to store"))
                return
        except Exception:
            continue

    spinner.stop_with_message("Error, try again.")


async def _store_chunks(chunks: List[Dict[str, str]], file_uuid: str, file_hash: str, file_size: int, keypair: Keypair, testnet: bool, event_uuid: str, simultaneous_uploads: int):
    """
    Stores the prepared chunks concurrently into multiple miners with redundancy.

    Args:
        chunks (List[Dict[str, str]]): The chunks to be stored.
        file_uuid (str): UUID of the parent file.
        file_hash (str): SHA256 hash of the file.
        file_size (int): Size of the file.
        keypair (Keypair): Client's keypair for signing requests.
        testnet (bool): Flag to indicate if the testnet should be used.
        event_uuid (str): UUID of the event.
        simultaneous_uploads (int): Number of simultaneous uploads.

    Raises:
        Exception: If any chunk fails to achieve sufficient redundancy.
    """
    logger.info(f"Uploading chunks, total {len(chunks)}...")

    miners = await get_filtered_modules(
        netuid=smartdrive.TESTNET_NETUID if testnet else smartdrive.NETUID,
        module_type=ModuleType.MINER,
        testnet=testnet,
        ss58_address=keypair.ss58_address
    )

    validators = await get_filtered_modules(
        netuid=smartdrive.TESTNET_NETUID if testnet else smartdrive.NETUID,
        module_type=ModuleType.VALIDATOR,
        testnet=testnet,
        ss58_address=keypair.ss58_address
    )

    if not miners:
        raise Exception("No miners available for storage.")

    semaphore = asyncio.Semaphore(simultaneous_uploads)
    successful_miners = []
    validation_events_info = []

    async def upload_chunk(chunk: Dict[str, str], miner):
        """
        Upload a single chunk to a specific miner.

        Args:
            chunk (dict): Metadata of the chunk.
            miner: Miner to upload the chunk to.

        Returns:
            bool: True if upload is successful, False otherwise.
        """
        async with semaphore:
            try:
                success = await execute_miner_request(
                    keypair,
                    miner.connection,
                    miner.ss58_address,
                    "store",
                    file={
                        'folder': keypair.ss58_address,
                        'chunk': chunk["chunk_path"],
                        'event_uuid': event_uuid,
                        'chunk_size': chunk["chunk_size"],
                        'chunk_hash': chunk["chunk_hash"]
                    },
                    timeout=MINER_STORE_TIMEOUT_SECONDS
                )
                if success and success["id"]:
                    successful_miners.append(
                        ChunkParams(
                            uuid=success["id"],
                            miner_ss58_address=miner.ss58_address,
                            chunk_index=chunk["chunk_index"],
                            chunk_hash=chunk["chunk_hash"],
                            chunk_size=chunk["chunk_size"],
                            miner_connection=miner.connection.__dict__
                        )
                    )

                    validation_events_info.append(
                        {
                            "chunk_path": chunk["chunk_path"],
                            "chunk_uuid": success["id"],
                            "miner_ss58_address": miner.ss58_address,
                        }
                    )

                    return True
            except Exception:
                return False
        return False

    async def upload_chunk_with_redundancy(chunk: Dict[str, str]):
        """
        Uploads a chunk to multiple miners, ensuring redundancy.

        Args:
            chunk (dict): Metadata of the chunk.

        Raises:
            Exception: If redundancy cannot be achieved.
        """
        if len(miners) < REDUNDANCY_PER_CHUNK:
            raise RedundancyException

        miners_copy = miners.copy()
        random.shuffle(miners_copy)

        redundancy_count = 0

        miner_iterator = iter(miners_copy)

        while redundancy_count < REDUNDANCY_PER_CHUNK:
            tasks = []
            try:
                for _ in range(REDUNDANCY_PER_CHUNK - redundancy_count):
                    miner = next(miner_iterator, None)
                    if not miner:
                        break
                    tasks.append(upload_chunk(chunk, miner))

                if not tasks:
                    break

                results = await asyncio.gather(*tasks)
                redundancy_count += sum(1 for result in results if result is True)

            except StopIteration:
                break

            except Exception:
                pass

        if redundancy_count < REDUNDANCY_PER_CHUNK:
            raise RedundancyException(f"Failed to achieve redundancy {REDUNDANCY_PER_CHUNK}")

        logger.info(f"Chunk {int(chunk['chunk_index']) + 1} uploaded")

    try:
        tasks = [upload_chunk_with_redundancy(chunk) for chunk in chunks]
        await asyncio.gather(*tasks)

        # It is then used to build an object file.
        event_params = StoreParams(
            chunks_params=[
                ChunkParams(
                    **{**chunk.__dict__, "miner_connection": None, "chunk_hash": None, "chunk_size": None}
                )
                for chunk in successful_miners
            ],
            file_hash=file_hash,
            file_size=file_size,
            file_uuid=file_uuid
        )

        validation_events = generate_validation_events_per_validator(
            stored_chunks=validation_events_info,
            validators_len=len(validators) + 1,  # To include myself
            user_ss58_address=keypair.ss58_address,
            file_uuid=file_uuid
        )

        event_signed_params = sign_data(event_params.dict(), keypair)

        # It is used to obtain all the chunk_params data to be able to approve the request later.
        event_params.chunks_params = [
            ChunkParams(
                **{**chunk.__dict__, "miner_connection": None}
            )
            for chunk in successful_miners
        ]
        validation_events_serialized = [
            [validation_event.dict() for validation_event in validator_events]
            for validator_events in validation_events
        ]

        json = {"event_params": event_params.dict(), "event_signed_params": event_signed_params.hex(), "validation_events": validation_events_serialized}
        signed_data = sign_data(json, keypair)

        validator_url = await _get_validator_url_async(keypair, testnet)
        headers = create_headers(signed_data, keypair, show_content_type=False)
        headers.update({"X-Event-UUID": event_uuid})

        response = requests.post(
            url=f"{validator_url}/store/approve",
            headers=headers,
            json=json,
            verify=False
        )

        if response.status_code == requests.codes.ok:
            logger.info(f"All chunks stored successfully with redundancy! File UUID: {file_uuid}")
        else:
            await _cleanup_chunks(stored_chunks=successful_miners, keypair=keypair)
            logger.error("Error storing chunks with redundancy")
    except Exception as e:
        logger.error(f"Error during chunk upload: {e}")
        await _cleanup_chunks(stored_chunks=successful_miners, keypair=keypair)
        raise


async def _download_decrypt_and_decompress_chunks(
        miner_with_chunks: Dict[int, List[Dict[str, str]]],
        file_size: int,
        aes_key: bytes,
        keypair: Keypair,
        file_path: str
) -> str:
    """
    Download all chunks from miners, store them in a temporary directory,
    then decrypt, decompress, and assemble them into the final file.

    Args:
        miner_with_chunks (Dict[int, List[Dict[str, str]]]): Mapping of chunk indices to miners holding those chunks.
        file_size (int): Size of the file.
        aes_key (bytes): AES key for decryption.
        keypair (Keypair): Client's keypair for signing requests.
        file_path (str): Path to the final directory.

    Returns:
        str: Path to the final file.

    Raises:
        FileNotAvailableException: If any chunk cannot be retrieved or processed.
        Exception: For other unexpected errors.
    """
    client_dir = os.path.expanduser(DEFAULT_CLIENT_PATH)
    os.makedirs(client_dir, exist_ok=True)
    semaphore_limit = determine_semaphore_limit(file_size=file_size)
    semaphore = asyncio.Semaphore(semaphore_limit)
    chunk_paths = []

    async def download_chunk(chunk_index: int, miners_info: List[Dict[str, str]]) -> str:
        """
        Download a single chunk from the available miners and save it to the temp directory.

        Args:
            chunk_index (int): Index of the chunk to download.
            miners_info (List[Dict[str, str]]): Information about miners holding the chunk.

        Returns:
            str: Path to the downloaded chunk file.
        """
        async with semaphore:
            for miner_info in miners_info:
                connection = ConnectionInfo(
                    miner_info["connection"]["ip"],
                    miner_info["connection"]["port"]
                )
                miner = ModuleInfo(
                    miner_info["uid"],
                    miner_info["ss58_address"],
                    connection
                )
                try:
                    miner_response = await execute_miner_request(
                        validator_key=keypair,
                        connection=miner.connection,
                        miner_key=miner.ss58_address,
                        action="retrieve",
                        params={
                            "folder": keypair.ss58_address,
                            "chunk_uuid": miner_info["chunk_uuid"],
                            "chunk_index": chunk_index,
                            "user_path": client_dir
                        },
                        timeout=MINER_RETRIEVE_TIMEOUT_SECONDS
                    )

                    if miner_response:
                        return miner_response
                except Exception:
                    continue

        raise ChunkNotAvailableException(f"Chunk {chunk_index} could not be retrieved from any miner.")

    try:
        chunk_paths = await asyncio.gather(*[
            download_chunk(chunk_index, miners_info)
            for chunk_index, miners_info in miner_with_chunks.items()
        ])

        chunk_paths = sorted(
            chunk_paths,
            key=lambda path: int(path.split("_")[-1].split(".")[0])
        )

        return decompress_decrypt_and_unify_file(aes_key=aes_key, chunk_paths=chunk_paths, file_path=file_path)

    except Exception as e:
        try:
            if chunk_paths:
                for chunk in chunk_paths:
                    os.remove(chunk)
        except FileNotFoundError:
            pass
        logger.error(f"Error during file download and reconstruction: {e}")
        raise FileNotAvailableException("Failed to retrieve or process all chunks.")


async def _cleanup_chunks(stored_chunks: List[ChunkParams], keypair: Keypair):
    """
    Cleans up partially stored chunks by sending delete requests to the corresponding miners.

    Args:
        stored_chunks (list): List of dictionaries containing chunk UUIDs and the miners storing them.
    """
    for chunk in stored_chunks:
        try:
            await execute_miner_request(
                keypair, ConnectionInfo(ip=chunk.miner_connection["ip"], port=chunk.miner_connection["port"]),
                Ss58Address(chunk.miner_ss58_address), "remove", {
                    "folder": keypair.ss58_address,
                    "chunk_uuid": chunk.uuid
                }
            )
        except Exception:
            continue


def generate_validation_events_per_validator(stored_chunks: List[dict], validators_len: int, user_ss58_address: str, file_uuid: str) -> List[List[ValidationEvent]]:
    validations_events_per_validator = []

    for _ in range(validators_len):
        validator_events = []
        for chunk in stored_chunks:
            chunk_path = chunk["chunk_path"]
            chunk_uuid = chunk["chunk_uuid"]
            miner_ss58_address = chunk["miner_ss58_address"]

            with open(chunk_path, 'rb') as chunk_file:
                file_size = os.path.getsize(chunk_path)
                sub_chunk_start = random.randint(0, max(0, file_size - MAX_ENCODED_RANGE_SUB_CHUNKS))
                sub_chunk_end = min(sub_chunk_start + MAX_ENCODED_RANGE_SUB_CHUNKS, file_size)

                chunk_file.seek(sub_chunk_start)
                sub_chunk = chunk_file.read(sub_chunk_end - sub_chunk_start)
                sub_chunk_encoded = sub_chunk.hex()

                validation_event = ValidationEvent(
                    uuid=chunk_uuid,
                    miner_ss58_address=miner_ss58_address,
                    sub_chunk_start=sub_chunk_start,
                    sub_chunk_end=sub_chunk_end,
                    sub_chunk_encoded=sub_chunk_encoded,
                    file_uuid=file_uuid,
                    user_owner_ss58_address=user_ss58_address
                )
                validator_events.append(validation_event)

        if validator_events:
            validations_events_per_validator.append(validator_events)

    return validations_events_per_validator
