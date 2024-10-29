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
import os
import random
import sys

import aiofiles
import aiohttp
import requests
from substrateinterface import Keypair

import smartdrive
from smartdrive.cli.errors import NoValidatorsAvailableException
from smartdrive.cli.handlers import _get_key
from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_active_validators, EXTENDED_PING_TIMEOUT
from smartdrive.models.event import StoreInputParams
from smartdrive.sign import sign_data

SLEEP_TIME_SECONDS = 20


async def _check_permission_store(file_path: str, file_hash: str, file_size_bytes: int, store_request_event_uuid: str, key: Keypair, testnet: bool, file_uuid: str):
    for i in range(3):
        try:
            await asyncio.sleep(SLEEP_TIME_SECONDS)

            input_params = {"store_request_event_uuid": store_request_event_uuid}
            signed_data = sign_data(input_params, key)
            headers = create_headers(signed_data, key, show_content_type=False)
            validator_url = await _get_validator_url(key=key, testnet=testnet)

            response = requests.get(
                url=f"{validator_url}/store/check-permission",
                headers=headers,
                params=input_params,
                verify=False,
                timeout=30
            )

            if response.status_code == requests.codes.ok:
                await _store_file(
                    file_path=file_path,
                    file_hash=file_hash,
                    file_size_bytes=file_size_bytes,
                    keypair=key,
                    testnet=testnet,
                    file_uuid=file_uuid,
                    event_uuid=store_request_event_uuid
                )
                break
            elif response.status_code != requests.codes.accepted:
                break
        except Exception:
            continue


async def _store_file(file_path: str, file_hash: str, file_size_bytes: int, keypair: Keypair, testnet: bool, file_uuid: str, event_uuid: str):
    for i in range(3):
        try:
            input_params = StoreInputParams(file_hash=file_hash, file_size_bytes=file_size_bytes)
            signed_data = sign_data(input_params.dict(), keypair)
            headers = create_headers(signed_data, keypair, show_content_type=False)
            headers["X-File-Hash"] = file_hash
            headers["X-File-Size"] = str(file_size_bytes)
            headers["X-Event-UUID"] = event_uuid
            headers["X-File-UUID"] = file_uuid
            validator_url = await _get_validator_url(key=key, testnet=testnet)

            async with aiohttp.ClientSession(timeout=aiohttp.ClientTimeout(connect=5, sock_connect=5, total=20 * 60)) as session:
                async with aiofiles.open(file_path, 'rb') as file:
                    async with session.post(f"{validator_url}/store", data=file, headers=headers, ssl=False) as response:
                        if response.status == 200:
                            break

        except Exception:
            continue

    if os.path.exists(file_path):
        os.remove(file_path)


async def _get_validator_url(key: Keypair, testnet: bool = False) -> str:
    """
    Get the URL of an active validator.

    Params:
        key (Keypair): The keypair object.
        testnet (bool, optional): Flag to indicate if the testnet should be used.

    Returns:
        str: The URL of an active validator.
    """
    netuid = smartdrive.TESTNET_NETUID if testnet else smartdrive.NETUID

    try:
        validators = await get_active_validators(key, netuid, EXTENDED_PING_TIMEOUT)
    except CommuneNetworkUnreachable:
        raise NoValidatorsAvailableException

    if not validators:
        raise NoValidatorsAvailableException

    validator = random.choice(validators)
    return f"https://{validator.connection.ip}:{validator.connection.port}"


if __name__ == "__main__":
    file_path = sys.argv[1]
    file_hash = sys.argv[2]
    file_size_bytes = int(sys.argv[3])
    store_request_event_uuid = sys.argv[4]
    key_name = sys.argv[5]
    testnet = sys.argv[6] == 'True'
    file_uuid = sys.argv[7]
    key = _get_key(key_name)

    async def main():
        await _check_permission_store(file_path, file_hash, file_size_bytes, store_request_event_uuid, key, testnet, file_uuid)

    asyncio.run(main())
