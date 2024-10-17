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
import os
import sys
import time

import requests
from substrateinterface import Keypair

from smartdrive.cli.handlers import _get_key, _get_validator_url
from smartdrive.commune.connection_pool import initialize_commune_connection_pool
from smartdrive.commune.module._protocol import create_headers
from smartdrive.models.event import StoreInputParams
from smartdrive.sign import sign_data

SLEEP_TIME_SECONDS = 20


def _check_permission_store(file_path: str, file_hash: str, file_size_bytes: int, store_request_event_uuid: str, key: Keypair, testnet: bool, file_uuid: str):
    for i in range(3):
        try:
            time.sleep(SLEEP_TIME_SECONDS)

            input_params = {"store_request_event_uuid": store_request_event_uuid}
            signed_data = sign_data(input_params, key)
            headers = create_headers(signed_data, key, show_content_type=False)
            validator_url = _get_validator_url(key=key, testnet=testnet)

            response = requests.get(
                url=f"{validator_url}/store/check-permission",
                headers=headers,
                params=input_params,
                verify=False,
                timeout=30
            )

            if response.status_code == requests.codes.ok:
                _store_file(
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


def _store_file(file_path: str, file_hash: str, file_size_bytes: int, keypair: Keypair, testnet: bool, file_uuid: str, event_uuid: str):
    for i in range(3):
        try:
            input_params = StoreInputParams(file_hash=file_hash, file_size_bytes=file_size_bytes)
            signed_data = sign_data(input_params.dict(), keypair)
            headers = create_headers(signed_data, keypair, show_content_type=False)
            headers["X-File-Hash"] = file_hash
            headers["X-File-Size"] = str(file_size_bytes)
            headers["X-Event-UUID"] = event_uuid
            headers["X-File-UUID"] = file_uuid
            validator_url = _get_validator_url(key=key, testnet=testnet)

            with open(file_path, 'rb') as file:
                request = requests.post(
                    url=f"{validator_url}/store",
                    headers=headers,
                    data=file,
                    verify=False,
                    stream=True
                )

            if request.status_code == requests.codes.ok:
                break

        except Exception:
            continue

    if os.path.exists(file_path):
        os.remove(file_path)


if __name__ == "__main__":
    file_path = sys.argv[1]
    file_hash = sys.argv[2]
    file_size_bytes = int(sys.argv[3])
    store_request_event_uuid = sys.argv[4]
    key_name = sys.argv[5]
    testnet = sys.argv[6] == 'True'
    file_uuid = sys.argv[7]
    key = _get_key(key_name)

    initialize_commune_connection_pool(testnet, num_connections=1, max_pool_size=1)
    _check_permission_store(file_path, file_hash, file_size_bytes, store_request_event_uuid, key, testnet, file_uuid)
