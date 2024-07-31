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


import json
import requests

from substrateinterface import Keypair
from ._protocol import create_method_endpoint, create_request_data
from ..utils import calculate_hash


class ModuleClient:
    host: str
    port: int
    key: Keypair

    def __init__(self, host: str, port: int, key: Keypair):
        self.host = host
        self.port = port
        self.key = key

    def call(self, fn, target_key, params=None, files=None, timeout=16):
        if params is None:
            params = {}

        url = create_method_endpoint(self.host, self.port, fn)

        try:
            if files:
                file_content = files['chunk']
                file_hash = calculate_hash(file_content)
                files = {
                    'folder': (None, files['folder']),
                    'chunk': ('file', file_content, 'application/octet-stream'),
                    'target_key': (None, target_key)
                }
                data_to_sign = {'folder': files['folder'][1], 'chunk': file_hash}
                serialized_data, headers = create_request_data(self.key, target_key, data_to_sign, False)

                response = requests.post(
                    url,
                    files=files,
                    headers=headers,
                    timeout=timeout,
                    verify=False
                )
            else:
                serialized_data, headers = create_request_data(self.key, target_key, params)

                response = requests.post(
                    url,
                    json=json.loads(serialized_data),
                    headers=headers,
                    timeout=timeout,
                    verify=False
                )

            response.raise_for_status()

            if response.status_code != 200:
                raise Exception(f"Unexpected status code: {response.status_code}, response: {response.text}")

            if response.headers.get('Content-Type') == 'application/json':
                return response.json()
            elif response.headers.get('Content-Type') == 'application/octet-stream':
                return response.content
            else:
                raise Exception(f"Unknown content type: {response.headers.get('Content-Type')}")

        except requests.exceptions.Timeout as e:
            raise Exception(f"The call took longer than the timeout of {timeout} second(s)").with_traceback(e.__traceback__)
        except requests.exceptions.RequestException as e:
            raise Exception(f"An error occurred: {e}").with_traceback(e.__traceback__)
