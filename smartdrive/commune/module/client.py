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
import asyncio
import json
import aiohttp
import requests
from aiohttp import ClientSession, ClientResponse
from urllib3.exceptions import InsecureRequestWarning

from substrateinterface import Keypair

from ._protocol import create_method_endpoint, create_request_data
from ..utils import calculate_hash

requests.packages.urllib3.disable_warnings(InsecureRequestWarning)


class ModuleClient:
    host: str
    port: int
    key: Keypair

    def __init__(self, host: str, port: int, key: Keypair):
        self.host = host
        self.port = port
        self.key = key

    async def call(self, fn, target_key, params=None, file=None, timeout=16):
        if params is None:
            params = {}

        url = create_method_endpoint(self.host, self.port, fn)

        async def _get_body(response: ClientResponse):
            response.raise_for_status()
            if response.status != 200:
                raise Exception(f"Unexpected status code: {response.status}, response: {await response.text()}")

            content_type = response.headers.get('Content-Type')
            if content_type == 'application/json':
                return await response.json()
            elif content_type == 'application/octet-stream':
                return await response.read()
            else:
                raise Exception(f"Unknown content type: {content_type}")

        try:
            async with ClientSession() as session:
                if file:
                    file_content = file['chunk']
                    file_hash = calculate_hash(file_content)
                    data_to_sign = {'folder': file['folder'], 'chunk': file_hash}
                    serialized_data, headers = create_request_data(self.key, target_key, data_to_sign, False)

                    form_data = aiohttp.FormData()
                    form_data.add_field('folder', file['folder'])
                    form_data.add_field('chunk', file_content, filename='file', content_type='application/octet-stream')
                    form_data.add_field('target_key', target_key)

                    async with session.post(url, data=form_data, headers=headers, timeout=timeout, ssl=False) as response:
                        return await _get_body(response)
                else:
                    serialized_data, headers = create_request_data(self.key, target_key, params)
                    async with session.post(url, json=json.loads(serialized_data), headers=headers, timeout=timeout, ssl=False) as response:
                        return await _get_body(response)

        except asyncio.TimeoutError as e:
            raise Exception(f"The call took longer than the timeout of {timeout} second(s)").with_traceback(e.__traceback__)
        except aiohttp.ClientError as e:
            raise Exception(f"An error occurred: {e}").with_traceback(e.__traceback__)