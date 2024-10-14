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
import json
import os

import aiofiles
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

        async def _read_streaming_response(response: ClientResponse):
            # Currently buffer 16KB
            chunk_size = 16384
            data = bytearray()
            async for chunk in response.content.iter_chunked(chunk_size):
                data.extend(chunk)
            return bytes(data)

        async def _get_body(response: ClientResponse):
            response.raise_for_status()
            if response.status != 200:
                raise Exception(f"Unexpected status code: {response.status}, response: {await response.text()}")

            content_type = response.headers.get('Content-Type')
            if content_type == 'application/json':
                return await response.json()
            elif content_type == 'application/octet-stream':
                return await _read_streaming_response(response)
            else:
                raise Exception(f"Unknown content type: {content_type}")

        async def chunk_generator(file_path):
            async with aiofiles.open(file_path, 'rb') as file:
                while True:
                    chunk = await file.read(8192)
                    if not chunk:
                        break
                    yield chunk

        try:
            async with ClientSession(timeout=aiohttp.ClientTimeout(connect=5, sock_connect=5, total=timeout)) as session:
                if file:
                    file_size = os.path.getsize(file["chunk"])
                    file_hash = await calculate_hash(file["chunk"])
                    _, headers = create_request_data(self.key, target_key, {"file_hash": file_hash, "file_size_bytes": file_size}, content_type="application/octet-stream")
                    headers["X-File-Size"] = str(file_size)
                    headers["X-File-Hash"] = file_hash
                    headers["Folder"] = file['folder']
                    headers["Target-Key"] = target_key

                    async with session.post(url, data=chunk_generator(file["chunk"]), headers=headers, ssl=False) as response:
                        return await _get_body(response)
                else:
                    serialized_data, headers = create_request_data(self.key, target_key, params)
                    if fn == "remove":
                        async with session.delete(url, json=json.loads(serialized_data), headers=headers, ssl=False) as response:
                            return await _get_body(response)
                    else:
                        async with session.post(url, json=json.loads(serialized_data), headers=headers, ssl=False) as response:
                            return await _get_body(response)

        except asyncio.TimeoutError as e:
            raise Exception(f"The call took longer than the timeout of {timeout} second(s)").with_traceback(e.__traceback__)
        except aiohttp.ClientError as e:
            raise Exception(f"An error occurred: {e}").with_traceback(e.__traceback__)
