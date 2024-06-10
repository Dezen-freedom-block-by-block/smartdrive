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

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp
from typing import Awaitable, Callable, Optional
from scalecodec.utils.ss58 import is_valid_ss58_address

from substrateinterface import Keypair
from substrateinterface.utils.ss58 import ss58_encode
from communex.client import CommuneClient
from communex.types import Ss58Address

Callback = Callable[[Request], Awaitable[Response]]


class SubnetMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, key: Keypair, comx_client: CommuneClient, netuid: int):
        super().__init__(app)
        self._key = key
        self._comx_client = comx_client
        self._netuid = netuid

    async def dispatch(self, request: Request, call_next: Callback) -> Response:
        if request.client is None:
            response = JSONResponse(
                status_code=401,
                content={
                    "error": "Address should be present in request"
                }
            )
            return response
        key = request.headers.get('x-key')
        if not key:
            response = JSONResponse(
                status_code=401,
                content={"error": "Valid X-Key not provided on headers"}
            )
            return response

        ss58_address = get_ss58_address_from_public_key(request.headers["x-key"])
        if not ss58_address:
            response = JSONResponse(
                status_code=401,
                content={"error": "Not a valid public key provided"}
            )
            return response

        staketo_modules = self._comx_client.get_staketo(ss58_address, self._netuid)
        staketo_addresses = staketo_modules.keys()
        if not staketo_addresses:
            response = JSONResponse(
                status_code=401,
                content={"error": "You must stake to at least one active validator in the subnet"}
            )
            return response

        response = await call_next(request)

        return response


def get_ss58_address_from_public_key(public_key_hex) -> Optional[Ss58Address]:
    public_key_bytes = bytes.fromhex(public_key_hex)
    ss58_address = ss58_encode(public_key_bytes)
    return Ss58Address(ss58_address) if is_valid_ss58_address(ss58_address) else None
