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
from urllib.parse import parse_qs

from starlette.middleware.base import BaseHTTPMiddleware
from fastapi import Request, Response, UploadFile
from fastapi.responses import JSONResponse
from starlette.types import ASGIApp
from typing import Awaitable, Callable, Optional
from scalecodec.utils.ss58 import is_valid_ss58_address

from substrateinterface import Keypair
from substrateinterface.utils.ss58 import ss58_encode
from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.validator.api.middleware.sign import verify_data_signature

Callback = Callable[[Request], Awaitable[Response]]
exclude_paths = ["/method/ping"]


class SubnetMiddleware(BaseHTTPMiddleware):
    def __init__(self, app: ASGIApp, key: Keypair, comx_client: CommuneClient, netuid: int):
        super().__init__(app)
        self._key = key
        self._comx_client = comx_client
        self._netuid = netuid

    async def dispatch(self, request: Request, call_next: Callback) -> Response:
        if request.url.path in exclude_paths:
            return await call_next(request)

        if request.client is None:
            response = JSONResponse(
                status_code=401,
                content={
                    "detail": "Address should be present in request"
                }
            )
            return response

        key = request.headers.get('X-Key')
        if not key:
            response = JSONResponse(
                status_code=401,
                content={"detail": "Valid X-Key not provided on headers"}
            )
            return response

        ss58_address = get_ss58_address_from_public_key(key)
        if not ss58_address:
            response = JSONResponse(
                status_code=401,
                content={"detail": "Not a valid public key provided"}
            )
            return response

        staketo_modules = self._comx_client.get_staketo(ss58_address, self._netuid)
        staketo_addresses = staketo_modules.keys()
        if not staketo_addresses:
            response = JSONResponse(
                status_code=401,
                content={"detail": "You must stake to at least one active validator in the subnet"}
            )
            return response

        signature = request.headers.get('X-Signature')
        if request.method == "GET":
            body = dict(request.query_params)
        else:
            content_type = request.headers.get("Content-Type")
            if "multipart/form-data" in content_type:
                body_bytes = await request.body()
                request._body = body_bytes
                form = await request.form()
                body = {key: form[key] for key in form}
                if "file" in form:
                    file = form["file"]
                    body["file"] = str(await file.read())
                request._body = body_bytes
            elif content_type and "application/json" in content_type:
                body = await request.json()
            else:
                body = await request.body()
                try:
                    body = {key: value[0] if isinstance(value, list) else value for key, value in parse_qs(body.decode("utf-8")).items()}
                except UnicodeDecodeError:
                    body = body

        is_verified_signature = verify_data_signature(body, signature, ss58_address)
        if not is_verified_signature:
            response = JSONResponse(
                status_code=401,
                content={"detail": "Valid X-Signature not provided on headers"}
            )
            return response
        response = await call_next(request)

        return response


def get_ss58_address_from_public_key(public_key_hex) -> Optional[Ss58Address]:
    public_key_bytes = bytes.fromhex(public_key_hex)
    ss58_address = ss58_encode(public_key_bytes)
    return Ss58Address(ss58_address) if is_valid_ss58_address(ss58_address) else None
