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

import json
from datetime import datetime, timezone
from typing import Any

from starlette.requests import Request
from starlette.responses import JSONResponse
from substrateinterface import Keypair

from communex.module._util import json_error, try_ss58_decode, log_reffusal
from communex.module.routers.module_routers import InputHandlerVerifier, is_hex_string
from communex.types import Ss58Address
from communex.util import parse_hex
from communex.util.memo import TTLDict
from communex.module import _signer as signer

from smartdrive.commune.utils import calculate_hash


class CustomInputHandlerVerifier(InputHandlerVerifier):
    def __init__(
            self,
            subnets_whitelist: list[int] | None,
            module_key: Ss58Address,
            request_staleness: int,
            blockchain_cache: TTLDict[str, list[Ss58Address]],
            host_key: Keypair,
            use_testnet: bool,
    ):
        super().__init__(subnets_whitelist, module_key, request_staleness, blockchain_cache, host_key, use_testnet)

    async def verify(self, request: Request):
        body = await request.body()

        match await self._check_inputs(request, body, self.module_key):
            case (False, error):
                return error  # noqa F821
            case (True, _):
                pass

        body_dict = await self._get_signed_body(request)
        timestamp = body_dict['params'].get("timestamp", None)
        legacy_timestamp = request.headers.get("X-Timestamp", None)
        try:
            timestamp_to_use = timestamp if not legacy_timestamp else legacy_timestamp
            request_time = datetime.fromisoformat(timestamp_to_use)
        except Exception:
            return JSONResponse(status_code=400, content={"error": "Invalid ISO timestamp given"})
        if (datetime.now(timezone.utc) - request_time).total_seconds() > self.request_staleness:
            return JSONResponse(status_code=400, content={"error": "Request is too stale"})
        return None

    async def _check_signature(
            self,
            headers_dict: dict[str, str],
            request: Request,
            module_key: Ss58Address
    ):
        key = headers_dict["x-key"]
        signature = headers_dict["x-signature"]
        crypto = int(headers_dict["x-crypto"])

        if not is_hex_string(key):
            reason = "X-Key should be a hex value"
            log_reffusal(key, reason)
            return (False, json_error(400, reason))
        try:
            signature = parse_hex(signature)
        except Exception:
            reason = "Signature sent is not a valid hex value"
            log_reffusal(key, reason)
            return False, json_error(400, reason)
        try:
            key = parse_hex(key)
        except Exception:
            reason = "Key sent is not a valid hex value"
            log_reffusal(key, reason)
            return False, json_error(400, reason)
        key_ss58 = try_ss58_decode(key)
        if key_ss58 is None:
            reason = "Caller key could not be decoded into a ss58address"
            log_reffusal(key.decode(), reason)
            return (False, json_error(400, reason))

        legacy_verified = False
        signed_body = await self._get_signed_body(request)
        stamped_body = json.dumps(signed_body).encode()
        verified = signer.verify(key, crypto, stamped_body, signature)

        if not verified and not legacy_verified:
            reason = "Signature doesn't match"
            log_reffusal(key_ss58, reason)
            return (False, json_error(401, "Signatures doesn't match"))

        body_dict: dict[str, dict[str, Any]] = signed_body
        target_key = body_dict['params'].get("target_key", None)
        if not target_key or target_key != module_key:
            reason = "Wrong target_key in body"
            log_reffusal(key_ss58, reason)
            return (False, json_error(401, "Wrong target_key in body"))

        return (True, None)

    async def _check_inputs(
            self,
            request: Request,
            body: bytes,
            module_key: Ss58Address
    ):
        required_headers = ["x-signature", "x-key", "x-crypto"]
        optional_headers = ["x-timestamp"]

        match self._get_headers_dict(request.headers, required_headers, optional_headers):
            case (False, error):
                return (False, error)  # noqa F821
            case (True, headers_dict):
                pass

        match await self._check_signature(headers_dict, request, module_key):  # noqa F821
            case (False, error):
                return (False, error)  # noqa F821
            case (True, _):
                pass

        match self._check_key_registered(
            self.subnets_whitelist,
            headers_dict,  # noqa F821
            self.blockchain_cache,
            self.host_key,
            self.use_testnet,
        ):
            case (False, error):
                return (False, error)  # noqa F821
            case (True, _):
                pass

        return (True, None)

    async def _get_signed_body(self, request: Request):
        body_bytes = await request.body()
        request._body = body_bytes

        content_type = request.headers.get("Content-Type")
        if content_type and "multipart/form-data" in content_type:
            body = await request.form()
            body_dict = {}
            for key in body:
                if key == "chunk":
                    body_dict[key] = await body[key].read()
                else:
                    body_dict[key] = body[key]

            return {
                "params": {
                    "folder": body_dict["folder"],
                    "chunk": calculate_hash(body_dict["chunk"]),
                    "target_key": body_dict["target_key"]
                }
            }
        else:
            body = await request.json()
            return body
