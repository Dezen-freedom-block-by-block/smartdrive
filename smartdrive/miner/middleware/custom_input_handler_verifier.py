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
from functools import partial
from typing import Any, List

from communex._common import get_node_url
from starlette.requests import Request
from starlette.responses import JSONResponse
from substrateinterface import Keypair

from communex.module._util import json_error, try_ss58_decode, log_reffusal, log, make_client
from communex.module.routers.module_routers import InputHandlerVerifier, is_hex_string
from communex.types import Ss58Address
from communex.util import parse_hex
from communex.util.memo import TTLDict
from communex.module import _signer as signer

from smartdrive.config import MINER_STORE_TIMEOUT_SECONDS


class CustomInputHandlerVerifier(InputHandlerVerifier):
    def __init__(
            self,
            subnets_whitelist: List[int] | None,
            module_key: Ss58Address,
            request_staleness: int,
            blockchain_cache: TTLDict[str, List[Ss58Address]],
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

        timestamp = body_dict.get("params").get("timestamp", None) if body_dict.get("params") else None
        legacy_timestamp = request.headers.get("X-Timestamp", None)
        try:
            timestamp_to_use = timestamp if not legacy_timestamp else legacy_timestamp
            request_time = datetime.fromisoformat(timestamp_to_use)
        except Exception:
            return JSONResponse(status_code=400, content={"error": "Invalid ISO timestamp given"})
        if (datetime.now(timezone.utc) - request_time).total_seconds() > (MINER_STORE_TIMEOUT_SECONDS if request.url.path == "/method/store" else self.request_staleness):  # For miner storage
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

        content_type = request.headers.get("Content-Type")
        legacy_verified = False

        if content_type and "application/octet-stream" in content_type:
            signed_body = {"params": {"chunk_hash": headers_dict.get("X-Chunk-Hash"), "chunk_size_bytes": int(headers_dict.get("X-Chunk-Size")), "target_key": headers_dict.get("Target-Key")}}
        else:
            signed_body = await self._get_signed_body(request)

        stamped_body = json.dumps(signed_body).encode()
        verified = signer.verify(key, crypto, stamped_body, signature)

        if not verified and not legacy_verified:
            reason = "Signature doesn't match"
            log_reffusal(key_ss58, reason)
            return (False, json_error(401, "Signatures doesn't match"))

        body_dict: dict[str, dict[str, Any]] = signed_body
        target_key = (body_dict.get('params') and body_dict.get('params').get("target_key", None)) or headers_dict.get("Target-Key", None)
        if not target_key or target_key != module_key:
            reason = "Wrong target_key in body"
            log_reffusal(key_ss58, reason)
            return (False, json_error(401, "Wrong target_key in body"))
        return (True, None)

    def _check_key_registered(
            self,
            subnets_whitelist: list[int] | None,
            headers_dict: dict[str, str],
            blockchain_cache: TTLDict[str, list[Ss58Address]],
            host_key: Keypair,
            use_testnet: bool,
            request: Request = None
    ):
        key = headers_dict["x-key"]
        if not is_hex_string(key):
            return (False, json_error(400, "X-Key should be a hex value"))
        key = parse_hex(key)

        # TODO: checking for key being registered should be smarter
        # e.g. query and store all registered modules periodically.

        ss58 = try_ss58_decode(key)
        if ss58 is None:
            reason = "Caller key could not be decoded into a ss58address"
            log_reffusal(key.decode(), reason)
            return (False, json_error(400, reason))

        # If subnets whitelist is specified, checks if key is registered in one
        # of the given subnets

        allowed_subnets: dict[int, bool] = {}
        caller_subnets: list[int] = []
        if subnets_whitelist is not None:
            def query_keys(subnet: int):
                try:
                    node_url = get_node_url(None, use_testnet=use_testnet)
                    client = make_client(node_url)  # TODO: get client from outer context
                    return [*client.query_map_key(subnet).values()]
                except Exception:
                    log(
                        "WARNING: Could not connect to a blockchain node"
                    )
                    return_list: list[Ss58Address] = []
                    return return_list

            # TODO: client pool for entire module server

            got_keys = False
            no_keys_reason = (
                "Miner could not connect to a blockchain node "
                "or there is no key registered on the subnet(s) {} "
            )
            for subnet in subnets_whitelist:
                get_keys_on_subnet = partial(query_keys, subnet)
                cache_key = f"keys_on_subnet_{subnet}"
                keys_on_subnet = blockchain_cache.get_or_insert_lazy(
                    cache_key, get_keys_on_subnet
                )
                if len(keys_on_subnet) == 0:
                    reason = no_keys_reason.format(subnet)
                    log(f"WARNING: {reason}")
                else:
                    got_keys = True
                if host_key.ss58_address not in keys_on_subnet:
                    log(
                        f"WARNING: This miner is deregistered on subnet {subnet}"
                    )
                else:
                    allowed_subnets[subnet] = True
                if ss58 in keys_on_subnet:
                    caller_subnets.append(subnet)
            if not got_keys:
                return False, json_error(503, no_keys_reason.format(subnets_whitelist))
            if not allowed_subnets:
                log("WARNING: Miner is not registered on any subnet")
                return False, json_error(403, "Miner is not registered on any subnet")

            # searches for a common subnet between caller and miner
            # TODO: use sets
            allowed_subnets = {
                subnet: allowed for subnet, allowed in allowed_subnets.items() if (
                        subnet in caller_subnets  # noqa E126
                )
            }

            # Allow to connect clients to miner in [store, remove, retrieve] method
            if (request and request.url.path not in ["/method/store", "/method/remove", "/method/retrieve"] and not allowed_subnets) or (not request and not allowed_subnets):
                reason = "Caller key is not registered in any subnet that the miner is"
                log_reffusal(ss58, reason)
                return False, json_error(
                    403, reason
                )
        else:
            # accepts everything
            pass

        return (True, None)

    async def _check_inputs(
            self,
            request: Request,
            body: bytes,
            module_key: Ss58Address
    ):
        required_headers = ["x-signature", "x-key", "x-crypto"]
        optional_headers = ["x-timestamp", 'X-Chunk-Size', 'Target-Key', 'X-Chunk-Hash', 'X-Event-UUID']

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
            request
        ):
            case (False, error):
                return (False, error)  # noqa F821
            case (True, _):
                pass

        return (True, None)

    async def _get_signed_body(self, request: Request):
        if request.headers.get("X-Chunk-Size", None):
            body = {}
        else:
            body_bytes = await request.body()
            request._body = body_bytes
            body = await request.json()

        return body
