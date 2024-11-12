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

import random
from substrateinterface import Keypair
from fastapi import Request, Response
from typing import Awaitable, Callable, List
from starlette.types import ASGIApp
from starlette.middleware.base import BaseHTTPMiddleware

from communex.key import check_ss58_address
from communex.module._rate_limiters.limiters import IpLimiterParams, StakeLimiterParams
from communex.module._util import json_error
from communex.module.routers.module_routers import IpLimiterVerifier, ListVerifier
from communex.types import Ss58Address
from communex.util.memo import TTLDict

from smartdrive.miner.middleware.custom_input_handler_verifier import CustomInputHandlerVerifier

Callback = Callable[[Request], Awaitable[Response]]


class MinerMiddleware(BaseHTTPMiddleware):

    def __init__(
            self,
            app: ASGIApp,
            key: Keypair,
            max_request_staleness: int = 120,
            whitelist: List[Ss58Address] | None = None,
            blacklist: List[Ss58Address] | None = None,
            subnets_whitelist: List[int] | None = None,
            lower_ttl: int = 600,
            upper_ttl: int = 700,
            limiter: StakeLimiterParams | IpLimiterParams = StakeLimiterParams(),
            ip_blacklist: List[str] | None = None,
            use_testnet: bool = False
    ):
        super().__init__(app)
        self._subnets_whitelist = subnets_whitelist
        self.key = key
        self.max_request_staleness = max_request_staleness
        ttl = random.randint(lower_ttl, upper_ttl)
        self._blockchain_cache = TTLDict[str, List[Ss58Address]](ttl)
        self._ip_blacklist = ip_blacklist
        whitelist_: List[Ss58Address] = [] if whitelist is None else whitelist
        blacklist_: List[Ss58Address] = [] if blacklist is None else blacklist
        self._whitelist = whitelist_
        self._blacklist = blacklist_

        self._limiter_verifier = IpLimiterVerifier(limiter)
        self._input_handler = CustomInputHandlerVerifier(
            self._subnets_whitelist,
            check_ss58_address(key.ss58_address),
            self.max_request_staleness,
            self._blockchain_cache,
            self.key,
            use_testnet
        )
        self._check_lists = ListVerifier(
            self._blacklist,
            self._whitelist,
            self._ip_blacklist
        )

    async def dispatch(self, request: Request, call_next: Callback) -> Response:
        """
        Middleware function to handle request authentication and authorization.

        Params:
            request (Request): The incoming request object.
            call_next (Callback): The next request handler in the middleware chain.

        Returns:
            Response: The response object.
        """
        try:
            check_list_response = await self._check_lists.verify(request)
            if check_list_response:
                return check_list_response

            input_handler_response = await self._input_handler.verify(request)
            if input_handler_response:
                return input_handler_response

            limiter_verifier_response = await self._limiter_verifier.verify(request)
            if limiter_verifier_response:
                return limiter_verifier_response

            response = await call_next(request)
            return response
        except Exception as e:
            return json_error(400, str(e))
