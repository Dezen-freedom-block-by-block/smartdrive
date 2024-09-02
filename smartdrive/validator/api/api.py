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

import uvicorn
import os
from fastapi import FastAPI

import smartdrive
from smartdrive.validator.api.middleware.subnet_middleware import SubnetMiddleware
from smartdrive.validator.config import config_manager
from smartdrive.validator.api.retrieve_api import RetrieveAPI
from smartdrive.validator.api.remove_api import RemoveAPI
from smartdrive.validator.api.store_api import StoreAPI
from smartdrive.validator.node.node import Node


class API:
    _app = FastAPI()

    _store_api: StoreAPI = None
    _retrieve_api: RetrieveAPI = None
    _remove_api: RemoveAPI = None

    def __init__(self, node: Node):
        self._store_api = StoreAPI(node)
        self._retrieve_api = RetrieveAPI(node)
        self._remove_api = RemoveAPI(node)

        self._app.add_middleware(SubnetMiddleware)

        self._app.add_api_route("/method/ping", self._ping_endpoint, methods=["POST"])
        self._app.add_api_route("/store", self._store_api.store_endpoint, methods=["POST"])
        self._app.add_api_route("/retrieve", self._retrieve_api.retrieve_endpoint, methods=["GET"])
        self._app.add_api_route("/remove", self._remove_api.remove_endpoint, methods=["DELETE"])

    async def run_server(self) -> None:
        """
        Starts and runs an asynchronous web server using Uvicorn.

        This method configures and starts an Uvicorn server for an ASGI application
        with SSL/TLS support. The server listens on all network interfaces (0.0.0.0)
        and on the port specified in the instance configuration.
        """
        dir = os.path.dirname(os.path.abspath(__file__))
        config = uvicorn.Config(self._app, workers=8, host="0.0.0.0", port=config_manager.config.port, ssl_keyfile=f"{dir}/cert/key.pem", ssl_certfile=f"{dir}/cert/cert.pem", log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    def _ping_endpoint(self):
        """
        Return a dictionary with validator information.

        Returns:
            dict: Identify information.
        """
        return {"type": "validator", "version": smartdrive.__version__}
