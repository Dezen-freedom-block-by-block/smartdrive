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
from substrateinterface import Keypair

from communex.client import CommuneClient

import smartdrive
from smartdrive.validator.api.middleware.subnet_middleware import SubnetMiddleware
from smartdrive.validator.database.database import Database
from smartdrive.validator.api.retrieve_api import RetrieveAPI
from smartdrive.validator.api.remove_api import RemoveAPI
from smartdrive.validator.api.store_api import StoreAPI
from smartdrive.validator.api.database_api import DatabaseAPI
from smartdrive.validator.network.network import Network


class API:
    app = FastAPI()
    _config = None
    _key: Keypair = None
    _database: Database = None
    _comx_client: CommuneClient = None
    _network: Network = None

    store_api: StoreAPI = None
    retrieve_api: RetrieveAPI = None
    remove_api: RemoveAPI = None

    def __init__(self, config, key: Keypair, database: Database, comx_client: CommuneClient, network: Network):
        self._config = config
        self._key = key
        self._database = database
        self._comx_client = comx_client
        self._network = network

        self.database_api = DatabaseAPI(config, key, database, comx_client)
        self.store_api = StoreAPI(config, key, database, comx_client, network)
        self.retrieve_api = RetrieveAPI(config, key, database, comx_client, network)
        self.remove_api = RemoveAPI(config, key, database, comx_client, network)

        self.app.add_middleware(SubnetMiddleware, key=key, comx_client=comx_client, netuid=self._config._netuid)

        self.app.add_api_route("/method/ping", self.ping_endpoint, methods=["POST"])
        self.app.add_api_route("/database-block", self.database_api.database_block_endpoint, methods=["GET"])
        self.app.add_api_route("/database", self.database_api.database_endpoint, methods=["GET"])
        self.app.add_api_route("/store", self.store_api.store_endpoint, methods=["POST"])
        self.app.add_api_route("/retrieve", self.retrieve_api.retrieve_endpoint, methods=["GET"])
        self.app.add_api_route("/remove", self.remove_api.remove_endpoint, methods=["POST"])

    async def run_server(self) -> None:
        """
        Starts and runs an asynchronous web server using Uvicorn.

        This method configures and starts a Uvicorn server for an ASGI application
        with SSL/TLS support. The server listens on all network interfaces (0.0.0.0)
        and on the port specified in the instance configuration.
        """
        dir = os.path.dirname(os.path.abspath(__file__))
        config = uvicorn.Config(self.app, host="0.0.0.0", port=self._config.port, ssl_keyfile=f"{dir}/cert/key.pem", ssl_certfile=f"{dir}/cert/cert.pem", log_level="info")
        server = uvicorn.Server(config)
        await server.serve()

    def ping_endpoint(self):
        """
        Return a dictionary with validator information.

        Returns:
            dict: Identify information.
        """
        return {"type": "validator", "version": smartdrive.__version__}
