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

import multiprocessing

from substrateinterface import Keypair

from smartdrive.models.event import Event
from smartdrive.validator.database.database import Database
from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.server import Server


class Node:

    _key: Keypair = None
    _ip = None
    _netuid = None

    _server_process = None
    _mempool = None
    _connection_pool = None
    _testnet = False

    def __init__(self, keypair: Keypair, ip: str, netuid: int, database: Database, testnet: bool):
        self._keypair = keypair
        self._ip = ip
        self._netuid = netuid
        self._database = database

        self._mempool = multiprocessing.Manager().list()
        self._connection_pool = ConnectionPool(cache_size=Server.MAX_N_CONNECTIONS)
        self._testnet = testnet

        # Although these variables are managed by multiprocessing.Manager(),
        # we explicitly pass them as parameters to make it clear that they are dependencies of the server process.
        self._server_process = multiprocessing.Process(target=self.run_server, args=(self._mempool, self._connection_pool,))
        self._server_process.start()

    def run_server(self, mempool, connection_pool: ConnectionPool):
        server = Server(
            mempool=mempool,
            connection_pool=connection_pool,
            bind_address=self._ip,
            keypair=self._keypair,
            netuid=self._netuid,
            database=self._database,
            testnet=self._testnet
        )
        server.run()

    def get_connections(self):
        return self._connection_pool.get_all_connections()

    def get_mempool_events(self):
        return list(self._mempool)

    def consume_mempool_events(self, count: int):
        items = []
        with multiprocessing.Lock():
            for _ in range(min(count, len(self._mempool))):
                items.append(self._mempool.pop(0))
        return items

    def insert_mempool_event(self, event: Event):
        return self._mempool.append(event)
