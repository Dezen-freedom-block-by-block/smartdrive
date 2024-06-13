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
from multiprocessing import Queue, Lock

from substrateinterface import Keypair

from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.server import Server


class Node:

    _key: Keypair = None
    _ip = None
    _netuid = None

    _server_process = None
    _mempool_process = None
    _connection_pool = ConnectionPool(cache_size=Server.MAX_N_CONNECTIONS)
    mempool_queue = Queue()
    mempool_lock = Lock()

    def __init__(self, keypair: Keypair, ip: str, netuid: int):
        self._keypair = keypair
        self._ip = ip
        self._netuid = netuid

        self._server_process = multiprocessing.Process(target=self.run_server)
        self._server_process.start()

    def run_server(self):
        server = Server(self._ip, self._connection_pool, self._keypair, self._netuid, self.mempool_queue, self.mempool_lock)
        server.run()

    def get_all_mempool_items(self):
        with self.mempool_lock:
            items = []
            while not self.mempool_queue.empty():
                items.append(self.mempool_queue.get())
            return items
