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

# MIT License
#
#
#
from multiprocessing import Manager, Lock


class ConnectionPool:

    CONNECTION = 'Connection'
    SERVER_IDENTIFIER = 'ServerIdentifier'

    def __init__(self, cache_size):
        self._connections = Manager().dict()
        self._cache_size = cache_size
        self._pool_lock = Lock()

    def add_connection(self, identifier, connection, server_identifier=None):
        if len(self._connections) <= self._cache_size:
            self._pool_lock.acquire()

            if identifier not in self._connections:
                self._connections[identifier] = {ConnectionPool.CONNECTION: connection, ConnectionPool.SERVER_IDENTIFIER: server_identifier}
                # print(f"Added new connection {identifier}")
                self._pool_lock.release()
            else:
                self._pool_lock.release()
                print(f"Connection exists already {identifier}")

        else:
            print(f"Max num of connections reached {self._cache_size}")

    def remove_connection(self, identifier):
        self._pool_lock.acquire()
        removed_connection = self._connections.pop(identifier, None)
        if removed_connection:
            print(f"Removed connection {identifier}")
            self._pool_lock.release()
            return removed_connection[ConnectionPool.CONNECTION]
        self._pool_lock.release()

    def get_all_connections(self):
        with self._pool_lock:
            return list(self._connections.values())

    def get_identifiers(self):
        self._pool_lock.acquire()
        identifiers = self._connections.keys()
        self._pool_lock.release()
        return identifiers

    def get_identifiers_connections(self):
        self._pool_lock.acquire()
        identifiers_connections = self._connections
        self._pool_lock.release()
        return identifiers_connections

    def get_remaining_capacity(self):
        return self._cache_size - len(self._connections)
