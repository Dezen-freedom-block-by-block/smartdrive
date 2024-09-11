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

import time
from multiprocessing import Manager, Lock
import socket

from smartdrive.commune.models import ModuleInfo


INACTIVITY_TIMEOUT_SECONDS = 10


class Connection:
    def __init__(self, module: ModuleInfo, socket: socket, last_response_time: float):
        self.module = module
        self.socket = socket
        self.last_response_time = last_response_time

    def __repr__(self):
        return f"Connection(module={self.module}, socket={self.socket}, last_response_time={self.last_response_time})"


class ConnectionPool:

    def __init__(self, cache_size):
        self._connections = Manager().dict()
        self._cache_size = cache_size
        self._pool_lock = Lock()

    def upsert_connection(self, identifier, module_info, socket):
        with self._pool_lock:
            connection = Connection(module_info, socket, time.monotonic())

            if identifier not in self._connections:
                if len(self._connections) <= self._cache_size:
                    self._connections[identifier] = connection
                else:
                    print(f"Max num of connections reached {self._cache_size}")

            else:
                for key, c in self._connections.items():
                    if key == identifier:
                        self._connections[key] = connection
                        print(f"Connection updated {identifier}")
                        break

    def remove_if_exists(self, identifier):
        with self._pool_lock:
            if identifier in self._connections.keys():
                removed_connection = self._connections.pop(identifier)
                print(f"Removed connection {identifier}")
                return removed_connection.socket
            return None

    def remove_and_return_inactive_sockets(self):
        with self._pool_lock:
            current_time = time.monotonic()
            connections_to_remove = [identifier for identifier, c in self._connections.items() if current_time - c.last_response_time > INACTIVITY_TIMEOUT_SECONDS]
            sockets_to_remove = [self._connections[identifier].socket for identifier in connections_to_remove]

            print("connections_to_remove")
            print(connections_to_remove)
            print(sockets_to_remove)
            for identifier in connections_to_remove:
                del self._connections[identifier]
            return sockets_to_remove

    def get_all(self):
        with self._pool_lock:
            return list(self._connections.values())

    def get(self, identifier):
        with self._pool_lock:
            if identifier in self._connections:
                return self._connections[identifier]

        return None

    def get_module(self, identifier: str) -> ModuleInfo | None:
        connection = self.get(identifier)
        validator = None

        if connection:
            validator = connection.module

        return validator

    def get_module_identifiers(self):
        with self._pool_lock:
            identifiers = self._connections.keys()
            return identifiers

    def get_remaining_capacity(self):
        with self._pool_lock:
            return self._cache_size - len(self._connections)
