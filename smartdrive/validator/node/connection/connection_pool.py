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

import time
from _socket import SocketType
from multiprocessing import Manager
from multiprocessing.managers import DictProxy
from typing import Optional

from communex.types import Ss58Address

from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.node.connection.utils.lock_proxy_wrapper import LockProxyWrapper
from smartdrive.validator.node.util.exceptions import ConnectionPoolMaxSizeReached

INACTIVITY_TIMEOUT_SECONDS = 10


class Connection:
    def __init__(self, module: ModuleInfo, socket: SocketType, last_response_time: float):
        self.module = module
        self.socket = socket
        self.last_response_time = last_response_time

    def __repr__(self):
        return f"Connection(module={self.module}, socket={self.socket}, last_response_time={self.last_response_time})"


class ConnectionPool:

    def __init__(self, manager: Manager, cache_size):
        self._connections: DictProxy[Ss58Address, Connection] = manager.dict()
        self._cache_size = cache_size
        self._lock: LockProxyWrapper = manager.Lock()

    def get(self, identifier) -> Optional[Connection]:
        with self._lock:
            if identifier in self._connections:
                return self._connections[identifier]

        return None

    def get_all(self) -> list[Connection]:
        return list(self._connections.items())

    def get_module(self, identifier: Ss58Address) -> Optional[ModuleInfo]:
        connection = self.get(identifier)
        validator = None

        if connection:
            validator = connection.module

        return validator

    def get_module_ss58_addresses(self) -> list[Ss58Address]:
        return self._connections.keys()

    def get_remaining_capacity(self) -> int:
        return self._cache_size - len(self._connections)

    def update_or_append(self, identifier: Ss58Address, module_info: ModuleInfo, socket: SocketType):
        with self._lock:
            connection = Connection(module_info, socket, time.monotonic())

            if identifier not in self._connections:
                if len(self._connections) <= self._cache_size:
                    self._connections[identifier] = connection
                else:
                    raise ConnectionPoolMaxSizeReached(f"Max num of connections reached {self._cache_size}")

            else:
                self.update(identifier, module_info, socket)

    def update(self, identifier: Ss58Address, module_info: ModuleInfo, socket: SocketType):
        with self._lock:
            if identifier in self._connections:
                connection = Connection(module_info, socket, time.monotonic())
                self._connections[identifier] = connection

    def update_last_response_time(self, identifier):
        with self._lock:
            if identifier in self._connections:
                self._connections[identifier].last_response_time = time.monotonic()

    def remove(self, identifier: Ss58Address) -> Optional[SocketType]:
        connection = self._connections.pop(identifier, None)
        return connection.socket if connection else None

    def remove_multiple(self, identifiers: list[Ss58Address]) -> list[SocketType]:
        sockets = []
        with self._lock:
            for identifier in identifiers:
                connection = self._connections.pop(identifier, None)
                if connection:
                    sockets.append(connection.socket)
        return sockets

    def remove_if_inactive(self, identifier: Ss58Address) -> Optional[SocketType]:
        with self._lock:
            connection = self._connections.get(identifier)
            if connection and connection.last_response_time > INACTIVITY_TIMEOUT_SECONDS:
                return self._connections.pop(identifier).socket
            return None

    def remove_inactive(self) -> list[SocketType]:
        with self._lock:
            current_time = time.monotonic()
            connections_to_remove = [identifier for identifier, c in self._connections.items() if current_time - c.last_response_time > INACTIVITY_TIMEOUT_SECONDS]
            sockets_to_remove = [self._connections[identifier].socket for identifier in connections_to_remove]

            for identifier in connections_to_remove:
                del self._connections[identifier]
            return sockets_to_remove
