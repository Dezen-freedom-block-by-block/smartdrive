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

import threading
from queue import Queue, Empty
from typing import Dict, List, Any
from functools import wraps

from communex.key import check_ss58_address
from websocket import WebSocketException
from threading import Semaphore, Lock

from communex._common import ComxSettings, transform_stake_dmap
from communex.client import CommuneClient
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.commune.models import ModuleInfo
from smartdrive.commune.utils import _get_ip_port

DEFAULT_NUM_CONNECTIONS = 5
POOL_SIZE = 10
RETRIES = 5
TIMEOUT = 30


class TimeoutException(Exception):
    pass


class ThreadWithReturnValue(threading.Thread):
    def __init__(self, target=None, args=()):
        super().__init__(target=target, args=args)
        self._return = None
        self._exception = None

    def run(self):
        try:
            if self._target:
                self._return = self._target(*self._args)
        except Exception as e:
            self._exception = e

    def join(self, *args, **kwargs):
        super().join(*args, **kwargs)
        if self._exception:
            raise self._exception
        return self._return


def _run_with_timeout(target, args=(), timeout=TIMEOUT):
    thread = ThreadWithReturnValue(target=target, args=args)
    thread.start()
    thread.join(timeout)
    if thread.is_alive():
        raise TimeoutException("Operation timed out")
    if thread._exception:
        raise thread._exception
    return thread._return


class ConnectionPool:
    def __init__(self, testnet, max_pool_size=POOL_SIZE, num_connections=DEFAULT_NUM_CONNECTIONS):
        comx_settings = ComxSettings()
        self.urls = comx_settings.TESTNET_NODE_URLS if testnet else comx_settings.NODE_URLS
        self.max_pool_size = max_pool_size
        self.num_connections = num_connections
        self.pool = Queue()
        self.semaphore = Semaphore(max_pool_size)
        self.clients_lock = Lock()

    def _try_get_client(self, urls, num_connections):
        for url in urls:
            try:
                return CommuneClient(url, num_connections=num_connections)
            except Exception as e:
                continue
        return None

    def get_client(self):
        acquired = self.semaphore.acquire(timeout=1)
        if not acquired:
            return self._try_get_client(self.urls, self.num_connections)

        try:
            client = self.pool.get_nowait()
            return client
        except Empty:
            client = self._try_get_client(self.urls, self.num_connections)
            return client

    def release_client(self, client):
        with self.clients_lock:
            try:
                self.pool.put(client)
            finally:
                self.semaphore.release()

    def replace_broken_client(self):
        with self.clients_lock:
            new_client = self._try_get_client(self.urls, self.num_connections)
            if new_client:
                try:
                    self.pool.put(new_client)
                finally:
                    self.semaphore.release()


def retry_on_failure(retries):
    def decorator(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            pool = comx_pool
            for i in range(retries):
                client = pool.get_client()
                try:
                    result = func(client, *args, **kwargs)
                    pool.release_client(client)
                    return result
                except (WebSocketException, TimeoutException) as e:
                    print(f"Replacing broken commune client... {e}")
                    pool.replace_broken_client()
                except Exception as e:
                    print(f"Retrying with another commune client due to {e}...")
                    if client:
                        pool.release_client(client)
                    else:
                        pool.replace_broken_client()
            raise Exception("Operation failed after several retries")
        return wrapper
    return decorator


@retry_on_failure(retries=RETRIES)
def _get_staketo_with_timeout(client, ss58_address, netuid, timeout=TIMEOUT):
    return _run_with_timeout(client.get_staketo, (ss58_address, netuid), timeout)


def get_staketo(ss58_address: Ss58Address, netuid: int, timeout=TIMEOUT) -> Dict[str, int]:
    try:
        result = _get_staketo_with_timeout(ss58_address=ss58_address, netuid=netuid, timeout=timeout)
        if result is not None:
            return result
    except Exception as e:
        print(f"Error in get_staketo: {e}")
    raise CommuneNetworkUnreachable()


@retry_on_failure(retries=RETRIES)
def _vote_with_timeout(client, key, uids, weights, netuid, timeout=TIMEOUT):
    return _run_with_timeout(client.vote, (key, uids, weights, netuid), timeout=timeout)


def vote(key: Keypair, uids: List[int], weights: List[int], netuid: int, timeout=TIMEOUT):
    print(f"Voting uids: {uids} - weights: {weights}")
    try:
        vote_thread = threading.Thread(target=_vote_with_timeout, args=(key, uids, weights, netuid, timeout))
        vote_thread.start()
    except Exception as e:
        print(f"Error in vote: {e}")


@retry_on_failure(retries=RETRIES)
def _get_modules_with_timeout(client, request_dict, timeout=TIMEOUT):
    return _run_with_timeout(client.query_batch_map, (request_dict,), timeout=timeout)


def get_modules(netuid: int, timeout=TIMEOUT) -> List[ModuleInfo]:
    request_dict: dict[Any, Any] = {
        "SubspaceModule": [
            ("Keys", [netuid]),
            ("Address", [netuid]),
            ("Incentive", []),
            ("Dividends", []),
            ("StakeFrom", [])
        ]
    }

    try:
        result = _get_modules_with_timeout(request_dict=request_dict, timeout=timeout)
        if result is not None:

            modules_info = []

            uid_to_key = result.get("Keys", {})
            if uid_to_key:

                ss58_to_stakefrom = result.get("StakeFrom", {})
                ss58_to_stakefrom = transform_stake_dmap(ss58_to_stakefrom)
                uid_to_address = result["Address"]
                uid_to_incentive = result["Incentive"]
                uid_to_dividend = result["Dividends"]

                for uid, key in uid_to_key.items():
                    key = check_ss58_address(key)
                    address = uid_to_address[uid]
                    incentive = uid_to_incentive[netuid][uid]
                    dividend = uid_to_dividend[netuid][uid]
                    stake_from = ss58_to_stakefrom.get(key, [])
                    stake = sum(stake for _, stake in stake_from)

                    if address:
                        connection = _get_ip_port(address)
                        if connection:
                            modules_info.append(ModuleInfo(uid, key, connection, incentive, dividend, stake))

            return modules_info

    except Exception as e:
        print(f"Error in get_modules: {e}")

    raise CommuneNetworkUnreachable()


comx_pool: ConnectionPool | None = None


def initialize_commune_connection_pool(testnet, max_pool_size=POOL_SIZE, num_connections=DEFAULT_NUM_CONNECTIONS):
    global comx_pool
    comx_pool = ConnectionPool(testnet=testnet, max_pool_size=max_pool_size, num_connections=num_connections)
