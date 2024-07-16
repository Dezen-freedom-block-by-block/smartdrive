import random
import time
import threading
from queue import Queue, Empty
from typing import Dict, List
from functools import wraps
from websocket import WebSocketException
from threading import Semaphore, Lock

from communex._common import ComxSettings
from communex.client import CommuneClient
from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.commune.models import ModuleInfo
from smartdrive.commune.utils import _get_ip_port

DEFAULT_NUM_CONNECTIONS = 5
POOL_SIZE = 10
DELAY = 3
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


class CommuneConnectionPool:
    def __init__(self, testnet, pool_size=POOL_SIZE, num_connections=DEFAULT_NUM_CONNECTIONS):
        comx_settings = ComxSettings()
        self.urls = comx_settings.TESTNET_NODE_URLS if testnet else comx_settings.NODE_URLS
        self.pool_size = pool_size
        self.num_connections = num_connections
        self.pool = Queue(maxsize=pool_size)
        self.semaphore = Semaphore(pool_size)
        self.active_connections = 0
        self.active_connections_lock = Lock()
        self._initialize_pool()

    def _initialize_pool(self):
        for _ in range(self.pool_size):
            random.shuffle(self.urls)
            client = self._try_get_client(self.urls, self.num_connections)
            if client:
                self.pool.put(client)
                with self.active_connections_lock:
                    self.active_connections += 1

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
            client = self._try_get_client(self.urls, self.num_connections)
            if client:
                return client
            else:
                raise Exception("No available connections in the pool")
        try:
            client = self.pool.get_nowait()
            return client
        except Empty:
            client = self._try_get_client(self.urls, self.num_connections)
            return client

    def release_client(self, client):
        with self.active_connections_lock:
            if self.active_connections > self.pool_size:
                self.active_connections -= 1
            else:
                self.pool.put(client)
            self.semaphore.release()

    def replace_broken_client(self):
        with self.active_connections_lock:
            self.active_connections -= 1

        self.semaphore.release()
        new_client = self._try_get_client(self.urls, self.num_connections)
        if new_client:
            self.pool.put(new_client)
            with self.active_connections_lock:
                self.active_connections += 1


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
                    print(f"Replacing broken commune client...")
                    pool.replace_broken_client()
                except Exception as e:
                    print(f"Retrying with another commune client... {e}")
                    pool.release_client(client)
                    time.sleep(DELAY)
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
def _get_modules_with_timeout(client, queries, timeout=TIMEOUT):
    return _run_with_timeout(client.query_batch_map, (queries,), timeout=timeout)


def get_modules(netuid: int, timeout=TIMEOUT) -> List[ModuleInfo]:
    queries = {
        "SubspaceModule": [
            ("Keys", [netuid]),
            ("Address", [netuid]),
            ("Incentive", []),
            ("Dividends", []),
            ("StakeFrom", [netuid])
        ]
    }
    try:
        result = _get_modules_with_timeout(queries=queries, timeout=timeout)
        if result is not None:
            keys_map = result["Keys"]
            address_map = result["Address"]
            modules_info = []
            for uid, ss58_address in keys_map.items():
                address = address_map.get(uid)
                total_stake = 0
                stake = result["StakeFrom"].get(ss58_address, [])
                for _, s in stake:
                    total_stake += s
                if address:
                    connection = _get_ip_port(address)
                    if connection:
                        modules_info.append(
                            ModuleInfo(uid, ss58_address, connection, result["Incentive"][netuid][uid], result["Dividends"][netuid][uid], total_stake)
                        )
            return modules_info
    except Exception as e:
        print(f"Error in get_modules: {e}")
    raise CommuneNetworkUnreachable()


comx_pool: CommuneConnectionPool | None = None


def initialize_commune_connection_pool(testnet, pool_size=POOL_SIZE, num_connections=DEFAULT_NUM_CONNECTIONS):
    global comx_pool
    comx_pool = CommuneConnectionPool(testnet=testnet, pool_size=pool_size, num_connections=num_connections)
