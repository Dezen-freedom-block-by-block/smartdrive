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

import asyncio
from functools import wraps
from typing import Dict, Any, List

from communex._common import get_node_url, transform_stake_dmap
from communex.client import CommuneClient
from communex.key import check_ss58_address
from communex.module._util import retry
from substrateinterface import Keypair

from communex.types import Ss58Address

from smartdrive import logger
from smartdrive.commune.errors import CommuneNetworkUnreachable, TimeoutException
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.commune.module.client import ModuleClient
from smartdrive.commune.utils import _get_ip_port
from smartdrive.validator.config import config_manager
from smartdrive.validator.models.models import ModuleType

PING_TIMEOUT = 5
EXTENDED_PING_TIMEOUT = 60
CALL_TIMEOUT = 10
RETRIES = 5
TIMEOUT = 30


async def get_filtered_modules(netuid: int, module_type: ModuleType, ss58_address: str = None, testnet=False) -> List[ModuleInfo]:
    """
    Retrieve a list of miners or validators.

    This function queries the network to retrieve module information and filters the modules to
    identify miner or validator. A module is considered a miner or validator if its incentive is equal to its dividends
    and both are zero, or if its incentive is greater than its dividends.

    Params:
        netuid (int): Network identifier used for the queries.
        module_type (ModuleType): ModuleType.MINER or ModuleType.VALIDATOR.
        ss58_address (str): Own Ss58 address.

    Returns:
        List[ModuleInfo]: A list of `ModuleInfo` objects representing miners.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    modules = await get_modules(netuid, testnet=testnet)
    result = []

    for module in modules:
        condition = module.incentives > module.dividends if module_type == ModuleType.MINER else module.incentives < module.dividends
        if (module.incentives == module.dividends == 0 or condition) and (ss58_address is None or module.ss58_address != ss58_address):
            result.append(module)

    return result


# This function should only be called by the smartdrive client
async def get_active_validators(key: Keypair, netuid: int, timeout=PING_TIMEOUT, testnet=False) -> List[ModuleInfo]:
    """
    Retrieve a list of active validators.

    This function queries the network to retrieve module information and then pings each module to
    determine if it is an active validator. Only modules that respond with a type of "validator" are
    considered active validators.

    Params:
        key (Keypair): Key used to authenticate the requests.
        netuid (int): Network identifier used for the queries.

    Returns:
        List[ModuleInfo]: A list of `ModuleInfo` objects representing active validators.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    validators = await get_filtered_modules(netuid, ModuleType.VALIDATOR, testnet=testnet)

    async def _get_active_validators(validator):
        ping_response = await execute_miner_request(key, validator.connection, validator.ss58_address, "ping", timeout=timeout)
        if ping_response and ping_response["type"] == "validator":
            return validator
        return None

    futures = [_get_active_validators(validator) for validator in validators if validator.ss58_address != key.ss58_address]
    results = await asyncio.gather(*futures, return_exceptions=True)
    active_validators = [result for result in results if isinstance(result, ModuleInfo)]
    return active_validators


async def execute_miner_request(
        validator_key: Keypair,
        connection: ConnectionInfo,
        miner_key: Ss58Address,
        action: str,
        params: Dict[str, Any] = None,
        file: Any = None,
        timeout: int = CALL_TIMEOUT
):
    """
    Executes a request to a miner and returns the response.

    This method sends a request to a miner using the specified action and parameters.
    It handles exceptions that may occur during the request and returns the miner's response.

    Params:
        validator_key (Keypair): The validator Keypair.
        connection (ConnectionInfo): A dictionary containing the miner's IP and port information.
        miner_key (Ss58Address): The SS58 address key of the miner.
        action (str): The action to be performed by the miner.
        params (Dict[str, Any], optional): Additional parameters for the action. Defaults to an empty dictionary.
        files (Any): Additional parameters for the action. Defaults to None.
        timeout (int, optional): Timeout for the call.

    Raises:
        Exception: If an unexpected error occurs during the request.
    """
    if params is None:
        params = {}

    try:
        client = ModuleClient(connection.ip, int(connection.port), validator_key)
        miner_answer = await client.call(fn=action, target_key=miner_key, params=params, file=file, timeout=timeout)

    except Exception:
        miner_answer = None

    return miner_answer


@retry(RETRIES, [Exception])
def make_client(node_url: str):
    return CommuneClient(url=node_url, num_connections=1, wait_for_finalization=False, timeout=10)


def retry_on_failure(retries):
    def decorator(func):
        @wraps(func)
        async def wrapper(*args, testnet=False, **kwargs):
            for i in range(retries):
                client = make_client(get_node_url(use_testnet=testnet))
                try:
                    result = await func(client, *args, **kwargs)
                    return result
                except Exception:
                    logger.debug("Retrying with another commune client", exc_info=True)
                    await asyncio.sleep(1)
            raise Exception("Operation failed after several retries")
        return wrapper
    return decorator


@retry_on_failure(retries=RETRIES)
async def _get_staketo_with_timeout(client, ss58_address, timeout=TIMEOUT):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(loop.run_in_executor(None, client.get_staketo, ss58_address), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutException("Operation timed out")


async def get_staketo(ss58_address: Ss58Address, timeout=TIMEOUT) -> Dict[str, int]:
    try:
        result = await _get_staketo_with_timeout(ss58_address=ss58_address, timeout=timeout)
        if result is not None:
            return result
    except Exception:
        logger.error("Can not fetch module's related stakes", exc_info=True)
    raise CommuneNetworkUnreachable()


@retry_on_failure(retries=RETRIES)
async def _vote_with_timeout(client, key, uids, weights, netuid, timeout=TIMEOUT):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(loop.run_in_executor(None, client.vote, key, uids, weights, netuid), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutException("Operation timed out")


async def vote(key: Keypair, uids: List[int], weights: List[int], netuid: int, timeout=TIMEOUT):
    logger.info(f"Voting uids: {uids} - weights: {weights}")
    try:
        await _vote_with_timeout(key=key, uids=uids, weights=weights, netuid=netuid, timeout=timeout)
    except Exception:
        logger.error("Error voting", exc_info=True)


@retry_on_failure(retries=RETRIES)
async def _get_modules_with_timeout(client, request_dict, timeout=TIMEOUT):
    loop = asyncio.get_running_loop()
    try:
        return await asyncio.wait_for(loop.run_in_executor(None, client.query_batch_map, request_dict), timeout=timeout)
    except asyncio.TimeoutError:
        raise TimeoutException("Operation timed out")


async def get_modules(netuid: int, timeout=TIMEOUT, testnet=False) -> List[ModuleInfo]:
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
        result = await _get_modules_with_timeout(request_dict=request_dict, timeout=timeout, testnet=testnet)
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

    except Exception:
        logger.error("Error getting modules", exc_info=True)
    raise CommuneNetworkUnreachable()
