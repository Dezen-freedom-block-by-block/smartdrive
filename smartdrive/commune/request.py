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

import asyncio
from typing import Dict, Any, List
from substrateinterface import Keypair

from communex.types import Ss58Address

from smartdrive.commune.connection_pool import get_modules
from smartdrive.commune.models import ConnectionInfo, ModuleInfo
from smartdrive.commune.module.client import ModuleClient
from smartdrive.validator.models.models import ModuleType

PING_TIMEOUT = 5
EXTENDED_PING_TIMEOUT = 60
CALL_TIMEOUT = 60


def get_filtered_modules(netuid: int, type: ModuleType = ModuleType.MINER) -> List[ModuleInfo]:
    """
    Retrieve a list of miners or validators.

    This function queries the network to retrieve module information and filters the modules to
    identify miner or validator. A module is considered a miner or validator if its incentive is equal to its dividends
    and both are zero, or if its incentive is greater than its dividends.

    Params:
        netuid (int): Network identifier used for the queries.

    Returns:
        List[ModuleInfo]: A list of `ModuleInfo` objects representing miners.

    Raises:
        CommuneNetworkUnreachable: Raised if a valid result cannot be obtained from the network.
    """
    modules = get_modules(netuid)
    result = []

    for module in modules:
        condition = module.incentives > module.dividends if type == ModuleType.MINER else module.incentives < module.dividends
        if (module.incentives == module.dividends == 0) or condition:
            result.append(module)

    return result


# This function should only be called by the smartdrive client
async def get_active_validators(key: Keypair, netuid: int, timeout=PING_TIMEOUT) -> List[ModuleInfo]:
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
    validators = get_filtered_modules(netuid, ModuleType.VALIDATOR)

    async def _get_active_validators(validator):
        ping_response = await execute_miner_request(key, validator.connection, validator.ss58_address, "ping", timeout=timeout)
        if ping_response and ping_response["type"] == "validator":
            return validator
        return None

    futures = [_get_active_validators(validator) for validator in validators if validator.ss58_address != key.ss58_address]
    results = await asyncio.gather(*futures, return_exceptions=True)
    active_validators = [result for result in results if isinstance(result, ModuleInfo)]
    return active_validators


async def execute_miner_request(validator_key: Keypair, connection: ConnectionInfo, miner_key: Ss58Address, action: str, params: Dict[str, Any] = None, files: Any = None, timeout: int = CALL_TIMEOUT):
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
        miner_answer = await client.call(fn=action, target_key=miner_key, params=params, files=files, timeout=timeout)

    except Exception as e:
        miner_answer = None

    return miner_answer
