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

import re
import asyncio
from typing import Dict, Any, Optional, List
from substrateinterface import Keypair

from communex.types import Ss58Address
from communex.client import CommuneClient

from smartdrive.commune.module.client import ModuleClient
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT
from smartdrive.validator.models.models import ModuleType

PING_TIMEOUT = 5
CALL_TIMEOUT = 60


class ConnectionInfo:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

    def __repr__(self):
        return f"ConnectionInfo(ip={self.ip}, port={self.port})"


class ModuleInfo:
    def __init__(self, uid: str, ss58_address: Ss58Address, connection: ConnectionInfo, incentives: Optional[int] = None, dividends: Optional[int] = None, stake: Optional[int] = None):
        self.uid = uid
        self.ss58_address = ss58_address
        self.connection = connection
        self.incentives = incentives
        self.dividends = dividends
        self.stake = stake

    def __repr__(self):
        return f"ModuleInfo(uid={self.uid}, ss58_address={self.ss58_address}, connection={self.connection}, incentives={self.incentives}, dividends={self.dividends}, stake={self.stake})"


def get_modules(comx_client: CommuneClient, netuid: int) -> List[ModuleInfo]:
    """
    Retrieves a list of modules with their respective SS58 addresses and connections.

    This function performs system queries to retrieve module information, including SS58 addresses,
    connection details, incentives, and dividends. The information is compiled into a list of
    `ModuleInfo` objects.

    Params:
        comx_client (CommuneClient): Client to perform system queries.
        netuid (int): Network identifier used for the queries.

    Returns:
        List[ModuleInfo]: A list of `ModuleInfo` objects containing module details such as UID,
                          SS58 address, connection info, incentives, and dividends.
    """
    queries = {
        "SubspaceModule": [
            ("Keys", [netuid]),
            ("Address", [netuid]),
            ("Incentive", []),
            ("Dividends", []),
            ("StakeFrom", [netuid])
        ]
    }

    result = comx_client.query_batch_map(queries)
    keys_map = result["Keys"]
    address_map = result["Address"]

    modules_info = []

    for uid, ss58_address in keys_map.items():
        address = address_map.get(uid)
        total_stake = 0
        stake = result["StakeFrom"].get(ss58_address, [])
        for _, stake in stake:
            total_stake += stake

        if address:
            connection = _get_ip_port(address)
            if connection:
                modules_info.append(
                    ModuleInfo(uid, ss58_address, connection, result["Incentive"][netuid][uid], result["Dividends"][netuid][uid], total_stake)
                )
    return modules_info


async def vote(key: Keypair, comx_client: CommuneClient, uids: list[int], weights: list[int], netuid: int):
    """
    Perform a vote on the network.

    This function sends a voting transaction to the network using the provided key for authentication.
    Each UID in the `uids` list is voted for with the corresponding weight from the `weights` list.

    Params:
        key (Keypair): Key used to authenticate the vote.
        comx_client (CommuneClient): Client to perform the vote.
        uids (list[int]): List of unique identifiers (UIDs) of the nodes to vote for.
        weights (list[int]): List of weights associated with each UID.
        netuid (int): Network identifier used for the votes.
    """
    print(f"Voting uids: {uids} - weights: {weights}")
    try:
        loop = asyncio.get_running_loop()
        await loop.run_in_executor(None, comx_client.vote, key, uids, weights, netuid)
    except Exception as e:
        print(e)


async def get_active_validators(key: Keypair, comx_client: CommuneClient, netuid: int, modules: Optional[list[ModuleInfo]] = None) -> List[ModuleInfo]:
    """
    Retrieve a list of active validators.

    This function queries the network to retrieve module information and then pings each module to
    determine if it is an active validator. Only modules that respond with a type of "validator" are
    considered active validators.

    Params:
        key (Keypair): Key used to authenticate the requests.
        comx_client (CommuneClient): Client to perform system queries.
        netuid (int): Network identifier used for the queries.
        modules (Optional[list[ModuleInfo]]): Optional list of modules to check. If not provided, the function will query the network.

    Returns:
        List[ModuleInfo]: A list of `ModuleInfo` objects representing active validators.
    """
    if not modules:
        modules = get_modules(comx_client, netuid)

    async def _get_active_validators(module):
        ping_response = await execute_miner_request(key, module.connection, module.ss58_address, "ping", timeout=PING_TIMEOUT)
        if ping_response and ping_response["type"] == "validator":
            return module
        return None

    futures = [_get_active_validators(module) for module in modules]
    results = await asyncio.gather(*futures, return_exceptions=True)
    active_validators = [result for result in results if isinstance(result, ModuleInfo)]
    return active_validators


async def get_truthful_validators(key: Keypair, comx_client: CommuneClient, netuid: int) -> List[ModuleInfo]:
    """
    Retrieves a list of truthful validators based on a minimum stake requirement.

    Params:
        key (Keypair): The keypair used for signing requests.
        comx_client (CommuneClient): The CommuneX client used for network interactions.
        netuid (int): The network UID.

    Returns:
        List[ModuleInfo]: A list of ModuleInfo objects representing the truthful validators.
    """
    active_validators = await get_active_validators(key, comx_client, netuid)
    return list(filter(lambda validator: validator.stake > TRUTHFUL_STAKE_AMOUNT, active_validators))


async def ping_proposer_validator(key: Keypair, module: ModuleInfo, retries: int = 3, sleep_time: int = 5) -> bool:
    """
    Pings the proposer validator to check if it's available and active.

    Params:
        key (Keypair): The keypair used for signing the request.
        module (ModuleInfo): The module information of the proposer validator.
        retries (int): Number of retry attempts if the ping fails. Default is 3.
        sleep_time (int): Time to wait between retries in seconds. Default is 5.

    Returns:
        bool: True if the proposer validator responds correctly, False otherwise.
    """
    for _ in range(retries):
        if (response := await execute_miner_request(key, module.connection, module.ss58_address, "ping", timeout=PING_TIMEOUT)) and response["type"] == "validator":
            return True
        await asyncio.sleep(sleep_time)
    return False


def get_filtered_modules(comx_client: CommuneClient, netuid: int, type: ModuleType = ModuleType.MINER) -> List[ModuleInfo]:
    """
    Retrieve a list of miners or validators.

    This function queries the network to retrieve module information and filters the modules to
    identify miner or validator. A module is considered a miner or validator if its incentive is equal to its dividends
    and both are zero, or if its incentive is greater than its dividends.

    Params:
        comx_client (CommuneClient): Client to perform system queries.
        netuid (int): Network identifier used for the queries.

    Returns:
        List[ModuleInfo]: A list of `ModuleInfo` objects representing miners.
    """
    modules = get_modules(comx_client, netuid)
    result = []

    for module in modules:
        condition = module.incentives > module.dividends if type == ModuleType.MINER else module.incentives < module.dividends
        if (module.incentives == module.dividends == 0) or condition:
            result.append(module)

    return result


async def get_active_miners(key: Keypair, comx_client: CommuneClient, netuid: int, modules: Optional[list[ModuleInfo]] = None) -> List[ModuleInfo]:
    """
   Retrieve a list of active miners.

   This function queries the network to retrieve module information and then pings each module to
   determine if it is an active miner. Only modules that respond with a type of "miner" are
   considered active miners.

   Params:
       key (Keypair): Key used to authenticate the requests.
       comx_client (CommuneClient): Client to perform system queries.
       netuid (int): Network identifier used for the queries.
       modules (Optional[list[ModuleInfo]]): Optional list of modules to check. If not provided, the function will query the network.

   Returns:
       List[ModuleInfo]: A list of `ModuleInfo` objects representing active miners.
   """
    if not modules:
        modules = get_modules(comx_client, netuid)

    async def _get_active_miners(module):
        ping_response = await execute_miner_request(key, module.connection, module.ss58_address, "ping", timeout=PING_TIMEOUT)
        if ping_response and ping_response["type"] == "miner":
            return module
        return None

    futures = [_get_active_miners(module) for module in modules]
    results = await asyncio.gather(*futures, return_exceptions=True)
    active_miners = [result for result in results if isinstance(result, ModuleInfo)]
    return active_miners


async def execute_miner_request(validator_key: Keypair, connection: ConnectionInfo, miner_key: Ss58Address, action: str, params: Dict[str, Any] = None, timeout: int = CALL_TIMEOUT):
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
        timeout (int, optional): Timeout for the call.

    Raises:
        Exception: If an unexpected error occurs during the request.
    """
    if params is None:
        params = {}

    try:
        client = ModuleClient(connection.ip, int(connection.port), validator_key)
        miner_answer = await client.call(action, miner_key, params, timeout=timeout)

    except Exception as e:
        miner_answer = None

    return miner_answer


def _extract_address(string: str) -> Optional[List[str]]:
    """
    Extract an IP address and port from a given string.

    This function uses a regular expression to search for an IP address and port combination
    within the provided string. If a match is found, the IP address and port are returned
    as a list of strings. If no match is found, None is returned.

    Params:
        string (str): The input string containing the IP address and port.

    Returns:
        Optional[List[str]]: A list containing the IP address and port as strings if a match
                             is found, or None if no match is found.
    """
    ip_regex = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+")
    match = re.search(ip_regex, string)
    if match:
        return match.group(0).split(":")

    return None


def _get_ip_port(address_string: str) -> Optional[ConnectionInfo]:
    """
    Extract the IP address and port from a given address string and return them as a `ConnectionInfo` object.

    This function uses `_extract_address` to parse the IP address and port from the input string.
    If successful, it returns a `ConnectionInfo` object containing the IP address and port.
    If the extraction fails or an exception occurs, it returns `None`.

    Params:
        address_string (str): The input string containing the address.

    Returns:
        Optional[ConnectionInfo]: A `ConnectionInfo` object with the IP address and port if successful,
                                  or `None` if the extraction fails or an exception occurs.
    """
    try:
        extracted_address = _extract_address(address_string)
        if extracted_address:
            return ConnectionInfo(extracted_address[0], int(extracted_address[1]))
        return None

    except Exception as e:
        print(f"Error extracting IP and port: {e}")
        return None
