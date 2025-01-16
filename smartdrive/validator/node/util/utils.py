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
from typing import Tuple, List

from substrateinterface import Keypair

from smartdrive.commune.models import ModuleInfo
from smartdrive.commune.request import get_filtered_modules
from smartdrive.commune.utils import filter_truthful_validators
from smartdrive.validator.config import config_manager
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.connection.connection_pool import INACTIVITY_TIMEOUT_SECONDS as VALIDATOR_INACTIVITY_TIMEOUT_SECONDS
from smartdrive.validator.node.sync_service import SyncService


async def get_proposer_validator(keypair: Keypair, connected_modules: List[ModuleInfo], sync_service: SyncService) -> Tuple[bool, List[ModuleInfo]]:
    """
    Determines the proposer validator based on the validators' stake.

    Returns:
        is_current_validator_proposer (bool): True if the current validator is the proposer, False otherwise.
        active_validators (List[ModuleInfo]): List of currently active validators.
    """
    # Retrieving all active validators is crucial, so we attempt it an optimal number of times.
    # Between each attempt, we wait VALIDATOR_INACTIVITY_TIMEOUT_SECONDS / 2,
    # as new validators might be activated in the background.
    active_validators = []
    for _ in range(4):
        active_validators = connected_modules
        if active_validators:
            break
        await asyncio.sleep(VALIDATOR_INACTIVITY_TIMEOUT_SECONDS / 2)

    truthful_validators = filter_truthful_validators(active_validators)

    # Since the list of active validators never includes the current validator, we need to locate our own
    # validator within the complete list.
    all_validators = await get_filtered_modules(config_manager.config.netuid, ModuleType.VALIDATOR, config_manager.config.testnet)
    own_validator = next((v for v in all_validators if v.ss58_address == keypair.ss58_address), None)

    is_own_validator_truthful = own_validator and own_validator.stake >= TRUTHFUL_STAKE_AMOUNT
    if is_own_validator_truthful:
        truthful_validators.append(own_validator)

    if len(truthful_validators) == 0 and len(all_validators) == 0:
        return False, active_validators

    proposer_validator = max(truthful_validators or all_validators, key=lambda v: (v.stake or 0, v.ss58_address))

    is_current_validator_proposer = proposer_validator.ss58_address == keypair.ss58_address

    sync_service.set_current_proposer(proposer_validator.ss58_address)

    return is_current_validator_proposer, active_validators
