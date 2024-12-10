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
from enum import Enum
from typing import List

from substrateinterface import Keypair

from smartdrive import logger
from smartdrive.models.block import Block
from smartdrive.sign import sign_data
from smartdrive.validator.database.database import Database
from smartdrive.validator.node.connection.connection_pool import Connection
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.sync_service import SyncService, ConsensusState
from smartdrive.validator.node.util.message import MessageBody, MessageCode, Message
from smartdrive.validator.utils import prepare_sync_blocks, request_last_block

HASH_BATCH_SIZE = 250
MAX_REMOTE_CHAIN_VALIDATION = 15


class ChainState(Enum):
    SYNCHRONIZED = "synchronized"
    FORK_DETECTED = "fork_detected"
    OUT_OF_SYNC = "out_of_sync"


def handle_consensus_and_resolve_fork(
        sync_service: SyncService,
        database: Database,
        local_block: Block,
        active_connections: list,
        keypair: Keypair
):
    """
    Validates consensus among remote chains and resolves forks if consensus is reached.

    Args:
        sync_service (SyncService): The sync service managing the chain.
        database (Database): The local database instance.
        local_block (Block): The last local block.
        active_connections (list): List of active connections to other validators.
        keypair (Keypair): The keypair of this validator.
    """
    sync_service.start_sync(is_fork_syncing=True)
    request_last_block(key=keypair, connections=active_connections)
    total_local_events = database.get_total_event_count()
    time.sleep(10)
    consensus_state, target_validator, block_number = sync_service.validate_last_block_consensus(
        active_connections=active_connections,
        own_ss58_address=keypair.ss58_address,
        local_block=local_block,
        total_local_events=total_local_events
    )

    # Handle consensus states
    if consensus_state == ConsensusState.CONSENSUS_REACHED:
        logger.info(f"Consensus reached. Using chain from validator: {target_validator}")
        find_common_block_and_resolve_fork(
            database=database,
            sync_service=sync_service,
            local_block=local_block,
            active_connections=active_connections,
            keypair=keypair,
            target_validator=target_validator,
            expected_block_number=block_number
        )
    elif consensus_state == ConsensusState.TRUTHFUL_CHAIN:
        logger.info(f"No consensus. Adopting most advanced truthful chain from validator: {target_validator}")
        find_common_block_and_resolve_fork(
            database=database,
            sync_service=sync_service,
            local_block=local_block,
            active_connections=active_connections,
            keypair=keypair,
            target_validator=target_validator,
            expected_block_number=block_number
        )
    elif consensus_state == ConsensusState.PROPOSER_CHAIN:
        logger.info("No consensus. Falling back to proposer chain.")
        sync_service.complete_sync(is_synced=True)
    elif consensus_state == ConsensusState.LOCAL_CHAIN_ADVANCED:
        logger.info("Local chain is the most advanced. Assuming synchronized.")
        sync_service.complete_sync(is_synced=True)
    elif consensus_state == ConsensusState.NO_VALID_CHAIN:
        logger.error("No valid chain found during fork resolution. Aborting.")
        sync_service.complete_sync(is_synced=False)
    else:
        logger.error("Unexpected consensus state. Aborting.")
        sync_service.complete_sync(is_synced=False)


def find_common_block_and_resolve_fork(
        database: Database,
        sync_service: SyncService,
        local_block: Block,
        keypair: Keypair,
        active_connections: List[Connection],
        target_validator: str,
        expected_block_number: int
):
    """
    Finds the common block between local and remote chains and resolves any forks.
    If no common block is found, requests additional hashes from validators.

    Args:
        database (Database): The local database instance.
        sync_service (SyncService): The sync service managing the chain.
        local_block (Block): The local block.
        active_connections (list): List of active validator connections.
        keypair (Keypair): The validator's keypair for signing requests.
        target_validator (str): The SS58 address of the validator to synchronize with.
        expected_block_number (int): The expected block number.
    """

    remote_chain = sync_service.get_remote_chain()
    validator_blocks = remote_chain.get(target_validator, {})
    common_block = None
    current_block = local_block

    while current_block:
        if current_block.block_number in validator_blocks and current_block.hash == validator_blocks[current_block.block_number]["hash"]:
            common_block = current_block
            break
        if current_block.block_number == 1:
            break
        get_blocks = database.get_blocks(block_hash=current_block.previous_hash, include_events=False)
        current_block = get_blocks[0] if get_blocks else None

    if common_block:
        logger.info(f"Resolving fork starting, common block: {common_block.block_number}")
        _adopt_chain(
            database=database,
            local_block=local_block,
            common_block=common_block,
            sync_service=sync_service,
            active_connections=active_connections,
            keypair=keypair,
            target_validator=target_validator,
            expected_end_block_number=expected_block_number
        )
    else:
        min_block_number = min(validator_blocks.keys()) if validator_blocks else local_block.block_number
        start_block = max(0, min_block_number - HASH_BATCH_SIZE)
        logger.warning(f"No common block found. Requesting additional blocks between {start_block} and {min_block_number}.")
        connection = next((connection for connection in active_connections if connection.module.ss58_address == target_validator), None)
        if connection:
            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_FORK_BLOCK_DATA,
                data={"start_block": start_block, "end_block": min_block_number}
            )
            body_sign = sign_data(body.dict(), keypair)
            request_message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=keypair.public_key.hex()
            )

            send_message(connection, request_message)


def _get_chain_from_block(database: Database, block: Block, common_block: Block) -> list:
    """
    Retrieves the full chain from the given block back to the common block.

    Args:
        database (Database): The database instance.
        block (Block): The starting block.
        common_block (Block): The block where both chains converge.

    Returns:
        list: A list of blocks from the common block to the specified block.
    """
    chain = []
    current_block = block

    while current_block and current_block.hash != common_block.hash:
        chain.append(current_block)
        start_block = max(0, current_block.block_number - HASH_BATCH_SIZE + 1)
        get_blocks = database.get_blocks(start=start_block, end=current_block.block_number)
        blocks_by_hash = {b.hash: b for b in get_blocks}

        if current_block.previous_hash not in blocks_by_hash:
            break

        current_block = blocks_by_hash[current_block.previous_hash]

    return list(reversed(chain))


def _adopt_chain(
        database: Database,
        local_block: Block,
        common_block: Block,
        sync_service: SyncService,
        active_connections: List[Connection],
        keypair: Keypair,
        target_validator: str,
        expected_end_block_number: int
):
    """
    Resolves the fork by removing local blocks that diverge from the common block.

    Args:
        database (Database): The local database instance.
        local_block (Block): The most recent block in the local chain.
        common_block (Block): The block where both chains converge.
        sync_service (SyncService): The sync service managing the chain.
        active_connections: The list of active validator connections.
        keypair: The keypair used for signing messages.
        target_validator (str): The target validator ss58_address.
        expected_end_block_number (int): The expected end block number.
    """
    current_block = local_block
    while current_block and current_block.block_number > common_block.block_number:
        database.remove_block(current_block.block_number)
        get_blocks = database.get_blocks(block_hash=current_block.previous_hash, include_events=False)
        current_block = get_blocks[0] if get_blocks else None

    if common_block.block_number + 1 <= expected_end_block_number:
        sync_service.start_sync()
        prepare_sync_blocks(
            start=common_block.block_number + 1,
            end=expected_end_block_number,
            active_connections=active_connections,
            sync_service=sync_service,
            keypair=keypair,
            validator=target_validator
        )
    else:
        sync_service.complete_sync(is_synced=True)

    sync_service.clear_remote_chain()

    logger.info(f"Fork resolved. Local chain now aligned with block {common_block.block_number}.")


def evaluate_local_chain(local_block: Block, sync_service: SyncService, active_connections: List, database: Database, local_validator_ss58_address: str) -> ChainState:
    """
    Evaluates the state of the local block compared to the last blocks reported by other validators.

    Args:
        local_block (Block): The current last block of this validator.
        sync_service (SyncService): The sync service managing the chain.
        active_connections (list): List of active validator connections.
        database (Database): The database instance.
        local_validator_ss58_address (str): The SS58 address of the local validator.

    Returns:
        ChainState: The state of the local chain compared to other validators.
    """
    if not sync_service.get_last_block_other_validators():
        return ChainState.SYNCHRONIZED

    total_local_events = database.get_total_event_count()

    target_validator, target_block_number = sync_service.get_highest_block_validator(
        local_block=local_block,
        total_local_events=total_local_events,
        local_validator_ss58_address=local_validator_ss58_address,
        active_connections=active_connections,
        only_truthful=True,
        with_lock=True
    )

    if target_validator == local_validator_ss58_address:
        return ChainState.SYNCHRONIZED
    elif target_block_number > local_block.block_number:
        return ChainState.OUT_OF_SYNC
    else:
        return ChainState.FORK_DETECTED
