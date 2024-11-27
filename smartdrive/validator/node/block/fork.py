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

import random
import time
from typing import List

from substrateinterface import Keypair

from smartdrive import logger
from smartdrive.models.block import Block
from smartdrive.sign import sign_data
from smartdrive.validator.database.database import Database
from smartdrive.validator.node.connection.connection_pool import Connection
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.sync_service import SyncService
from smartdrive.validator.node.util.message import MessageBody, MessageCode, Message
from smartdrive.validator.utils import request_last_block, prepare_sync_blocks

HASH_BATCH_SIZE = 250


def find_common_block_and_resolve_fork(
        database: Database,
        sync_service: SyncService,
        local_block: Block,
        keypair: Keypair,
        active_connections: List[Connection]
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
    """
    remote_chain = sync_service.get_remote_chain()
    common_block = None
    current_block = local_block

    while current_block:
        if current_block.block_number in remote_chain and current_block.hash == remote_chain[current_block.block_number]["hash"]:
            common_block = current_block
            break
        if current_block.block_number == 1:
            break
        get_blocks = database.get_blocks(block_hash=current_block.previous_hash, include_events=False)
        current_block = get_blocks[0] if get_blocks else None

    if common_block:
        logger.info(f"Resolving fork starting, common block: {common_block.block_number}")

        local_chain_from_common = _get_chain_from_block(database, local_block, common_block)
        remote_chain_from_common = {
            block_number: block_data for block_number, block_data in remote_chain.items()
            if block_number > common_block.block_number
        }

        local_events_count = sum(len(block.events) for block in local_chain_from_common)
        remote_events_count = sum(block_data["event_count"] for block_data in remote_chain_from_common.values())

        logger.info(f"Comparing chains: Remote events {remote_events_count}, Remote len {len(remote_chain_from_common)}, Local events {local_events_count}, Local len {len(local_chain_from_common)}")
        if remote_events_count > local_events_count or (
                remote_events_count == local_events_count and len(remote_chain_from_common) > len(local_chain_from_common)
        ):
            logger.info("Adopting remote chain.")
            _adopt_chain(
                database=database,
                local_block=local_block,
                common_block=common_block,
                sync_service=sync_service,
                active_connections=active_connections,
                keypair=keypair
            )
        else:
            logger.info("Keeping local chain.")
            sync_service.complete_sync(is_synced=True)

    else:
        logger.warning("No common block found. Requesting additional hashes.")
        if remote_chain:
            min_block_number = min(remote_chain.keys())
            start_block = max(0, min_block_number - HASH_BATCH_SIZE)
            request_block_hashes(
                start_block=start_block,
                end_block=min_block_number,
                keypair=keypair,
                active_connections=active_connections,
                sync_service=sync_service
            )


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


def _adopt_chain(database: Database, local_block: Block, common_block: Block, sync_service: SyncService, active_connections, keypair):
    """
    Resolves the fork by removing local blocks that diverge from the common block.

    Args:
        database (Database): The local database instance.
        local_block (Block): The most recent block in the local chain.
        common_block (Block): The block where both chains converge.
        sync_service (SyncService): The sync service managing the chain.
        active_connections: The list of active validator connections.
        keypair: The keypair used for signing messages.
    """
    current_block = local_block
    while current_block and current_block.block_number > common_block.block_number:
        database.remove_block(current_block.block_number)
        get_blocks = database.get_blocks(block_hash=current_block.previous_hash, include_events=False)
        current_block = get_blocks[0] if get_blocks else None

    if common_block.block_number + 1 <= sync_service.get_expected_end_block():
        sync_service.start_sync()
        prepare_sync_blocks(
            start=common_block.block_number + 1,
            end=sync_service.get_expected_end_block(),
            active_connections=active_connections,
            sync_service=sync_service,
            keypair=keypair
        )

    sync_service.clear_remote_chain()

    logger.info(f"Fork resolved. Local chain now aligned with block {common_block.block_number}.")


def request_block_hashes(start_block: int, end_block: int, keypair: Keypair, active_connections: List[Connection], sync_service: SyncService):
    request_last_block(key=keypair, connections=active_connections)
    time.sleep(10)
    highest_block_validator = sync_service.get_highest_block_validator()
    connection = next((connection for connection in active_connections if connection.module.ss58_address == highest_block_validator), random.choice(active_connections))

    body = MessageBody(
        code=MessageCode.MESSAGE_CODE_FORK_BLOCK_DATA,
        data={"start_block": start_block, "end_block": end_block}
    )
    body_sign = sign_data(body.dict(), keypair)
    request_message = Message(
        body=body,
        signature_hex=body_sign.hex(),
        public_key_hex=keypair.public_key.hex()
    )

    send_message(connection, request_message)


def detect_potential_fork(sync_service: SyncService, last_block: Block, current_total_event_count: int) -> bool:
    """
    Detects if there is a potential fork based on the current state of the chain
    and the data from other validators.

    Args:
        sync_service (SyncService): The synchronization service instance.
        last_block (Block): The last local block.
        current_total_event_count (int): The total event count in the local chain.

    Returns:
        bool: True if a potential fork is detected, False otherwise.
    """
    highest_block_validator = sync_service.get_highest_block_validator()
    highest_block_data = sync_service.get_last_block_other_validator().get(highest_block_validator)

    if not highest_block_data:
        return False

    highest_block_number = highest_block_data["block_number"]
    highest_block_hash = highest_block_data["block_hash"]
    highest_event_count = highest_block_data["total_event_count"]

    # Case 1: Higher block number, but no divergence in hashes.
    if highest_block_number > last_block.block_number:
        # Check if they share the same ancestor (no fork, just outdated).
        if sync_service.get_remote_chain().get(last_block.block_number) == last_block.hash:
            return False  # No fork, just synchronize.

    # Case 2: Hash mismatch at the same block number.
    if highest_block_number == last_block.block_number and highest_block_hash != last_block.hash:
        return True

    # Case 3: Remote chain has more events (indicating potential divergence).
    if highest_event_count > current_total_event_count:
        return True

    return False
