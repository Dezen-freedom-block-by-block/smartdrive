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
from multiprocessing import Lock, Value, Manager
from typing import Tuple, List, Optional

from communex.balance import from_nano

from smartdrive import logger
from smartdrive.models.block import Block
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT


class ConsensusState(Enum):
    CONSENSUS_REACHED = "consensus_reached"
    NO_CONSENSUS = "no_consensus"
    PROPOSER_CHAIN = "proposer_chain"
    TRUTHFUL_CHAIN = "truthful_chain"
    NO_VALID_CHAIN = "no_valid_chain"
    LOCAL_CHAIN_ADVANCED = "local_chain_advanced"


class SyncService:
    MAX_SYNC_TIMEOUT_SECONDS: int = 600  # 10 minutes
    MAX_SUSPICIOUS_THRESHOLD = 3
    SUSPICIOUS_TIMEOUT_SECONDS = 15 * 60  # 15 minutes

    def __init__(self):
        manager = Manager()
        self._is_synced: Value = Value('b', False)
        self._is_syncing: Value = Value('b', False)
        self._is_fork_syncing: Value = Value('b', False)
        self._sync_start_time: Value = Value('d', time.monotonic())
        self._current_proposer = manager.dict()
        self._current_proposer["proposer"] = ""
        self._last_block_other_validators = manager.dict()
        self._remote_chain = manager.dict()
        self._suspicious_validators = manager.dict()
        self._sync_lock = Lock()
        self._proposer_lock = Lock()

    def start_sync(self, is_fork_syncing: bool = False):
        with self._sync_lock:
            self._is_syncing.value = True
            self._is_synced.value = False
            self._is_fork_syncing.value = is_fork_syncing
            self._sync_start_time.value = time.monotonic()

    def add_last_block_other_validators(self, validator_ss58_address: str, block_number: int, block_hash: str, total_event_count: int):
        """
        Adds or updates the last block information for a specific validator,
        unless the validator is marked as suspicious.

        Args:
            validator_ss58_address (str): The SS58 address of the validator.
            block_number (int): The block number reported by the validator.
            block_hash (str): The hash of the last block.
            total_event_count (int): The total number of events in the validator's chain.
        """
        if self.is_validator_suspicious(validator_ss58_address):
            return

        with self._sync_lock:
            self._last_block_other_validators[validator_ss58_address] = {
                "block_number": block_number,
                "block_hash": block_hash,
                "total_event_count": total_event_count
            }

    def complete_sync(self, is_synced: bool):
        with self._sync_lock:
            self._is_syncing.value = False
            self._is_fork_syncing.value = False
            self._is_synced.value = is_synced

            if is_synced:
                self._remote_chain.clear()
                self._last_block_other_validators.clear()

    def is_syncing(self) -> bool:
        with self._sync_lock:
            if self._is_syncing.value and (time.monotonic() - self._sync_start_time.value) >= self.MAX_SYNC_TIMEOUT_SECONDS:
                self._is_syncing.value = False
                self._is_synced.value = False

            return self._is_syncing.value

    def is_fork_syncing(self) -> bool:
        with self._sync_lock:
            if self._is_fork_syncing.value and (time.monotonic() - self._sync_start_time.value) >= self.MAX_SYNC_TIMEOUT_SECONDS:
                self._is_fork_syncing.value = False

            return self._is_fork_syncing.value

    def is_synced(self) -> bool:
        with self._sync_lock:
            return self._is_synced.value

    def get_highest_block_validator(
            self,
            local_block: Optional[Block] = None,
            total_local_events: Optional[int] = None,
            local_validator_ss58_address: Optional[str] = None,
            active_connections: List = None,
            only_truthful: bool = False,
            with_lock: bool = True,
    ) -> Tuple[str, int]:
        """
        Returns the validator with the highest total event count, including the local validator.
        In case of ties, prioritizes the highest block number and then the proposer.

        Args:
            active_connections (List): List of active connections to validate truthful validators.
            total_local_events (Optional[int]): The total number of events in the validator's chain.
            local_validator_ss58_address (Optional[str]): The SS58 address of the validator.
            only_truthful (bool): Whether to consider only truthful validators (stake >= TRUTHFUL_STAKE_AMOUNT).
            with_lock (bool): Whether to use locks for thread safety.
            local_block (Optional[Block]): The local block to include in the comparison.

        Returns:
            Tuple[str, int]: A tuple containing the SS58 address of the validator with the most updated chain
                             and the block number.
        """
        current_proposer = self.get_current_proposer(with_lock=with_lock)
        validators_to_compare = self.get_last_block_other_validators(with_lock=with_lock)

        def _get_validator():
            highest_validator = ""
            highest_total_event_count = -1
            highest_block_number = -1

            if local_block:
                validators_to_compare[local_validator_ss58_address] = {
                    "block_number": local_block.block_number,
                    "block_hash": local_block.hash,
                    "total_event_count": total_local_events
                }

            for validator, data in validators_to_compare.items():
                # Skip suspicious validators unless it's the local validator
                if validator != local_validator_ss58_address and self.is_validator_suspicious(validator, use_lock=False):
                    continue

                # If only_truthful is True, ensure the validator meets the stake criteria
                if validator != local_validator_ss58_address and only_truthful and active_connections:
                    target_connection = next((conn for conn in active_connections if conn.module.ss58_address == validator), None)
                    if not target_connection or from_nano(target_connection.module.stake) < TRUTHFUL_STAKE_AMOUNT:
                        continue

                block_number = data.get("block_number", -1)
                total_event_count = data.get("total_event_count", -1)

                is_more_events = total_event_count > highest_total_event_count
                is_same_events_and_more_blocks = (
                    total_event_count == highest_total_event_count and block_number > highest_block_number
                )
                is_same_events_blocks_and_proposer = (
                    total_event_count == highest_total_event_count and block_number == highest_block_number and validator == current_proposer
                )

                # Prioritize total_event_count > block_number > proposer
                if is_more_events or is_same_events_and_more_blocks or is_same_events_blocks_and_proposer:
                    highest_total_event_count = total_event_count
                    highest_block_number = block_number
                    highest_validator = validator

            return highest_validator, highest_block_number

        if with_lock:
            with self._sync_lock:
                return _get_validator()
        else:
            return _get_validator()

    def add_to_remote_chain(self, validator_ss58_address: str, block_map: dict):
        if self.is_validator_suspicious(validator_ss58_address):
            return

        with self._sync_lock:
            if validator_ss58_address in self._remote_chain:
                existing_blocks = self._remote_chain[validator_ss58_address]
                combined_blocks = {**existing_blocks, **block_map}
                self._remote_chain[validator_ss58_address] = combined_blocks
            else:
                self._remote_chain[validator_ss58_address] = block_map

    def get_remote_chain(self):
        with self._sync_lock:
            return self._remote_chain.copy()

    def clear_remote_chain(self):
        with self._sync_lock:
            self._remote_chain.clear()

    def validate_last_block_consensus(self, active_connections: List, own_ss58_address: str, local_block: Block, total_local_events: int) -> Tuple[ConsensusState, str, Optional[int]]:
        """
        Validates if there is consensus among validators and selects the preferred chain.

        Args:
            active_connections (List(Connection)): Active validators.
            own_ss58_address (str): The own SS58 address.
            local_block (Block): The current local block.
            total_local_events (int): The total number of local events.

        Returns:
            Tuple[ConsensusState, str, Optional[int]]: A tuple containing the consensus state, the SS58 address
                                                        of the validator with the preferred chain, and the block_number (if applicable).
        """
        active_validators_count = len(active_connections)
        current_proposer = self.get_current_proposer()

        with self._sync_lock:
            # Step 1: Group validators by their last block hash
            hash_groups = {}
            for validator, block_data in self._last_block_other_validators.items():
                last_block_hash = block_data.get("block_hash")
                if not last_block_hash:
                    continue

                if last_block_hash not in hash_groups:
                    hash_groups[last_block_hash] = []
                hash_groups[last_block_hash].append(validator)

            # Step 2: Find the majority group
            majority_group = max(hash_groups.values(), key=len, default=[])
            majority_count = len(majority_group)
            required_majority = active_validators_count // 2 + 1

            # Step 3: Consensus based on majority
            if majority_count >= required_majority:
                preferred_validator = majority_group[0]
                preferred_block_number = self._last_block_other_validators[preferred_validator]["block_number"]
                logger.info(f"Consensus reached: {majority_count}/{active_validators_count} validators agree.")
                return ConsensusState.CONSENSUS_REACHED, preferred_validator, preferred_block_number

            # Step 4: Check truthful validators and compare with local chain
            target_validator, block_number = self.get_highest_block_validator(
                active_connections=active_connections,
                only_truthful=True,
                with_lock=False,
                local_block=local_block,
                total_local_events=total_local_events,
                local_validator_ss58_address=own_ss58_address
            )

            # Step 5: Handle the result of get_highest_block_validator
            if target_validator == own_ss58_address:
                return ConsensusState.LOCAL_CHAIN_ADVANCED, own_ss58_address, None

            if target_validator:
                return ConsensusState.TRUTHFUL_CHAIN, target_validator, block_number

            # Step 6: Fallback to proposer chain
            if current_proposer == own_ss58_address:
                return ConsensusState.PROPOSER_CHAIN, own_ss58_address, None

            # Step 7: No valid chain
            return ConsensusState.NO_VALID_CHAIN, "", None

    def set_current_proposer(self, proposer: str):
        with self._proposer_lock:
            self._current_proposer["proposer"] = proposer

    def get_current_proposer(self, with_lock: bool = True) -> str:
        if with_lock:
            with self._proposer_lock:
                return self._current_proposer["proposer"]
        else:
            return self._current_proposer["proposer"]

    def mark_as_suspicious(self, validator: str):
        current_proposer = self.get_current_proposer()

        with self._sync_lock:
            if current_proposer == validator:
                return

            suspicious_validators = dict(self._suspicious_validators)

            if validator not in suspicious_validators:
                suspicious_validators[validator] = {"count": 0, "timestamp": 0}

            suspicious_validators[validator]["count"] += 1
            suspicious_validators[validator]["timestamp"] = time.time()

            self._suspicious_validators = suspicious_validators

            if self._suspicious_validators[validator]["count"] >= self.MAX_SUSPICIOUS_THRESHOLD:
                logger.error(f"Validator {validator} exceeded the suspicious threshold. Ignoring future blocks.")

                if validator in self._remote_chain:
                    del self._remote_chain[validator]
                    logger.info(f"Removed validator {validator} from remote chain.")

                if validator in self._last_block_other_validators:
                    del self._last_block_other_validators[validator]
                    logger.info(f"Removed validator {validator} from last block data.")

    def is_validator_suspicious(self, validator: str, use_lock: bool = True) -> bool:
        """
        Checks if a validator is suspicious. Optionally uses a lock for thread safety.
        Validators are considered suspicious if their count exceeds the threshold and the timeout has not expired.

        Args:
            validator (str): The SS58 address of the validator to check.
            use_lock (bool): Whether to use a lock for thread safety.

        Returns:
            bool: True if the validator is suspicious, False otherwise.
        """
        def _check_suspicious():
            suspicious_data = self._suspicious_validators.get(validator)
            if not suspicious_data:
                return False

            elapsed_time = time.time() - suspicious_data["timestamp"]
            if elapsed_time > self.SUSPICIOUS_TIMEOUT_SECONDS:
                logger.info(f"Validator {validator} timeout expired. Reconsidering for participation.")
                del self._suspicious_validators[validator]
                return False

            return suspicious_data["count"] >= self.MAX_SUSPICIOUS_THRESHOLD

        if use_lock:
            with self._sync_lock:
                return _check_suspicious()
        else:
            return _check_suspicious()

    def get_last_block_other_validators(self, with_lock: bool = True) -> dict:
        if with_lock:
            with self._sync_lock:
                return self._last_block_other_validators.copy()
        else:
            return self._last_block_other_validators.copy()

    def remove_validator_data(self, validator: str):
        """
        Cleans up all data related to a specific validator that has disconnected.

        Args:
            validator (str): The SS58 address of the validator to clean up.
        """
        with self._sync_lock:
            if validator in self._remote_chain:
                del self._remote_chain[validator]

            if validator in self._last_block_other_validators:
                del self._last_block_other_validators[validator]

            if validator in self._suspicious_validators:
                del self._suspicious_validators[validator]
