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
from multiprocessing import Lock, Value, Manager
from typing import Optional


class SyncService:
    MAX_SYNC_TIMEOUT_SECONDS: int = 600  # 10 minutes

    def __init__(self):
        self._is_synced: Value = Value('b', False)
        self._is_syncing: Value = Value('b', False)
        self._is_fork_syncing: Value = Value('b', False)
        self._sync_start_time: Value = Value('d', time.monotonic())
        self._last_block_other_validator = Manager().dict()
        self._accumulated_remote_chain = Manager().dict()
        self._sync_lock = Lock()

    def start_sync(self, is_fork_syncing: bool = False):
        with self._sync_lock:
            self._is_syncing.value = True
            self._is_synced.value = False
            self._is_fork_syncing.value = is_fork_syncing
            self._sync_start_time.value = time.monotonic()

    def add_last_block_other_validators(self, validator_ss58_address: str, block_number: int, block_hash: str, total_event_count: int):
        with self._sync_lock:
            self._last_block_other_validator[validator_ss58_address] = {
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
                self._accumulated_remote_chain.clear()
                self._last_block_other_validator.clear()

    def evaluate_initial_sync(self, current_block_number: int, current_block_hash: str) -> bool:
        """
        Evaluates whether the current validator is synchronized with others.

        Args:
            current_block_number (int): Current block number of the validator.
            current_block_hash (str): Current block hash of the validator.

        Returns:
            bool: True if synchronized, False otherwise.
        """
        with self._sync_lock:
            for validator, data in self._last_block_other_validator.items():
                block_number = data["block_number"]
                block_hash = data["block_hash"]

                if (block_number > current_block_number) or (block_number == current_block_number and block_hash != current_block_hash):
                    return False

            return True

    def has_reached_expected_block(self, current_block_number: int) -> bool:
        with self._sync_lock:
            return self.get_expected_end_block() != 0 and current_block_number >= self.get_expected_end_block()

    def is_syncing(self) -> bool:
        with self._sync_lock:
            if self._is_syncing.value and (time.monotonic() - self._sync_start_time.value) < self.MAX_SYNC_TIMEOUT_SECONDS:
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

    def get_expected_end_block(self) -> int:
        with self._sync_lock:
            if self._last_block_other_validator:
                best_candidate = max(
                    self._last_block_other_validator.values(),
                    key=lambda data: (data["total_event_count"], data["block_number"])
                )
                return best_candidate["block_number"]
            return 0

    def get_highest_block_validator(self) -> Optional[str]:
        """
        Returns the validator with the highest total event count,
        and in case of a tie, the highest block number.

        Returns:
            Optional[str]: The SS58 address of the validator with the most updated chain.
        """
        with self._sync_lock:
            if not self._last_block_other_validator:
                return None

            highest_validator = None
            highest_total_event_count = -1
            highest_block_number = -1

            for validator, data in self._last_block_other_validator.items():
                block_number = data.get("block_number", -1)
                total_event_count = data.get("total_event_count", -1)

                if total_event_count > highest_total_event_count or \
                        (total_event_count == highest_total_event_count and block_number > highest_block_number):
                    highest_total_event_count = total_event_count
                    highest_block_number = block_number
                    highest_validator = validator

            return highest_validator

    def add_to_remote_chain(self, block_map: dict):
        with self._sync_lock:
            for block_number, block_data in block_map.items():
                if block_number not in self._accumulated_remote_chain:
                    self._accumulated_remote_chain[block_number] = block_data
                else:
                    self._accumulated_remote_chain[block_number].update(block_data)

    def get_remote_chain(self):
        with self._sync_lock:
            return self._accumulated_remote_chain.copy()

    def get_last_block_other_validator(self) -> dict:
        with self._sync_lock:
            return self._last_block_other_validator.copy()

    def clear_remote_chain(self):
        with self._sync_lock:
            self._accumulated_remote_chain.clear()
