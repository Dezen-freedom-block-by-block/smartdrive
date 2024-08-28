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

import multiprocessing
import time

from smartdrive.validator.models.models import ActiveValidator
from smartdrive.validator.node.connection_pool import ConnectionPool

INACTIVITY_TIMEOUT_SECONDS = 10


class ActiveValidatorsManager:
    def __init__(self):
        manager = multiprocessing.Manager()
        self.active_validators = manager.list()
        self.lock = multiprocessing.Lock()

    def update_validator(self, validator, connection):
        with self.lock:
            for v in self.active_validators:
                if v.active_validators[ConnectionPool.MODULEINFO].ss58_address == validator.ss58_address:
                    v.last_response_time = time.monotonic()
                    return
            self.active_validators.append(ActiveValidator({ConnectionPool.MODULEINFO: validator, ConnectionPool.CONNECTION: connection}, time.monotonic()))

    def remove_inactive_validators(self):
        with self.lock:
            current_time = time.monotonic()
            self.active_validators[:] = [v for v in self.active_validators if current_time - v.last_response_time <= INACTIVITY_TIMEOUT_SECONDS]
            to_remove = [v.active_validators[ConnectionPool.MODULEINFO].ss58_address for v in self.active_validators if current_time - v.last_response_time > INACTIVITY_TIMEOUT_SECONDS]
            return to_remove

    def get_active_validators(self):
        with self.lock:
            return [v.active_validators.get(ConnectionPool.MODULEINFO) for v in self.active_validators]

    def get_active_validators_connections(self):
        with self.lock:
            return [v.active_validators.get(ConnectionPool.CONNECTION) for v in self.active_validators]
