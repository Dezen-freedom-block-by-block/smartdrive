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

# Validator
BLOCK_INTERVAL_SECONDS = 30
VALIDATION_VOTE_INTERVAL_SECONDS = 10 * 60  # 10 minutes
SLEEP_TIME_CHECK_STAKE_SECONDS = 1 * 60 * 60  # 1 hour
VALIDATOR_DELAY_SECONDS = 20
MAX_EVENTS_PER_BLOCK = 100  # TODO: REPLACE THIS WITH bytes
MAX_SIMULTANEOUS_VALIDATIONS = 15

# Storage
INITIAL_STORAGE_PER_USER = 50 * 1024 * 1024  # 50 MB
MAXIMUM_STORAGE_PER_USER_PER_FILE = 5 * 1024 * 1024 * 1024  # 5 GB
ADDITIONAL_STORAGE_PER_COMAI = 0.25 * 1024 * 1024  # 0.25 MB
MINIMUM_STAKE = 1  # 1 COMAI
REDUNDANCY_PER_CHUNK = 6
MINER_STORE_TIMEOUT_SECONDS = 8 * 60  # 8 minutes
MINER_RETRIEVE_TIMEOUT_SECONDS = 8 * 60  # 8 minutes
MAX_ENCODED_RANGE_SUB_CHUNKS = 100
READ_FILE_SIZE = 32 * 1024  # Read file size 32 KB
TIME_EXPIRATION_STORE_REQUEST_EVENT_SECONDS = 30 * 60  # 30 minutes

# Paths
DEFAULT_MINER_PATH = "~/.smartdrive/miner"
DEFAULT_VALIDATOR_PATH = "~/.smartdrive/validator"
DEFAULT_CLIENT_PATH = "~/.smartdrive/client"

INTERVAL_CHECK_VERSION_SECONDS = 30 * 60  # 30 min
