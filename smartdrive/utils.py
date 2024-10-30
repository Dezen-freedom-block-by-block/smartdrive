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

import smartdrive
from smartdrive import logger

INITIAL_STORAGE = 50 * 1024 * 1024  # 50 MB
MAXIMUM_STORAGE = 2 * 1024 * 1024 * 1024  # 2 GB
ADDITIONAL_STORAGE_PER_COMAI = 0.1 * 1024 * 1024  # 0.1 MB
MINIMUM_STAKE = 1  # 1 COMAI

DEFAULT_MINER_PATH = "~/.smartdrive/miner"
DEFAULT_VALIDATOR_PATH = "~/.smartdrive/validator"
DEFAULT_CLIENT_PATH = "~/.smartdrive/client"

INTERVAL_CHECK_VERSION_SECONDS = 12 * 60 * 60  # 12 hours


def format_size(size_in_bytes: int) -> str:
    """
    Format the size from bytes to a human-readable format (MB or GB).

    Params:
        size_in_bytes (int): The size in bytes.

    Returns:
        str: The size formatted in MB or GB.
    """
    size_in_mb = size_in_bytes / (1024 * 1024)
    if size_in_mb >= 1024:
        size_in_gb = size_in_mb / 1024
        return f"{size_in_gb:.2f} GB"
    else:
        return f"{size_in_mb:.2f} MB"


async def periodic_version_check():
    while True:
        logger.info("Checking for updates...")
        smartdrive.check_version()
        await asyncio.sleep(INTERVAL_CHECK_VERSION_SECONDS)
