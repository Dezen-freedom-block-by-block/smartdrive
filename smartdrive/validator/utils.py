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
import random

from smartdrive.logging_config import logger
from smartdrive.sign import sign_data
from smartdrive.validator.node.connection.connection_pool import Connection
from smartdrive.validator.node.connection.utils.utils import send_message
from smartdrive.validator.node.util.message import MessageCode, MessageBody, Message


def prepare_sync_blocks(start, keypair, end=None, active_connections=None):
    async def _prepare_sync_blocks():
        if not active_connections:
            return
        await get_synced_blocks(start, active_connections, keypair, end)

    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        loop = None

    if loop and loop.is_running():
        asyncio.create_task(_prepare_sync_blocks())
    else:
        asyncio.run(_prepare_sync_blocks())


async def get_synced_blocks(start: int, connections: list[Connection], keypair, end: int = None):
    async def _get_synced_blocks(connection: Connection):
        try:
            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_SYNC,
                data={"start": str(start)}
            )
            if end:
                body.data["end"] = str(end)

            body_sign = sign_data(body.dict(), keypair)

            message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=keypair.public_key.hex()
            )

            send_message(connection, message)
        except Exception:
            logger.error("Error getting synced blocks", exc_info=True)

    connection = random.choice(connections)
    await _get_synced_blocks(connection)


def get_file_expiration() -> int:
    """
    Generate a random expiration time in milliseconds within a range.

    Returns:
        int: A random expiration time between 30 minutes (min_ms) and 1 hour (max_ms) in milliseconds.
    """
    min_ms = 30 * 60 * 1000
    max_ms = 1 * 60 * 60 * 1000
    return random.randint(min_ms, max_ms)
