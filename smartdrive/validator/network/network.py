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

from substrateinterface import Keypair

from smartdrive.validator.models.block import Event
from smartdrive.validator.network.node.node import Node


class Network:

    _node: Node = None

    def __init__(self, keypair: Keypair, ip: str, netuid: int):
        self._node = Node(keypair=keypair, ip=ip, netuid=netuid)
        # TODO: Implement block creation
        asyncio.run(self.start_periodic_task())

    async def periodic_task(self):
        while True:
            await asyncio.sleep(1)
            print("Periodic check:")
            print(self._node.get_all_mempool_items())

    async def start_periodic_task(self):
        loop = asyncio.get_running_loop()
        loop.create_task(self.periodic_task())
        await asyncio.Event().wait()

    def emit_event(self, event: Event):
        identifiers_connections = self._node.get_identifiers_connections()
        # TODO: Emit event
