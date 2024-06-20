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

import asyncio
import multiprocessing
from typing import List

from communex._common import get_node_url
from communex.client import CommuneClient
from communex.compat.key import classic_load_key

from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.request import get_truthful_validators
from smartdrive.models.event import parse_event, MessageEvent, Action, Event
from smartdrive.validator.api.middleware.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import process_events
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.block import BlockEvent, block_event_to_block, Block
from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.util import packing
from smartdrive.validator.network.node.util.authority import are_all_block_events_valid, remove_invalid_block_events
from smartdrive.validator.network.node.util.exceptions import MessageException, ClientDisconnectedException, MessageFormatException, InvalidSignatureException
from smartdrive.validator.network.node.util.message_code import MessageCode
from smartdrive.validator.utils import fetch_with_retries


class Client(multiprocessing.Process):
    _client_socket = None
    _identifier: str = None
    _connection_pool = None
    _event_pool = None
    _keypair = None
    _comx_client = None
    _database = None

    def __init__(self, client_socket, identifier, connection_pool: ConnectionPool, event_pool):
        multiprocessing.Process.__init__(self)
        self._client_socket = client_socket
        self._identifier = identifier
        self._connection_pool = connection_pool
        self._event_pool = event_pool
        self._keypair = classic_load_key(config_manager.config.key)
        self._comx_client = CommuneClient(url=get_node_url(use_testnet=config_manager.config.testnet))
        self._database = Database()

    def run(self):
        try:
            self._handle_client()
        except ClientDisconnectedException:
            print(f"Removing connection from connection pool: {self._identifier}")
            removed_connection = self._connection_pool.remove_connection(self._identifier)
            if removed_connection:
                removed_connection.close()

    def _handle_client(self):
        try:
            while True:
                self._receive()
        except InvalidSignatureException:
            print("Received invalid sign")
        except (MessageException, MessageFormatException):
            print(f"Received undecodable or invalid message: {self._identifier}")
        except (ConnectionResetError, ConnectionAbortedError, ClientDisconnectedException):
            print(f"Client disconnected': {self._identifier}")
        finally:
            self._client_socket.close()
            raise ClientDisconnectedException(f"Lost {self._identifier}")

    def _receive(self):
        # Here the process is waiting till a new message is sent.
        msg = packing.receive_msg(self._client_socket)
        # Although mempool is managed by multiprocessing.Manager(),
        # we explicitly pass it as parameters to make it clear that it is dependency of the process_message process.
        process = multiprocessing.Process(target=self._process_message, args=(msg, self._event_pool,))
        process.start()
        process.join()

    def _process_message(self, msg, mempool):
        print(f"PROCESSING INCOMING MESSAGE - {msg}")
        body = msg["body"]

        try:
            if body['code'] in [code.value for code in MessageCode]:
                signature_hex = msg["signature_hex"]
                public_key_hex = msg["public_key_hex"]
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                is_verified_signature = verify_data_signature(body, signature_hex, ss58_address)

                if not is_verified_signature:
                    raise InvalidSignatureException()

                if body['code'] == MessageCode.MESSAGE_CODE_BLOCK.value:
                    block_event = BlockEvent(
                        block_number=body["data"]["block_number"],
                        events=list(map(lambda event: MessageEvent.from_json(event["event"], Action(event["event_action"])), body["data"]["events"])),
                        proposer_signature=body["data"]["proposer_signature"],
                        proposer_ss58_address=body["data"]["proposer_ss58_address"]
                    )
                    block = block_event_to_block(block_event)

                    if not verify_data_signature(
                            data={"block_number": block.block_number, "events": block.events},
                            signature_hex=block.proposer_signature,
                            ss58_address=block.proposer_ss58_address
                    ):
                        print("Block not verified")
                        return

                    remove_invalid_block_events(block)
                    
                    local_block_number = self._database.get_last_block() or 0
                    if block.block_number - 1 != local_block_number:
                        self._sync_blocks(local_block_number + 1, block.block_number, mempool)
                    else:
                        self._run_process_events(block.events)
                        self._remove_events(block.events, mempool)
                        self._database.create_block(block=block)

                elif body['code'] == MessageCode.MESSAGE_CODE_EVENT.value:
                    message_event = MessageEvent.from_json(body["data"]["event"], Action(body["data"]["event_action"]))
                    event = parse_event(message_event)
                    mempool.append(event)

        except InvalidSignatureException as e:
            raise e

        except Exception as e:
            print(e)
            raise MessageFormatException('%s' % e)

    def _run_process_events(self, processed_events):
        async def run_process_events(processed_events):
            await process_events(
                events=processed_events,
                is_proposer_validator=False,
                keypair=self._keypair,
                comx_client=self._comx_client,
                netuid=config_manager.config.netuid,
                database=self._database
            )

        loop = asyncio.get_event_loop()
        if not loop.is_running():
            loop.run_until_complete(run_process_events(processed_events))
        else:
            asyncio.ensure_future(run_process_events(processed_events))

    def _sync_blocks(self, start, end, mempool):
        async def sync_blocks():
            active_validators = await get_truthful_validators(self._keypair, self._comx_client, config_manager.config.netuid)

            if not active_validators:
                # Retry once more if no active validators are found initially
                active_validators = await get_truthful_validators(self._keypair, self._comx_client, config_manager.config.netuid)

            if not active_validators:
                return

            input = {"start": str(start), "end": str(end)}
            headers = create_headers(sign_data(input, self._keypair), self._keypair)

            blocks: List[Block] = []
            for validator in active_validators:
                response = await fetch_with_retries("block", validator.connection, params=input, headers=headers, timeout=30)
                if response and response.status_code == 200:

                    data = response.json()
                    if "blocks" not in data:
                        return

                    fetched_block_numbers = list(map(lambda block: block["block_number"], data["blocks"]))
                    fetched_min_block_number = min(fetched_block_numbers)
                    fetched_max_block_number = max(fetched_block_numbers)

                    if not range(start, end) == range(fetched_min_block_number, fetched_max_block_number):
                        return

                    blocks = list(map(lambda json_block: Block(**json_block), data["blocks"]))
                    break

            if not blocks:
                return

            for block in blocks:
                if not are_all_block_events_valid(block):
                    return

            for block in blocks:
                self._run_process_events(block.events)
                self._remove_events(block.events, mempool)
                self._database.create_block(block)

        loop = asyncio.get_event_loop()
        loop.run_until_complete(sync_blocks())

    def _remove_events(self, events: List[Event], mempool):
        uuids_to_remove = {event.uuid for event in events}
        with multiprocessing.Lock():
            updated_mempool = [event for event in mempool if event.uuid not in uuids_to_remove]
            mempool[:] = updated_mempool

