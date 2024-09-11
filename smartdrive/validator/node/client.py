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
import traceback
from typing import List

from communex.compat.key import classic_load_key

import smartdrive
from smartdrive.models.event import parse_event, MessageEvent, Action, Event, ValidationEvent
from smartdrive.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.block import BlockEvent, block_event_to_block, Block
from smartdrive.validator.node.connection_pool import ConnectionPool
from smartdrive.validator.node.util import packing
from smartdrive.validator.node.util.authority import are_all_block_events_valid, remove_invalid_block_events
from smartdrive.validator.node.util.exceptions import MessageException, ClientDisconnectedException, MessageFormatException, InvalidSignatureException
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.node.util.utils import send_json
from smartdrive.validator.utils import process_events, prepare_sync_blocks


class Client(multiprocessing.Process):
    MAX_BLOCKS_SYNC = 500
    MAX_VALIDATION_SYNC = 500

    _client_socket = None
    _identifier: str = None
    _connection_pool = None
    _event_pool = None
    _keypair = None
    _database = None
    _initial_sync_completed = None
    _synced_blocks = None

    def __init__(self, client_socket, identifier, connection_pool: ConnectionPool, event_pool, event_pool_lock, initial_sync_completed):
        multiprocessing.Process.__init__(self)
        self._client_socket = client_socket
        self._identifier = identifier
        self._connection_pool = connection_pool
        self._event_pool = event_pool
        self._event_pool_lock = event_pool_lock
        self._initial_sync_completed = initial_sync_completed
        self._keypair = classic_load_key(config_manager.config.key)
        self._database = Database()
        self._synced_blocks = []

    def run(self):
        try:
            self._handle_client()
        except ClientDisconnectedException:
            print(f"Removing connection from connection pool: {self._identifier}")
            removed_connection = self._connection_pool.remove_if_exists(self._identifier)
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
            print(f"Client disconnected: {self._identifier}")
        except Exception as e:
            traceback.print_exc()
            print(f"HANDLE CLIENT EXCEPTION {e}")
        finally:
            self._client_socket.close()
            raise ClientDisconnectedException(f"Lost {self._identifier}")

    def _receive(self):
        # Here the process is waiting till a new message is sent.
        json_message = packing.receive_msg(self._client_socket)
        # Although _event_pool is managed by multiprocessing.Manager(),
        # we explicitly pass it as parameters to make it clear that it is dependency of the process_message process.
        process = multiprocessing.Process(target=self._process_message, args=(json_message, self._event_pool, self._connection_pool,))
        process.start()
        process.join()

    def _process_message(self, json_message, event_pool, connection_pool):
        message = Message(**json_message)

        try:
            if message.body.code in [code for code in MessageCode]:
                signature_hex = message.signature_hex
                public_key_hex = message.public_key_hex
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                is_verified_signature = verify_data_signature(message.body.dict(), signature_hex, ss58_address)

                if not is_verified_signature:
                    raise InvalidSignatureException()

                if message.body.code == MessageCode.MESSAGE_CODE_BLOCK:
                    block_event = BlockEvent(
                        block_number=message.body.data["block_number"],
                        events=list(map(lambda event: MessageEvent.from_json(event["event"], Action(event["event_action"])), message.body.data["events"])),
                        signed_block=message.body.data["signed_block"],
                        proposer_ss58_address=message.body.data["proposer_ss58_address"]
                    )
                    block = block_event_to_block(block_event)

                    if not verify_data_signature(
                            data={"block_number": block.block_number, "events": [event.dict() for event in block.events]},
                            signature_hex=block.signed_block,
                            ss58_address=block.proposer_ss58_address
                    ):
                        print(f"Block {block.block_number} not verified")
                        return

                    remove_invalid_block_events(block)

                    # TODO: Check when a validator creates a block (it is not a proposer) and just enters to validate
                    #  the proposer and creates another block, in this case, the blocks will be repeated
                    local_block_number = self._database.get_last_block_number() or 0
                    if block.block_number - 1 != local_block_number:
                        prepare_sync_blocks(start=local_block_number + 1, end=block.block_number, active_connections=connection_pool.get_all(), keypair=self._keypair)
                    else:
                        self._run_process_events(block.events)
                        self._remove_events(block.events, event_pool)
                        self._database.create_block(block)

                        if not self._initial_sync_completed.value:
                            self._initial_sync_completed.value = True

                elif message.body.code == MessageCode.MESSAGE_CODE_EVENT:
                    message_event = MessageEvent.from_json(message.body.data["event"], Action(message.body.data["event_action"]))
                    event = parse_event(message_event)
                    if not any(e.uuid == event.uuid for e in event_pool):
                        event_pool.append(event)

                elif message.body.code == MessageCode.MESSAGE_CODE_PING:
                    body = MessageBody(
                        code=MessageCode.MESSAGE_CODE_PONG,
                        data={"version": smartdrive.__version__}
                    )
                    body_sign = sign_data(body.dict(), self._keypair)
                    message = Message(
                        body=body,
                        signature_hex=body_sign.hex(),
                        public_key_hex=self._keypair.public_key.hex()
                    )
                    send_json(self._client_socket, message.dict())

                elif message.body.code == MessageCode.MESSAGE_CODE_PONG:
                    connection = self._connection_pool.get(self._identifier)
                    if connection:
                        connection_pool.upsert_connection(connection.module.ss58_address, connection.module, connection.socket)

                elif message.body.code == MessageCode.MESSAGE_CODE_SYNC_BLOCK:
                    start = int(message.body.data['start'])
                    end = int(message.body.data['end']) if message.body.data.get("end") else (self._database.get_last_block_number() or 0)
                    segment_size = self.MAX_BLOCKS_SYNC

                    if start <= end:
                        for segment_start in range(start, end + 1, segment_size):
                            segment_end = min(segment_start + segment_size - 1, end)
                            blocks = self._database.get_blocks(segment_start, segment_end)

                            if blocks:
                                body = MessageBody(
                                    code=MessageCode.MESSAGE_CODE_SYNC_BLOCK_RESPONSE,
                                    data={
                                        "blocks": [block.dict() for block in blocks],
                                        "start": segment_start,
                                        "end": segment_end
                                    }
                                )
                                body_sign = sign_data(body.dict(), self._keypair)
                                message = Message(
                                    body=body,
                                    signature_hex=body_sign.hex(),
                                    public_key_hex=self._keypair.public_key.hex()
                                )
                                send_json(self._client_socket, message.dict())

                        # Send event pool too
                        for event in event_pool:
                            message_event = MessageEvent.from_json(event.dict(), event.get_event_action())
                            body = MessageBody(
                                code=MessageCode.MESSAGE_CODE_EVENT,
                                data=message_event.dict()
                            )
                            body_sign = sign_data(body.dict(), self._keypair)
                            message = Message(
                                body=body,
                                signature_hex=body_sign.hex(),
                                public_key_hex=self._keypair.public_key.hex()
                            )
                            send_json(self._client_socket, message.dict())

                elif message.body.code == MessageCode.MESSAGE_CODE_SYNC_BLOCK_RESPONSE:
                    if message.body.data["blocks"]:
                        fetched_block_numbers = list(map(lambda block: block["block_number"], message.body.data["blocks"]))
                        fetched_min_block_number = min(fetched_block_numbers)
                        fetched_max_block_number = max(fetched_block_numbers)

                        if not range(int(message.body.data["start"]), int(message.body.data["end"])) == range(fetched_min_block_number, fetched_max_block_number):
                            return

                        blocks = []
                        for block in message.body.data["blocks"]:
                            block = Block(**block)

                            if not are_all_block_events_valid(block):
                                print(f"Invalid blocks {block}")
                                self._synced_blocks = []
                                return

                            blocks.append(block)

                        for block in blocks:
                            self._run_process_events(block.events)
                            self._remove_events(block.events, event_pool)
                            self._database.create_block(block)

                elif message.body.code == MessageCode.MESSAGE_CODE_VALIDATION_EVENTS:
                    validation_events = [ValidationEvent(**validation_event) for validation_event in message.body.data["list"]]
                    if validation_events:
                        self._database.insert_validation_events(validation_events=validation_events)

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
                netuid=config_manager.config.netuid,
                database=self._database
            )

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.create_task(run_process_events(processed_events))
        else:
            asyncio.run(run_process_events(processed_events))

    def _remove_events(self, events: List[Event], event_pool):
        uuids_to_remove = {event.uuid for event in events}
        with self._event_pool_lock:
            updated_event_pool = [event for event in event_pool if event.uuid not in uuids_to_remove]
            event_pool[:] = updated_event_pool
