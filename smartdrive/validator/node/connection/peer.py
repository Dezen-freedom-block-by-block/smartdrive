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

import queue
import threading
from _socket import SocketType
from multiprocessing import Value

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

import smartdrive
import smartdrive.validator.node.connection.utils.utils
from smartdrive.logging_config import logger
from smartdrive.models.event import parse_event, MessageEvent, Action, ValidationEvent
from smartdrive.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.block import BlockEvent, block_event_to_block, Block
from smartdrive.validator.node.connection.connection_pool import ConnectionPool, Connection
from smartdrive.validator.node.event.event_pool import EventPool
from smartdrive.validator.node.util.block_integrity import check_block_integrity
from smartdrive.validator.node.util.exceptions import ClientDisconnectedException, MessageFormatException, InvalidSignatureException, BlockIntegrityException
from smartdrive.validator.node.util.message import MessageCode, Message, MessageBody
from smartdrive.validator.utils import prepare_sync_blocks
from smartdrive.validator.node.connection.utils.utils import send_message, receive_msg


class Peer(threading.Thread):
    MAX_BLOCKS_SYNC = 500
    MAX_VALIDATION_SYNC = 500

    _socket: SocketType = None
    _connection: Connection = None
    _connection_pool: ConnectionPool = None
    _event_pool: EventPool = None
    _keypair: Keypair = None
    _database: Database = None
    _message_queue: queue.Queue = None
    _initial_sync_completed: Value = None
    _running: bool = True

    def __init__(self, connection: Connection, connection_pool: ConnectionPool, event_pool: EventPool, initial_sync_completed: Value):
        threading.Thread.__init__(self)
        self._connection = connection
        self._connection_pool = connection_pool
        self._event_pool = event_pool
        self._initial_sync_completed = initial_sync_completed
        self._keypair = classic_load_key(config_manager.config.key)
        self._database = Database()
        self._message_queue = queue.Queue()
        self._running = True
        threading.Thread(target=self._consume_queue).start()

    def run(self):
        while True:
            try:
                # Here the process is waiting till a new message is received
                json_message = receive_msg(self._socket)
                self._message_queue.put(json_message)
            except (ConnectionResetError, ConnectionAbortedError, ClientDisconnectedException):
                logger.debug(f"Peer {self._connection.module.ss58_address} disconnected")
                break
            except Exception:
                logger.error(f"Unexpected error in connection {self._connection.module.ss58_address}", exc_info=True)
                break
        self._running = False

    def _consume_queue(self):
        while self._running:
            try:
                json_message = self._message_queue.get(timeout=1)
                self._process_message(json_message)
            except queue.Empty:
                continue

    def _process_message(self, json_message):
        try:
            message = Message(**json_message)

            if message.body.code not in set(MessageCode):
                raise MessageFormatException("Unknown message code")

            signature_hex = message.signature_hex
            public_key_hex = message.public_key_hex
            ss58_address = get_ss58_address_from_public_key(public_key_hex)

            is_verified_signature = verify_data_signature(message.body.dict(), signature_hex, ss58_address)
            if not is_verified_signature:
                raise InvalidSignatureException()

            if message.body.code == MessageCode.MESSAGE_CODE_BLOCK:
                self._process_message_block(message)

            elif message.body.code == MessageCode.MESSAGE_CODE_EVENT:
                message_event = MessageEvent.from_json(message.body.data["event"], Action(message.body.data["event_action"]))
                event = parse_event(message_event)
                try:
                    self._event_pool.append(event)
                except InvalidSignatureException:
                    logger.error(f"Invalid signature in event {event}", exc_info=True)

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
                send_message(self._connection, message)

            elif message.body.code == MessageCode.MESSAGE_CODE_PONG:
                self._connection_pool.update_ping(self._connection.module.ss58_address)

            elif message.body.code == MessageCode.MESSAGE_CODE_SYNC:
                self._process_message_sync(message)

            elif message.body.code == MessageCode.MESSAGE_CODE_SYNC_BLOCKS_RESPONSE:
                self._process_message_sync_blocks_response(message)

            elif message.body.code == MessageCode.MESSAGE_CODE_VALIDATION_EVENTS:
                validation_events = [ValidationEvent(**validation_event) for validation_event in message.body.data["list"]]
                if validation_events:
                    self._database.insert_validation_events(validation_events=validation_events)

        except Exception:
            logger.error("Can not process an incoming message", exc_info=True)

    def _process_message_block(self, message: Message):
        block_event = BlockEvent(
            block_number=message.body.data["block_number"],
            events=list(map(lambda event: MessageEvent.from_json(event["event"], Action(event["event_action"])), message.body.data["events"])),
            signed_block=message.body.data["signed_block"],
            proposer_ss58_address=message.body.data["proposer_ss58_address"]
        )
        block = block_event_to_block(block_event)

        try:
            check_block_integrity(
                block=block,
                database=self._database
            )

            local_block_number = self._database.get_last_block_number() or 0
            if block.block_number - 1 != local_block_number:
                prepare_sync_blocks(start=local_block_number + 1, end=block.block_number, active_connections=self._connection_pool.get_all(), keypair=self._keypair)
            else:
                self._database.create_block(block)
                self._event_pool.remove_multiple(block.events)

                if not self._initial_sync_completed.value:
                    self._initial_sync_completed.value = True

        except BlockIntegrityException:
            logger.error(exc_info=True)

    def _process_message_sync(self, message: Message):
        start = int(message.body.data['start'])
        end = int(message.body.data['end']) if message.body.data.get("end") else (self._database.get_last_block_number() or 0)
        segment_size = self.MAX_BLOCKS_SYNC

        if start <= end:
            for segment_start in range(start, end + 1, segment_size):
                segment_end = min(segment_start + segment_size - 1, end)
                blocks = self._database.get_blocks(segment_start, segment_end)

                if blocks:
                    body = MessageBody(
                        code=MessageCode.MESSAGE_CODE_SYNC_BLOCKS_RESPONSE,
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
                    send_message(self._connection, message)

            for event in self._event_pool.get_all():
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
                send_message(self._connection, message)

    def _process_message_sync_blocks_response(self, message: Message):
        if message.body.data["blocks"]:
            for block in message.body.data["blocks"]:
                block = Block(**block)

                try:
                    check_block_integrity(
                        block=block,
                        database=self._database
                    )
                    self._database.create_block(block)
                    self._event_pool.remove_multiple(block.events)

                except BlockIntegrityException as e:
                    logger.error(e, exc_info=True)
                    return
