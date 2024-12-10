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
from typing import Optional

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
from smartdrive.validator.node.sync_service import SyncService
from smartdrive.validator.node.block.integrity import check_block_integrity
from smartdrive.validator.node.block.fork import handle_consensus_and_resolve_fork
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
    _sync_service: SyncService = None
    _running: bool = True
    _is_syncing: bool = False
    _sync_lock: threading.Lock = threading.Lock()
    _sync_start_time: Optional[float] = None
    _expected_block_number: int = None

    def __init__(self, connection: Connection, connection_pool: ConnectionPool, event_pool: EventPool, sync_service: SyncService):
        threading.Thread.__init__(self)
        self._connection = connection
        self._connection_pool = connection_pool
        self._event_pool = event_pool
        self._sync_service = sync_service
        self._keypair = classic_load_key(config_manager.config.key)
        self._database = Database()
        self._message_queue = queue.Queue()
        self._running = True
        threading.Thread(target=self._consume_queue).start()

    def run(self):
        while True:
            try:
                # Here the process is waiting till a new message is received
                json_message = receive_msg(self._connection.socket)
                message = Message(**json_message)

                # These message types (PING, PONG, LAST_BLOCK, LAST_BLOCK_RESPONSE) are processed immediately
                # in separate threads to avoid waiting in the queue, as they are critical for maintaining
                # the validator's network state and synchronization. Other messages are added to the main queue.
                if message.body.code in {
                    MessageCode.MESSAGE_CODE_PING,
                    MessageCode.MESSAGE_CODE_PONG,
                    MessageCode.MESSAGE_CODE_LAST_BLOCK,
                    MessageCode.MESSAGE_CODE_LAST_BLOCK_RESPONSE,
                }:
                    threading.Thread(
                        target=self._process_message, args=(json_message,)
                    ).start()
                else:
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
                self._process_message_block(message=message, validator_ss58_address=ss58_address)

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
                self._connection_pool.update_ping(self._connection.module.ss58_address)

            elif message.body.code == MessageCode.MESSAGE_CODE_PONG:
                self._connection_pool.update_ping(self._connection.module.ss58_address)

            elif message.body.code == MessageCode.MESSAGE_CODE_SYNC:
                self._process_message_sync(message)

            elif message.body.code == MessageCode.MESSAGE_CODE_SYNC_RESPONSE:
                self._process_message_sync_response(message=message, validator_ss58_address=ss58_address)

            elif message.body.code == MessageCode.MESSAGE_CODE_VALIDATION_EVENTS:
                validation_events = [ValidationEvent(**validation_event) for validation_event in message.body.data["list"]]
                if validation_events:
                    self._database.insert_validation_events(validation_events=validation_events)

            elif message.body.code == MessageCode.MESSAGE_CODE_LAST_BLOCK:
                self._process_message_last_block()

            elif message.body.code == MessageCode.MESSAGE_CODE_LAST_BLOCK_RESPONSE:
                self._process_message_last_block_response(message=message, validator_ss58_address=ss58_address)

            elif message.body.code == MessageCode.MESSAGE_CODE_FORK_BLOCK_DATA:
                self._process_message_fork_block_data(message=message)

            elif message.body.code == MessageCode.MESSAGE_CODE_FORK_BLOCK_DATA_RESPONSE:
                self._process_message_fork_block_data_response(message=message, validator_ss58_address=ss58_address)

        except Exception:
            logger.error("Can not process an incoming message", exc_info=True)

    def _process_message_block(self, message: Message, validator_ss58_address: str):
        if self._sync_service.is_syncing() or self._sync_service.is_fork_syncing() or self._sync_service.is_validator_suspicious(validator_ss58_address):
            return

        current_proposer = self._sync_service.get_current_proposer()
        if current_proposer and validator_ss58_address != current_proposer:
            logger.warning(f"Block proposer {validator_ss58_address} is not the current proposer {current_proposer}. Marking as suspicious.")
            self._sync_service.mark_as_suspicious(validator=validator_ss58_address)
            return

        block_event = BlockEvent(
            block_number=message.body.data["block_number"],
            events=list(map(lambda event: MessageEvent.from_json(event["event"], Action(event["event_action"])), message.body.data["events"])),
            signed_block=message.body.data["signed_block"],
            proposer_ss58_address=message.body.data["proposer_ss58_address"],
            previous_hash=message.body.data["previous_hash"],
            hash=message.body.data["hash"]
        )
        block = block_event_to_block(block_event)
        get_blocks = self._database.get_blocks(last_block_only=True)
        previous_block = get_blocks[0] if get_blocks else None

        if not previous_block:
            return

        try:
            if block.block_number > previous_block.block_number + 1:
                self._sync_service.start_sync()
                prepare_sync_blocks(
                    start=previous_block.block_number + 1,
                    end=block.block_number,
                    active_connections=self._connection_pool.get_all(),
                    sync_service=self._sync_service,
                    keypair=self._keypair,
                    validator=block.proposer_ss58_address
                )
            elif previous_block.hash != block.previous_hash:
                self._sync_service.add_to_remote_chain(
                    validator_ss58_address=validator_ss58_address,
                    block_map={
                        block.block_number: {
                            "hash": block.hash,
                            "event_count": len(block.events)
                        }
                    }
                )
                handle_consensus_and_resolve_fork(
                    database=self._database,
                    local_block=previous_block,
                    sync_service=self._sync_service,
                    active_connections=self._connection_pool.get_all(),
                    keypair=self._keypair
                )
                return
            else:
                try:
                    check_block_integrity(previous_block=previous_block, current_block=block, database=self._database)
                except Exception as e:
                    self._sync_service.mark_as_suspicious(validator=validator_ss58_address)
                    self._sync_service.complete_sync(is_synced=False)

                    if isinstance(e, BlockIntegrityException):
                        raise

                    return

                self._database.create_block(previous_hash=previous_block.hash, block=block)
                self._event_pool.remove_multiple(block.events)
                self._sync_service.complete_sync(is_synced=True)

        except BlockIntegrityException:
            logger.error("BlockIntegrityError", exc_info=True)

    def _process_message_sync(self, message: Message):
        start = int(message.body.data['start'])
        if message.body.data.get("end"):
            end = int(message.body.data['end'])
        else:
            # If the end field does not appear, it means that we are in the initial synchronization
            get_blocks = self._database.get_blocks(last_block_only=True, include_events=False)
            last_block = get_blocks[0] if get_blocks else None
            end = last_block.block_number if last_block is not None else 0

        segment_size = self.MAX_BLOCKS_SYNC
        if start <= end:
            for segment_start in range(start, end + 1, segment_size):
                segment_end = min(segment_start + segment_size - 1, end)
                get_blocks = self._database.get_blocks(start=segment_start, end=segment_end)

                if get_blocks:
                    body = MessageBody(
                        code=MessageCode.MESSAGE_CODE_SYNC_RESPONSE,
                        data={
                            "blocks": [block.dict() for block in get_blocks],
                            "start": segment_start,
                            "end": segment_end,
                            "expected_end_block": end
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

    def _process_message_sync_response(self, message: Message, validator_ss58_address: str):
        if self._sync_service.is_fork_syncing() or self._sync_service.is_validator_suspicious(validator_ss58_address):
            return

        blocks = message.body.data["blocks"]

        if blocks:
            expected_end_block = message.body.data.get("expected_end_block", None)
            if expected_end_block:
                self._sync_service.start_sync()

            for i, block in enumerate(blocks):
                block = Block(**block)

                if i == 0:
                    get_blocks = self._database.get_blocks(last_block_only=True)
                    previous_block = get_blocks[0] if get_blocks else None
                else:
                    previous_block = Block(**blocks[i - 1])

                if previous_block.hash != block.previous_hash:
                    self._sync_service.add_to_remote_chain(
                        validator_ss58_address=validator_ss58_address,
                        block_map={
                            b["block_number"]: {
                                "hash": b["hash"],
                                "event_count": len(b["events"])
                            }
                            for b in blocks
                        }
                    )

                    handle_consensus_and_resolve_fork(
                        sync_service=self._sync_service,
                        database=self._database,
                        local_block=previous_block,
                        active_connections=self._connection_pool.get_all(),
                        keypair=self._keypair
                    )
                    return

                try:
                    check_block_integrity(
                        previous_block=previous_block,
                        current_block=block,
                        database=self._database
                    )
                    self._database.create_block(previous_hash=previous_block.hash, block=block)
                    self._event_pool.remove_multiple(block.events)

                except Exception as e:
                    self._sync_service.mark_as_suspicious(validator=validator_ss58_address)
                    self._sync_service.complete_sync(is_synced=False)

                    if isinstance(e, BlockIntegrityException):
                        logger.error(e, exc_info=True)

                    return

                get_blocks = self._database.get_blocks(last_block_only=True, include_events=False)
                last_block = get_blocks[0] if get_blocks else None

                if expected_end_block and expected_end_block == last_block.block_number:
                    self._sync_service.complete_sync(is_synced=True)

    def _process_message_last_block(self):
        try:
            get_blocks = self._database.get_blocks(last_block_only=True, include_events=True)
            last_block = get_blocks[0] if get_blocks else None
            total_event_count = self._database.get_total_event_count()

            response_data = {
                "last_block_number": last_block.block_number if last_block else 0,
                "last_hash": last_block.hash,
                "total_event_count": total_event_count
            }
            body = MessageBody(
                code=MessageCode.MESSAGE_CODE_LAST_BLOCK_RESPONSE,
                data=response_data
            )
            body_sign = sign_data(body.dict(), self._keypair)
            response_message = Message(
                body=body,
                signature_hex=body_sign.hex(),
                public_key_hex=self._keypair.public_key.hex()
            )
            send_message(self._connection, response_message)
        except Exception as e:
            logger.error(f"Error sending sync status response: {e}", exc_info=True)

    def _process_message_last_block_response(self, message: Message, validator_ss58_address: str):
        try:
            last_block_number = message.body.data.get("last_block_number")
            last_hash = message.body.data.get("last_hash")
            total_event_count = message.body.data.get("total_event_count")

            if last_block_number is not None and last_hash:
                self._sync_service.add_last_block_other_validators(
                    validator_ss58_address=validator_ss58_address,
                    block_number=last_block_number,
                    block_hash=last_hash,
                    total_event_count=total_event_count
                )
        except Exception as e:
            logger.error(f"Error processing sync status response: {e}", exc_info=True)

    def _process_message_fork_block_data(self, message: Message):
        start_block = message.body.data.get("start_block")
        end_block = message.body.data.get("end_block")

        if start_block is None or end_block is None:
            return

        try:
            block_data = []
            get_blocks = self._database.get_blocks(start=start_block, end=end_block)
            if get_blocks:
                for block in get_blocks:
                    block_data.append({
                        "block_number": block.block_number,
                        "hash": block.hash,
                        "event_count": len(block.events)
                    })

                body = MessageBody(
                    code=MessageCode.MESSAGE_CODE_FORK_BLOCK_DATA_RESPONSE,
                    data={"block_data": block_data}
                )
                body_sign = sign_data(body.dict(), self._keypair)
                response_message = Message(
                    body=body,
                    signature_hex=body_sign.hex(),
                    public_key_hex=self._keypair.public_key.hex()
                )
                send_message(self._connection, response_message)
        except Exception as e:
            logger.error(f"Error processing block hashes request: {e}", exc_info=True)

    def _process_message_fork_block_data_response(self, message: Message, validator_ss58_address: str):
        block_data = message.body.data.get("block_data", [])

        if not block_data:
            return

        try:
            block_map = {
                block["block_number"]: {
                    "hash": block["hash"],
                    "event_count": block["event_count"]
                }
                for block in block_data
            }

            self._sync_service.add_to_remote_chain(validator_ss58_address, block_map)
            get_blocks = self._database.get_blocks(last_block_only=True)
            local_block = get_blocks[0] if get_blocks else None
            handle_consensus_and_resolve_fork(
                sync_service=self._sync_service,
                database=self._database,
                local_block=local_block,
                active_connections=self._connection_pool.get_all(),
                keypair=self._keypair
            )

        except Exception as e:
            logger.error(f"Error processing block hashes response: {e}", exc_info=True)
            self._sync_service.complete_sync(is_synced=False)
