import asyncio
import multiprocessing
import random
from typing import List

from communex.compat.key import classic_load_key

import smartdrive
from smartdrive.models.event import parse_event, MessageEvent, Action, Event
from smartdrive.validator.api.middleware.sign import verify_data_signature, sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.block import BlockEvent, block_event_to_block, Block
from smartdrive.validator.node.connection_pool import ConnectionPool
from smartdrive.validator.node.util import packing
from smartdrive.validator.node.util.authority import are_all_block_events_valid, remove_invalid_block_events
from smartdrive.validator.node.util.exceptions import MessageException, ClientDisconnectedException, MessageFormatException, InvalidSignatureException
from smartdrive.validator.node.util.message_code import MessageCode
from smartdrive.validator.node.util.utils import send_json, prepare_body_tcp
from smartdrive.validator.utils import process_events


class Client(multiprocessing.Process):
    _client_socket = None
    _identifier: str = None
    _connection_pool = None
    _event_pool = None
    _keypair = None
    _database = None
    _active_validators_manager = None
    _initial_sync_completed = None
    _synced_blocks = None

    def __init__(self, client_socket, identifier, connection_pool: ConnectionPool, event_pool, event_pool_lock, active_validators_manager, initial_sync_completed):
        multiprocessing.Process.__init__(self)
        self._client_socket = client_socket
        self._identifier = identifier
        self._connection_pool = connection_pool
        self._event_pool = event_pool
        self._event_pool_lock = event_pool_lock
        self._active_validators_manager = active_validators_manager
        self._initial_sync_completed = initial_sync_completed
        self._keypair = classic_load_key(config_manager.config.key)
        self._database = Database()
        self._synced_blocks = []

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
            print(f"Client disconnected: {self._identifier}")
        finally:
            self._client_socket.close()
            raise ClientDisconnectedException(f"Lost {self._identifier}")

    def _receive(self):
        # Here the process is waiting till a new message is sent.
        msg = packing.receive_msg(self._client_socket)
        # Although _event_pool is managed by multiprocessing.Manager(),
        # we explicitly pass it as parameters to make it clear that it is dependency of the process_message process.
        process = multiprocessing.Process(target=self._process_message, args=(msg, self._event_pool, self._active_validators_manager,))
        process.start()
        process.join()

    def _process_message(self, msg, event_pool, active_validators_manager):
        body = msg["body"]

        try:
            if not self._initial_sync_completed.value and body['code'] not in [
                MessageCode.MESSAGE_CODE_PONG.value,
                MessageCode.MESSAGE_CODE_BLOCK_NUMBER_RESPONSE.value,
                MessageCode.MESSAGE_CODE_SYNC_BLOCK_RESPONSE.value
            ]:
                return

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
                            data={"block_number": block.block_number, "events": [event.dict() for event in block.events]},
                            signature_hex=block.proposer_signature,
                            ss58_address=block.proposer_ss58_address
                    ):
                        print(f"Block {block.block_number} not verified")
                        return

                    remove_invalid_block_events(block)

                    # TODO: Check when a validator creates a block (it is not a proposer) and just enters to validate
                    #  the proposer and creates another block, in this case, the blocks will be repeated
                    local_block_number = self._database.get_last_block() or 0
                    if block.block_number - 1 != local_block_number:
                        self._prepare_sync_blocks(local_block_number + 1, block.block_number, active_validators_manager)
                    else:
                        self._run_process_events(block.events)
                        self._remove_events(block.events, event_pool)
                        self._database.create_block(block)

                elif body['code'] == MessageCode.MESSAGE_CODE_EVENT.value:
                    message_event = MessageEvent.from_json(body["data"]["event"], Action(body["data"]["event_action"]))
                    event = parse_event(message_event)
                    event_pool.append(event)

                elif body['code'] == MessageCode.MESSAGE_CODE_PING.value:
                    body = {
                        "code": MessageCode.MESSAGE_CODE_PONG.value,
                        "type": "validator",
                        "version": smartdrive.__version__
                    }
                    message = prepare_body_tcp(body, self._keypair)
                    send_json(self._client_socket, message)

                elif body['code'] == MessageCode.MESSAGE_CODE_PONG.value:
                    if body["type"] == "validator":
                        module_info = self._connection_pool.get_connection(self._identifier)
                        validator = self._connection_pool.get_validator(self._identifier)
                        connection = None

                        if module_info:
                            connection = module_info.get(ConnectionPool.CONNECTION)

                        if validator and connection:
                            active_validators_manager.update_validator(validator, connection)

                elif body['code'] == MessageCode.MESSAGE_CODE_BLOCK_NUMBER.value:
                    body = {"code": MessageCode.MESSAGE_CODE_BLOCK_NUMBER_RESPONSE.value, "block": self._database.get_last_block()}
                    message = prepare_body_tcp(body, self._keypair)
                    send_json(self._client_socket, message)

                elif body['code'] == MessageCode.MESSAGE_CODE_BLOCK_NUMBER_RESPONSE.value:
                    if body["block"]:
                        validator = self._connection_pool.get_validator(self._identifier)
                        if validator:
                            validator_dict = {
                                "uid": validator.uid,
                                "ss58_address": validator.ss58_address,
                                "connection": {
                                    "ip": validator.connection.ip,
                                    "port": validator.connection.port
                                },
                                "database_block": int(body["block"] or 0)
                            }
                            active_validators_manager.update_validator_block_number(validator_dict)

                elif body['code'] == MessageCode.MESSAGE_CODE_SYNC_BLOCK.value:
                    if int(body['start']) < int(body['end']):
                        blocks = self._database.get_blocks(body['start'], body['end'])

                        if blocks:
                            body = {
                                "code": MessageCode.MESSAGE_CODE_SYNC_BLOCK_RESPONSE.value,
                                "blocks": [block.dict() for block in blocks],
                                "start": body['start'],
                                "end": body['end']
                            }
                            message = prepare_body_tcp(body, self._keypair)
                            send_json(self._client_socket, message)

                elif body['code'] == MessageCode.MESSAGE_CODE_SYNC_BLOCK_RESPONSE.value:
                    if body["blocks"]:
                        fetched_block_numbers = list(map(lambda block: block["block_number"], body["blocks"]))
                        fetched_min_block_number = min(fetched_block_numbers)
                        fetched_max_block_number = max(fetched_block_numbers)

                        if not range(int(body["start"]), int(body["end"])) == range(fetched_min_block_number, fetched_max_block_number):
                            return

                        blocks = []
                        for block in body["blocks"]:
                            block = Block(**block)

                            if not are_all_block_events_valid(block):
                                print(f"INVALID BLOCKS {block}")
                                self._synced_blocks = []
                                return

                            blocks.append(block)

                        for block in blocks:
                            self._run_process_events(block.events)
                            self._remove_events(block.events, event_pool)
                            self._database.create_block(block)

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

    def _prepare_sync_blocks(self, start, end, active_validators_manager):
        async def prepare_sync_blocks():
            connections = active_validators_manager.get_active_validators_connections()
            if not connections:
                return
            await self.get_synced_blocks(start, end, connections)

        try:
            loop = asyncio.get_running_loop()
        except RuntimeError:
            loop = None

        if loop and loop.is_running():
            asyncio.create_task(prepare_sync_blocks())
        else:
            asyncio.run(prepare_sync_blocks())

    async def get_synced_blocks(self, start: int, end: int, connections):
        async def _get_synced_blocks(c):
            try:
                body = {"code": MessageCode.MESSAGE_CODE_SYNC_BLOCK.value, "start": str(start), "end": str(end)}
                body_sign = sign_data(body, self._keypair)
                message = {
                    "body": body,
                    "signature_hex": body_sign.hex(),
                    "public_key_hex": self._keypair.public_key.hex()
                }
                send_json(c, message)
            except Exception as e:
                print(f"Error getting synced blocks: {e}")

        connection = random.choice(connections)
        await _get_synced_blocks(connection)

    def _remove_events(self, events: List[Event], event_pool):
        uuids_to_remove = {event.uuid for event in events}
        with self._event_pool_lock:
            updated_event_pool = [event for event in event_pool if event.uuid not in uuids_to_remove]
            event_pool[:] = updated_event_pool
