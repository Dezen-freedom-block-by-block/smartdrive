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

from communex.client import CommuneClient
from substrateinterface import Keypair

from smartdrive.models.event import parse_event, UserEvent, MessageEvent, Action
from smartdrive.validator.api.middleware.sign import verify_data_signature
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.api.utils import process_events
from smartdrive.validator.database.database import Database
from smartdrive.models.block import Block, BlockEvent, block_event_to_block
from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.util import packing
from smartdrive.validator.network.node.util.exceptions import MessageException, ClientDisconnectedException, MessageFormatException, InvalidSignatureException
from smartdrive.validator.network.node.util.message_code import MessageCode


class Client(multiprocessing.Process):

    def __init__(self, client_socket, identifier, connection_pool: ConnectionPool, mempool, keypair: Keypair, comx_client: CommuneClient, netuid: int, database: Database):
        multiprocessing.Process.__init__(self)
        self.client_socket = client_socket
        self.identifier = identifier
        self.connection_pool = connection_pool
        self.mempool = mempool
        self.keypair = keypair
        self.comx_client = comx_client
        self.netuid = netuid
        self.database = database

    def run(self):
        try:
            self.handle_client()
        except ClientDisconnectedException:
            print(f"Removing connection from connection pool: {self.identifier}")
            removed_connection = self.connection_pool.remove_connection(self.identifier)
            if removed_connection:
                removed_connection.close()

    def handle_client(self):
        try:
            while True:
                self.receive()
        except InvalidSignatureException:
            print("Received invalid sign")
        except (MessageException, MessageFormatException):
            print(f"Received undecodable or invalid message: {self.identifier}")
        except (ConnectionResetError, ConnectionAbortedError, ClientDisconnectedException):
            print(f"Client disconnected': {self.identifier}")
        finally:
            self.client_socket.close()
            raise ClientDisconnectedException(f"Lost {self.identifier}")

    def receive(self):
        # Here the process is waiting till a new message is sended.
        msg = packing.receive_msg(self.client_socket)
        print(f"Message received")
        process = multiprocessing.Process(target=self.process_message, args=(msg,))
        process.start()

    def process_message(self, msg):
        print("PROCESS MESSAGE")
        print(msg)
        body = msg["body"]

        try:
            if body['code'] in [code.value for code in MessageCode]:
                signature_hex = msg["signature_hex"]
                public_key_hex = msg["public_key_hex"]
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                is_verified_signature = verify_data_signature(body, signature_hex, ss58_address)

                if not is_verified_signature:
                    raise InvalidSignatureException()

                message = None

                if body['code'] == MessageCode.MESSAGE_CODE_BLOCK.value:
                    block_event = BlockEvent(**body["data"])
                    block = block_event_to_block(block_event)

                    processed_events = []
                    for event in block.events:
                        input_params_verified = True
                        if isinstance(event, UserEvent):
                            input_params_verified = verify_data_signature(event.input_params.dict(), event.input_signed_params, event.user_ss58_address)

                        event_params_verified = verify_data_signature(event.event_params.dict(), event.event_signed_params, event.validator_ss58_address)

                        if input_params_verified and event_params_verified:
                            processed_events.append(event)

                    block.events = processed_events
                    print(f"EVENTS PROCESSED -> {processed_events}")
                    asyncio.run_coroutine_threadsafe(
                        process_events(
                            events=processed_events,
                            is_proposer_validator=False,
                            keypair=self.keypair,
                            comx_client=self.comx_client,
                            netuid=self.netuid,
                            database=self.database
                        ), asyncio.get_event_loop()
                    )
                    self.database.create_block(block=block)

            elif body['code'] == MessageCode.MESSAGE_CODE_EVENT.value:
                print(f"LLEGA UN MESSAGE CODE EVENT - {body['data']}")
                print(f"CODE ACTION -> {Action(body['data']['event_action'])}")
                message_event = MessageEvent(event_action=Action(body["data"]["event_action"]), event=body["data"]["event"])
                message = parse_event(message_event)

                self.mempool.append(message)

        except InvalidSignatureException as e:
            raise e

        except Exception as e:
            print(e)
            raise MessageFormatException('%s' % e)
