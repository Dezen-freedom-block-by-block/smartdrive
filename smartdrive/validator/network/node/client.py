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

#  MIT License
#
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#
#  MIT License
#
#
#  Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
#
import multiprocessing

from smartdrive.validator.api.middleware.sign import verify_json_signature
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.network.node.connection_pool import ConnectionPool
from smartdrive.validator.network.node.util import packing
from smartdrive.validator.network.node.util.exceptions import MessageException, ClientDisconnectedException, MessageFormatException, InvalidSignatureException
from smartdrive.validator.network.node.util.message import MESSAGE_CODE_TYPES


class Client(multiprocessing.Process):

    def __init__(self, client_socket, identifier, connection_pool: ConnectionPool, notification_queue: multiprocessing.Queue):
        multiprocessing.Process.__init__(self)
        self.client_socket = client_socket
        self.identifier = identifier
        self.connection_pool = connection_pool
        self.notification_queue = notification_queue

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
        body = msg["body"]

        if body['code'] in MESSAGE_CODE_TYPES.keys():
            try:
                signature_hex = msg["signature_hex"]
                public_key_hex = msg["public_key_hex"]
                ss58_address = get_ss58_address_from_public_key(public_key_hex)

                is_verified_signature = verify_json_signature(body, signature_hex, ss58_address)

                if not is_verified_signature:
                    raise InvalidSignatureException()

                # TODO: Process message - if it is a block, author needs to be checked
                print(f"Message processed: {body}")
                self.notification_queue.put(f"Message processed by {self.identifier}: {body}")

            except InvalidSignatureException as e:
                raise e

            except Exception as e:
                print(e)
                raise MessageFormatException('%s' % e)
