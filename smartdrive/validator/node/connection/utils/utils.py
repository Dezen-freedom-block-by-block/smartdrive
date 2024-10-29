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

import json
import select
import socket
import struct
import threading
from _socket import SocketType

from substrateinterface import Keypair

from smartdrive import logger
from smartdrive.commune.models import ModuleInfo
from smartdrive.sign import sign_data
from smartdrive.validator.node.connection.connection_pool import Connection
from smartdrive.validator.node.util.exceptions import ClientDisconnectedException, MessageException
from smartdrive.validator.node.util.message import MessageBody, MessageCode, Message

CONNECTION_TIMEOUT_SECONDS = 5


def connect_to_peer(keypair: Keypair, module_info: ModuleInfo) -> SocketType:
    peer_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    peer_socket.settimeout(CONNECTION_TIMEOUT_SECONDS)
    peer_socket.connect((module_info.connection.ip, module_info.connection.port + 1))
    # When you call settimeout(), the timeout applies to all subsequent blocking operations on the socket, such as connect(), recv(), send(), and others.
    # Since we only want to set the timeout at the connection level, it should be reset.
    peer_socket.settimeout(None)

    body = MessageBody(
        code=MessageCode.MESSAGE_CODE_IDENTIFIER
    )

    body_sign = sign_data(body.dict(), keypair)

    message = Message(
        body=body,
        signature_hex=body_sign.hex(),
        public_key_hex=keypair.public_key.hex()
    )

    _send_json(peer_socket, message.dict())

    return peer_socket


def send_message(connection: Connection, message: Message):
    threading.Thread(target=_send_json, args=(connection, message.dict(),)).start()


def receive_msg(sock):
    msg_hdr = _recv_all(sock, 4)
    if len(msg_hdr) == 0:
        raise ClientDisconnectedException('Client disconnected')
    elif len(msg_hdr) < 4:
        raise MessageException('Invalid header (< 4)')

    msg_len = struct.unpack('!I', msg_hdr)[0]

    data = _recv_all(sock, msg_len)

    obj = json.loads(data.decode('utf-8'))

    return obj


def _send_json(connection: Connection, obj: dict):
    try:
        msg = json.dumps(obj).encode('utf-8')
        msg_len = len(msg)
        packed_len = struct.pack('!I', msg_len)

        with connection.get_socket() as _socket:
            _, ready_to_write, _ = select.select([], [_socket], [], 5)
            if ready_to_write:
                _socket.sendall(packed_len + msg)
            else:
                raise TimeoutError("Socket send info time out")

    except (BrokenPipeError, TimeoutError):
        pass

    except Exception:
        logger.debug("Error sending json", exc_info=True)


def _recv_all(sock, length):
    """ Helper function to receive all data for a given length. """
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ClientDisconnectedException('Client disconnected')
        data.extend(packet)
    return data
