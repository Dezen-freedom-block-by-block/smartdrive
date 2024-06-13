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
import struct

from smartdrive.validator.network.node.util.exceptions import MessageException, ClientDisconnectedException


def recv_all(sock, length):
    """ Helper function to receive all data for a given length. """
    data = bytearray()
    while len(data) < length:
        packet = sock.recv(length - len(data))
        if not packet:
            raise ClientDisconnectedException('Client disconnected')
        data.extend(packet)
    return data


def receive_msg(sock):
    msg_hdr = recv_all(sock, 4)
    if len(msg_hdr) == 0:
        raise ClientDisconnectedException('Client disconnected')
    elif len(msg_hdr) < 4:
        raise MessageException('Invalid header (< 4)')

    msg_len = struct.unpack('!I', msg_hdr)[0]

    data = recv_all(sock, msg_len)

    # Decode JSON object
    obj = json.loads(data.decode('utf-8'))

    return obj
