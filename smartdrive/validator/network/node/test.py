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

import json
import socket
import struct
import time
from communex.compat.key import classic_load_key
from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.network.node.util.message_code import MessageCode


def send_json(sock, obj):
    # Convert the object to JSON and encode it to bytes
    msg = json.dumps(obj).encode('utf-8')
    # Get the length of the message
    msg_len = len(msg)
    # Pack the length of the message as a 4-byte integer in network byte order (big-endian)
    packed_len = struct.pack('!I', msg_len)
    # Send the length of the message followed by the message
    print(packed_len)
    sock.sendall(packed_len + msg)

def receive_json(sock):
    msg_len = struct.unpack('!I', sock.recv(4))[0]
    data = sock.recv(msg_len)
    return data.decode()

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    client_socket.connect(('127.0.0.1', 8803))
    print("Connection established with the server")

    keypair = classic_load_key("validator")

    # Identification message
    body = {
        "code": MessageCode.MESSAGE_CODE_IDENTIFIER,
        "data": {"ss58_address": keypair.ss58_address}
    }
    body_sign = sign_data(body, keypair)
    message = {
        "body": body,
        "signature_hex": body_sign.hex(),
        "public_key_hex": keypair.public_key.hex()
    }
    send_json(client_socket, message)

    for i in range(255):
        # Normal message
        body = {
            "code": MessageCode.MESSAGE_CODE_BLOCK,
            "data": [
                {"id": i, "uuid": "9789283RO2NCOV2HOFIDJ"},
            ]
        }
        body_sign = sign_data(body, keypair)
        message = {
            "body": body,
            "signature_hex": body_sign.hex(),
            "public_key_hex": keypair.public_key.hex()
        }
        send_json(client_socket, message)
        print(f"Message {i+1} sent: {message}")

        time.sleep(3)

    time.sleep(50)

except Exception as e:
    print(f"Connection error: {e}")

finally:
    print("Connection closed")

