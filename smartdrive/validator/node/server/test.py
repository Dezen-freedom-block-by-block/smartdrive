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
import json
import socket
import struct
import time
from communex.compat.key import classic_load_key
from smartdrive.validator.api.middleware.sign import sign_json
from smartdrive.validator.node.server.util.message_code import MESSAGE_CODE_IDENTIFIER, MESSAGE_CODE_BLOCK


def send_json(sock, obj):
    # Convertir el objeto a JSON y codificarlo a bytes
    msg = json.dumps(obj).encode('utf-8')
    # Obtener la longitud del mensaje
    msg_len = len(msg)
    # Empaquetar la longitud del mensaje como un entero de 4 bytes en formato de red (big-endian)
    packed_len = struct.pack('!I', msg_len)
    # Enviar la longitud del mensaje seguida del mensaje
    print(packed_len)
    sock.sendall(packed_len + msg)

def receive_json(sock):
    msg_len = struct.unpack('!I', sock.recv(4))[0]
    data = sock.recv(msg_len)
    return data.decode()

client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
try:
    client_socket.connect(('127.0.0.1', 8803))
    print("Conexión establecida con el servidor")

    keypair = classic_load_key("validator")

    # Identification message
    body = {
        "code": MESSAGE_CODE_IDENTIFIER,
        "data": {"ss58_address": keypair.ss58_address}
    }
    body_sign = sign_json(body, keypair)
    message = {
        "body": body,
        "signature_hex": body_sign.hex(),
        "public_key_hex": keypair.public_key.hex()
    }
    send_json(client_socket, message)

    # Normal message
    body = {
        "code": MESSAGE_CODE_BLOCK,
        "data": [
            {"id": 1, "uuid": "9789283RO2NCOV2HOFIDJ"},
            {"id": 2, "uuid": "9789283RO2NCOV2HOFIDJ"}
        ]
    }
    body_sign = sign_json(body, keypair)
    message = {
        "body": body,
        "signature_hex": body_sign.hex(),
        "public_key_hex": keypair.public_key.hex()
    }

    for i in range(255):
        send_json(client_socket, message)
        print(f"Mensaje {i+1} enviado: {message}")

        time.sleep(20)

    time.sleep(50)

except Exception as e:
    print(f"Error de conexión: {e}")

finally:
    print("Conexión cerrada")
