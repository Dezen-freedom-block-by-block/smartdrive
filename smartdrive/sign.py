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
from typing import Union

from substrateinterface import Keypair


def sign_data(data: dict, keypair: Keypair) -> bytes:
    """
    Signs the provided JSON data using the given keypair.

    This function takes a dictionary representing the JSON data, converts it to a UTF-8 encoded byte string,
    and then signs the byte string using the provided keypair.

    Params:
        data (dict): The data to be signed.
        keypair (Keypair): The keypair used to sign the JSON data.

    Returns:
        bytes: The generated signature in bytes format.
    """
    message = json.dumps(data).encode('utf-8')
    signature = keypair.sign(message)

    return signature


def verify_data_signature(data: Union[dict, bytes], signature_hex: str, ss58_address: str) -> bool:
    """
    Verifies the signature of a data using the provided SS58 address.

    This function verifies the signature of a JSON data against the provided SS58 address.
    It converts the JSON data to bytes if necessary, decodes the hexadecimal signature, and
    uses the Keypair associated with the SS58 address to verify the signature.

    Params:
        data (Union[dict, bytes]): The data to be verified. It can be either a dictionary or bytes.
        signature_hex (str): The signature in hexadecimal format.
        ss58_address (str): The SS58 address associated with the public key to verify the signature.

    Returns:
        bool: True if the signature is valid, otherwise False.
    """
    keypair = Keypair(ss58_address=ss58_address)
    message = data if isinstance(data, bytes) else json.dumps(data).encode('utf-8')
    signature = bytes.fromhex(signature_hex)
    is_valid = keypair.verify(message, signature)

    return is_valid
