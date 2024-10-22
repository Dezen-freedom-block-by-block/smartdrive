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

import os

import zstandard as zstd
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from smartdrive.utils import DEFAULT_CLIENT_PATH


def encrypt_with_aes(data_stream, aes_key, output_stream):
    iv = get_random_bytes(16)
    cipher = AES.new(aes_key, AES.MODE_CFB, iv)
    output_stream.write(iv)

    while True:
        chunk = data_stream.read(16384)
        if not chunk:
            break
        encrypted_chunk = cipher.encrypt(chunk)
        output_stream.write(encrypted_chunk)


def compress_encrypt_and_save(file_path, aes_key):
    cctx = zstd.ZstdCompressor(level=6, threads=-1)

    filename = os.path.basename(file_path)
    filename_bytes = filename.encode('utf-8')
    filename_length = len(filename_bytes)

    output_path = os.path.expanduser(DEFAULT_CLIENT_PATH)
    os.makedirs(output_path, exist_ok=True)
    encrypted_file_path = os.path.join(output_path, f"{filename}.enc")
    with open(file_path, 'rb') as f_in, open(encrypted_file_path, 'wb') as f_out:
        f_out.write(filename_length.to_bytes(4, 'big'))
        f_out.write(filename_bytes)

        with cctx.stream_reader(f_in) as compressed_stream:
            encrypt_with_aes(compressed_stream, aes_key, f_out)

    return encrypted_file_path


def decrypt_with_aes(input_stream, aes_key, output_stream):
    iv = input_stream.read(16)
    cipher = AES.new(aes_key, AES.MODE_CFB, iv)

    while True:
        chunk = input_stream.read(16384)
        if not chunk:
            break
        decrypted_chunk = cipher.decrypt(chunk)
        output_stream.write(decrypted_chunk)


def decompress_decrypt_and_save(input_stream, aes_key, output_dir):
    filename_length_bytes = input_stream.read(4)
    filename_length = int.from_bytes(filename_length_bytes, 'big')
    filename = input_stream.read(filename_length).decode('utf-8')

    output_file_path = os.path.join(output_dir, filename)
    if not os.path.exists(output_dir):
        os.makedirs(output_dir, exist_ok=True)

    with open(output_file_path, 'wb') as f_out:
        dctx = zstd.ZstdDecompressor()
        with dctx.stream_writer(f_out) as decompressor:
            decrypt_with_aes(input_stream, aes_key, decompressor)

    return output_file_path
