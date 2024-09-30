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
import io
import zstandard as zstd
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes


def encrypt_with_aes(data, aes_key):
    iv = get_random_bytes(16)
    cipher = AES.new(aes_key, AES.MODE_CFB, iv)
    encrypted_data = iv + cipher.encrypt(data)
    return encrypted_data


def compress_file_with_zstd(file_path):
    cctx = zstd.ZstdCompressor(level=6, threads=-1)
    compressed_io = io.BytesIO()

    filename = os.path.basename(file_path)
    filename_bytes = filename.encode('utf-8')
    filename_length = len(filename_bytes)
    compressed_io.write(filename_length.to_bytes(4, 'big'))
    compressed_io.write(filename_bytes)

    with open(file_path, 'rb') as f_in:
        cctx.copy_stream(f_in, compressed_io)

    compressed_io.seek(0)
    return compressed_io.read()


def compress_and_encrypt(file_path, private_key_bytes):
    aes_key = private_key_bytes[:32]
    compressed_data = compress_file_with_zstd(file_path)
    encrypted_data = encrypt_with_aes(compressed_data, aes_key)
    return encrypted_data


def decrypt_with_aes(encrypted_data, aes_key):
    iv = encrypted_data[:16]
    cipher = AES.new(aes_key, AES.MODE_CFB, iv)
    decrypted_data = cipher.decrypt(encrypted_data[16:])
    return decrypted_data


def decompress_data_with_zstd(decompressed_data, output_dir):
    dctx = zstd.ZstdDecompressor()
    data_io = io.BytesIO(decompressed_data)
    filename_length = int.from_bytes(data_io.read(4), 'big')
    filename = data_io.read(filename_length).decode('utf-8')

    output_file_path = os.path.join(output_dir, filename)

    with open(output_file_path, 'wb') as f_out:
        dctx.copy_stream(data_io, f_out)

    return output_file_path


def decrypt_and_decompress(encrypted_data, aes_key, output_path):
    decrypted_data = decrypt_with_aes(encrypted_data, aes_key)
    filename = decompress_data_with_zstd(decrypted_data, output_path)
    return filename
