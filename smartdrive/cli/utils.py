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
import uuid

import zstandard as zstd
from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes

from smartdrive.commune.utils import calculate_hash_sync
from smartdrive.config import READ_FILE_SIZE


def compress_encrypt_and_split_file(file_path: str, aes_key: bytes, chunk_size: int, output_dir: str):
    iv = get_random_bytes(16)
    cipher = AES.new(aes_key, AES.MODE_CFB, iv)

    chunk_metadata = []
    total_size = 0
    cctx = zstd.ZstdCompressor(level=6, threads=-1)

    filename = os.path.basename(file_path)
    filename_bytes = filename.encode('utf-8')
    filename_length = len(filename_bytes)

    with open(file_path, 'rb') as f_in:
        compressed_stream = cctx.stream_reader(f_in)

        while True:
            chunk_data = compressed_stream.read(chunk_size)
            if not chunk_data:
                break

            chunk_name = f"{uuid.uuid4()}.chunk_{len(chunk_metadata)}"
            chunk_path = os.path.join(output_dir, chunk_name)

            with open(chunk_path, 'wb') as chunk_file:
                if len(chunk_metadata) == 0:
                    header = filename_length.to_bytes(4, 'big') + filename_bytes + iv
                    chunk_file.write(header)
                    total_size += len(header)

                encrypted_data = cipher.encrypt(chunk_data)
                chunk_file.write(encrypted_data)
                total_size += len(encrypted_data)

            chunk_metadata.append({
                "chunk_index": len(chunk_metadata),
                "chunk_path": chunk_path,
                "chunk_size": os.path.getsize(chunk_path),
                "chunk_hash": calculate_hash_sync(chunk_path),
            })

    return chunk_metadata, calculate_hash_sync(file_path), total_size


def decompress_decrypt_and_unify_file(aes_key: bytes, chunk_paths: list, file_path: str):
    dctx = zstd.ZstdDecompressor()

    with open(chunk_paths[0], 'rb') as first_chunk_file:
        filename_length = int.from_bytes(first_chunk_file.read(4), 'big')
        filename_bytes = first_chunk_file.read(filename_length)
        iv = first_chunk_file.read(16)
        original_file_name = filename_bytes.decode('utf-8')
        reconstructed_file_path = os.path.join(file_path, original_file_name)

        cipher = AES.new(aes_key, AES.MODE_CFB, iv)

    with open(reconstructed_file_path, 'wb') as final_file:
        decompressed_stream = dctx.stream_writer(final_file)

        for chunk_index, chunk_path in enumerate(chunk_paths):
            with open(chunk_path, 'rb') as chunk_file:
                if chunk_index == 0:
                    chunk_file.seek(4 + filename_length + 16)

                while True:
                    encrypted_data = chunk_file.read(READ_FILE_SIZE)
                    if not encrypted_data:
                        break

                    decrypted_data = cipher.decrypt(encrypted_data)
                    decompressed_stream.write(decrypted_data)

            os.remove(chunk_path)

    return reconstructed_file_path


def determine_semaphore_limit(file_size: int) -> int:
    """
    Determine the semaphore limit based on the total file size.

    Args:
        file_size (int): Total size of the file in bytes.

    Returns:
        int: Recommended semaphore limit.
    """
    thresholds = [
        (100 * 1024 * 1024, 5),  # Files < 100 MB -> 5 concurrent operations
        (500 * 1024 * 1024, 10),   # Files < 500 MB -> 10 concurrent operations
        (3 * 1024 * 1024 * 1024, 5),  # Files < 3 GB -> 5 concurrent operations
    ]

    default_semaphore_limit = 4  # Default semaphore limit for files > 3 GB

    for max_file_size, semaphore_limit in thresholds:
        if file_size <= max_file_size:
            return semaphore_limit

    return default_semaphore_limit


def determine_chunk_size_and_semaphore_limit(file_size: int) -> tuple:
    """
    Determine the chunk size and semaphore limit based on the total file size.

    Args:
        file_size (int): Total size of the file in bytes.

    Returns:
        tuple:
            - int: Recommended chunk size in bytes.
            - int: Recommended semaphore limit.
    """
    thresholds = [
        (100 * 1024 * 1024, 20 * 1024 * 1024),  # Files < 100 MB -> 20 MB chunks
        (500 * 1024 * 1024, 50 * 1024 * 1024),  # Files < 500 MB -> 50 MB chunks
        (3 * 1024 * 1024 * 1024, 250 * 1024 * 1024),  # Files < 3 GB -> 250 MB chunks
    ]

    default_chunk_size = 500 * 1024 * 1024  # Default chunk size for files > 3 GB

    for max_file_size, chunk_size in thresholds:
        if file_size <= max_file_size:
            semaphore_limit = determine_semaphore_limit(file_size)
            return chunk_size, semaphore_limit

    semaphore_limit = determine_semaphore_limit(file_size)
    return default_chunk_size, semaphore_limit
