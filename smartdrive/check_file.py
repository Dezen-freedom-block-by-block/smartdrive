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

from smartdrive.config import MAXIMUM_STORAGE_PER_USER_PER_FILE
from smartdrive.validator.api.exceptions import FileHashMismatchException, FileSizeMismatchException, FileTooLargeException


async def check_file(file_hash: str, file_size: int, original_file_size: int, original_file_hash: str):
    """
    Validates a file by checking its hash, size, and storage limits.

    This function verifies that one file has the same
    hash and size as the original file, ensuring data integrity. It also ensures
    that the file does not exceed the maximum allowed storage size.

    Params:
        file_hash (str): The actual SHA-256 hash of the file.
        file_size (int): The actual size of the file in bytes.
        original_file_size (int): The original size of the file in bytes.
        original_file_hash (str): The original SHA-256 hash of the file.

    Raises:
        FileHashMismatchException: If the file's hash does not match the original hash.
        FileSizeMismatchException: If the file's size does not match the original size.
        FileTooLargeException: If the file exceeds the maximum storage size defined.
    """
    if original_file_hash != file_hash:
        raise FileHashMismatchException

    if original_file_size != file_size:
        raise FileSizeMismatchException

    if file_size > MAXIMUM_STORAGE_PER_USER_PER_FILE:
        raise FileTooLargeException
