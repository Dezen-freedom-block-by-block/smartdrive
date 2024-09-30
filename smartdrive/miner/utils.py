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
import os
import shutil

from fastapi import HTTPException


def get_directory_size(dir_path: str) -> int:
    """
    Calculate the total size of all files in a directory.

    This function walks through all subdirectories and files in the specified directory
    and sums up the sizes of all files to compute the total size.

    Params:
        dir_path (str): The path to the directory for which to calculate the total size.

    Returns:
        int: The total size of all files in the directory, in bytes.

    Raises:
        OSError: If there is an error accessing the file system, such as a permission error or
                 if the directory does not exist.
    """
    total_size = 0

    try:
        for dirpath, dirnames, filenames in os.walk(dir_path):
            for f in filenames:
                fp = os.path.join(dirpath, f)
                total_size += os.path.getsize(fp)
    except OSError as e:
        print(f"An error occurred calculating total size: {e}")

    return total_size


def parse_body(body_bytes: bytes) -> dict:
    try:
        return json.loads(body_bytes).get("params", {})
    except json.JSONDecodeError:
        raise HTTPException(status_code=400, detail="Invalid JSON in request body")


def has_enough_space(file_size: int, max_size_gb: float, dir_path: str) -> bool:
    """
    Check if there is enough space in the directory to store a new file.

    This function calculates the total size of all files currently in the specified directory
    and checks if adding a new file of the given size would exceed the maximum allowed size,
    and whether there is enough free space in the filesystem.

    Params:
        file_size (int): The size of the file to be added, in bytes.
        max_size_gb (float): The maximum allowed size for the directory, in gigabytes.
        dir_path (str): The path to the directory to check.

    Returns:
        bool: True if there is enough space to add the file, False otherwise.

    Raises:
        OSError: If there is an error accessing the file system, such as a permission error or
                 if the directory does not exist.
    """
    try:
        # Get the total, used, and free space in the filesystem containing dir_path
        total, used, free = shutil.disk_usage(dir_path)

        # Convert the maximum allowed size to bytes
        max_size_bytes = max_size_gb * 1024 * 1024 * 1024

        # Calculate the current directory size
        current_size = get_directory_size(dir_path)

        # Check if there is enough space to add the file
        return (current_size + file_size <= max_size_bytes) and (file_size <= free)

    except OSError as e:
        print(f"An error occurred calculating total size: {e}")
        return False
