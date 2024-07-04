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

import time
import uuid
import shutil

import argparse
from argparse import Namespace

import os
import uvicorn
from fastapi import HTTPException

from communex.module.server import ModuleServer
from communex.compat.key import classic_load_key
from communex.module.module import Module, endpoint
from communex.module._rate_limiters.limiters import IpLimiterParams

import smartdrive
from smartdrive.commune.request import get_modules
from smartdrive.miner.utils import has_enough_space, get_directory_size


def get_config() -> Namespace:
    """
    Configure the miner's settings from command line arguments.

    This function creates an argument parser to capture configuration parameters for the miner
    from the command line, sets default values, and returns the resulting configuration object.

    Returns:
        argparse.Namespace: An object containing the configuration parameters for the miner,
                            including data path, key name, miner name, maximum size, API port,
                            and network UID.

    Raises:
        SystemExit: If the required arguments are not provided or if there are other errors
                    during argument parsing.
    """
    # Create parser and add all params.
    parser = argparse.ArgumentParser(description="Configure the miner.")
    parser.add_argument("--key", required=True, help="Name of key.")
    parser.add_argument("--data_path", default="~/smartdrive-data", required=False, help="Path to the data.")
    parser.add_argument("--max_size", type=float, default=100, required=False, help="Size (in GB) of path to fill.")
    parser.add_argument("--port", type=int, default=8000, required=False, help="Default api port.")
    parser.add_argument("--testnet", action='store_true', help="Use testnet or not.")

    config = parser.parse_args()

    if config.data_path:
        os.makedirs(config.data_path, exist_ok=True)

    config.data_path = os.path.expanduser(config.data_path)
    config.netuid = smartdrive.TESTNET_NETUID if config.testnet else smartdrive.NETUID

    return config


class Miner(Module):
    def __init__(self, config):
        """
        Initialize the miner network with the given configuration.

        This constructor initializes the miner network, ensuring that the data path exists and that there is enough disk space available.

        Params:
            config: The configuration object containing settings such as data path and maximum storage size.

        Raises:
            Exception: If there is not enough disk space available.
        """
        super().__init__()

        self.config = config

        if not os.path.exists(self.config.data_path):
            os.makedirs(self.config.data_path)

        total, used, free = shutil.disk_usage(self.config.data_path)
        free_gb = free / (2 ** 30)

        if free_gb < self.config.max_size:
            raise Exception(f"Not enough disk space. Free space: {free_gb:.2f} GB, required: {self.config.max_size} GB")

        # Additional check to ensure the current used space does not exceed the max size
        current_dir_size = get_directory_size(self.config.data_path)
        max_size_bytes = self.config.max_size * 1024 * 1024 * 1024

        if current_dir_size > max_size_bytes:
            raise Exception(f"Current directory size exceeds the maximum allowed size. Current size: {current_dir_size / (2 ** 30):.2f} GB, allowed: {self.config.max_size} GB")

    @endpoint
    def ping(self) -> dict:
        """
        Ping the miner to check its status.

        This function returns the status of the miner, including its type, version, and maximum storage size.

        Returns:
            dict: A dictionary containing the type of the network, the version of the software, and the maximum storage size.

        Raises:
            None
        """
        smartdrive.check_version()

        return {"type": "miner", "version": smartdrive.__version__, "max_size": self.config.max_size}

    @endpoint
    def store(self, folder: str, chunk: bytes) -> dict:
        """
        Store a chunk in the filesystem.

        This function stores a chunk of data in the specified folder, generating a unique identifier for the chunk.

        Params:
            folder: The name of the folder where the chunk will be stored.
            chunk: The binary content of the chunk to be stored.

        Returns:
            dict: A dictionary containing the unique identifier of the stored chunk.

        Raises:
            HTTPException: If there is not enough space to store the file or if another error occurs.
        """
        try:
            if not has_enough_space(len(chunk), config.max_size, self.config.data_path):
                raise HTTPException(status_code=409, detail="Not enough space to store the file")

            client_dir = os.path.join(self.config.data_path, folder)
            if not os.path.exists(client_dir):
                os.makedirs(client_dir)

            file_uuid: str = f"{int(time.time())}_{str(uuid.uuid4())}"
            chunk_path = os.path.join(client_dir, file_uuid)
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(chunk)

            return {"id": file_uuid}
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: f{str(e)}")

    @endpoint
    def remove(self, folder: str, chunk_uuid: str) -> dict:
        """
        Remove a chunk from the filesystem.

        This function deletes a chunk file from the specified folder.

        Params:
            folder: The name of the folder where the chunk is stored.
            chunk_uuid: The UUID of the chunk to be removed.

        Returns:
            dict: A dictionary containing a success message if the chunk was deleted.

        Raises:
            HTTPException: If the chunk is not found or if another error occurs.
        """
        try:
            chunk_path = os.path.join(self.config.data_path, folder, chunk_uuid)
            os.remove(chunk_path)

            return {"message": "Chunk deleted successfully"}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: f{str(e)}")

    @endpoint
    def retrieve(self, folder: str, chunk_uuid: str) -> dict:
        """
        Retrieve a chunk from the filesystem.

        This function retrieves a chunk file from the specified folder and returns its content.

        Params:
            folder: The name of the folder where the chunk is stored.
            chunk_uuid: The UUID of the chunk to be retrieved.

        Returns:
            dict: A dictionary containing the chunk content in binary format.

        Raises:
            HTTPException: If the chunk is not found or if another error occurs.
        """
        try:
            chunk_path = os.path.join(self.config.data_path, folder, chunk_uuid)
            with open(chunk_path, 'rb') as chunk_file:
                chunk = chunk_file.read()

            return {"chunk": chunk}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: f{str(e)}")

    @endpoint
    def validation(self, folder: str, chunk_uuid: str, start: int, end: int) -> dict:
        """
        Validate a specific portion of a chunk from the filesystem.

        This function retrieves a specific portion of a chunk file from the specified folder, starting
        at a given byte offset and reading a specified number of bytes.

        Params:
            folder: The name of the folder where the chunk is stored.
            chunk_uuid: The UUID of the chunk to be validated.
            start: The byte offset from where to start reading the chunk.
            end: The byte offset from where to end reading the chunk.

        Returns:
            dict: A dictionary containing the requested portion of the chunk content in binary format.

        Raises:
            HTTPException: If the chunk is not found or if another error occurs.
        """

        try:
            chunk_path = os.path.join(self.config.data_path, folder, chunk_uuid)
            with open(chunk_path, 'rb') as chunk_file:
                chunk_file.seek(start)
                sub_chunk = chunk_file.read(end - start)

            return {"sub_chunk": sub_chunk}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: f{str(e)}")


if __name__ == "__main__":
    smartdrive.check_version()

    config = get_config()
    miner = Miner(config)
    key = classic_load_key(config.key)
    registered_modules = get_modules(config.netuid, config.testnet)

    if key.ss58_address not in list(map(lambda module: module.ss58_address, registered_modules)):
        raise Exception(f"Your key: {key.ss58_address} is not registered.")

    dir = os.path.dirname(os.path.abspath(__file__))
    server = ModuleServer(miner, key, subnets_whitelist=[config.netuid], limiter=IpLimiterParams(), use_testnet=config.testnet)
    uvicorn.run(server.get_fastapi_app(), host="0.0.0.0", port=config.port, ssl_keyfile=f"{dir}/cert/key.pem", ssl_certfile=f"{dir}/cert/cert.pem", log_level="info")
