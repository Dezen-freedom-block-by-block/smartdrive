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

import asyncio
import time
import uuid
import shutil
import argparse
from argparse import Namespace
import os

import uvicorn
from fastapi import Request, FastAPI
from fastapi import HTTPException
from starlette.responses import StreamingResponse
from communex.compat.key import classic_load_key
from communex.module.module import Module
from communex.module._rate_limiters.limiters import IpLimiterParams

import smartdrive
from smartdrive.commune.connection_pool import initialize_commune_connection_pool
from smartdrive.commune.request import get_modules
from smartdrive.miner.middleware.miner_middleware import MinerMiddleware
from smartdrive.miner.utils import has_enough_space, get_directory_size, parse_body


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
    parser.add_argument("--key-name", required=True, help="Name of key.")
    parser.add_argument("--data-path", default="~/smartdrive-data", required=False, help="Path to the data.")
    parser.add_argument("--max-size", type=float, default=100, required=False, help="Size (in GB) of path to fill.")
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
            raise Exception(f"Not enough disk space. Free space: ~{free_gb:.2f} GB, required: {self.config.max_size} GB")

        # Additional check to ensure the current used space does not exceed the max size
        current_dir_size = get_directory_size(self.config.data_path)
        max_size_bytes = self.config.max_size * 1024 * 1024 * 1024

        if current_dir_size >= max_size_bytes:
            raise Exception(f"Current directory size exceeds the maximum allowed size. Current size: ~{current_dir_size / (2 ** 30):.2f} GB, allowed: {self.config.max_size} GB")

    async def run_server(self, config) -> None:
        """
        Starts and runs an asynchronous web server using Uvicorn.

        This method configures and starts an Uvicorn server for an ASGI application
        with SSL/TLS support. The server listens on all network interfaces (0.0.0.0)
        and on the port specified in the instance configuration.
        """
        dir = os.path.dirname(os.path.abspath(__file__))

        app = FastAPI()
        app.add_middleware(MinerMiddleware, key=key, limiter=IpLimiterParams(), subnets_whitelist=[config.netuid], use_testnet=config.testnet)
        app.add_api_route("/method/store", self.store, methods=["POST"])
        app.add_api_route("/method/retrieve", self.retrieve, methods=["POST"])
        app.add_api_route("/method/remove", self.remove, methods=["DELETE"])
        app.add_api_route("/method/validation", self.validation, methods=["POST"])

        configuration = uvicorn.Config(app, workers=8, host="0.0.0.0", port=config.port, ssl_keyfile=f"{dir}/cert/key.pem", ssl_certfile=f"{dir}/cert/cert.pem", log_level="info")
        server = uvicorn.Server(configuration)
        await server.serve()

    async def store(self, request: Request) -> dict:
        """
        Store a chunk in the filesystem.

        This function stores a chunk of data in the specified folder, generating a unique identifier for the chunk.

        Params:
            request: The request.

        Returns:
            dict: A dictionary containing the unique identifier of the stored chunk.

        Raises:
            HTTPException: If there is not enough space to store the file or if another error occurs.
        """
        try:
            body = await request.form()
            chunk_data = await body["chunk"].read()

            if not has_enough_space(len(chunk_data), self.config.max_size, self.config.data_path):
                raise HTTPException(status_code=409, detail="Not enough space to store the file")

            client_dir = os.path.join(self.config.data_path, body["folder"])
            if not os.path.exists(client_dir):
                os.makedirs(client_dir)

            file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"
            chunk_path = os.path.join(client_dir, file_uuid)
            with open(chunk_path, 'wb') as chunk_file:
                chunk_file.write(chunk_data)

            return {"id": file_uuid}
        except Exception as e:
            print(f"ERROR store - {e}")
            raise HTTPException(status_code=409, detail=f"Error: {e}")

    async def remove(self, request: Request) -> dict:
        """
        Remove a chunk from the filesystem.

        This function deletes a chunk file from the specified folder.

        Params:
            request: The request.

        Returns:
            dict: A dictionary containing a success message if the chunk was deleted.

        Raises:
            HTTPException: If the chunk is not found or if another error occurs.
        """
        try:
            body_bytes = await request.body()
            body = parse_body(body_bytes)

            chunk_path = os.path.join(self.config.data_path, body["folder"], body["chunk_uuid"])
            os.remove(chunk_path)

            return {"message": "Chunk deleted successfully"}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: {e}")

    async def retrieve(self, request: Request) -> StreamingResponse:
        """
        Retrieve a chunk from the filesystem.

        This function retrieves a chunk file from the specified folder and returns its content.

        Params:
            request: The request.

        Returns:
            dict: A dictionary containing the chunk content in binary format.

        Raises:
            HTTPException: If the chunk is not found or if another error occurs.
        """
        try:
            body_bytes = await request.body()
            body = parse_body(body_bytes)

            chunk_path = os.path.join(self.config.data_path, body["folder"], body["chunk_uuid"])

            def iterfile():
                with open(chunk_path, 'rb') as chunk_file:
                    while True:
                        # Currently buffer 16KB
                        data = chunk_file.read(16384)
                        if not data:
                            break
                        yield data

            return StreamingResponse(iterfile(), media_type='application/octet-stream')
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            print(e)
            raise HTTPException(status_code=409, detail=f"Error: {e}")

    async def validation(self, request: Request) -> dict:
        """
        Validate a specific portion of a chunk from the filesystem.

        This function retrieves a specific portion of a chunk file from the specified folder, starting
        at a given byte offset and reading a specified number of bytes.

        Params:
            request: The request.

        Returns:
            dict: A dictionary containing the requested portion of the chunk content in hexadecimal format.

        Raises:
            HTTPException: If the chunk is not found or if another error occurs.
        """

        try:
            body_bytes = await request.body()
            body = parse_body(body_bytes)
            start = int(body["start"])
            end = int(body["end"])
            chunk_path = os.path.join(self.config.data_path, body["folder"], body["chunk_uuid"])
            with open(chunk_path, 'rb') as chunk_file:
                chunk_file.seek(start)
                chunk = chunk_file.read(end - start)

            return {"chunk": chunk.hex()}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: {e}")


if __name__ == "__main__":
    smartdrive.check_version()

    config = get_config()
    miner = Miner(config)

    initialize_commune_connection_pool(config.testnet, num_connections=1, max_pool_size=1)

    key = classic_load_key(config.key)
    registered_modules = get_modules(config.netuid)

    if key.ss58_address not in list(map(lambda module: module.ss58_address, registered_modules)):
        raise Exception(f"Your key: {key.ss58_address} is not registered.")

    async def run_tasks():
        await miner.run_server(config)

    asyncio.run(run_tasks())
