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

import asyncio
import hashlib
import time
import uuid
import shutil
import argparse
import os

import aiofiles
import aiohttp
import uvicorn
from fastapi import Request, FastAPI
from fastapi import HTTPException
from starlette.responses import StreamingResponse
from communex.compat.key import classic_load_key
from communex.module.module import Module
from communex.module._rate_limiters.limiters import IpLimiterParams

import smartdrive
from smartdrive.check_file import check_file
from smartdrive.commune.module._protocol import create_headers
from smartdrive.commune.utils import get_ss58_address_from_public_key
from smartdrive.logging_config import logger
from smartdrive.commune.request import get_modules, get_filtered_modules
from smartdrive.miner.config import config_manager, Config
from smartdrive.miner.middleware.miner_middleware import MinerMiddleware
from smartdrive.miner.utils import has_enough_space, get_directory_size, parse_body
from smartdrive.sign import sign_data
from smartdrive.utils import _get_validator_url_async
from smartdrive.config import READ_FILE_SIZE, DEFAULT_MINER_PATH
from smartdrive.validator.models.models import ModuleType


def get_config() -> Config:
    """
    Configure the miner's settings from command line arguments.

    This function creates an argument parser to capture configuration parameters for the miner
    from the command line, sets default values, and returns the resulting configuration object.

    Returns:
        Config: An object containing the configuration parameters for the miner

    Raises:
        SystemExit: If the required arguments are not provided or if there are other errors
                    during argument parsing.
    """
    # Create parser and add all params.
    parser = argparse.ArgumentParser(description="Configure the miner.")
    parser.add_argument("--key-name", required=True, help="Name of key.")
    parser.add_argument("--data-path", default=DEFAULT_MINER_PATH, required=False, help="Path to the data.")
    parser.add_argument("--max-size", type=float, default=100, required=False, help="Size (in GB) of path to fill.")
    parser.add_argument("--port", type=int, default=8000, required=False, help="Default api port.")
    parser.add_argument("--testnet", action='store_true', help="Use testnet or not.")

    args = parser.parse_args()
    args.data_path = os.path.expanduser(args.data_path)

    if args.data_path:
        os.makedirs(args.data_path, exist_ok=True)

    args.netuid = smartdrive.TESTNET_NETUID if args.testnet else smartdrive.NETUID

    _config = Config(
        key=args.key_name,
        data_path=args.data_path,
        max_size=args.max_size,
        port=args.port,
        testnet=args.testnet,
        netuid=args.netuid
    )

    return _config


class Miner(Module):
    def __init__(self):
        """
        Initialize the miner network with the given configuration.

        This constructor initializes the miner network, ensuring that the data path exists and that there is enough disk space available.

        Raises:
            Exception: If there is not enough disk space available.
        """
        super().__init__()

        if not os.path.exists(config_manager.config.data_path):
            os.makedirs(config_manager.config.data_path)

        total, used, free = shutil.disk_usage(config_manager.config.data_path)
        free_gb = free / (2 ** 30)

        if free_gb < config_manager.config.max_size:
            raise Exception(f"Not enough disk space. Free space: ~{free_gb:.2f} GB, required: {config_manager.config.max_size} GB")

        # Additional check to ensure the current used space does not exceed the max size
        current_dir_size = get_directory_size(config_manager.config.data_path)
        max_size_bytes = config_manager.config.max_size * 1024 * 1024 * 1024

        if current_dir_size >= max_size_bytes:
            raise Exception(f"Current directory size exceeds the maximum allowed size. Current size: ~{current_dir_size / (2 ** 30):.2f} GB, allowed: {config_manager.config.max_size} GB")

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
        chunk_path = ""
        try:
            folder = request.headers.get("Folder")
            chunk_size = int(request.headers.get("X-Chunk-Size"))
            chunk_hash = request.headers.get("X-Chunk-Hash")
            event_uuid = request.headers.get("X-Event-UUID", None)
            user_public_key = request.headers.get("X-Key")
            user_ss58_address = get_ss58_address_from_public_key(user_public_key)

            if not has_enough_space(chunk_size, config_manager.config.max_size, config_manager.config.data_path):
                raise HTTPException(status_code=409, detail="Not enough space to store the file")

            validators = await get_filtered_modules(
                config_manager.config.netuid,
                ModuleType.VALIDATOR,
                config_manager.config.testnet,
                without_address=True
            )
            is_validator = user_ss58_address in {v.ss58_address for v in validators} and not event_uuid

            if not is_validator and not await self._verify_store_permissions(
                store_request_event_uuid=event_uuid,
                user_ss58_address=folder,
                chunk_hash=chunk_hash,
                chunk_size=chunk_size
            ):
                raise HTTPException(status_code=403, detail="Permission denied to store the file")

            client_dir = os.path.join(config_manager.config.data_path, folder)
            os.makedirs(client_dir, exist_ok=True)

            sha256 = hashlib.sha256()
            total_size = 0
            file_uuid = f"{int(time.time())}_{str(uuid.uuid4())}"
            chunk_path = os.path.join(client_dir, file_uuid)

            async with aiofiles.open(chunk_path, 'wb') as chunk_file:
                async for chunk_data in request.stream():
                    total_size += len(chunk_data)
                    sha256.update(chunk_data)
                    await chunk_file.write(chunk_data)

            await check_file(
                file_hash=sha256.hexdigest(),
                file_size=total_size,
                original_file_size=chunk_size,
                original_file_hash=chunk_hash
            )

            return {"id": file_uuid}
        except Exception as e:
            logger.error("Error store", exc_info=True)
            if chunk_path and os.path.exists(chunk_path):
                os.remove(chunk_path)
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

            chunk_path = os.path.join(config_manager.config.data_path, body["folder"], body["chunk_uuid"])
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

            chunk_path = os.path.join(config_manager.config.data_path, body["folder"], body["chunk_uuid"])
            def iterfile():
                with open(chunk_path, 'rb') as chunk_file:
                    while True:
                        data = chunk_file.read(READ_FILE_SIZE)
                        if not data:
                            break
                        yield data

            return StreamingResponse(iterfile(), media_type='application/octet-stream')
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            logger.error("Error retrieving", exc_info=True)
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
            chunk_path = os.path.join(config_manager.config.data_path, body["folder"], body["chunk_uuid"])
            with open(chunk_path, 'rb') as chunk_file:
                chunk_file.seek(start)
                chunk = chunk_file.read(end - start)

            return {"chunk": chunk.hex()}
        except FileNotFoundError:
            raise HTTPException(status_code=404, detail="Chunk not found")
        except Exception as e:
            raise HTTPException(status_code=409, detail=f"Error: {e}")

    async def _verify_store_permissions(self, store_request_event_uuid: str, user_ss58_address: str, chunk_hash: str, chunk_size: int) -> bool:
        """
        Verifies if the client has permissions to store the file by consulting the validator.

        Args:
            store_request_event_uuid (str): The unique identifier of the event to validate.
            user_ss58_address (str): The SS58 address of the user.
            chunk_hash (str): The chunk hash.
            chunk_size (int): The chunk size.

        Returns:
            bool: True if the client has permissions, False otherwise.
        """
        try:
            input_params = {
                "store_request_event_uuid": store_request_event_uuid,
                "user_ss58_address": user_ss58_address,
                "chunk_size": chunk_size,
                "chunk_hash": chunk_hash,
            }
            signed_data = sign_data(input_params, key)
            headers = create_headers(signed_data, key)
            validator_url = await _get_validator_url_async(key=key, testnet=config_manager.config.testnet)

            async with aiohttp.ClientSession() as session:
                async with session.post(f"{validator_url}/store/check-approval", json=input_params, headers=headers, ssl=False) as response:
                    return response.status == 200
        except Exception as e:
            logger.error(e)
            return False


if __name__ == "__main__":
    config = get_config()
    config_manager.initialize(config)

    miner = Miner()

    key = classic_load_key(config.key)

    async def main():
        registered_modules = await get_modules(config.netuid, config.testnet)

        if key.ss58_address not in list(map(lambda module: module.ss58_address, registered_modules)):
            raise Exception(f"Your key: {key.ss58_address} is not registered.")

        async def run_tasks():
            await asyncio.gather(
                miner.run_server(config)
            )

        await run_tasks()

    asyncio.run(main())
