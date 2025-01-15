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

import random

from fastapi import Request

from communex.compat.key import classic_load_key
from substrateinterface import Keypair

from smartdrive.commune.errors import CommuneNetworkUnreachable
from smartdrive.validator.api.exceptions import FileDoesNotExistException, \
    CommuneNetworkUnreachable as HTTPCommuneNetworkUnreachable, NoMinersInNetworkException
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.models.utils import compile_miners_info_and_chunks
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.commune.request import get_filtered_modules
from smartdrive.validator.models.models import ModuleType
from smartdrive.validator.node.node import Node


class RetrieveAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database = Database()

    async def retrieve_endpoint(self, request: Request, file_uuid: str):
        """
        Handle the retrieval of file metadata and chunk distribution details for a given file UUID.

        This endpoint verifies the existence of the file and its associated chunks in the database,
        retrieves information about available miners, and organizes the data for chunk retrieval.
        It ensures the user has access to the requested file and returns details about the miners
        and chunk distribution.

        Args:
            request (Request): The incoming request object containing the user's public key.
            file_uuid (str): The unique identifier of the file to be retrieved.

        Returns:
            dict:
                - "miners_info": Dictionary mapping chunk indices to a shuffled list of miners storing those chunks.
                - "file_size": The total size of the requested file in bytes.

        Raises:
            FileDoesNotExistException:
                - Raised when the file or its associated chunks are not found in the database.
            HTTPCommuneNetworkUnreachable:
                - Raised when the commune network is unreachable.
            NoMinersInNetworkException:
                - Raised when no miners are available in the network.
        """
        user_public_key = request.headers.get("X-Key")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)

        file = self._database.get_file(user_ss58_address, file_uuid)
        if not file:
            raise FileDoesNotExistException

        chunks = self._database.get_chunks(file_uuid)
        if not chunks:
            # Using the same error detail as above as the end-user experience is essentially the same
            raise FileDoesNotExistException

        try:
            miners = await get_filtered_modules(config_manager.config.netuid, ModuleType.MINER, config_manager.config.testnet)
        except CommuneNetworkUnreachable:
            raise HTTPCommuneNetworkUnreachable

        if not miners:
            raise NoMinersInNetworkException

        miners_info_with_chunk = compile_miners_info_and_chunks(miners, chunks)

        miners_info_with_chunk_ordered_by_chunk_index = {}
        for miner_info_with_chunk in miners_info_with_chunk:
            chunk_index = miner_info_with_chunk["chunk_index"]

            if chunk_index not in miners_info_with_chunk_ordered_by_chunk_index:
                miners_info_with_chunk_ordered_by_chunk_index[chunk_index] = []

            miners_info_with_chunk_ordered_by_chunk_index[chunk_index].append(miner_info_with_chunk)

        # Randomly shuffle the list of miners for each chunk to ensure that requests are not always made to the same miner
        for chunk_index in miners_info_with_chunk_ordered_by_chunk_index:
            random.shuffle(miners_info_with_chunk_ordered_by_chunk_index[chunk_index])

        return {"miners_info": miners_info_with_chunk_ordered_by_chunk_index, "file_size": file.file_size_bytes}
