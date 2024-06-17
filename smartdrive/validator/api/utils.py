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

from smartdrive.commune.request import ModuleInfo
from smartdrive.validator.models.models import MinerWithChunk, MinerWithSubChunk


def get_miner_info_with_chunk(active_miners: list[ModuleInfo], miner_chunks: list[MinerWithChunk] | list[MinerWithSubChunk]) -> list:
    """
    Gather information about active miners and their associated chunks.

    This function matches active miners with their corresponding chunks and compiles
    the relevant information into a list of dictionaries.

    Params:
        active_miners (list[ModuleInfo]): A list of active miner objects.
        miner_chunks (list[MinerWithChunk] | list [MinerWithSubChunk]): A list of miner chunk objects.

    Returns:
        list[dict]: A list of dictionaries, each containing:
            uid (str): The unique identifier of the miner.
            ss58_address (SS58_address): The SS58 address of the miner.
            connection (dict): A dictionary containing the IP address and port of the miner's connection.
            chunk_uuid (str): The UUID of the chunk associated with the miner.
            sub_chunk (Optional): The sub-chunk information if available.
    """
    miner_info_with_chunk = []

    for active_miner in active_miners:
        for miner_chunk in miner_chunks:
            if active_miner.ss58_address == miner_chunk.ss58_address:
                data = {
                    "uid": active_miner.uid,
                    "ss58_address": active_miner.ss58_address,
                    "connection": {
                        "ip": active_miner.connection.ip,
                        "port": active_miner.connection.port
                    },
                    "chunk_uuid": miner_chunk.chunk_uuid,
                }

                if isinstance(miner_chunk, MinerWithSubChunk):
                    data["sub_chunk"] = miner_chunk.sub_chunk

                miner_info_with_chunk.append(data)

    return miner_info_with_chunk
