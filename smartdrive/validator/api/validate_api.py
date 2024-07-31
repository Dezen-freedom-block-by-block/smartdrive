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

from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.request import execute_miner_request
from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.models.models import SubChunk


async def validate_chunk_request(keypair: Keypair, user_owner_ss58_address: Ss58Address, miner_module_info: ModuleInfo, subchunk: SubChunk) -> bool:
    """
    Sends a request to a miner to validate a specific sub-chunk.

    This method sends an asynchronous request to a specified miner to validate a sub-chunk
    identified by its UUID. The request is executed using the miner's connection and
    address information.

    Params:
        keypair (Keypair): The validator key used to authorize the request.
        user_owner_ss58_address (Ss58Address): The SS58 address of the user associated with the sub-chunk.
        miner_module_info (ModuleInfo): The miner's module information.
        subchunk (SubChunk): The sub-chunk to be validated.

    Returns:
        bool: Returns True if the miner confirms the validation request, otherwise False.
    """
    miner_answer = await execute_miner_request(
        keypair, miner_module_info.connection, miner_module_info.ss58_address, "validation",
        {
            "folder": user_owner_ss58_address,
            "chunk_uuid": subchunk.chunk_uuid,
            "start": subchunk.start,
            "end": subchunk.end
        }
    )
    return str(miner_answer) == subchunk.data if miner_answer else False