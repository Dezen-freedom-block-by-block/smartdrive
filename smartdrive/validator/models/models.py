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

from enum import Enum
from typing import List, Optional

from communex.types import Ss58Address


class Chunk:
    """
    Represents a data chunk owned by a miner.

    Params:
        miner_owner_ss58address (Ss58Address): The SS58 address of the miner who owns the chunk.
        chunk_uuid (str): The UUID of the chunk.
        file_uuid (Optional[str]): The UUID of the file to which the chunk belongs.
        chunk_index (int): The index position in the file.
    """
    def __init__(self, miner_ss58_address: Ss58Address, chunk_uuid: str, file_uuid: Optional[str], chunk_index: int):
        self.miner_ss58_address = miner_ss58_address
        self.chunk_uuid = chunk_uuid
        self.file_uuid = file_uuid
        self.chunk_index = chunk_index

    def __repr__(self):
        return f"Chunk(miner_owner_ss58address={self.miner_ss58_address}, chunk_uuid={self.chunk_uuid}, file_uuid={self.file_uuid}, chunk_index={self.chunk_index})"


class MinerWithChunk:
    """
    Represents a miner associated with a specific chunk.

    Params:
        ss58_address (Ss58Address): The SS58 address of the miner.
        chunk_uuid (str): The UUID of the chunk associated with the miner.
        chunk_index (int): The index position in the file.
    """
    def __init__(self, ss58_address: Ss58Address, chunk_uuid: str, chunk_index: int = None):
        self.ss58_address = ss58_address
        self.chunk_uuid = chunk_uuid
        self.chunk_index = chunk_index

    def __repr__(self):
        return f"MinerWithChunk(ss58_address={self.ss58_address}, chunk_uuid={self.chunk_uuid}, chunk_index={self.chunk_index})"


class File:
    """
    Represents a file stored in the system.

    Params:
        user_owner_ss58address (Ss58Address): The SS58 address of the user who owns the file.
        file_uuid (Optional[str]): The UUID of the file.
        chunks (List[Chunk]): A list of chunks that make up the file.
    """
    def __init__(self, user_owner_ss58address: Ss58Address, total_chunks: int, file_uuid: Optional[str], chunks: List[Chunk]):
        self.user_owner_ss58address = user_owner_ss58address
        self.total_chunks = total_chunks
        self.file_uuid = file_uuid
        self.chunks = chunks

    def __repr__(self):
        return f"File(file_uuid={self.file_uuid}, user_owner_ss58address={self.user_owner_ss58address}, chunks={self.chunks})"


class ModuleType(Enum):
    MINER = 0
    VALIDATOR = 1
