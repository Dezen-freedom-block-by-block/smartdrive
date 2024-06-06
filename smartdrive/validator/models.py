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

from typing import List, Optional
import time
import random

from communex.types import Ss58Address


class SubChunk:
    """
    Represents a sub-chunk of a chunk.

    Params:
        id (Optional[int]): The unique identifier of the sub-chunk.
        start (int): The start position of the sub-chunk in the chunk.
        end (int): The end position of the sub-chunk in the chunk.
        chunk_uuid (Optional[str]): The UUID of its chunk.
        data (str): The data contained within the sub-chunk.
    """
    def __init__(self, id: Optional[int], start: int, end: int, chunk_uuid: Optional[str], data: str):
        self.id = id
        self.start = start
        self.end = end
        self.chunk_uuid = chunk_uuid
        self.data = data

    def __repr__(self):
        return f"SubChunk(id={self.id}, chunk_uuid={self.chunk_uuid}, start={self.start}, end={self.end}, data=Too long to show)"


class Chunk:
    """
    Represents a data chunk owned by a miner.

    Params:
        miner_owner_ss58address (Ss58Address): The SS58 address of the miner who owns the chunk.
        chunk_uuid (str): The UUID of the chunk.
        file_uuid (Optional[str]): The UUID of the file to which the chunk belongs.
        sub_chunk (Optional[SubChunk]): The sub-chunk contained within the chunk.
    """
    def __init__(self, miner_owner_ss58address: Ss58Address, chunk_uuid: str, file_uuid: Optional[str], sub_chunk: Optional[SubChunk]):
        self.miner_owner_ss58address = miner_owner_ss58address
        self.chunk_uuid = chunk_uuid
        self.file_uuid = file_uuid
        self.sub_chunk = sub_chunk

    def __repr__(self):
        return f"Chunk(miner_owner_ss58address={self.miner_owner_ss58address}, chunk_uuid={self.chunk_uuid}, file_uuid={self.file_uuid}, sub_chunk={self.sub_chunk})"


class MinerWithChunk:
    """
    Represents a miner associated with a specific chunk.

    Params:
        ss58_address (Ss58Address): The SS58 address of the miner.
        chunk_uuid (str): The UUID of the chunk associated with the miner.
    """
    def __init__(self, ss58_address: Ss58Address, chunk_uuid: str):
        self.ss58_address = ss58_address
        self.chunk_uuid = chunk_uuid

    def __repr__(self):
        return f"MinerWithChunk(ss58_address={self.ss58_address}, chunk_uuid={self.chunk_uuid})"


class MinerWithSubChunk(MinerWithChunk):
    """
    Represents a miner associated with a specific sub-chunk.

    Params:
        ss58_address (Ss58Address): The SS58 address of the miner.
        chunk_uuid (str): The UUID of the chunk associated with the miner.
        sub_chunk (SubChunk): The sub-chunk associated with the chunk.
    """
    def __init__(self, ss58_address: Ss58Address, chunk_uuid: str, sub_chunk: SubChunk):
        super().__init__(ss58_address, chunk_uuid)
        self.sub_chunk = sub_chunk

    def __repr__(self):
        return f"MinerWithSubChunk(ss58_address={self.ss58_address}, chunk_uuid={self.chunk_uuid}, sub_chunk={self.sub_chunk})"


class File:
    """
    Represents a file stored in the system.

    Params:
        user_owner_ss58address (Ss58Address): The SS58 address of the user who owns the file.
        file_uuid (Optional[str]): The UUID of the file.
        chunks (List[Chunk]): A list of chunks that make up the file.
        created_at (int): The timestamp when the file was created, in milliseconds, only available if it is a temporal file.
        expiration_ms (Optional[int]): The expiration time of the file in milliseconds, only available if it is a temporal file.
    """
    def __init__(self, user_owner_ss58address: Ss58Address, file_uuid: Optional[str], chunks: List[Chunk], created_at: Optional[int], expiration_ms: Optional[int]):
        self.user_owner_ss58address = user_owner_ss58address
        self.file_uuid = file_uuid
        self.chunks = chunks
        self.created_at = created_at or int(time.time() * 1000)
        self.expiration_ms = expiration_ms or self.get_expiration()

    def __repr__(self):
        return f"File(file_uuid={self.file_uuid}, user_owner_ss58address={self.user_owner_ss58address}, chunks={self.chunks}, created_at={self.created_at}, expiration_ms={self.expiration_ms})"

    def get_expiration(self) -> int:
        min_ms = 3 * 60 * 60 * 1000  # 3 hours
        max_ms = 24 * 60 * 60 * 1000  # 24 hours
        return random.randint(min_ms, max_ms)

    def has_expiration(self) -> bool:
        return self.expiration_ms > 0

    def has_expired(self, current_timestamp):
        return current_timestamp > (self.created_at + self.expiration_ms)

    def get_sub_chunks(self) -> List[SubChunk]:
        sub_chunks = []
        for chunk in self.chunks:
            sub_chunks.append(chunk.sub_chunk)
        return sub_chunks