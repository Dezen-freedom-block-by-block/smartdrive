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

from communex.types import Ss58Address
from substrateinterface import Keypair

from smartdrive.commune.models import ModuleInfo
from smartdrive.commune.request import execute_miner_request
from smartdrive.utils import calculate_storage_capacity, MAXIMUM_STORAGE
from smartdrive.validator.api.exceptions import StorageLimitException, FileTooLargeException
from smartdrive.validator.database.database import Database


async def remove_chunk_request(keypair: Keypair, user_ss58_address: Ss58Address, miner: ModuleInfo, chunk_uuid: str) -> bool:
    """
    Sends a request to a miner to remove a specific data chunk.

    This method sends an asynchronous request to a specified miner to remove a data chunk
    identified by its UUID. The request is executed using the miner's connection and
    address information.

    Params:
        keypair (Keypair): The validator key used to authorize the request.
        user_ss58_address (Ss58Address): The SS58 address of the user associated with the data chunk.
        miner (ModuleInfo): The miner's module information.
        chunk_uuid (str): The UUID of the data chunk to be removed.

    Returns:
        bool: Returns True if the miner confirms the removal request, otherwise False.
    """
    miner_answer = await execute_miner_request(
        keypair, miner.connection, miner.ss58_address, "remove",
        {
            "folder": user_ss58_address,
            "chunk_uuid": chunk_uuid
        }
    )
    return True if miner_answer else False


def validate_storage_capacity(database: Database, user_ss58_address: str, file_size_bytes: int, total_stake: float, only_files: bool = False) -> None:
    """
    Validates if the user has enough available storage to store the file.

    This function checks if the file size is below the maximum allowed file size (MAXIMUM_STORAGE),
    and whether the user has sufficient storage capacity based on their stake. If either condition is not met,
    it raises the appropriate exception.

    Params:
        database (Database): The database instance to query for the user's stored files.
        user_ss58_address (str): The SS58 address of the user making the request.
        file_size_bytes (int): The size of the file the user is attempting to store, in bytes.
        total_stake (float): The total stake the user has in the COMAI system.
        only_files (bool): Whether to only validate the files if the user has sufficient storage capacity.

    Raises:
        FileTooLargeException: If the file exceeds the maximum allowed storage size (MAXIMUM_STORAGE).
        StorageLimitException: If the user's total stored files plus the new file exceed their available storage capacity.
    """

    if file_size_bytes > MAXIMUM_STORAGE:
        raise FileTooLargeException

    total_size_stored_by_user = database.get_total_file_size_by_user(user_ss58_address=user_ss58_address, only_files=only_files)
    available_storage_of_user = calculate_storage_capacity(total_stake)
    if total_size_stored_by_user + file_size_bytes > available_storage_of_user:
        raise StorageLimitException(file_size_bytes, total_size_stored_by_user, available_storage_of_user)
