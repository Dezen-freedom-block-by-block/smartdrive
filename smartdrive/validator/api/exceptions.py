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

from typing import Optional
from fastapi import HTTPException

from smartdrive.utils import format_size
from smartdrive.config import MAXIMUM_STORAGE_PER_USER_PER_FILE


class UnexpectedErrorException(HTTPException):
    def __init__(self, detail: str = "Unexpected error"):
        super().__init__(status_code=500, detail=detail)


class ChunkNotAvailableException(Exception):
    def __init__(self, message: Optional[str] = "The chunk currently is not available"):
        super().__init__(message)
        self.message = message


class FileDoesNotExistException(HTTPException):
    def __init__(self, detail: str = "The file does not exist"):
        super().__init__(status_code=404, detail=detail)


class InvalidFileEventAssociationException(HTTPException):
    def __init__(self, detail: str = "File UUID does not match the event UUID."):
        super().__init__(status_code=422, detail=detail)


class FileNotAvailableException(HTTPException):
    def __init__(self, detail: str = "The file currently is not available"):
        super().__init__(status_code=503, detail=detail)


class FileTooLargeException(HTTPException):
    def __init__(self, detail: str = f"File size exceeds the maximum limit of {format_size(MAXIMUM_STORAGE_PER_USER_PER_FILE)}"):
        super().__init__(status_code=413, detail=detail)


class InvalidFileSizeException(HTTPException):
    def __init__(self, detail: str = "The received file size does not match the original size"):
        super().__init__(status_code=413, detail=detail)


class FileSizeMismatchException(HTTPException):
    def __init__(self, detail: str = "File size mismatch"):
        super().__init__(status_code=413, detail=detail)


class FileHashMismatchException(HTTPException):
    def __init__(self, detail: str = "File hash mismatch"):
        super().__init__(status_code=422, detail=detail)


class StorageLimitException(HTTPException):
    def __init__(self, file_size: int, total_size_stored_by_user: int, available_storage_of_user: int):
        detail = f"Storage limit exceeded. You have used {format_size(total_size_stored_by_user)} out of {format_size(available_storage_of_user)}. The file you are trying to upload is {format_size(file_size)}."
        super().__init__(status_code=413, detail=detail)


class StoreRequestNotApprovedException(HTTPException):
    def __init__(self, detail: str = "Store request is not approved due to limit storage", status_code: int = 403):
        super().__init__(status_code=status_code, detail=detail)


class CommuneNetworkUnreachable(HTTPException):
    def __init__(self, detail: str = "Commune network is unreachable"):
        super().__init__(status_code=503, detail=detail)


class NoMinersInNetworkException(HTTPException):
    def __init__(self, detail: str = "Currently there are no miners in the SmartDrive network"):
        super().__init__(status_code=503, detail=detail)


class NoValidMinerResponseException(HTTPException):
    def __init__(self, detail: str = "No miner answered with a valid response"):
        super().__init__(status_code=503, detail=detail)


class HTTPRedundancyException(HTTPException):
    def __init__(self, detail: str):
        super().__init__(status_code=503, detail=detail)


class RedundancyException(Exception):
    def __init__(self, message: Optional[str] = "Currently, redundancy in the network is not guaranteed"):
        super().__init__(message)
        self.message = message
