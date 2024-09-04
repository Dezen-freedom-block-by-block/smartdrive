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


class FileNotAvailableException(HTTPException):
    def __init__(self, detail: str = "The file currently is not available"):
        super().__init__(status_code=503, detail=detail)


class FileTooLargeException(HTTPException):
    def __init__(self, detail: str = "File size exceeds the maximum limit of 500 MB"):
        super().__init__(status_code=413, detail=detail)


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
