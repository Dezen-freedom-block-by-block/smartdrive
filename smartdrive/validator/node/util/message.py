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
from typing import Optional

from pydantic import BaseModel


class MessageCode(Enum):
    MESSAGE_CODE_IDENTIFIER = 0
    MESSAGE_CODE_BLOCK = 1
    MESSAGE_CODE_EVENT = 2
    MESSAGE_CODE_PING = 3
    MESSAGE_CODE_PONG = 4
    MESSAGE_CODE_SYNC = 5
    MESSAGE_CODE_SYNC_BLOCKS_RESPONSE = 6
    MESSAGE_CODE_VALIDATION_EVENTS = 7
    MESSAGE_CODE_IDENTIFIER_OK = 8


class MessageBody(BaseModel):
    code: MessageCode
    data: Optional[dict] = None

    def dict(self, **kwargs):
        result = super().dict(**kwargs)
        result['code'] = self.code.value
        return result


class Message(BaseModel):
    body: MessageBody
    signature_hex: str
    public_key_hex: str

    def dict(self, **kwargs):
        result = super().dict(**kwargs)
        result['body'] = self.body.dict()
        return result
