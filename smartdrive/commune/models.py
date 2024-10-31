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

from communex.types import Ss58Address


class ConnectionInfo:
    def __init__(self, ip: str, port: int):
        self.ip = ip
        self.port = port

    def __repr__(self):
        return f"ConnectionInfo(ip={self.ip}, port={self.port})"


class ModuleInfo:
    def __init__(self, uid: str, ss58_address: Ss58Address, connection: Optional[ConnectionInfo] = None, incentives: Optional[int] = None, dividends: Optional[int] = None, stake: Optional[int] = None):
        self.uid = uid
        self.ss58_address = ss58_address
        self.connection = connection
        self.incentives = incentives
        self.dividends = dividends
        self.stake = stake

    def __eq__(self, other):
        if isinstance(other, ModuleInfo):
            return self.ss58_address == other.ss58_address
        return False

    def __hash__(self):
        return hash(self.ss58_address)

    def __repr__(self):
        return f"ModuleInfo(uid={self.uid}, ss58_address={self.ss58_address}, connection={self.connection}, incentives={self.incentives}, dividends={self.dividends}, stake={self.stake})"
