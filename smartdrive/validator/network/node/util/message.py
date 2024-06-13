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

import json

from smartdrive.validator.network.node.util.message_code import MESSAGE_CODE_IDENTIFIER, MESSAGE_CODE_BLOCK


class Message:

    def __init__(self, code, data):
        data_dict = json.loads(data.decode('utf-8'))

        if 'code' not in data_dict or 'data' not in data_dict:
            raise ValueError("Invalid message format")

        self.code = data_dict['code']
        self.data = data_dict['data']


class MessageIdentifier(Message):

    def __init__(self, data, code):
        super().__init__(code, data)

    def __repr__(self):
        return f"MessageIdentifier(code={self.code}, data={self.data})"


class MessageBlock(Message):

    def __init__(self, data, code):
        super().__init__(code, data)

    def __repr__(self):
        return f"MessageBlock(code={self.code}, data={self.data})"


MESSAGE_CODE_TYPES = {MESSAGE_CODE_IDENTIFIER: MessageIdentifier, MESSAGE_CODE_BLOCK: MessageBlock}
