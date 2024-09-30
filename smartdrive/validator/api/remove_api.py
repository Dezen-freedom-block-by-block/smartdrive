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

import time
import uuid
from fastapi import Request
from substrateinterface import Keypair

from communex.compat.key import classic_load_key
from communex.types import Ss58Address

from smartdrive.sign import sign_data
from smartdrive.validator.api.exceptions import FileDoesNotExistException
from smartdrive.validator.api.middleware.api_middleware import get_ss58_address_from_public_key
from smartdrive.validator.config import config_manager
from smartdrive.validator.database.database import Database
from smartdrive.models.event import RemoveEvent, RemoveInputParams, EventParams
from smartdrive.validator.node.node import Node


class RemoveAPI:
    _node: Node = None
    _key: Keypair = None
    _database: Database = None

    def __init__(self, node: Node):
        self._node = node
        self._key = classic_load_key(config_manager.config.key)
        self._database: Database = Database()

    async def remove_endpoint(self, request: Request, file_uuid: str):
        """
        Send an event with the user's intention to remove a specific file from the SmartDrive network.

        Params:
            request (Request): The incoming request containing necessary headers for validation.
            file_uuid (str): The UUID of the file to be removed, provided as a form parameter.

        Raises:
           FileDoesNotExistException: If the file does not exist or there are no miners with the files.
        """
        user_public_key = request.headers.get("X-Key")
        input_signed_params = request.headers.get("X-Signature")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)

        file = self._database.get_file(user_ss58_address, file_uuid)
        if not file:
            raise FileDoesNotExistException

        chunks = self._database.get_chunks(file_uuid)
        if not chunks:
            # Using the same error detail for both cases as the end-user experience is essentially the same
            raise FileDoesNotExistException

        event_params = EventParams(file_uuid=file_uuid)

        signed_params = sign_data(event_params.dict(), self._key)

        event = RemoveEvent(
            uuid=f"{int(time.time())}_{str(uuid.uuid4())}",
            validator_ss58_address=Ss58Address(self._key.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params=RemoveInputParams(file_uuid=file_uuid),
            input_signed_params=input_signed_params
        )

        self._node.add_event(event)
