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

from fastapi import HTTPException, Form, Request
from substrateinterface import Keypair

from communex.client import CommuneClient
from communex.types import Ss58Address

from smartdrive.validator.api.middleware.sign import sign_data
from smartdrive.validator.api.middleware.subnet_middleware import get_ss58_address_from_public_key
from smartdrive.validator.database.database import Database
from smartdrive.models.event import RemoveEvent, RemoveParams
from smartdrive.validator.network.network import Network
from smartdrive.commune.request import get_active_miners, execute_miner_request, ModuleInfo


class RemoveAPI:
    _config = None
    _key: Keypair = None
    _database: Database = None
    _comx_client: CommuneClient = None
    _network: Network = None

    def __init__(self, config, key, database, comx_client, network: Network):
        self._config = config
        self._key = key
        self._database = database
        self._comx_client = comx_client
        self._network = network

    async def remove_endpoint(self, request: Request, file_uuid: str = Form()):
        """
        Send an event with the user's intention to remove a specific file from the SmartDrive network.

        Params:
            request (Request): The incoming request containing necessary headers for validation.
            file_uuid (str): The UUID of the file to be removed, provided as a form parameter.

        Raises:
           HTTPException: If the file does not exist, there are no miners with the file, or there are no active miners available.
        """
        # Get headers related info
        user_public_key = request.headers.get("X-Key")
        input_signed_params = request.headers.get("X-Signature")
        user_ss58_address = get_ss58_address_from_public_key(user_public_key)

        # Check if the file exists
        file_exists = self._database.check_if_file_exists(user_ss58_address, file_uuid)
        if not file_exists:
            raise HTTPException(status_code=404, detail="The file does not exist")

        # Get miners and chunks for the file
        miner_chunks = self._database.get_miner_chunks(file_uuid)
        if not miner_chunks:
            raise HTTPException(status_code=404, detail="Currently there are no miners with this file name")

        # Get active miners
        active_miners = await get_active_miners(self._key, self._comx_client, self._config.netuid)
        if not active_miners:
            raise HTTPException(status_code=404, detail="Currently there are no active miners")

        # Create event
        event_params = RemoveParams(
            file_uuid=file_uuid
        )

        signed_params = sign_data(event_params.dict(), self._key)

        event = RemoveEvent(
            validator_ss58_address=Ss58Address(self._key.ss58_address),
            event_params=event_params,
            event_signed_params=signed_params.hex(),
            user_ss58_address=user_ss58_address,
            input_params={"file_uuid": file_uuid},
            input_signed_params=input_signed_params
        )

        # Emit event
        self._network.emit_event(event)