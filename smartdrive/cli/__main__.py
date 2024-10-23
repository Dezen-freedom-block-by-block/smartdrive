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

import typer

from smartdrive.cli.handlers import store_handler, retrieve_handler, remove_handler
from smartdrive.commune.connection_pool import initialize_commune_connection_pool

app = typer.Typer()


@app.command()
def store(file_path: str, key_name: str = None, testnet: bool = False):
    """
    Store a file on the SmartDrive network.

    Params:
        file_path (str): The path to the file to be stored.
        key_name (str, optional): An optional key for encryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used.
    """
    initialize_commune_connection_pool(testnet=testnet, max_pool_size=1, num_connections=1)
    store_handler(file_path, key_name, testnet)


@app.command()
def retrieve(file_uuid: str, file_path: str, key_name: str = None, testnet: bool = False):
    """
    Retrieve a file from the SmartDrive network.

    Params:
        file_uuid (str): The ID of the file to be retrieved.
        file_path (str): The path where the retrieved file will be saved.
        key_name (str, optional): An optional key for decryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used.
    """
    initialize_commune_connection_pool(testnet=testnet, max_pool_size=1, num_connections=1)
    retrieve_handler(file_uuid, file_path, key_name, testnet)


@app.command()
def remove(file_uuid: str, key_name: str = None, testnet: bool = False):
    """
    Remove a file from the SmartDrive network.

    Params:
        file_uuid (str): The ID of the file to be removed.
        key_name (str, optional): An optional key for decryption. If not provided, it will be requested.
        testnet (bool, optional): Flag to indicate if the testnet should be used.
    """
    initialize_commune_connection_pool(testnet=testnet, max_pool_size=1, num_connections=1)
    remove_handler(file_uuid, key_name, testnet)


if __name__ == "__main__":
    app()
