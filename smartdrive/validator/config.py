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

import multiprocessing
from multiprocessing import Manager
import os

multiprocessing.set_start_method("fork")

class Config:
    def __init__(self, key: str, database_path: str, ip: str, port: int, testnet: bool, netuid: int):
        self.key: str = key
        self.database_path: str = database_path
        self.ip: str = ip
        self.port: int = port
        self.testnet: bool = testnet
        self.netuid: int = netuid
        self.database_file: str = os.path.join(database_path, "smartdrive.db")
        self.database_export_file: str = os.path.join(database_path, "export.zip")


class ConfigManager:
    def __init__(self):
        self.manager = Manager()
        self.config = self.manager.Namespace()

    def initialize(self, config: Config):
        self.config.key = config.key
        self.config.database_path = config.database_path
        self.config.ip = config.ip
        self.config.port = config.port
        self.config.testnet = config.testnet
        self.config.netuid = config.netuid
        self.config.database_file = config.database_file
        self.config.database_export_file = config.database_export_file


config_manager = ConfigManager()
