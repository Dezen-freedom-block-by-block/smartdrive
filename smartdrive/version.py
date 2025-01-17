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

import fcntl
import os
import random
import re
import subprocess
import time
from pathlib import Path
import tomli
import requests

from smartdrive.logging_config import logger


LOCK_FILE = "update.lock"
UPDATE_DELAY_RANGE = (5, 30)


def get_version() -> str:
    """
    Retrieve the version of the project from the pyproject.toml file.

    This function reads the pyproject.toml file located in the parent directory of the script's
    location, parses its content, and returns the version specified under the [tool.poetry] section.

    Returns:
        str: The version of the project as specified in the pyproject.toml file.

    Raises:
        FileNotFoundError: If the pyproject.toml file does not exist.
        tomli.TOMLDecodeError: If there is an error parsing the pyproject.toml file.
        KeyError: If the version key is not found in the pyproject.toml file.
    """
    pyproject_path = os.path.join(os.path.dirname(__file__), '..', 'pyproject.toml')
    with open(pyproject_path, 'rb') as f:
        pyproject_data = tomli.load(f)
    return pyproject_data['tool']['poetry']['version']


__version__ = get_version()


def version_str_to_num(version: str) -> int:
    """
    Convert version number as string to number (1.2.0 => 120).
    Multiply the first version number by one hundred, the second by ten, and the last by one. Finally add them all.

    Params:
        version (str): The version number as string.

    Returns:
        int: Version number as int.
    """
    version_split = version.split(".")
    return (100 * int(version_split[0])) + (10 * int(version_split[1])) + int(version_split[2])


def check_version():
    """
    Check current version of the module on GitHub. If it is greater than the local version, download and update the module.
    """
    latest_version = get_latest_version()

    current_file_path = Path(__file__).resolve()
    root_directory = current_file_path.parent.parent

    # If version in GitHub is greater, update module.
    if version_str_to_num(__version__) < version_str_to_num(latest_version) and latest_version is not None:
        lock_acquired = False
        try:
            with open(LOCK_FILE, "w+") as lock_file:
                try:
                    fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                    lock_acquired = True
                    logger.info(f"Updating to the latest version ({latest_version})...")

                    subprocess.run(["git", "reset", "--hard"], cwd=root_directory, check=True)
                    subprocess.run(["git", "pull"], cwd=root_directory, timeout=60, check=True)
                    subprocess.run(["pip", "install", "-e", "."], cwd=root_directory, timeout=60, check=True)
                except BlockingIOError:
                    while True:
                        try:
                            fcntl.flock(lock_file, fcntl.LOCK_EX | fcntl.LOCK_NB)
                            break
                        except BlockingIOError:
                            time.sleep(5)
        except Exception as e:
            logger.error(f"Unexpected error during update: {e}")
        finally:
            if lock_acquired:
                try:
                    os.remove(LOCK_FILE)
                except FileNotFoundError:
                    pass

            time.sleep(random.randint(*UPDATE_DELAY_RANGE))
            exit(0)


def get_latest_version() -> str:
    """
    Retrieve the latest version number from GitHub repository.

    Returns:
        str: Version number as string (X.X.X).
    """

    # The raw content URL of the file on GitHub.
    url = "https://raw.githubusercontent.com/Dezen-freedom-block-by-block/smartdrive/main/pyproject.toml"

    # Send an HTTP GET request to the raw content URL.
    response = requests.get(url)

    # Check if the request was successful.
    if response.status_code == 200:
        version_match = re.search(r'version = "(.*?)"', response.text)

        if not version_match:
            raise Exception("Version information not found in the specified line.")

        return version_match.group(1)

    else:
        logger.error(f"Failed to fetch file content. Status code: {response.status_code}")
