import hashlib
import re
from typing import List, Optional

from communex.types import Ss58Address
from substrateinterface.utils.ss58 import is_valid_ss58_address, ss58_encode

from smartdrive.logging_config import logger
from smartdrive.commune.models import ModuleInfo, ConnectionInfo
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT


def filter_truthful_validators(active_validators: list[ModuleInfo]) -> List[ModuleInfo]:
    return list(filter(lambda validator: validator.stake > TRUTHFUL_STAKE_AMOUNT, active_validators))


def _extract_address(string: str) -> Optional[List[str]]:
    """
    Extract an IP address and port from a given string.

    This function uses a regular expression to search for an IP address and port combination
    within the provided string. If a match is found, the IP address and port are returned
    as a list of strings. If no match is found, None is returned.

    Params:
        string (str): The input string containing the IP address and port.

    Returns:
        Optional[List[str]]: A list containing the IP address and port as strings if a match
                             is found, or None if no match is found.
    """
    ip_regex = re.compile(r"\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}:\d+")
    match = re.search(ip_regex, string)
    if match:
        return match.group(0).split(":")

    return None


def _get_ip_port(address_string: str) -> Optional[ConnectionInfo]:
    """
    Extract the IP address and port from a given address string and return them as a `ConnectionInfo` object.

    This function uses `_extract_address` to parse the IP address and port from the input string.
    If successful, it returns a `ConnectionInfo` object containing the IP address and port.
    If the extraction fails or an exception occurs, it returns `None`.

    Params:
        address_string (str): The input string containing the address.

    Returns:
        Optional[ConnectionInfo]: A `ConnectionInfo` object with the IP address and port if successful,
                                  or `None` if the extraction fails or an exception occurs.
    """
    try:
        extracted_address = _extract_address(address_string)
        if extracted_address:
            return ConnectionInfo(extracted_address[0], int(extracted_address[1]))
        return None

    except Exception:
        logger.error("Error extracting IP and port", exc_info=True)
        return None


def get_ss58_address_from_public_key(public_key_hex) -> Optional[Ss58Address]:
    """
    Convert a public key in hexadecimal format to an Ss58Address if valid.

    Params:
        public_key_hex (str): The public key in hexadecimal format.

    Returns:
        Optional[Ss58Address]: The corresponding Ss58Address if valid, otherwise None.
    """
    public_key_bytes = bytes.fromhex(public_key_hex)
    ss58_address = ss58_encode(public_key_bytes)
    return Ss58Address(ss58_address) if is_valid_ss58_address(ss58_address) else None


def calculate_hash(data: bytes) -> str:
    """
    Calculates the SHA-256 hash of the given data.

    Params:
        data (bytes): The data to hash, provided as a byte string.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash of the input data.
    """
    sha256 = hashlib.sha256()
    sha256.update(data)
    return sha256.hexdigest()


def calculate_hash_stream(file_stream) -> str:
    """
    Calculates the SHA-256 hash of the given file stream by reading it in chunks.

    Params:
        file_stream (file-like object): The file stream to hash.

    Returns:
        str: The hexadecimal representation of the SHA-256 hash of the input data.
    """
    sha256 = hashlib.sha256()
    for chunk in iter(lambda: file_stream.read(8192), b''):
        sha256.update(chunk)
    return sha256.hexdigest()