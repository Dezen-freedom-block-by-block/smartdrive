import random
from typing import List

from communex._common import ComxSettings
from communex.client import CommuneClient

from smartdrive.commune.models import ModuleInfo
from smartdrive.validator.constants import TRUTHFUL_STAKE_AMOUNT

DEFAULT_NUM_CONNECTIONS = 1

# TODO: Currently, this is an initial phase since an instance of the Commune client is created in each connection.
#  This is necessary because if you create an instance of Commune and the node you are connected to crashes, future
#  calls to the node through Commune are indefinitely blocked.


def _try_get_client(url, num_connections):
    try:
        return CommuneClient(url, num_connections=num_connections)
    except Exception:
        return None


def get_comx_client(testnet: bool, num_connections: int = DEFAULT_NUM_CONNECTIONS) -> CommuneClient:
    comx_settings = ComxSettings()
    urls = comx_settings.TESTNET_NODE_URLS if testnet else comx_settings.NODE_URLS
    random.shuffle(urls)

    for url in urls:
        comx_client = _try_get_client(url, num_connections)
        if comx_client is not None:
            return comx_client

    raise Exception("No valid comx_client could be found")


def filter_truthful_validators(active_validators: list[ModuleInfo]) -> List[ModuleInfo]:
    return list(filter(lambda validator: validator.stake > TRUTHFUL_STAKE_AMOUNT, active_validators))
