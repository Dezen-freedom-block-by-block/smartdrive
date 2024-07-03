import asyncio

from communex._common import ComxSettings
from communex.client import CommuneClient


async def _try_get_client(url, num_connections):
    try:
        return CommuneClient(url, num_connections=num_connections)
    except Exception:
        return None


async def get_comx_client(num_connections, testnet) -> CommuneClient:
    comx_settings = ComxSettings()
    urls = comx_settings.TESTNET_NODE_URLS if testnet else comx_settings.NODE_URLS

    tasks = [asyncio.create_task(_try_get_client(url, num_connections)) for url in urls]

    done, pending = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)

    first_valid_client = None
    for task in done:
        comx_client = task.result()
        if comx_client is not None:
            first_valid_client = comx_client
            break

    for task in pending:
        task.cancel()

    if first_valid_client:
        return first_valid_client

    raise Exception("No valid comx_client could be found")
