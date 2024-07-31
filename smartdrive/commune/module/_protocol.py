import json
import datetime

from typing import Any

from ._signer import sign

from substrateinterface import Keypair

from communex.types import Ss58Address


def serialize(data: Any) -> bytes:
    txt = json.dumps(data)
    return txt.encode()


def iso_timestamp_now() -> str:
    now = datetime.datetime.now(tz=datetime.timezone.utc)
    iso_now = now.isoformat()
    return iso_now


def create_headers(signature: bytes, key: Keypair, timestamp_iso: str = iso_timestamp_now(), content_type: str = "application/json", show_content_type: bool = True):
    headers = {
        "X-Signature": signature.hex(),
        "X-Key": key.public_key.hex(),
        "X-Crypto": str(key.crypto_type),
        "X-Timestamp": timestamp_iso,
    }
    if show_content_type:
        headers["Content-Type"] = content_type

    return headers


def create_request_data(
    my_key: Keypair,
    target_key: Ss58Address,
    params: Any,
    show_content_type=True,
    content_type="application/json"
) -> tuple[bytes, dict[str, str]]:
    timestamp_iso = iso_timestamp_now()

    params["target_key"] = target_key

    request_data = {
        "params": params,
    }

    serialized_data = serialize(request_data)
    serialized_stamped_data = serialize(request_data)
    signature = sign(my_key, serialized_stamped_data)

    headers = create_headers(signature, my_key, timestamp_iso, show_content_type=show_content_type, content_type=content_type)

    return serialized_data, headers


def create_method_endpoint(host: str, port: str | int, method_name: str) -> str:
    return f"https://{host}:{port}/method/{method_name}"
