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

import random
from time import sleep

from substrateinterface import Keypair
from communex._common import get_node_url
from communex.client import CommuneClient

from smartdrive.commune.request import vote
from smartdrive.validator.evaluation.sigmoid import threshold_sigmoid_reward_distribution

# TODO: Set with the subnet production value
MAX_ALLOWED_WEIGHTS = 1000
MAX_ALLOWED_UIDS = 100
MAX_RESPONSE_TIME = 8

# Miner weights
DISK_WEIGHT = 0.5
SPEED_WEIGHT = 0.15
SUCCESS_WEIGHT = 0.35


def score_miner(successful_store_responses: int, total_store_responses: int, avg_response_time: float, successful_responses: int, total_responses: int) -> float:
    """
    Calculate the score for a miner based on successful store responses, average response time, and overall success rate.

    Params:
        successful_store_responses (int): The number of successful 'store' responses with code 200.
        total_store_responses (int): The total number of 'store' responses.
        avg_response_time (float | None): The average response time of the miner (in seconds).
        successful_responses (int): The number of successful responses with code 200.
        total_responses (int): The total number of responses.

    Returns:
        float: The calculated score for the miner.
    """
    # Normalize success store responses
    normalized_store_responses = 0 if total_store_responses == 0 else min(successful_store_responses / total_store_responses, 1)

    # Normalize response time (inverted, since lower time is better)
    normalized_response_time = 0 if avg_response_time is None else (1 - min(avg_response_time / MAX_RESPONSE_TIME, 1))

    # Normalize success responses
    normalized_success_responses = 0 if total_responses == 0 else min(successful_responses / total_responses, 1)

    # Calculate weighted score
    score = (
        (normalized_store_responses * DISK_WEIGHT) +
        (normalized_response_time * SPEED_WEIGHT) +
        (normalized_success_responses * SUCCESS_WEIGHT)
    )

    return score

# MIT No Attribution
# 
# Copyright (c) 2024 agicommies
# 
# Permission is hereby granted, free of charge, to any person obtaining a copy of this
# software and associated documentation files (the "Software"), to deal in the Software
# without restriction, including without limitation the rights to use, copy, modify,
# merge, publish, distribute, sublicense, and/or sell copies of the Software, and to
# permit persons to whom the Software is furnished to do so.
# 
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR IMPLIED,
# INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY, FITNESS FOR A
# PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT
# HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN ACTION
# OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN CONNECTION WITH THE
# SOFTWARE OR THE USE OR OTHER DEALINGS IN THE SOFTWARE.

def set_weights(score_dict: dict[int, float], netuid: int, client: CommuneClient, key: Keypair):
    """
    Set weights for miners based on their scores.

    Params:
        score_dict (dict[int, float]): A dictionary mapping miner UIDs to their scores.
        netuid (int): The network UID.
        client (CommuneClient): The CommuneX client.
        key (Keypair): The keypair for signing transactions.
    """

    cut_weights = _cut_to_max_allowed_uids(score_dict)
    adjusted_to_sigmoid = threshold_sigmoid_reward_distribution(cut_weights)

    # Create a new dictionary to store the weighted scores
    weighted_scores: dict[int, int] = {}

    # Calculate the sum of all inverted scores
    scores = sum(adjusted_to_sigmoid.values())

    # Iterate over the items in the score_dict
    for uid, score in adjusted_to_sigmoid.items():
        # Calculate the normalized weight as an integer
        weight = int(score * MAX_ALLOWED_WEIGHTS / scores)

        # Add the weighted evaluation to the new dictionary
        weighted_scores[uid] = weight

    # filter out 0 weights
    weighted_scores = {k: v for k, v in weighted_scores.items() if v != 0}

    uids = list(weighted_scores.keys())
    weights = list(weighted_scores.values())

    try:
        vote(key, client, uids, weights, netuid)
    except Exception as e:
        print(f"Failed to set weights with exception: {e}. Will retry.")
        sleep_time = random.uniform(1, 2)
        sleep(sleep_time)

        # Try another client just in case the first one fails
        client = CommuneClient(get_node_url())
        vote(key, client, uids, weights, netuid)


def _cut_to_max_allowed_uids(score_dict: dict[int, float]) -> dict[int, float]:
    # sort the evaluation by highest to lowest
    sorted_scores = sorted(score_dict.items(), key=lambda x: x[1], reverse=True)

    # cut to max_allowed_weights
    cut_scores = sorted_scores[:MAX_ALLOWED_UIDS]

    return dict(cut_scores)
