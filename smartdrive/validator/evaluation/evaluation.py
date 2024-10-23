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

from substrateinterface import Keypair

from smartdrive.commune.connection_pool import vote
from smartdrive.validator.evaluation.sigmoid import threshold_sigmoid_reward_distribution

MAX_ALLOWED_WEIGHTS = 256
MAX_ALLOWED_UIDS = 256


def score_miners(result_miners: dict[int, bool]) -> dict[int, float]:
    """
    Calculate the score for a miner based on the response

    Params:
        result_miners (dict[int, bool]): A dictionary mapping miner UIDs to their success responses in validation.

    Returns:
        score_dict (dict[int, float]): A dictionary mapping miner UIDs to their scores.

    """

    # TODO: Right now the miner scoring is quite basic, in the future it will be changed to another system
    score_dict = {}
    for uid, success in result_miners.items():
        score_dict[uid] = 1.0 if success else 0.0

    return score_dict


async def set_weights(score_dict: dict[int, float], netuid: int, key: Keypair):
    """
    Set weights for miners based on their scores.

    Params:
        score_dict (dict[int, float]): A dictionary mapping miner UIDs to their scores.
        netuid (int): The network UID.
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
        weighted_scores[uid] = int(score * MAX_ALLOWED_WEIGHTS / scores)

    # filter out 0 weights
    weighted_scores = {k: v for k, v in weighted_scores.items() if v != 0}

    uids = list(weighted_scores.keys())
    weights = list(weighted_scores.values())

    await vote(key, uids, weights, netuid)


def _cut_to_max_allowed_uids(score_dict: dict[int, float]) -> dict[int, float]:
    # sort the evaluation by highest to lowest
    sorted_scores = sorted(score_dict.items(), key=lambda x: x[1], reverse=True)

    # cut to max_allowed_weights
    cut_scores = sorted_scores[:MAX_ALLOWED_UIDS]

    return dict(cut_scores)
