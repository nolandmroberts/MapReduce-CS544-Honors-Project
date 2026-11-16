"""Word count with extra CPU work per word.

It gives the same result as wordcount.py, but each map call does some hashing
so the job is CPU-bound. That makes the effect of adding one-CPU workers easy
to see in the parallelism, speculation, and worker-loss experiments.
"""

import hashlib

ROUNDS = 20


def map(key, value):
    for word in value.split():
        digest = word.encode("utf-8")
        for _ in range(ROUNDS):
            digest = hashlib.sha256(digest).digest()
        yield word, 1


def reduce(key, values):
    yield key, sum(values)


def combine(key, values):
    yield key, sum(values)
