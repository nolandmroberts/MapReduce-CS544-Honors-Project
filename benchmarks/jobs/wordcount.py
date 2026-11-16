"""Plain word count, used for the combiner experiment and as the reference result."""


def map(key, value):
    for word in value.split():
        yield word, 1


def reduce(key, values):
    yield key, sum(values)


def combine(key, values):
    yield key, sum(values)
