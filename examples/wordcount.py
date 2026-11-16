"""Lowercase whitespace-delimited word count."""


def map(key, value):
    # The input key identifies the line's location; word count only needs its text.
    # yield emits one pair per word, allowing a line to produce many output rows.
    for word in value.lower().split():
        yield word, 1


def reduce(key, values):
    # The runtime supplies all counts for this word as an iterator.
    yield key, sum(values)


def combine(key, values):
    # Sum locally within a map task to write fewer intermediate rows. Summation
    # works on both raw 1s and partial totals, so the reducer handles either form.
    yield key, sum(values)
