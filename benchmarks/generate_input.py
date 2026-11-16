"""Generate a deterministic word-count input file for the benchmarks.

Examples:
    # ~60 MB of text with a large vocabulary (parallelism, speculation, worker loss)
    python benchmarks/generate_input.py benchmarks/data/words.txt --lines 1000000

    # many repeated keys, so a combiner has a lot to merge (combiner experiment)
    python benchmarks/generate_input.py benchmarks/data/repeated.txt --lines 1000000 --vocab 50 --skew 1.2
"""

import argparse
import itertools
import os
import random


def vocabulary(size):
    return [f"word{i:05d}" for i in range(size)]


def generate(path, lines, words_per_line=10, vocab=20000, skew=1.0, seed=544):
    """Write `lines` lines of space-separated words and return the total word count.

    Words are drawn from a Zipf-like distribution: word i has weight 1 / (i + 1) ** skew,
    so a higher skew means more repeated keys. The same arguments always produce
    the same file.
    """
    rng = random.Random(seed)
    words = vocabulary(vocab)
    cumulative = list(itertools.accumulate(1 / (i + 1) ** skew for i in range(vocab)))
    os.makedirs(os.path.dirname(os.path.abspath(path)), exist_ok=True)
    with open(path, "w", encoding="utf-8", newline="\n") as f:
        for _ in range(lines):
            f.write(" ".join(rng.choices(words, cum_weights=cumulative, k=words_per_line)))
            f.write("\n")
    return lines * words_per_line


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("path", help="output .txt file")
    parser.add_argument("--lines", type=int, default=1_000_000)
    parser.add_argument("--words-per-line", type=int, default=10)
    parser.add_argument("--vocab", type=int, default=20000, help="number of distinct words")
    parser.add_argument("--skew", type=float, default=1.0, help="Zipf exponent; higher = more repeated words")
    parser.add_argument("--seed", type=int, default=544)
    args = parser.parse_args(argv)
    total = generate(args.path, args.lines, args.words_per_line, args.vocab, args.skew, args.seed)
    size_mb = os.path.getsize(args.path) / 1e6
    print(f"wrote {args.path}: {args.lines} lines, {total} words, {size_mb:.1f} MB")


if __name__ == "__main__":
    main()
