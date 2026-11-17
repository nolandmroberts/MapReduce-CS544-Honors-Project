"""Execute one assignment, leaving staged files for the coordinator to publish."""

import importlib.util
import json
from itertools import groupby
from pathlib import Path
import uuid
import zlib


def partition_for(key, reduces):
    """Use the same partition in every Python process."""
    # Python's built-in hash() varies between processes. CRC32 keeps all values
    # for a key together even when different workers produce them.
    return zlib.crc32(key.encode("utf-8")) % reduces


def _load_job(path):
    """Import the submitted source and check the required job interface."""
    # Each assignment gets a fresh module object, so module globals are not
    # shared between task executions. This executes trusted user Python code.
    spec = importlib.util.spec_from_file_location(f"job_{uuid.uuid4().hex}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    for name in ("map", "reduce"):
        if not callable(getattr(module, name, None)):
            raise ValueError(f"Job must define a callable {name}")
    return module


def _validate_pair(pair):
    if not isinstance(pair, (tuple, list)) or len(pair) != 2:
        raise ValueError("Job functions must yield (key, value) pairs")
    key, value = pair
    if not isinstance(key, str):
        raise ValueError("Output keys must be strings")
    # Reject unserializable values before buffering them for a combiner;
    # otherwise combining could hide invalid mapper output. NaN/Infinity are
    # excluded because they are not standard JSON numbers.
    json.dumps([key, value], ensure_ascii=False, allow_nan=False)
    return key, value


def _write_pair(output, pair):
    # One JSON array per line is the shared format for intermediate and final rows.
    key, value = _validate_pair(pair)
    output.write(json.dumps([key, value], ensure_ascii=False, allow_nan=False) + "\n")


def _records(shared_dir, segment):
    """Yield lines whose first byte belongs to this file-local byte range."""
    start, end = segment["start"], segment["end"]
    if start >= end:
        return
    relative_path = segment["path"]
    # Binary mode keeps tell()/seek() in bytes, matching coordinator assignments
    # even when characters occupy several bytes or lines use CRLF endings.
    with (shared_dir / relative_path).open("rb") as source:
        if start > 0:
            source.seek(start - 1)
            if source.read(1) != b"\n":
                source.readline()  # Skip a partial line before decoding UTF-8.
        # Ownership depends on the line's starting byte. Read the entire owned
        # line even if it extends past end; the next segment skips its remainder.
        while source.tell() < end:
            offset = source.tell()
            line = source.readline()
            if not line:
                break
            if line.endswith(b"\n"):
                # Remove only the line ending; spaces and a standalone CR are data.
                line = line[:-1]
                if line.endswith(b"\r"):
                    line = line[:-1]
            value = line.decode("utf-8")
            if relative_path.endswith(".jsonl"):
                value = json.loads(value)
            # The original file and byte offset identify the mapper's input record.
            yield f"{relative_path}:{offset}", value


def _groups(rows):
    # groupby merges adjacent equal keys, so sorting must come first. Buffering
    # and sorting in memory keeps this implementation simple for course datasets.
    rows.sort(key=lambda pair: pair[0])
    for key, pairs in groupby(rows, key=lambda pair: pair[0]):
        # Consume this iterator before requesting the next group: groupby shares
        # its underlying iterator. Map/reduce functions consume it synchronously.
        yield key, (value for _, value in pairs)


def _run_map(shared_dir, staging_dir, assignment, module):
    # Each bucket becomes one reducer's input from this map task.
    partitions = [[] for _ in range(assignment["reduces"])]
    for segment in assignment["segments"]:
        for record_key, value in _records(shared_dir, segment):
            for pair in module.map(record_key, value):
                key, emitted_value = _validate_pair(pair)
                partitions[partition_for(key, len(partitions))].append((key, emitted_value))

    combiner = getattr(module, "combine", None)
    # Combining is optional: jobs without it still produce valid reducer input.
    use_combiner = assignment.get("combine", True) and callable(combiner)
    for partition, rows in enumerate(partitions):
        # Create every partition file, including empty ones, so reducers can
        # distinguish a valid empty partition from a missing map output.
        with (staging_dir / f"r-{partition}.jsonl").open("w", encoding="utf-8") as output:
            if use_combiner:
                for key, values in _groups(rows):
                    for pair in combiner(key, values):
                        combined_key, value = _validate_pair(pair)
                        if combined_key != key:
                            # Changing the key could require a different partition.
                            raise ValueError("Combiner must preserve the input key")
                        _write_pair(output, (combined_key, value))
            else:
                for pair in rows:
                    _write_pair(output, pair)


def _run_reduce(job_dir, staging_dir, assignment, module):
    rows = []
    # Read this reducer's partition from every committed map. Let a missing file
    # fail the attempt rather than silently producing an incomplete result.
    for map_index in range(assignment["maps"]):
        path = job_dir / "map" / f"m-{map_index}" / f"r-{assignment['partition']}.jsonl"
        with path.open(encoding="utf-8") as source:
            rows.extend(_validate_pair(json.loads(line)) for line in source)
    with (staging_dir / "part.jsonl").open("w", encoding="utf-8") as output:
        # Values for a key may come from many maps, with or without combining.
        for key, values in _groups(rows):
            for pair in module.reduce(key, values):
                _write_pair(output, pair)


def run_task(shared_dir, assignment):
    """Run trusted job code; return only after closing all staged output files.

    Exceptions propagate to the worker, which reports a failed attempt. Only the
    coordinator may move staged files into committed map/output directories.
    """
    shared_dir = Path(shared_dir)
    phase = assignment["phase"]
    if phase not in ("map", "reduce"):
        raise ValueError(f"Unknown task phase: {phase}")
    job_dir = shared_dir / "jobs" / assignment["job_id"]
    module = _load_job(job_dir / "job.py")
    # Retries and speculative copies have different attempt IDs. Their output
    # stays separate until the coordinator chooses and publishes one winner.
    staging_dir = job_dir / "tmp" / f"{assignment['task_id']}-{assignment['attempt_id']}"
    staging_dir.mkdir(parents=True, exist_ok=True)
    if phase == "map":
        _run_map(shared_dir, staging_dir, assignment, module)
    else:
        _run_reduce(job_dir, staging_dir, assignment, module)
