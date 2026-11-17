"""Run assignments against temporary shared storage without a coordinator."""

import json
import os
from pathlib import Path
import subprocess
import sys

import pytest

from task_runtime import partition_for, run_task


ROOT = Path(__file__).resolve().parents[1]
# Echo records unchanged so split tests can inspect the exact offsets and values
# delivered to a mapper, independently of word-count behavior.
IDENTITY_JOB = """
def map(key, value):
    yield key, value

def reduce(key, values):
    for value in values:
        yield key, value
"""


def prepare_job(shared, code=IDENTITY_JOB):
    """Build the coordinator/client's expected layout inside pytest's tmp_path."""
    job_dir = shared / "jobs" / "test-job"
    job_dir.mkdir(parents=True)
    (job_dir / "job.py").write_text(code, encoding="utf-8")
    (shared / "inputs" / "sample").mkdir(parents=True)
    return job_dir


def map_assignment(index, segments, reduces=3, combine=False):
    return dict(job_id="test-job", task_id=f"m-{index}", attempt_id="attempt",
                phase="map", segments=segments, reduces=reduces, combine=combine)


def staged(job_dir, task_id):
    return job_dir / "tmp" / f"{task_id}-attempt"


def read_rows(path):
    return [json.loads(line) for line in path.read_text(encoding="utf-8").splitlines()]


def map_rows(job_dir, index, reduces=3):
    return [row for partition in range(reduces)
            for row in read_rows(staged(job_dir, f"m-{index}") / f"r-{partition}.jsonl")]


@pytest.mark.parametrize("combine", [False, True])
def test_wordcount_across_maps_and_reducers(tmp_path, combine):
    # Both modes must produce the same final counts across multiple map outputs.
    job_dir = prepare_job(tmp_path, (ROOT / "examples" / "wordcount.py").read_text())
    relative = "inputs/sample/input.txt"
    data = (ROOT / "examples" / "input.txt").read_bytes()
    (tmp_path / relative).write_bytes(data)
    (job_dir / "map").mkdir()

    for index, (start, end) in enumerate([(0, 12), (12, len(data))]):
        assignment = map_assignment(index, [dict(path=relative, start=start, end=end)],
                                    combine=combine)
        run_task(tmp_path, assignment)
        for partition in range(3):
            path = staged(job_dir, f"m-{index}") / f"r-{partition}.jsonl"
            assert path.exists()
            assert all(partition_for(key, 3) == partition for key, _ in read_rows(path))
        # Simulate the coordinator committing a successful map attempt.
        staged(job_dir, f"m-{index}").rename(job_dir / "map" / f"m-{index}")

    output = []
    for partition in range(3):
        run_task(tmp_path, dict(job_id="test-job", task_id=f"r-{partition}",
                               attempt_id="attempt", phase="reduce", partition=partition,
                               maps=2))
        rows = read_rows(staged(job_dir, f"r-{partition}") / "part.jsonl")
        assert all(partition_for(key, 3) == partition for key, _ in rows)
        output.extend(rows)
    assert sorted(output) == [["hello", 2], ["world", 1]]


def test_every_byte_split_preserves_lines_and_offsets(tmp_path):
    job_dir = prepare_job(tmp_path)
    relative = "inputs/sample/input.txt"
    data = "éclair\r\n  hello  \nlast\r".encode("utf-8")
    (tmp_path / relative).write_bytes(data)
    expected = [[f"{relative}:0", "éclair"],
                [f"{relative}:9", "  hello  "],
                [f"{relative}:19", "last\r"]]
    # Includes UTF-8 interiors, CRLF interiors, exact boundaries and empty ranges.
    for boundary in range(len(data) + 1):
        actual = []
        for index, (start, end) in enumerate([(0, boundary), (boundary, len(data))]):
            run_task(tmp_path, map_assignment(index, [dict(path=relative, start=start, end=end)]))
            actual.extend(map_rows(job_dir, index))
        assert sorted(actual) == sorted(expected)


@pytest.mark.parametrize("filename", ["input.jsonl", ".jsonl"])
def test_jsonl_parses_values(tmp_path, filename):
    job_dir = prepare_job(tmp_path)
    relative = f"inputs/sample/{filename}"
    data = b'{"message":"hello"}\n[1,2]\n'
    (tmp_path / relative).write_bytes(data)
    run_task(tmp_path, map_assignment(0, [dict(path=relative, start=0, end=len(data))]))
    assert sorted(map_rows(job_dir, 0)) == sorted([
        [f"{relative}:0", {"message": "hello"}], [f"{relative}:20", [1, 2]]])


def test_malformed_json_fails(tmp_path):
    prepare_job(tmp_path)
    relative = "inputs/sample/bad.jsonl"
    (tmp_path / relative).write_text("not json\n")
    with pytest.raises(json.JSONDecodeError):
        run_task(tmp_path, map_assignment(0, [dict(path=relative, start=0, end=9)]))


def test_empty_map_and_reduce_create_files(tmp_path):
    job_dir = prepare_job(tmp_path)
    run_task(tmp_path, map_assignment(0, []))
    assert map_rows(job_dir, 0) == []
    (job_dir / "map").mkdir()
    staged(job_dir, "m-0").rename(job_dir / "map" / "m-0")
    run_task(tmp_path, dict(job_id="test-job", task_id="r-0", attempt_id="attempt",
                           phase="reduce", partition=0, maps=1))
    assert (staged(job_dir, "r-0") / "part.jsonl").read_bytes() == b""


def test_missing_committed_map_fails(tmp_path):
    prepare_job(tmp_path)
    with pytest.raises(FileNotFoundError):
        run_task(tmp_path, dict(job_id="test-job", task_id="r-0", attempt_id="attempt",
                               phase="reduce", partition=0, maps=1))


@pytest.mark.parametrize("expression", ["123", "('key',)", "(42, 1)", "('key', {1, 2})"])
def test_invalid_emitted_pairs_fail(tmp_path, expression):
    code = f"def map(key, value):\n    yield {expression}\n\ndef reduce(key, values):\n    yield key, 1\n"
    prepare_job(tmp_path, code)
    relative = "inputs/sample/input.txt"
    (tmp_path / relative).write_text("hello\n")
    with pytest.raises((ValueError, TypeError)):
        run_task(tmp_path, map_assignment(0, [dict(path=relative, start=0, end=6)]))


def test_combiner_reduces_rows_and_rejects_changed_keys(tmp_path):
    code = (ROOT / "examples" / "wordcount.py").read_text()
    job_dir = prepare_job(tmp_path, code)
    relative = "inputs/sample/input.txt"
    (tmp_path / relative).write_text("hello hello hello\n")
    segments = [dict(path=relative, start=0, end=18)]
    run_task(tmp_path, map_assignment(0, segments, combine=True))
    assert map_rows(job_dir, 0) == [["hello", 3]]
    (job_dir / "job.py").write_text(code + '\ndef combine(key, values):\n    yield "changed", sum(values)\n')
    with pytest.raises(ValueError, match="preserve"):
        run_task(tmp_path, map_assignment(1, segments, combine=True))


def test_missing_required_function_fails(tmp_path):
    prepare_job(tmp_path, "map = 1\nreduce = None\n")
    with pytest.raises(ValueError, match="callable map"):
        run_task(tmp_path, map_assignment(0, []))


def test_partitions_are_stable_across_processes():
    script = "from task_runtime import partition_for; print(partition_for('éclair', 7))"
    # Different hash seeds catch accidental use of Python's randomized hash().
    for seed in ("1", "123"):
        result = subprocess.check_output([sys.executable, "-c", script], cwd=ROOT,
                                         env={**os.environ, "PYTHONHASHSEED": seed}, text=True)
        assert int(result) == partition_for("éclair", 7)
