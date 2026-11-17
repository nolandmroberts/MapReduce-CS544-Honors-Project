"""End-to-end tests against the running Docker Compose services.

These tests are skipped unless RUN_COMPOSE_TESTS=1. Start the services first:

    docker compose up --build -d
    RUN_COMPOSE_TESTS=1 python -m pytest -q tests/test_end_to_end.py

They drive the real client.py as a subprocess, so they cover the whole path:
upload -> submit -> coordinator -> workers -> shared storage -> results.
"""

import json
import os
import subprocess
import sys
import time
import uuid
from collections import Counter
from pathlib import Path

import pytest
import requests

pytestmark = pytest.mark.skipif(
    os.environ.get("RUN_COMPOSE_TESTS") != "1",
    reason="set RUN_COMPOSE_TESTS=1 with the Compose services running",
)

REPO_ROOT = Path(__file__).resolve().parent.parent
SHARED_DIR = Path(os.environ.get("SHARED_DIR", REPO_ROOT / "shared")).resolve()
COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://127.0.0.1:8080").rstrip("/")
EXPECTED_WORKERS = 4
JOB_DEADLINE_SECONDS = 120
POLL_INTERVAL_SECONDS = 0.5

INPUT_TEXT = "hello world\nhello\n"
EXPECTED_COUNTS = {"hello": 2, "world": 1}

WORDCOUNT_JOB = '''\
def map(key, value):
    for word in value.split():
        yield word.lower(), 1


def reduce(key, values):
    yield key, sum(values)


def combine(key, values):
    yield key, sum(values)
'''

FAILING_JOB = '''\
def map(key, value):
    raise RuntimeError("map failed on purpose")
    yield key, value


def reduce(key, values):
    yield key, sum(values)
'''


def unique_name(label):
    return f"e2e-{label}-{uuid.uuid4().hex[:8]}"


def run_client(*args, check=True):
    """Run client.py the way a user would and return the finished process."""
    env = dict(os.environ, SHARED_DIR=str(SHARED_DIR), COORDINATOR_URL=COORDINATOR_URL)
    result = subprocess.run(
        [sys.executable, "client.py", *args],
        cwd=REPO_ROOT,
        env=env,
        capture_output=True,
        text=True,
        timeout=30,
    )
    if check and result.returncode != 0:
        pytest.fail(f"client.py {' '.join(args)} exited {result.returncode}:\n{result.stderr}")
    return result


def wait_for_job(job_id):
    """Poll status until the job reaches a terminal state or the deadline passes."""
    deadline = time.monotonic() + JOB_DEADLINE_SECONDS
    while True:
        status = json.loads(run_client("status", job_id).stdout)
        if status["state"] in ("succeeded", "failed"):
            return status
        if time.monotonic() > deadline:
            pytest.fail(f"job {job_id} did not finish within {JOB_DEADLINE_SECONDS}s; last status: {status}")
        time.sleep(POLL_INTERVAL_SECONDS)


def read_output(output_dir):
    """Return {part file name: [(key, value), ...]} for every part file."""
    parts = {}
    for path in sorted(Path(output_dir).glob("part-*.jsonl")):
        rows = []
        for line in path.read_text(encoding="utf-8").splitlines():
            if line.strip():
                key, value = json.loads(line)
                rows.append((key, value))
        parts[path.name] = rows
    return parts


@pytest.fixture(scope="module", autouse=True)
def services_are_up():
    try:
        requests.get(f"{COORDINATOR_URL}/jobs/does-not-exist", timeout=5)
    except requests.RequestException as error:
        pytest.fail(
            f"coordinator is not reachable at {COORDINATOR_URL} ({error}). "
            "Start it with: docker compose up --build -d"
        )


@pytest.fixture(scope="module")
def uploaded_input(tmp_path_factory):
    source = tmp_path_factory.mktemp("e2e") / "input.txt"
    source.write_text(INPUT_TEXT, encoding="utf-8")
    name = unique_name("input")
    assert run_client("upload", str(source), "--name", name).stdout.strip() == name
    return name


@pytest.fixture(scope="module")
def job_files(tmp_path_factory):
    directory = tmp_path_factory.mktemp("jobs")
    wordcount = directory / "wordcount.py"
    wordcount.write_text(WORDCOUNT_JOB, encoding="utf-8")
    failing = directory / "failing.py"
    failing.write_text(FAILING_JOB, encoding="utf-8")
    return {"wordcount": wordcount, "failing": failing}


def test_four_workers_are_running():
    result = subprocess.run(
        ["docker", "compose", "ps", "-q", "worker"],
        cwd=REPO_ROOT,
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, result.stderr
    assert len(result.stdout.split()) == EXPECTED_WORKERS


@pytest.mark.parametrize("combine", [True, False], ids=["combine", "no-combine"])
@pytest.mark.parametrize("maps,reduces", [(1, 1), (8, 4)], ids=["1x1", "8x4"])
def test_word_count(uploaded_input, job_files, maps, reduces, combine):
    output = unique_name(f"wc-{maps}x{reduces}")
    args = [
        "submit", "--input", uploaded_input, "--output", output,
        "--code", str(job_files["wordcount"]),
        "--maps", str(maps), "--reduces", str(reduces),
    ]
    if not combine:
        args.append("--no-combine")
    job_id = run_client(*args).stdout.strip()
    assert job_id

    status = wait_for_job(job_id)

    assert status["state"] == "succeeded", status
    assert status["error"] is None
    assert status["map"] == {"done": maps, "total": maps}
    assert status["reduce"] == {"done": reduces, "total": reduces}
    assert status["seconds"]["total"] is not None

    output_dir = run_client("results", job_id).stdout.strip()
    parts = read_output(output_dir)
    assert sorted(parts) == [f"part-{j}.jsonl" for j in range(reduces)]

    # Each key must come out of exactly one reducer, and the totals must match.
    keys = Counter(key for rows in parts.values() for key, _ in rows)
    assert all(count == 1 for count in keys.values()), keys
    counts = {key: value for rows in parts.values() for key, value in rows}
    assert counts == EXPECTED_COUNTS


def test_failing_job_reports_error(uploaded_input, job_files):
    job_id = run_client(
        "submit", "--input", uploaded_input, "--output", unique_name("fail"),
        "--code", str(job_files["failing"]), "--maps", "1", "--reduces", "1",
    ).stdout.strip()

    status = wait_for_job(job_id)

    assert status["state"] == "failed"
    assert "map failed on purpose" in status["error"]
    result = run_client("results", job_id, check=False)
    assert result.returncode != 0
    assert "failed" in result.stderr


def test_reused_output_name_is_rejected(uploaded_input, job_files):
    output = unique_name("dup")
    submit = [
        "submit", "--input", uploaded_input, "--output", output,
        "--code", str(job_files["wordcount"]), "--maps", "1", "--reduces", "1",
    ]
    wait_for_job(run_client(*submit).stdout.strip())

    result = run_client(*submit, check=False)

    assert result.returncode != 0
    assert "409" in result.stderr


def test_unknown_input_is_rejected(job_files):
    result = run_client(
        "submit", "--input", unique_name("missing"), "--output", unique_name("out"),
        "--code", str(job_files["wordcount"]), "--maps", "1", "--reduces", "1",
        check=False,
    )

    assert result.returncode != 0
    assert "400" in result.stderr
