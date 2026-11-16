"""Exercise the worker with an in-process fake coordinator HTTP transport."""

from copy import deepcopy
import json
import threading

import pytest
import requests

import worker


ASSIGNMENT = dict(job_id="job", task_id="m-0", attempt_id="try-1", phase="map",
                  segments=[], reduces=2, combine=True)


def response(status, body=None):
    result = requests.Response()
    result.status_code = status
    result._content = json.dumps(body).encode()
    result._content_consumed = True  # Match requests.post's default buffered response.
    return result


class Peer:
    """Record requests and provide scripted claim/completion responses."""

    def __init__(self):
        self.claims = [response(204), response(200, ASSIGNMENT)]
        self.completions = [response(200, {"accepted": True})]
        self.calls = []
        self.finished = threading.Event()
        self.heartbeat = threading.Event()

    def post(self, url, json, timeout):
        assert timeout > 0
        self.calls.append((url, deepcopy(json)))
        if url.endswith("/workers/heartbeat"):
            self.heartbeat.set()
            return response(204)
        if url.endswith("/tasks/claim"):
            return self.claims.pop(0) if self.claims else response(204)
        assert url.endswith("/tasks/complete")
        result = self.completions.pop(0)
        if isinstance(result, Exception):
            raise result
        if result.status_code == 200:
            self.finished.set()
        return result


@pytest.fixture
def setup_worker(tmp_path, monkeypatch):
    peer = Peer()
    monkeypatch.setattr(worker.requests, "post", peer.post)
    monkeypatch.setattr(worker, "HEARTBEAT_INTERVAL", 0.01)
    monkeypatch.setattr(worker, "POLL_INTERVAL", 0.001)
    instance = worker.Worker(tmp_path, "http://coordinator:8080/")
    return instance, peer


def run_until_complete(instance, peer):
    # Bound the whole test and always stop the worker, including on failure.
    stop = threading.Event()
    errors = []

    def target():
        try:
            instance.run(stop)
        except BaseException as error:
            errors.append(error)

    thread = threading.Thread(target=target, daemon=True)
    thread.start()
    try:
        assert peer.finished.wait(3), "Worker did not report completion"
    finally:
        stop.set()
        thread.join(timeout=3)
    assert not thread.is_alive()
    if errors:
        raise errors[0]


@pytest.mark.parametrize("accepted", [True, False])
def test_idle_claim_heartbeats_success_and_cleanup(setup_worker, monkeypatch, capsys, accepted):
    instance, peer = setup_worker
    peer.completions = [response(200, {"accepted": accepted})]
    staging = instance.shared_dir / "jobs/job/tmp/m-0-try-1"
    executions = []

    def execute(shared_dir, assignment):
        executions.append(assignment)
        staging.mkdir(parents=True)
        (staging / "r-0.jsonl").write_text("")
        # Require a fresh heartbeat while task execution is blocked.
        peer.heartbeat.clear()
        assert peer.heartbeat.wait(1)

    monkeypatch.setattr(worker, "run_task", execute)
    run_until_complete(instance, peer)
    assert executions == [ASSIGNMENT]
    assert not staging.exists()
    reports = [body for url, body in peer.calls if url.endswith("/tasks/complete")]
    assert reports == [dict(job_id="job", task_id="m-0", attempt_id="try-1",
                            worker_id=instance.worker_id, outcome="success")]
    assert json.loads(capsys.readouterr().out) == {"worker_id": instance.worker_id}
    assert all(body["worker_id"] == instance.worker_id for _, body in peer.calls)


def test_failure_reports_traceback_and_cleans_partial_output(setup_worker, monkeypatch):
    instance, peer = setup_worker
    staging = instance.shared_dir / "jobs/job/tmp/m-0-try-1"

    def fail(shared_dir, assignment):
        staging.mkdir(parents=True)
        raise RuntimeError("bad user function")

    monkeypatch.setattr(worker, "run_task", fail)
    run_until_complete(instance, peer)
    report = next(body for url, body in peer.calls if url.endswith("/tasks/complete"))
    assert report["outcome"] == "failure"
    assert "Traceback" in report["error"]
    assert "RuntimeError: bad user function" in report["error"]
    assert not staging.exists()


def test_completion_retries_same_report_without_rerunning(setup_worker, monkeypatch):
    instance, peer = setup_worker
    peer.completions = [requests.ConnectionError("lost response"), response(503),
                        response(200, {"accepted": False})]
    executions = []
    monkeypatch.setattr(worker, "run_task", lambda *args: executions.append(args))
    run_until_complete(instance, peer)
    assert len(executions) == 1
    completion_indices = [i for i, (url, _) in enumerate(peer.calls)
                          if url.endswith("/tasks/complete")]
    reports = [peer.calls[i][1] for i in completion_indices]
    assert len(reports) == 3
    assert reports[0] == reports[1] == reports[2]
    # Heartbeats may interleave; new claims must wait for acknowledgment.
    assert not any(url.endswith("/tasks/claim")
                   for url, _ in peer.calls[completion_indices[0]:completion_indices[-1]])


def test_lost_claim_response_retries_with_same_worker_id(setup_worker, monkeypatch):
    instance, peer = setup_worker
    original = peer.post
    lost = False

    def post(url, **kwargs):
        nonlocal lost
        if url.endswith("/tasks/claim") and not lost:
            lost = True
            peer.calls.append((url, deepcopy(kwargs["json"])))
            raise requests.Timeout("lost claim response")
        return original(url, **kwargs)

    monkeypatch.setattr(worker.requests, "post", post)
    monkeypatch.setattr(worker, "run_task", lambda *args: None)
    run_until_complete(instance, peer)
    claims = [body for url, body in peer.calls if url.endswith("/tasks/claim")]
    assert len(claims) >= 3
    assert all(body == {"worker_id": instance.worker_id} for body in claims)


def test_delay_keeps_heartbeats_active(setup_worker, monkeypatch):
    instance, peer = setup_worker
    instance.task_delay = 0.06
    counts = []

    def execute(*args):
        counts.append(sum(url.endswith("/workers/heartbeat") for url, _ in peer.calls))

    monkeypatch.setattr(worker, "run_task", execute)
    run_until_complete(instance, peer)
    assert counts[0] >= 2


def test_shutdown_during_report_retains_staging(setup_worker, monkeypatch):
    instance, peer = setup_worker
    stop = threading.Event()
    staging = instance.shared_dir / "jobs/job/tmp/m-0-try-1"
    staging.mkdir(parents=True)

    def post(url, **kwargs):
        if url.endswith("/tasks/complete"):
            stop.set()
            raise requests.ConnectionError("coordinator unavailable")
        return peer.post(url, **kwargs)

    monkeypatch.setattr(worker.requests, "post", post)
    monkeypatch.setattr(worker, "run_task", lambda *args: None)
    instance.run(stop)
    assert staging.exists()


def test_heartbeat_recovers_from_connection_failure(setup_worker, monkeypatch):
    instance, peer = setup_worker
    attempts = []

    def post(url, **kwargs):
        if url.endswith("/workers/heartbeat"):
            attempts.append(url)
            if len(attempts) == 1:
                raise requests.ConnectionError("temporary outage")
        return peer.post(url, **kwargs)

    def execute(*args):
        assert peer.heartbeat.wait(1), "Heartbeats did not resume after the outage"

    monkeypatch.setattr(worker.requests, "post", post)
    monkeypatch.setattr(worker, "run_task", execute)
    run_until_complete(instance, peer)
    assert len(attempts) >= 2


def test_worker_runs_real_runtime_and_coordinator_publishes(setup_worker, monkeypatch):
    instance, peer = setup_worker
    job_dir = instance.shared_dir / "jobs/job"
    job_dir.mkdir(parents=True)
    (job_dir / "job.py").write_text(
        "def map(key, value):\n    yield value.lower(), 1\n"
        "def reduce(key, values):\n    yield key, sum(values)\n")
    source = instance.shared_dir / "inputs/sample/input.txt"
    source.parent.mkdir(parents=True)
    source.write_bytes(b"HELLO\n")
    assignment = {**ASSIGNMENT, "segments": [dict(path="inputs/sample/input.txt", start=0, end=6)]}
    peer.claims = [response(200, assignment)]
    original = peer.post

    def post(url, **kwargs):
        if url.endswith("/tasks/complete"):
            assert kwargs["json"]["outcome"] == "success"
            staging = job_dir / "tmp/m-0-try-1"
            rows = [row for path in staging.glob("*.jsonl")
                    for row in path.read_text().splitlines()]
            assert [json.loads(row) for row in rows] == [["hello", 1]]
            # Publication moves the staging directory before acknowledging success.
            (job_dir / "map").mkdir()
            staging.rename(job_dir / "map/m-0")
        return original(url, **kwargs)

    monkeypatch.setattr(worker.requests, "post", post)
    run_until_complete(instance, peer)
    assert (job_dir / "map/m-0").is_dir()


@pytest.mark.parametrize("delay", [-1, float("nan"), float("inf")])
def test_invalid_delay_rejected(tmp_path, delay):
    with pytest.raises(ValueError, match="finite nonnegative"):
        worker.Worker(tmp_path, "http://coordinator:8080", delay)
