import json
from pathlib import Path

import pytest

from scheduler import Scheduler


@pytest.fixture
def clock():
    return [0.0]


@pytest.fixture
def scheduler(tmp_path, clock):
    return Scheduler(tmp_path, lambda: clock[0])


def add_job(scheduler, tmp_path, job_id="job", maps=1, speculation=True):
    (tmp_path / "jobs" / job_id / "tmp").mkdir(parents=True)
    (tmp_path / "jobs" / job_id / "map").mkdir()
    (tmp_path / "outputs" / job_id).mkdir(parents=True)
    scheduler.add_job(job_id, job_id, [[] for _ in range(maps)], 1, combine=True, speculation=speculation)


def finish(scheduler, task, worker, error=None):
    outcome = "success" if error is None else "failure"
    return scheduler.complete(task["job_id"], task["task_id"], task["attempt_id"], worker,
                              outcome=outcome, error=error)


def stage(tmp_path, task, text=""):
    directory = tmp_path / "jobs" / task["job_id"] / "tmp" / f"{task['task_id']}-{task['attempt_id']}"
    directory.mkdir()
    filename = "r-0.jsonl" if task["phase"] == "map" else "part.jsonl"
    (directory / filename).write_text(text, encoding="utf-8")
    return directory


def test_timeout_at_ten_seconds_leaves_committed_maps_and_rejects_late_result(scheduler, tmp_path, clock, capsys):
    add_job(scheduler, tmp_path, maps=2, speculation=False)
    first = scheduler.claim("healthy")
    clock[0] = 1.0
    stage(tmp_path, first, "first\n")
    assert finish(scheduler, first, "healthy")
    lost = scheduler.claim("lost")
    stale = stage(tmp_path, lost, "stale\n")

    clock[0] = 10.9
    assert scheduler.claim("spare") is None
    clock[0] = 11.0
    scheduler.heartbeat("lost")  # A late heartbeat cannot revive a lease.
    retry = scheduler.claim("spare")
    assert retry["task_id"] == lost["task_id"]
    assert retry["attempt_id"] != lost["attempt_id"]
    assert not finish(scheduler, lost, "lost")
    assert stale.is_dir()
    assert (tmp_path / "jobs/job/map/m-0/r-0.jsonl").read_text() == "first\n"

    stage(tmp_path, retry, "retry\n")
    assert finish(scheduler, retry, "spare")
    reducer = scheduler.claim("spare")
    clock[0] = 12.0
    stage(tmp_path, reducer)
    assert finish(scheduler, reducer, "spare")
    assert scheduler.status("job")["seconds"] == {"map": 11.0, "reduce": 1.0, "total": 12.0}
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert next(event for event in events if event["event"] == "expired") == {
        "event": "expired", "job_id": "job", "task_id": lost["task_id"],
        "attempt_id": lost["attempt_id"], "worker_id": "lost", "speculative": False, "elapsed_s": 11.0,
    }


def test_three_expirations_fail_the_job(scheduler, tmp_path, clock):
    add_job(scheduler, tmp_path)
    for index in range(3):
        task = scheduler.claim(f"worker-{index}")
        assert task["task_id"] == "m-0"
        clock[0] += 10.0
    status = scheduler.status("job")
    assert status["state"] == "failed" and "worker-2" in status["error"]
    assert status["seconds"] == {"map": None, "reduce": None, "total": 30.0}
    assert scheduler.claim("spare") is None


def finish_quick_maps(scheduler, tmp_path, clock):
    slow = scheduler.claim("slow")
    quick = [(f"quick-{index}", scheduler.claim(f"quick-{index}")) for index in range(3)]
    clock[0] = 1.0
    for worker, task in quick:
        stage(tmp_path, task)
        assert finish(scheduler, task, worker)
    return slow


@pytest.mark.parametrize("backup_wins", [False, True])
def test_speculation_threshold_and_first_winner(scheduler, tmp_path, clock, capsys, backup_wins):
    add_job(scheduler, tmp_path, maps=4)
    original = finish_quick_maps(scheduler, tmp_path, clock)
    clock[0] = 1.5
    assert scheduler.claim("backup") is None
    clock[0] = 1.6
    backup = scheduler.claim("backup")
    assert backup["task_id"] == original["task_id"] and backup["attempt_id"] != original["attempt_id"]
    assert scheduler.claim("another") is None
    attempts = [(original, "slow"), (backup, "backup")]
    if backup_wins:
        attempts.reverse()
    (winner, winner_worker), (loser, loser_worker) = attempts
    stage(tmp_path, winner, "winner\n")
    stage(tmp_path, loser, "loser\n")
    assert finish(scheduler, winner, winner_worker)
    reducer = scheduler.claim(loser_worker)
    assert reducer["task_id"] == "r-0"
    assert not finish(scheduler, loser, loser_worker)
    assert scheduler.claim(loser_worker) == reducer
    assert Path(tmp_path / "jobs/job/map/m-0/r-0.jsonl").read_text() == "winner\n"
    events = [json.loads(line) for line in capsys.readouterr().out.splitlines()]
    assert [event["attempt_id"] for event in events if event["event"] == "assigned" and event["speculative"]] == [
        backup["attempt_id"],
    ]


def test_disabling_speculation_suppresses_backups(scheduler, tmp_path, clock):
    add_job(scheduler, tmp_path, maps=4, speculation=False)
    finish_quick_maps(scheduler, tmp_path, clock)
    clock[0] = 4.0
    assert scheduler.claim("spare") is None


@pytest.mark.parametrize("last_error", [None, "backup traceback"])
def test_last_try_can_finish_after_its_sibling_fails(scheduler, tmp_path, clock, last_error):
    add_job(scheduler, tmp_path, maps=4)
    original = finish_quick_maps(scheduler, tmp_path, clock)
    assert finish(scheduler, original, "slow", error="first traceback")
    retry = scheduler.claim("slow")
    clock[0] = 3.0
    backup = scheduler.claim("backup")
    assert finish(scheduler, retry, "slow", error="second traceback")
    assert scheduler.status("job")["state"] == "map"
    assert scheduler.claim("idle") is None
    if last_error is None:
        stage(tmp_path, backup)
    assert finish(scheduler, backup, "backup", error=last_error)
    status = scheduler.status("job")
    assert status["state"] == ("reduce" if last_error is None else "failed")
    assert status["error"] == last_error
