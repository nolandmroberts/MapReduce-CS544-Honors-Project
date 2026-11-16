from concurrent.futures import ThreadPoolExecutor

import pytest

from coordinator import create_app


@pytest.fixture
def app(tmp_path):
    folder = tmp_path / "inputs" / "sample"
    folder.mkdir(parents=True)
    (folder / "a.txt").write_bytes(b"one\n")
    (folder / "b.jsonl").write_bytes(b"123\n456\n")
    return create_app(tmp_path)


def submit(client, **options):
    payload = {
        "input": "sample", "output": "result", "code": "# example job\n",
        "maps": 2, "reduces": 2,
    }
    return client.post("/jobs", json=payload | options)


def claim(client, worker):
    return client.post("/tasks/claim", json={"worker_id": worker})


def report(task, worker):
    return {key: task[key] for key in ("job_id", "task_id", "attempt_id")} | {
        "worker_id": worker, "outcome": "success",
    }


def stage(tmp_path, task, files):
    path = tmp_path / "jobs" / task["job_id"] / "tmp" / f"{task['task_id']}-{task['attempt_id']}"
    path.mkdir()
    for name, content in files.items():
        (path / name).write_text(content, encoding="utf-8")
    return path


@pytest.mark.parametrize("options", [
    {"input": "../sample"}, {"input": "missing"}, {"output": "a/b"},
    {"maps": True}, {"maps": 0}, {"reduces": "2"}, {"code": 5},
    {"code": "\ud800"}, {"combine": 0}, {"speculation": None},
])
def test_invalid_submissions_return_json_errors(app, tmp_path, options):
    response = submit(app.test_client(), **options)
    assert response.status_code == 400
    assert response.json["error"]
    assert not (tmp_path / "jobs").exists()


def test_missing_jobs_and_malformed_worker_messages(app):
    client = app.test_client()
    assert client.get("/jobs/missing").status_code == 404
    assert client.get("/jobs/missing").json["error"]
    for path in ("/workers/heartbeat", "/tasks/claim", "/tasks/complete"):
        response = client.post(path, json={"worker_id": 42})
        assert response.status_code == 400
        assert response.json["error"]


def test_upload_splits_cross_files_and_job_is_saved(app, tmp_path):
    client = app.test_client()
    response = submit(client, combine=False)
    assert response.status_code == 201
    job_id = response.json["job_id"]
    assert (tmp_path / "jobs" / job_id / "job.py").read_text() == "# example job\n"
    first = claim(client, "worker-1").json
    assert first["job_id"] == job_id
    assert first["task_id"] == "m-0" and first["phase"] == "map"
    assert first["reduces"] == 2 and first["combine"] is False
    assert first["segments"] == [
        {"path": "inputs/sample/a.txt", "start": 0, "end": 4},
        {"path": "inputs/sample/b.jsonl", "start": 0, "end": 2},
    ]
    assert claim(client, "worker-1").json == first
    second = claim(client, "worker-2").json
    assert second["segments"] == [{"path": "inputs/sample/b.jsonl", "start": 2, "end": 8}]
    assert submit(client).status_code == 409
    assert [job.name for job in (tmp_path / "jobs").iterdir()] == [job_id]


@pytest.mark.parametrize("content,expected", [(b"", [[], [], []]), (b"x", [[], [], [(0, 1)]])])
def test_more_maps_than_bytes_including_empty_input(app, tmp_path, content, expected):
    directory = tmp_path / "inputs" / "tiny"
    directory.mkdir()
    (directory / ".txt").write_bytes(content)
    client = app.test_client()
    assert submit(client, input="tiny", maps=3).status_code == 201
    actual = []
    for index in range(3):
        segments = claim(client, f"worker-{index}").json["segments"]
        actual.append([(item["start"], item["end"]) for item in segments])
    assert actual == expected


def test_failed_storage_setup_keeps_names_available(app, tmp_path):
    blocked = tmp_path / "outputs"
    blocked.touch()
    client = app.test_client()
    assert submit(client).status_code == 500
    assert not list((tmp_path / "jobs").iterdir())
    assert claim(client, "idle").status_code == 204
    blocked.unlink()
    assert submit(client).status_code == 201


def test_maps_gate_reduces_and_each_reducer_publishes_its_own_file(app, tmp_path):
    client = app.test_client()
    job_id = submit(client).json["job_id"]
    for index in range(2):
        task = claim(client, "worker").json
        assert task["task_id"] == f"m-{index}"
        stage(tmp_path, task, {"r-0.jsonl": "", "r-1.jsonl": ""})
        assert client.post("/tasks/complete", json=report(task, "intruder")).json == {"accepted": False}
        assert client.post("/tasks/complete", json=report(task, "worker")).json == {"accepted": True}
        assert client.post("/tasks/complete", json=report(task, "worker")).json == {"accepted": False}
    reducers = [claim(client, f"worker-{index}").json for index in range(2)]
    for index, task in enumerate(reducers):
        assert task["phase"] == "reduce" and task["partition"] == index and task["maps"] == 2
        stage(tmp_path, task, {"part.jsonl": f'["value", {index}]\n'})
        assert client.post("/tasks/complete", json=report(task, f"worker-{index}")).json == {"accepted": True}
    status = client.get(f"/jobs/{job_id}").json
    assert status["state"] == "succeeded" and status["error"] is None
    assert status["map"] == status["reduce"] == {"done": 2, "total": 2}
    assert status["output"] == "outputs/result"
    for index in range(2):
        assert (tmp_path / status["output"] / f"part-{index}.jsonl").read_text() == f'["value", {index}]\n'


def test_publish_error_requeues_without_opening_reduce_phase(app):
    client = app.test_client()
    job_id = submit(client, maps=1).json["job_id"]
    task = claim(client, "worker").json
    assert claim(client, "idle").status_code == 204
    assert client.post("/tasks/complete", json=report(task, "worker")).json == {"accepted": True}
    status = client.get(f"/jobs/{job_id}").json
    assert status["state"] == "map" and status["map"]["done"] == 0
    retry = claim(client, "idle").json
    assert retry["task_id"] == task["task_id"]
    assert retry["attempt_id"] != task["attempt_id"]


def test_simultaneous_duplicate_completions_commit_once(app, tmp_path):
    client = app.test_client()
    submit(client, maps=1)
    task = claim(client, "worker").json
    stage(tmp_path, task, {"r-0.jsonl": "", "r-1.jsonl": ""})

    def finish(_):
        return app.test_client().post("/tasks/complete", json=report(task, "worker")).json["accepted"]

    with ThreadPoolExecutor(max_workers=2) as threads:
        assert sorted(threads.map(finish, range(2))) == [False, True]
