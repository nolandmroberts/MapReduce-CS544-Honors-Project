import json
import os

import pytest
import requests

import client


@pytest.fixture
def shared(tmp_path, monkeypatch):
    """Point the client at an empty shared directory and a fake coordinator URL."""
    shared = tmp_path / "shared"
    shared.mkdir()
    monkeypatch.setenv("SHARED_DIR", str(shared))
    monkeypatch.setenv("COORDINATOR_URL", "http://coordinator.test:8080/")
    return shared


class FakeResponse:
    def __init__(self, status_code, data=None, text=None):
        self.status_code = status_code
        self._data = data
        self.text = text if text is not None else json.dumps(data)

    def json(self):
        if self._data is None:
            raise ValueError("no JSON body")
        return self._data


@pytest.fixture
def coordinator(monkeypatch):
    """Replace requests.request with a fake that records calls and returns a queued response."""

    class Fake:
        def __init__(self):
            self.calls = []
            self.response = FakeResponse(200, {})

        def __call__(self, method, url, json=None, timeout=None):
            self.calls.append({"method": method, "url": url, "json": json, "timeout": timeout})
            if isinstance(self.response, Exception):
                raise self.response
            return self.response

    fake = Fake()
    monkeypatch.setattr(client.requests, "request", fake)
    return fake


def listing(root):
    """All files under root, as sorted relative paths with forward slashes."""
    found = []
    for directory, _, names in os.walk(root):
        for name in names:
            found.append(os.path.relpath(os.path.join(directory, name), root).replace(os.sep, "/"))
    return sorted(found)


# ---------------------------------------------------------------------------
# upload


def test_upload_single_file(shared, tmp_path, capsys):
    source = tmp_path / "input.txt"
    source.write_text("hello world\nhello\n")

    assert client.main(["upload", str(source), "--name", "sample"]) == 0

    assert capsys.readouterr().out == "sample\n"
    uploaded = shared / "inputs" / "sample" / "input.txt"
    assert uploaded.read_text() == "hello world\nhello\n"
    assert listing(shared / "inputs") == ["sample/input.txt"]


def test_upload_directory_tree(shared, tmp_path):
    source = tmp_path / "data"
    (source / "nested").mkdir(parents=True)
    (source / "a.txt").write_text("a\n")
    (source / "nested" / "b.jsonl").write_text('"b"\n')

    assert client.main(["upload", str(source), "--name", "tree"]) == 0

    assert listing(shared / "inputs") == ["tree/a.txt", "tree/nested/b.jsonl"]
    assert (shared / "inputs" / "tree" / "nested" / "b.jsonl").read_text() == '"b"\n'


def test_upload_refuses_existing_name(shared, tmp_path, capsys):
    first = tmp_path / "first.txt"
    first.write_text("first\n")
    second = tmp_path / "second.txt"
    second.write_text("second\n")
    assert client.main(["upload", str(first), "--name", "sample"]) == 0
    capsys.readouterr()

    assert client.main(["upload", str(second), "--name", "sample"]) == 1

    assert "already exists" in capsys.readouterr().err
    assert listing(shared / "inputs") == ["sample/first.txt"]


@pytest.mark.parametrize("name", ["", ".hidden", "-dash", "has space", "a/b", "../escape"])
def test_upload_rejects_invalid_name(shared, tmp_path, capsys, name):
    source = tmp_path / "input.txt"
    source.write_text("x\n")

    assert client.main(["upload", str(source), f"--name={name}"]) == 1

    assert "invalid upload name" in capsys.readouterr().err
    assert not (shared / "inputs").exists()


def test_upload_rejects_unsupported_file(shared, tmp_path, capsys):
    source = tmp_path / "input.csv"
    source.write_text("a,b\n")

    assert client.main(["upload", str(source), "--name", "sample"]) == 1

    assert "not a .txt or .jsonl file" in capsys.readouterr().err


def test_upload_rejects_directory_with_unsupported_file(shared, tmp_path, capsys):
    source = tmp_path / "data"
    source.mkdir()
    (source / "a.txt").write_text("a\n")
    (source / "notes.md").write_text("notes\n")

    assert client.main(["upload", str(source), "--name", "sample"]) == 1

    assert "notes.md" in capsys.readouterr().err
    assert not (shared / "inputs").exists()


def test_upload_requires_a_data_file(shared, tmp_path, capsys):
    source = tmp_path / "empty"
    source.mkdir()

    assert client.main(["upload", str(source), "--name", "sample"]) == 1

    assert "contains no .txt or .jsonl files" in capsys.readouterr().err


def test_upload_rejects_missing_path(shared, tmp_path, capsys):
    assert client.main(["upload", str(tmp_path / "missing.txt"), "--name", "sample"]) == 1

    assert "error:" in capsys.readouterr().err


def make_symlink(target, link):
    try:
        os.symlink(target, link)
    except (OSError, NotImplementedError):
        pytest.skip("this platform does not allow creating symlinks")


def test_upload_rejects_symlink(shared, tmp_path, capsys):
    real = tmp_path / "real.txt"
    real.write_text("x\n")
    link = tmp_path / "link.txt"
    make_symlink(real, link)

    assert client.main(["upload", str(link), "--name", "sample"]) == 1

    assert "symlink" in capsys.readouterr().err


def test_upload_rejects_symlink_inside_directory(shared, tmp_path, capsys):
    real = tmp_path / "real.txt"
    real.write_text("x\n")
    source = tmp_path / "data"
    source.mkdir()
    (source / "a.txt").write_text("a\n")
    make_symlink(real, source / "link.txt")

    assert client.main(["upload", str(source), "--name", "sample"]) == 1

    assert "symlink" in capsys.readouterr().err
    assert listing(shared / "inputs") == []


def test_upload_failure_leaves_no_staging_directory(shared, tmp_path, monkeypatch, capsys):
    source = tmp_path / "data"
    source.mkdir()
    (source / "a.txt").write_text("a\n")
    (source / "b.txt").write_text("b\n")

    def fail_copy(src, dst):
        raise OSError("disk full")

    monkeypatch.setattr(client.shutil, "copyfile", fail_copy)

    assert client.main(["upload", str(source), "--name", "sample"]) == 1

    assert "disk full" in capsys.readouterr().err
    assert os.listdir(shared / "inputs") == []


# ---------------------------------------------------------------------------
# submit


@pytest.fixture
def job_file(tmp_path):
    path = tmp_path / "wordcount.py"
    path.write_text("def map(key, value):\n    yield value, 1\n", encoding="utf-8")
    return path


def submit_args(job_file, *extra):
    return [
        "submit", "--input", "sample", "--output", "counts",
        "--code", str(job_file), "--maps", "8", "--reduces", "4", *extra,
    ]


def test_submit_sends_job_and_prints_only_the_id(shared, coordinator, job_file, capsys):
    coordinator.response = FakeResponse(201, {"job_id": "job-123"})

    assert client.main(submit_args(job_file)) == 0

    assert capsys.readouterr().out == "job-123\n"
    [call] = coordinator.calls
    assert call["method"] == "POST"
    assert call["url"] == "http://coordinator.test:8080/jobs"
    assert call["timeout"] == client.REQUEST_TIMEOUT_SECONDS
    assert call["json"] == {
        "input": "sample",
        "output": "counts",
        "code": "def map(key, value):\n    yield value, 1\n",
        "maps": 8,
        "reduces": 4,
        "speculation": True,
        "combine": True,
    }


def test_submit_flags_turn_features_off(shared, coordinator, job_file):
    coordinator.response = FakeResponse(201, {"job_id": "job-123"})

    assert client.main(submit_args(job_file, "--no-speculation", "--no-combine")) == 0

    body = coordinator.calls[0]["json"]
    assert body["speculation"] is False
    assert body["combine"] is False


@pytest.mark.parametrize("flag,value", [("--maps", "0"), ("--reduces", "-1"), ("--maps", "two")])
def test_submit_rejects_bad_counts(shared, coordinator, job_file, flag, value):
    args = submit_args(job_file)
    args[args.index(flag) + 1] = value

    with pytest.raises(SystemExit) as exit_info:
        client.main(args)

    assert exit_info.value.code == 2
    assert coordinator.calls == []


def test_submit_reports_coordinator_error(shared, coordinator, job_file, capsys):
    coordinator.response = FakeResponse(409, {"error": "output 'counts' already exists"})

    assert client.main(submit_args(job_file)) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "HTTP 409" in captured.err
    assert "output 'counts' already exists" in captured.err


def test_submit_reports_connection_error(shared, coordinator, job_file, capsys):
    coordinator.response = requests.ConnectionError("connection refused")

    assert client.main(submit_args(job_file)) == 1

    assert "could not reach the coordinator" in capsys.readouterr().err


def test_submit_reports_missing_job_file(shared, coordinator, tmp_path, capsys):
    assert client.main(submit_args(tmp_path / "missing.py")) == 1

    assert "could not read job file" in capsys.readouterr().err
    assert coordinator.calls == []


def test_submit_rejects_invalid_output_name(shared, coordinator, job_file, capsys):
    args = submit_args(job_file)
    args[args.index("--output") + 1] = "../escape"

    assert client.main(args) == 1

    assert "invalid output name" in capsys.readouterr().err
    assert coordinator.calls == []


# ---------------------------------------------------------------------------
# status and results


def job_status(state, error=None):
    return {
        "job_id": "job-123",
        "state": state,
        "map": {"done": 8, "total": 8},
        "reduce": {"done": 4 if state == "succeeded" else 0, "total": 4},
        "output": "outputs/counts",
        "error": error,
        "seconds": {"map": 1.2, "reduce": None, "total": None},
    }


def test_status_prints_job_json(shared, coordinator, capsys):
    coordinator.response = FakeResponse(200, job_status("reduce"))

    assert client.main(["status", "job-123"]) == 0

    assert json.loads(capsys.readouterr().out) == job_status("reduce")
    assert coordinator.calls[0]["method"] == "GET"
    assert coordinator.calls[0]["url"] == "http://coordinator.test:8080/jobs/job-123"


def test_status_quotes_job_id_in_url(shared, coordinator):
    coordinator.response = FakeResponse(200, job_status("map"))

    client.main(["status", "a/b c"])

    assert coordinator.calls[0]["url"] == "http://coordinator.test:8080/jobs/a%2Fb%20c"


def test_status_reports_unknown_job(shared, coordinator, capsys):
    coordinator.response = FakeResponse(404, {"error": "unknown job"})

    assert client.main(["status", "nope"]) == 1

    assert "unknown job" in capsys.readouterr().err


def test_results_prints_output_path_when_succeeded(shared, coordinator, capsys):
    coordinator.response = FakeResponse(200, job_status("succeeded"))

    assert client.main(["results", "job-123"]) == 0

    assert capsys.readouterr().out == os.path.join(str(shared), "outputs/counts") + "\n"


def test_results_before_success_exits_nonzero(shared, coordinator, capsys):
    coordinator.response = FakeResponse(200, job_status("map"))

    assert client.main(["results", "job-123"]) == 1

    captured = capsys.readouterr()
    assert captured.out == ""
    assert "has not finished (state: map)" in captured.err


def test_results_for_failed_job_reports_error(shared, coordinator, capsys):
    coordinator.response = FakeResponse(200, job_status("failed", error="Traceback: boom"))

    assert client.main(["results", "job-123"]) == 1

    assert "failed: Traceback: boom" in capsys.readouterr().err


def test_non_json_error_response(shared, coordinator, capsys):
    coordinator.response = FakeResponse(502, None, text="Bad Gateway")

    assert client.main(["status", "job-123"]) == 1

    assert "HTTP 502: Bad Gateway" in capsys.readouterr().err
