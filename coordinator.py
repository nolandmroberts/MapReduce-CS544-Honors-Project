"""HTTP entry point for job submission, worker polling, and status."""

import os
import re
import shutil
import time
from pathlib import Path
from threading import Lock
from uuid import uuid4

from flask import Flask, abort, jsonify, request
from werkzeug.exceptions import HTTPException

from scheduler import Scheduler

NAME = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


def job_splits(shared_dir, input_name, maps):
    input_dir = shared_dir / "inputs" / input_name
    if input_dir.is_symlink() or not input_dir.is_dir():
        abort(400, f"Input {input_name!r} is unavailable")

    files = []
    total = 0
    for path in sorted(input_dir.rglob("*"), key=lambda item: item.as_posix()):
        if path.suffix in (".txt", ".jsonl") and not path.is_symlink() and path.is_file():
            end = total + path.stat().st_size
            files.append((path.relative_to(shared_dir).as_posix(), total, end))
            total = end
    if not files:
        abort(400, f"Input {input_name!r} has no .txt or .jsonl files")

    splits = [[] for _ in range(maps)]
    file_index = 0
    for index, segments in enumerate(splits):
        start, end = index * total // maps, (index + 1) * total // maps
        while file_index < len(files):
            path, file_start, file_end = files[file_index]
            if file_start >= end:
                break
            local_start = max(start, file_start) - file_start
            local_end = min(end, file_end) - file_start
            if local_start < local_end:
                segments.append({"path": path, "start": local_start, "end": local_end})
            if file_end > end:
                break
            file_index += 1
    return splits


def prepare_storage(shared_dir, job_id, output_name, code):
    job_dir = shared_dir / "jobs" / job_id
    job_dir.mkdir(parents=True)
    try:
        (job_dir / "job.py").write_bytes(code)
        (job_dir / "tmp").mkdir()
        (job_dir / "map").mkdir()
        outputs = shared_dir / "outputs"
        outputs.mkdir(parents=True, exist_ok=True)
        try:
            (outputs / output_name).mkdir()
        except FileExistsError:
            abort(409, f"Output {output_name!r} already exists")
    except (OSError, HTTPException):
        shutil.rmtree(job_dir, ignore_errors=True)
        raise


def json_body():
    body = request.get_json(silent=True)
    if not isinstance(body, dict):
        abort(400, "Expected a JSON object")
    return body


def required_text(body, name):
    value = body.get(name)
    if not isinstance(value, str) or not value.strip():
        abort(400, f"{name} must be a nonempty string")
    return value


def create_app(shared_dir, clock=time.monotonic):
    app = Flask(__name__)
    shared_dir = Path(shared_dir)
    scheduler = Scheduler(shared_dir, clock)
    lock = Lock()

    @app.errorhandler(HTTPException)
    def http_error(error):
        response = error.get_response()
        response.data = app.json.dumps({"error": error.description})
        response.content_type = "application/json"
        return response

    @app.post("/jobs")
    def submit():
        body = json_body()
        for field in ("input", "output"):
            if not NAME.fullmatch(required_text(body, field)):
                abort(400, f"{field} must be a valid upload or output name")
        for field in ("maps", "reduces"):
            if type(body.get(field)) is not int or body[field] < 1:
                abort(400, f"{field} must be a positive integer")
        combine, speculation = body.get("combine", True), body.get("speculation", True)
        if not isinstance(combine, bool) or not isinstance(speculation, bool):
            abort(400, "combine and speculation must be booleans")
        try:
            code = required_text(body, "code").encode("utf-8")
        except UnicodeEncodeError:
            abort(400, "code must be UTF-8 source text")
        try:
            splits = job_splits(shared_dir, body["input"], body["maps"])
        except OSError:
            abort(400, f"Could not read input {body['input']!r}")

        job_id = uuid4().hex
        with lock:
            try:
                prepare_storage(shared_dir, job_id, body["output"], code)
            except OSError:
                app.logger.exception("Could not prepare job storage")
                abort(500, "Could not prepare job storage")
            scheduler.add_job(job_id, body["output"], splits, body["reduces"], combine=combine, speculation=speculation)
        return jsonify(job_id=job_id), 201

    @app.get("/jobs/<job_id>")
    def status(job_id):
        with lock:
            result = scheduler.status(job_id)
        if result is None:
            abort(404, "Unknown job")
        return jsonify(result)

    @app.post("/workers/heartbeat")
    def heartbeat():
        worker_id = required_text(json_body(), "worker_id")
        with lock:
            scheduler.heartbeat(worker_id)
        return "", 204

    @app.post("/tasks/claim")
    def claim():
        worker_id = required_text(json_body(), "worker_id")
        with lock:
            assignment = scheduler.claim(worker_id)
        if assignment is None:
            return "", 204
        return jsonify(assignment)

    @app.post("/tasks/complete")
    def complete():
        body = json_body()
        ids = [required_text(body, field) for field in ("job_id", "task_id", "attempt_id", "worker_id")]
        outcome = required_text(body, "outcome")
        if outcome not in ("success", "failure"):
            abort(400, "outcome must be success or failure")
        error = required_text(body, "error") if outcome == "failure" else None
        with lock:
            accepted = scheduler.complete(*ids, outcome=outcome, error=error)
        return jsonify(accepted=accepted)

    return app


if __name__ == "__main__":
    create_app(os.environ.get("SHARED_DIR", "./shared")).run(host="0.0.0.0", port=8080)
