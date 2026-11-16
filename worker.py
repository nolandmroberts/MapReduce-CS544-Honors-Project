"""Poll the coordinator and execute one task at a time on shared storage."""

import json
import logging
import math
import os
import shutil
import threading
import traceback
import uuid
from pathlib import Path

import requests

from task_runtime import run_task

LOG = logging.getLogger(__name__)
HTTP_TIMEOUT = 3
HEARTBEAT_INTERVAL = 2
POLL_INTERVAL = 0.25


class Worker:
    def __init__(self, shared_dir, coordinator_url, task_delay=0):
        if not math.isfinite(task_delay) or task_delay < 0:
            raise ValueError("TASK_DELAY_SECONDS must be a finite nonnegative number")
        self.shared_dir = Path(shared_dir)
        self.coordinator_url = coordinator_url.rstrip("/")
        self.task_delay = task_delay
        self.worker_id = uuid.uuid4().hex

    def _post(self, endpoint, body):
        # Each call uses its own connection; the heartbeat thread does not share
        # a requests.Session with the main thread. Timeouts bound network stalls.
        return requests.post(self.coordinator_url + endpoint, json=body,
                             timeout=HTTP_TIMEOUT)

    def _heartbeats(self, stop):
        # This thread stays active during user code, injected delays and report
        # retries, so a long task does not look like a failed worker.
        while not stop.is_set():
            try:
                with self._post("/workers/heartbeat", {"worker_id": self.worker_id}) as response:
                    if response.status_code != 204:
                        LOG.warning("Heartbeat returned HTTP %s", response.status_code)
            except requests.RequestException as error:
                LOG.warning("Heartbeat failed: %s", error)
            stop.wait(HEARTBEAT_INTERVAL)

    def _claim(self):
        with self._post("/tasks/claim", {"worker_id": self.worker_id}) as response:
            if response.status_code == 204:
                return None
            response.raise_for_status()
            if response.status_code != 200:
                raise ValueError(f"Unexpected claim status: {response.status_code}")
            return response.json()

    def _execute(self, assignment):
        report = {name: assignment[name] for name in ("job_id", "task_id", "attempt_id")}
        report.update(worker_id=self.worker_id, outcome="success")
        try:
            # run_task returns only after all output files have been closed.
            run_task(self.shared_dir, assignment)
        except Exception:
            report.update(outcome="failure", error=traceback.format_exc())
        return report

    def _complete(self, report, stop):
        # A lost response may mean the coordinator already committed our output.
        # Retry the exact report, without executing the task or claiming again.
        while not stop.is_set():
            try:
                with self._post("/tasks/complete", report) as response:
                    response.raise_for_status()
                    if response.status_code != 200:
                        raise ValueError(f"Unexpected completion status: {response.status_code}")
                    if not isinstance(response.json().get("accepted"), bool):
                        raise ValueError("Completion response must contain a boolean accepted")
                    return True
            except (requests.RequestException, ValueError) as error:
                LOG.warning("Completion report failed; retrying: %s", error)
            stop.wait(POLL_INTERVAL)
        return False

    def _cleanup(self, assignment):
        staging = (self.shared_dir / "jobs" / assignment["job_id"] / "tmp" /
                   f"{assignment['task_id']}-{assignment['attempt_id']}")
        try:
            shutil.rmtree(staging)
        except FileNotFoundError:
            pass  # The coordinator may already have moved the winning directory.
        except OSError as error:
            LOG.warning("Could not remove attempt staging directory %s: %s", staging, error)

    def run(self, stop=None):
        """Run until interrupted or an optional shutdown Event is set."""
        if stop is None:
            stop = threading.Event()
        print(json.dumps({"worker_id": self.worker_id}), flush=True)
        heartbeat = threading.Thread(target=self._heartbeats, args=(stop,), daemon=True)
        heartbeat.start()
        try:
            while not stop.is_set():
                try:
                    assignment = self._claim()
                except (requests.RequestException, ValueError) as error:
                    LOG.warning("Task claim failed; retrying: %s", error)
                    stop.wait(POLL_INTERVAL)
                    continue
                if assignment is None:
                    stop.wait(POLL_INTERVAL)
                    continue
                if stop.wait(self.task_delay):
                    break
                report = self._execute(assignment)
                if self._complete(report, stop):
                    # Both accepted=true and accepted=false acknowledge the report.
                    # Stale/losing attempts can discard their scratch files too.
                    self._cleanup(assignment)
        finally:
            stop.set()
            heartbeat.join()


def main():
    logging.basicConfig(level=logging.INFO)
    worker = Worker(
        shared_dir=os.environ.get("SHARED_DIR", "./shared"),
        coordinator_url=os.environ.get("COORDINATOR_URL", "http://coordinator:8080"),
        task_delay=float(os.environ.get("TASK_DELAY_SECONDS", "0")),
    )
    try:
        worker.run()
    except KeyboardInterrupt:
        pass


if __name__ == "__main__":
    main()
