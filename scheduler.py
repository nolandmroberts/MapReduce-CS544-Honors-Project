"""In-memory task scheduling and publication on the shared filesystem."""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from statistics import median
from uuid import uuid4

WORKER_TIMEOUT = 10
MAX_ATTEMPTS = 3


@dataclass
class Task:
    task_id: str
    phase: str
    details: dict
    active: dict[str, Attempt] = field(default_factory=dict)
    attempts_started: int = 0
    backup_started: bool = False
    duration: float | None = None


@dataclass
class Job:
    job_id: str
    output: str
    phases: dict[str, list[Task]]
    speculation: bool
    started: float
    state: str = "map"
    map_finished: float | None = None
    finished: float | None = None
    error: str | None = None


@dataclass
class Attempt:
    attempt_id: str
    job: Job
    task: Task
    worker_id: str
    started: float
    speculative: bool

    def assignment(self):
        return {
            "job_id": self.job.job_id,
            "task_id": self.task.task_id,
            "attempt_id": self.attempt_id,
            "phase": self.task.phase,
            **self.task.details,
        }


@dataclass
class Worker:
    last_seen: float
    attempt: Attempt | None = None


class Scheduler:
    """Call each operation under the coordinator's lock."""

    def __init__(self, shared_dir, clock):
        self.shared_dir = shared_dir
        self.clock = clock
        self.jobs: dict[str, Job] = {}
        self.workers: dict[str, Worker] = {}

    def add_job(self, job_id, output_name, splits, reduces, *, combine, speculation):
        phases = {
            "map": [
                Task(f"m-{i}", "map", {"segments": segments, "reduces": reduces, "combine": combine})
                for i, segments in enumerate(splits)
            ],
            "reduce": [
                Task(f"r-{i}", "reduce", {"partition": i, "maps": len(splits)})
                for i in range(reduces)
            ],
        }
        self.jobs[job_id] = Job(job_id, f"outputs/{output_name}", phases, speculation, self.clock())

    def status(self, job_id):
        self._expire_workers(self.clock())
        job = self.jobs.get(job_id)
        if job is None:
            return None
        progress = {
            phase: {"done": sum(task.duration is not None for task in tasks), "total": len(tasks)}
            for phase, tasks in job.phases.items()
        }
        return {
            "job_id": job_id,
            "state": job.state,
            "output": job.output,
            "error": job.error,
            **progress,
            "seconds": {
                "map": None if job.map_finished is None else job.map_finished - job.started,
                "reduce": job.finished - job.map_finished if job.state == "succeeded" else None,
                "total": None if job.finished is None else job.finished - job.started,
            },
        }

    def heartbeat(self, worker_id):
        self._contact(worker_id)

    def claim(self, worker_id):
        worker, now = self._contact(worker_id)
        if worker.attempt is not None:
            return worker.attempt.assignment()

        phases = [(job, job.phases[job.state]) for job in self.jobs.values() if job.state in job.phases]
        for job, tasks in phases:
            for task in tasks:
                if task.duration is None and not task.active and task.attempts_started < MAX_ATTEMPTS:
                    return self._assign(job, task, worker_id, now, speculative=False)

        for job, tasks in phases:
            if job.speculation:
                task = self._straggler(tasks, now)
                if task is not None:
                    return self._assign(job, task, worker_id, now, speculative=True)
        return None

    def complete(self, job_id, task_id, attempt_id, worker_id, *, outcome, error=None):
        worker, now = self._contact(worker_id)
        attempt = worker.attempt
        if attempt is None or (job_id, task_id, attempt_id) != (
            attempt.job.job_id, attempt.task.task_id, attempt.attempt_id,
        ):
            return False

        if outcome == "success":
            try:
                self._publish(attempt)
            except OSError as publish_error:
                error = f"Could not publish {task_id}: {publish_error}"
            else:
                self._commit(attempt, self.clock())
                return True
        self._fail_attempt(attempt, self.clock(), error)
        return True

    def _contact(self, worker_id):
        now = self.clock()
        self._expire_workers(now)
        worker = self.workers.setdefault(worker_id, Worker(now))
        worker.last_seen = now
        return worker, now

    def _expire_workers(self, now):
        for worker_id, worker in list(self.workers.items()):
            if now - worker.last_seen < WORKER_TIMEOUT:
                continue
            if worker.attempt is not None:
                self._log("expired", worker.attempt, now)
                self._fail_attempt(worker.attempt, now, f"Worker {worker_id} timed out")
            del self.workers[worker_id]

    @staticmethod
    def _straggler(tasks, now):
        durations = [task.duration for task in tasks if task.duration is not None]
        if len(durations) * 4 < len(tasks) * 3:
            return None
        cutoff = 1.5 * median(durations)
        for task in tasks:
            if task.active and not task.backup_started and task.attempts_started < MAX_ATTEMPTS:
                original = next(iter(task.active.values()))
                if now - original.started > cutoff:
                    return task
        return None

    def _assign(self, job, task, worker_id, now, *, speculative):
        attempt = Attempt(uuid4().hex, job, task, worker_id, now, speculative)
        task.active[attempt.attempt_id] = attempt
        task.attempts_started += 1
        task.backup_started |= speculative
        self.workers[worker_id].attempt = attempt
        self._log("assigned", attempt, now)
        return attempt.assignment()

    def _release(self, attempt):
        del attempt.task.active[attempt.attempt_id]
        self.workers[attempt.worker_id].attempt = None

    def _fail_attempt(self, attempt, now, error):
        self._release(attempt)
        task, job = attempt.task, attempt.job
        if task.active or task.attempts_started < MAX_ATTEMPTS:
            return
        for pending in job.phases[job.state]:
            for active in list(pending.active.values()):
                self._release(active)
        job.state = "failed"
        job.finished = now
        job.error = error

    def _publish(self, attempt):
        job, task = attempt.job, attempt.task
        job_dir = self.shared_dir / "jobs" / job.job_id
        stage = job_dir / "tmp" / f"{task.task_id}-{attempt.attempt_id}"
        if task.phase == "map":
            if not stage.is_dir():
                raise FileNotFoundError(f"Missing map attempt directory {stage}")
            stage.rename(job_dir / "map" / task.task_id)
        else:
            partition = task.details["partition"]
            (stage / "part.jsonl").rename(self.shared_dir / job.output / f"part-{partition}.jsonl")

    def _commit(self, attempt, now):
        task, job = attempt.task, attempt.job
        task.duration = now - attempt.started
        for active in list(task.active.values()):
            self._release(active)
        self._log("committed", attempt, now)
        if any(item.duration is None for item in job.phases[job.state]):
            return
        if job.state == "map":
            job.state = "reduce"
            job.map_finished = now
        else:
            job.state = "succeeded"
            job.finished = now

    @staticmethod
    def _log(event, attempt, now):
        print(json.dumps({
            "event": event,
            "job_id": attempt.job.job_id,
            "task_id": attempt.task.task_id,
            "attempt_id": attempt.attempt_id,
            "worker_id": attempt.worker_id,
            "speculative": attempt.speculative,
            "elapsed_s": now - attempt.job.started,
        }), flush=True)
