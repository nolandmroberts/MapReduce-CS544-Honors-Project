"""Run the performance experiments against the Docker Compose deployment.

Run from the repository root with the services already built and the input
uploaded (see benchmarks/README.md):

    python benchmarks/run_benchmarks.py parallelism --input words --code benchmarks/jobs/wordcount_cpu.py
    python benchmarks/run_benchmarks.py speculation --input words --code benchmarks/jobs/wordcount_cpu.py
    python benchmarks/run_benchmarks.py worker-loss --input words --code benchmarks/jobs/wordcount_cpu.py
    python benchmarks/run_benchmarks.py combiner --input repeated --code benchmarks/jobs/wordcount.py

Every run appends one row to benchmarks/results/<experiment>.csv and checks the
job's output against a reference computed locally, so a fast but wrong run is
recorded as incorrect instead of looking like a win.
"""

import argparse
import csv
import datetime
import hashlib
import importlib.util
import json
import os
import statistics
import subprocess
import sys
import time
import uuid
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
SHARED_DIR = Path(os.environ.get("SHARED_DIR", REPO_ROOT / "shared")).resolve()
RESULTS_DIR = REPO_ROOT / "benchmarks" / "results"
COORDINATOR_URL = os.environ.get("COORDINATOR_URL", "http://127.0.0.1:8080")

JOB_DEADLINE_SECONDS = 1800
POLL_INTERVAL_SECONDS = 0.5

CSV_FIELDS = [
    "timestamp", "experiment", "rep", "job_id", "workers", "maps", "reduces",
    "speculation", "combine", "delay_s", "state", "correct",
    "map_s", "reduce_s", "total_s", "wall_s", "intermediate_bytes",
    "kill_s", "expired_s", "reassigned_s",
]


class BenchmarkError(Exception):
    pass


# ---------------------------------------------------------------------------
# helpers that do not touch Docker (unit-tested in test_benchmarks.py)


def load_job(path):
    spec = importlib.util.spec_from_file_location(f"job_{uuid.uuid4().hex}", path)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def iter_input_records(input_dir):
    """Yield (key, value) map inputs the way the workers do: sorted files, one record per line."""
    input_dir = Path(input_dir)
    for path in sorted(p for p in input_dir.rglob("*") if p.is_file()):
        relative = path.relative_to(input_dir.parent.parent).as_posix()
        offset = 0
        with open(path, "rb") as f:
            for raw in f:
                text = raw.decode("utf-8")
                if path.suffix == ".jsonl":
                    value = json.loads(text)
                else:
                    value = text.rstrip("\r\n")
                yield f"{relative}:{offset}", value
                offset += len(raw)


def reference_result(job_path, input_dir):
    """Run the job in this process, without a combiner, and return {key: value}."""
    job = load_job(job_path)
    groups = {}
    for key, value in iter_input_records(input_dir):
        for out_key, out_value in job.map(key, value):
            groups.setdefault(out_key, []).append(out_value)
    result = {}
    for key in sorted(groups):
        for out_key, out_value in job.reduce(key, iter(groups[key])):
            result[out_key] = out_value
    return result


def cached_reference(job_path, input_dir, cache_dir):
    """reference_result, cached on disk by the job file's and input's contents."""
    digest = hashlib.sha256(Path(job_path).read_bytes())
    for path in sorted(p for p in Path(input_dir).rglob("*") if p.is_file()):
        digest.update(path.relative_to(input_dir).as_posix().encode())
        stat = path.stat()
        digest.update(f"{stat.st_size}:{stat.st_mtime_ns}".encode())
    cache = Path(cache_dir) / f"reference-{digest.hexdigest()[:16]}.json"
    if cache.exists():
        return json.loads(cache.read_text(encoding="utf-8"))
    print(f"computing reference result for {input_dir} (cached afterwards)...", flush=True)
    result = reference_result(job_path, input_dir)
    cache.parent.mkdir(parents=True, exist_ok=True)
    cache.write_text(json.dumps(result), encoding="utf-8")
    return result


def read_job_output(output_dir):
    """Decode every part file. Returns ({key: value}, number of duplicate keys)."""
    result = {}
    duplicates = 0
    for path in sorted(Path(output_dir).glob("part-*.jsonl")):
        with open(path, encoding="utf-8") as f:
            for line in f:
                if not line.strip():
                    continue
                key, value = json.loads(line)
                if key in result:
                    duplicates += 1
                result[key] = value
    return result, duplicates


def output_is_correct(output_dir, expected):
    result, duplicates = read_job_output(output_dir)
    return duplicates == 0 and result == expected


def directory_bytes(path):
    path = Path(path)
    if not path.exists():
        return 0
    return sum(p.stat().st_size for p in path.rglob("*") if p.is_file())


def parse_events(log_text, job_id=None):
    """Return the coordinator's JSON log events, optionally only for one job."""
    events = []
    for line in log_text.splitlines():
        line = line.strip()
        if not line.startswith("{"):
            continue
        try:
            event = json.loads(line)
        except ValueError:
            continue
        if not isinstance(event, dict) or "event" not in event:
            continue
        if job_id is None or event.get("job_id") == job_id:
            events.append(event)
    return events


def active_assignments(events):
    """Assigned attempts that have not been committed or expired yet, oldest first."""
    finished = {e["attempt_id"] for e in events if e["event"] in ("committed", "expired")}
    return [e for e in events if e["event"] == "assigned" and e["attempt_id"] not in finished]


def recovery_times(events, killed_attempt, kill_s):
    """Seconds from the kill until the attempt expired and until its task was reassigned."""
    task_id = next(e["task_id"] for e in events if e["attempt_id"] == killed_attempt)
    expired = next((e["elapsed_s"] for e in events
                    if e["event"] == "expired" and e["attempt_id"] == killed_attempt), None)
    reassigned = next((e["elapsed_s"] for e in events
                       if e["event"] == "assigned" and e["task_id"] == task_id
                       and e["attempt_id"] != killed_attempt and e["elapsed_s"] >= kill_s), None)
    return (
        None if expired is None else expired - kill_s,
        None if reassigned is None else reassigned - kill_s,
    )


def find_worker_id(log_text):
    """The worker_id from the most recent startup line (a restarted container logs a new one)."""
    worker_id = None
    for line in log_text.splitlines():
        line = line.strip()
        if line.startswith("{"):
            try:
                data = json.loads(line)
            except ValueError:
                continue
            if isinstance(data, dict) and "worker_id" in data and "event" not in data:
                worker_id = data["worker_id"]
    return worker_id


def append_row(csv_path, row):
    csv_path = Path(csv_path)
    csv_path.parent.mkdir(parents=True, exist_ok=True)
    new_file = not csv_path.exists()
    with open(csv_path, "a", newline="", encoding="utf-8") as f:
        writer = csv.DictWriter(f, fieldnames=CSV_FIELDS)
        if new_file:
            writer.writeheader()
        writer.writerow({field: row.get(field, "") for field in CSV_FIELDS})


def unique_name(label):
    return f"bench-{label}-{uuid.uuid4().hex[:8]}"


# ---------------------------------------------------------------------------
# Docker Compose and client.py


def run(cmd, check=True, timeout=600):
    result = subprocess.run(cmd, cwd=REPO_ROOT, capture_output=True, text=True, timeout=timeout)
    if check and result.returncode != 0:
        raise BenchmarkError(f"{' '.join(cmd)} failed:\n{result.stderr or result.stdout}")
    return result


def client(*args, check=True):
    env = dict(os.environ, SHARED_DIR=str(SHARED_DIR), COORDINATOR_URL=COORDINATOR_URL)
    result = subprocess.run([sys.executable, "client.py", *args], cwd=REPO_ROOT, env=env,
                            capture_output=True, text=True, timeout=60)
    if check and result.returncode != 0:
        raise BenchmarkError(f"client.py {' '.join(args)} failed:\n{result.stderr}")
    return result.stdout.strip()


def worker_containers():
    return run(["docker", "compose", "ps", "-q", "--status", "running", "worker"]).stdout.split()


def scale_workers(count, settle_s):
    run(["docker", "compose", "up", "-d", "--no-build", "--scale", f"worker={count}"])
    deadline = time.monotonic() + 120
    while len(worker_containers()) != count:
        if time.monotonic() > deadline:
            raise BenchmarkError(f"expected {count} running workers, found {len(worker_containers())}")
        time.sleep(1)
    time.sleep(settle_s)  # let every worker send a heartbeat before the job starts


def coordinator_logs():
    return run(["docker", "compose", "logs", "--no-color", "--no-log-prefix", "coordinator"]).stdout


def container_logs(container):
    return run(["docker", "logs", container], check=False).stdout


def wait_for_worker_id(container, timeout_s=60):
    deadline = time.monotonic() + timeout_s
    while time.monotonic() < deadline:
        worker_id = find_worker_id(container_logs(container))
        if worker_id:
            return worker_id
        time.sleep(0.5)
    raise BenchmarkError(f"worker container {container} did not log its worker_id")


def submit(args, output, speculation, combine):
    command = ["submit", "--input", args.input, "--output", output, "--code", str(args.code),
               "--maps", str(args.maps), "--reduces", str(args.reduces)]
    if not speculation:
        command.append("--no-speculation")
    if not combine:
        command.append("--no-combine")
    return client(*command)


def wait_for_job(job_id, on_poll=None):
    deadline = time.monotonic() + JOB_DEADLINE_SECONDS
    while True:
        status = json.loads(client("status", job_id))
        if status["state"] in ("succeeded", "failed"):
            return status
        if on_poll:
            on_poll(status)
        if time.monotonic() > deadline:
            raise BenchmarkError(f"job {job_id} did not finish in {JOB_DEADLINE_SECONDS}s")
        time.sleep(POLL_INTERVAL_SECONDS)


def run_job(args, experiment, rep, workers, expected, speculation=True, combine=True,
            delay_s=0, on_poll=None):
    """Submit one job, wait for it, check its output, and return its CSV row."""
    output = unique_name(experiment)
    started = time.monotonic()
    job_id = submit(args, output, speculation, combine)
    status = wait_for_job(job_id, on_poll=lambda s: on_poll(job_id, started, s) if on_poll else None)
    wall_s = time.monotonic() - started

    correct = status["state"] == "succeeded" and output_is_correct(SHARED_DIR / status["output"], expected)
    seconds = status.get("seconds") or {}
    row = {
        "timestamp": datetime.datetime.now().isoformat(timespec="seconds"),
        "experiment": experiment, "rep": rep, "job_id": job_id, "workers": workers,
        "maps": args.maps, "reduces": args.reduces, "speculation": speculation,
        "combine": combine, "delay_s": delay_s, "state": status["state"], "correct": correct,
        "map_s": seconds.get("map"), "reduce_s": seconds.get("reduce"),
        "total_s": seconds.get("total"), "wall_s": round(wall_s, 3),
        "intermediate_bytes": directory_bytes(SHARED_DIR / "jobs" / job_id / "map"),
    }
    flag = "ok" if correct else "WRONG OUTPUT" if status["state"] == "succeeded" else "FAILED"
    print(f"  {experiment} rep={rep} workers={workers} spec={speculation} combine={combine} "
          f"delay={delay_s}: total={row['total_s']}s [{flag}]", flush=True)
    return row


def save_timeline(job_id):
    events = parse_events(coordinator_logs(), job_id)
    path = RESULTS_DIR / "timelines" / f"{job_id}.jsonl"
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text("".join(json.dumps(e) + "\n" for e in events), encoding="utf-8")
    return path


# ---------------------------------------------------------------------------
# experiments


def experiment_parallelism(args, expected):
    for rep in range(1, args.reps + 1):
        for workers in args.workers:
            scale_workers(workers, args.settle)
            append_row(args.csv, run_job(args, "parallelism", rep, workers, expected))
    scale_workers(4, args.settle)


def experiment_speculation(args, expected):
    for delay in args.delays:
        scale_workers(3, args.settle)
        container = run(["docker", "compose", "run", "-d", "--no-deps",
                         "-e", f"TASK_DELAY_SECONDS={delay}", "worker"]).stdout.strip().splitlines()[-1]
        try:
            wait_for_worker_id(container)
            time.sleep(args.settle)
            for rep in range(1, args.reps + 1):
                for speculation in (True, False):
                    row = run_job(args, "speculation", rep, 4, expected,
                                  speculation=speculation, delay_s=delay)
                    append_row(args.csv, row)
                    save_timeline(row["job_id"])
        finally:
            run(["docker", "rm", "-f", container], check=False)
    scale_workers(4, args.settle)


def experiment_combiner(args, expected):
    scale_workers(4, args.settle)
    for rep in range(1, args.reps + 1):
        for combine in (True, False):
            append_row(args.csv, run_job(args, "combiner", rep, 4, expected, combine=combine))


def experiment_worker_loss(args, expected):
    for rep in range(1, args.reps + 1):
        scale_workers(4, args.settle)
        ids = {wait_for_worker_id(c): c for c in worker_containers()}
        killed = {}

        def kill_active_worker(job_id, started, status):
            if killed:
                return
            active = active_assignments(parse_events(coordinator_logs(), job_id))
            for event in reversed(active):
                container = ids.get(event["worker_id"])
                if container:
                    run(["docker", "kill", container])
                    killed.update(attempt=event["attempt_id"], kill_s=time.monotonic() - started,
                                  worker_id=event["worker_id"], task_id=event["task_id"])
                    print(f"  killed worker {event['worker_id']} running {event['task_id']}", flush=True)
                    return

        row = run_job(args, "worker-loss", rep, 4, expected, speculation=False,
                      on_poll=kill_active_worker)
        if not killed:
            print("  warning: job finished before a worker could be killed; use more input", flush=True)
        else:
            events = parse_events(coordinator_logs(), row["job_id"])
            expired_s, reassigned_s = recovery_times(events, killed["attempt"], killed["kill_s"])
            row.update(kill_s=round(killed["kill_s"], 3), expired_s=expired_s, reassigned_s=reassigned_s)
            save_timeline(row["job_id"])
        append_row(args.csv, row)
    scale_workers(4, args.settle)


EXPERIMENTS = {
    "parallelism": experiment_parallelism,
    "speculation": experiment_speculation,
    "combiner": experiment_combiner,
    "worker-loss": experiment_worker_loss,
}


def summarize(csv_path):
    with open(csv_path, newline="", encoding="utf-8") as f:
        rows = [r for r in csv.DictReader(f) if r["state"] == "succeeded" and r["correct"] == "True"]
    groups = {}
    for r in rows:
        key = (r["workers"], r["speculation"], r["combine"], r["delay_s"])
        groups.setdefault(key, []).append(float(r["total_s"]))
    print(f"\nmedian total seconds ({csv_path}):")
    for (workers, spec, combine, delay), times in sorted(groups.items()):
        print(f"  workers={workers} speculation={spec} combine={combine} delay={delay}: "
              f"{statistics.median(times):.2f}s over {len(times)} runs")


def main(argv=None):
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("experiment", choices=sorted(EXPERIMENTS))
    parser.add_argument("--input", required=True, help="name of an uploaded input")
    parser.add_argument("--code", required=True, type=Path, help="job file to submit")
    parser.add_argument("--reference-code", type=Path, default=REPO_ROOT / "benchmarks" / "jobs" / "wordcount.py",
                        help="job file used to compute the expected output locally (default: plain word count)")
    parser.add_argument("--maps", type=int, default=8)
    parser.add_argument("--reduces", type=int, default=4)
    parser.add_argument("--reps", type=int, default=3, help="repetitions of each configuration")
    parser.add_argument("--workers", type=int, nargs="+", default=[1, 2, 4], help="parallelism: worker counts")
    parser.add_argument("--delays", type=float, nargs="+", default=[5], help="speculation: TASK_DELAY_SECONDS values")
    parser.add_argument("--settle", type=float, default=3, help="seconds to wait after changing workers")
    parser.add_argument("--csv", type=Path, help="default: benchmarks/results/<experiment>.csv")
    args = parser.parse_args(argv)
    args.csv = args.csv or RESULTS_DIR / f"{args.experiment}.csv"

    input_dir = SHARED_DIR / "inputs" / args.input
    if not input_dir.is_dir():
        parser.error(f"no upload named {args.input!r}; upload it first with client.py upload")
    expected = cached_reference(args.reference_code, input_dir, RESULTS_DIR / "cache")

    print(f"running {args.experiment}, writing rows to {args.csv}", flush=True)
    try:
        EXPERIMENTS[args.experiment](args, expected)
    except BenchmarkError as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    summarize(args.csv)
    return 0


if __name__ == "__main__":
    sys.exit(main())
