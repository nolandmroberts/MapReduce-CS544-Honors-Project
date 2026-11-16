# Anton Grachev · Coordinator and task recovery

Work on `anton-coordinator`. Own `coordinator.py`, `scheduler.py`, `tests/test_coordinator.py` and `tests/test_scheduler.py`. Build job intake, task leases, winner selection, heartbeats, retry and speculation. Use the [shared contract](architecture.md) for the exact wire format and filesystem layout. Flask's test client can play both the host CLI and workers.

## 1. Set up job intake and the phase barrier

- In `coordinator.py`, make a Flask app that accepts a shared directory and a clock (default `time.monotonic`) for tests. Its script entry point reads `SHARED_DIR` and listens on `0.0.0.0:8080`.
- For `POST /jobs`, validate both names, strictly positive integer counts, source text, and boolean flags. Verify the named input directory contains at least one supported file. List `.txt` and `.jsonl` files recursively in stable order. The files can be zero bytes. Use the split formula in the contract and send paths relative to the mount. Preserve exactly `maps` assignments, including empty splits.
- Keep jobs and attempts in `scheduler.py` as in-memory records. For each accepted job, write `job.py`, make `tmp/` and `map/`, and reserve the named output directory with `mkdir` that fails if the name exists. Roll back staging on a failed submission. Workers see a job only after setup succeeds.
- Keep map tasks eligible until all maps commit. Then make the reduce tasks eligible with the agreed `partition` and `maps` fields. Compute status from committed counts, phase timestamps and the last terminal error. Serve pending work in submission order and prefer normal work to backups.

## 2. Make attempts safe to retry

Use one lock around the coordinator's scheduler changes and final filesystem rename. An attempt record needs its task, worker ID, attempt ID, start time, and whether it is speculative. Track the last contact and live assignment for each worker. Claiming with the same worker ID returns its existing live assignment if it has one.

On `/tasks/complete`, match the complete set of IDs to a live attempt. For success, rename the entire staged map directory or only the staged reduce file, per the shared contract. Count the task as complete only after the rename works. Failure reports and rename errors consume the attempt. Retired attempts, reports from other workers, and later reports for a won task return `accepted: false`. A late report may clear only its own worker assignment. Never remove an attempt's staging directory while a worker may be writing to it.

Add the time-based features after a fake worker can complete a job.

1. **Loss and retry.** Use monotonic time. Sweep for expiry before worker requests and status reads, before crediting any new contact. When a worker has been silent for at least 10 seconds, retire its active attempt and log `expired`. Retry only when no live sibling exists. Fail the job only when the task has no live attempt and has used its three attempts. Surface a traceback, publish error or timeout message in job status.
2. **Speculation.** On an idle worker's claim, when there is no normal work available, check the current phase's committed fraction and median committed duration. Offer a backup for an eligible slow task when `speculation` is on. Apply the same attempt limit, winner rule and publish path as for normal work. A backup can win even if the original is still running.
3. **Measurements.** Log `assigned`, `expired` and `committed` as JSON lines with the fields in the contract. Use the job acceptance time as the origin for all events. This is the benchmark interface.

## 3. Prove the branch

Use `tmp_path` for the mount and a fake clock you can advance without sleeping. The test can write `r-<j>.jsonl` and `part.jsonl` directly, then call `/tasks/complete` with an assignment's IDs. Cover these cases.

- Positive counts, missing input, duplicate output name and exact split ranges across several files. Exercise `M > S` and an input with zero total bytes.
- A lost claim response followed by a second claim, and a map completion report repeated after a lost HTTP response. Verify the same assignment is returned for the claim and commits are counted once.
- No reduce is assigned before the last map commit. Two reducers commit separate files. A publish error does not advance the phase.
- Expire a worker, reassign its task, and replay the expired worker's completion. Confirm the replacement's files win and previously committed maps remain readable. Check the exact timeout boundary and a terminal failure after three attempts.
- Back up a straggler after the threshold. Test both completion orders and a failure while a sibling is still running. Verify disabling speculation suppresses backups and that a duplicate completion cannot overwrite the winner.

Install `flask` and `pytest`, then run `python3 -m pytest -q tests/test_coordinator.py tests/test_scheduler.py`. First PR the job lifecycle and atomic commits with fake-worker tests. Follow with the timing policies and tests.
