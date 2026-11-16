# MapReduce implementation contract

Run one Python 3.12 Flask coordinator and four one-CPU Python workers on the course VM. The host CLI uploads data to a bind mount and sends jobs over HTTP. The coordinator assigns work and publishes results. Workers execute the submitted Python functions. Keep job state in memory and files on the shared mount. Run trusted job files in this course deployment.

This contract fills in the implementation details from [PR #3](https://github.com/nolandmroberts/MapReduce-CS544-Honors-Project/pull/3). It is the shared reference for the three work branches.

## Ownership

| Branch | Owner | Files |
| --- | --- | --- |
| `ayaan-worker` | Ayaan Padela | `worker.py`, `task_runtime.py`, `examples/`, `tests/test_worker.py`, `tests/test_task_runtime.py` |
| `anton-coordinator` | Anton Grachev | `coordinator.py`, `scheduler.py`, `tests/test_coordinator.py`, `tests/test_scheduler.py` |
| `noland-client-integration` | Noland Roberts | `client.py`, `Dockerfile`, `docker-compose.yml`, `requirements.txt`, `.gitignore`, `.dockerignore`, `.github/workflows/tests.yml`, `README.md`, `tests/test_client.py`, `tests/test_end_to_end.py`, `benchmarks/` |

The HTTP messages and the shared files are the integration points. Run each component's own tests with temporary directories or a fake HTTP peer. Keep each PR within the owner's paths.

## Running the services

| Setting | Host CLI | Coordinator | Worker |
| --- | --- | --- | --- |
| Entry point | `python3 client.py` | `python coordinator.py` | `python worker.py` |
| `SHARED_DIR` | Defaults to `./shared` | `/shared` in Compose | `/shared` in Compose |
| `COORDINATOR_URL` | Defaults to `http://127.0.0.1:8080` | — | `http://coordinator:8080` in Compose |

Set both services' bind mount to `./shared:/shared`. The coordinator listens on `0.0.0.0:8080` in its container and publishes on host `127.0.0.1:8080`. Workers use the `worker` service with `cpus: 1` and `scale: 4`. The Python dependencies are Flask, requests and pytest. [Docker Compose service settings](https://docs.docker.com/reference/compose-file/services/)

The worker generates a unique `worker_id` on process start and logs one JSON line with `{"worker_id":"<id>"}` so a benchmark can match an assignment to a container. The optional worker environment variable `TASK_DELAY_SECONDS` defaults to `0`. When set, sleep for that many seconds after a claim and before running the task, with heartbeats still active.

## HTTP API

Send JSON for requests with a body. Invalid requests return `400`. All HTTP errors use `{"error":"<message>"}`. Counts must be positive integers and flags must be booleans. Input and output are single upload/output names matching `[A-Za-z0-9][A-Za-z0-9._-]*`. Job, worker and attempt IDs are unique strings. Map IDs `m-0` through `m-(M-1)` and reduce IDs `r-0` through `r-(R-1)` are scoped to their job.

### Client to coordinator

`POST /jobs` takes `input`, `output`, `code` (the UTF-8 Python source text), `maps` and `reduces`. Optional `speculation` and `combine` default to `true`. A successful submission returns `201 {"job_id":"<id>"}`. An unavailable input is `400`. A reserved or existing output name is `409`. The coordinator saves the source as `job.py` and reserves the output directory before the job becomes visible to workers.

```json
{"input":"sample","output":"wordcount-results","code":"<Python source>","maps":8,"reduces":4,"speculation":true,"combine":true}
```

`GET /jobs/<job_id>` returns the status below, or `404` for an unknown job. States are `map`, `reduce`, `succeeded` and `failed`. Counts measure committed tasks. `error` is `null` unless the job fails. `seconds` entries are `null` until the corresponding phase or job ends. Measure map from job acceptance to the last map commit, reduce from that commit to the last reduce commit, and total from acceptance to terminal state. For a failed job, include total time and durations for any completed phases.

```json
{
  "job_id":"<id>", "state":"succeeded",
  "map":{"done":8,"total":8}, "reduce":{"done":4,"total":4},
  "output":"outputs/wordcount-results", "error":null,
  "seconds":{"map":1.2,"reduce":0.3,"total":1.5}
}
```

### Worker to coordinator

| Request | Body | Response |
| --- | --- | --- |
| `POST /workers/heartbeat` | `{"worker_id":"<id>"}` | `204` |
| `POST /tasks/claim` | `{"worker_id":"<id>"}` | `200` with the assignment below, or `204` when idle |
| `POST /tasks/complete` | `job_id`, `task_id`, `attempt_id`, `worker_id`, `outcome` (`success` or `failure`), plus `error` with a traceback on failure | `200 {"accepted":true}` for an applied report, `200 {"accepted":false}` for a stale, duplicate, unknown or mismatched attempt |

```json
{"job_id":"<id>","task_id":"m-0","attempt_id":"<id>","phase":"map","segments":[{"path":"inputs/sample/input.txt","start":0,"end":32}],"reduces":4,"combine":true}
```

```json
{"job_id":"<id>","task_id":"r-0","attempt_id":"<id>","phase":"reduce","partition":0,"maps":8}
```

One worker runs one assignment at a time. A repeated claim by a worker with a live assignment returns the same assignment. Retry an interrupted completion with the same IDs and outcome. A worker can remove its staging directory after receiving a `200` completion response with either value of `accepted`.

## Shared storage and job functions

Paths are relative to `SHARED_DIR`. The host CLI writes uploads. The coordinator saves job code, makes assignments and publishes output. Workers write only inside the directory for their attempt.

```text
inputs/<input-name>/...                            uploaded .txt and .jsonl files
jobs/<job-id>/job.py                                submitted source
jobs/<job-id>/tmp/<task-id>-<attempt-id>/           attempt staging directory
  r-<j>.jsonl                                      one file per partition for a map attempt
  part.jsonl                                       one file for a reduce attempt
jobs/<job-id>/map/m-<i>/r-<j>.jsonl                  committed map partition
outputs/<output-name>/part-<j>.jsonl                committed reduce output
```

Each phase's task IDs and partitions are zero-based. Every successful map produces all `R` files, including empty partitions. Each reducer reads its partition from all `M` committed maps and writes a file even if it has no rows. Intermediate and final rows are UTF-8 JSONL arrays `[key, value]`, with string keys and JSON-serializable values. Read final results after the job reaches `succeeded`. Use a new output name for each run.

Job modules provide `map(key, value)` and `reduce(key, values)` and may provide `combine(key, values)`. Each yields `(key, value)` pairs. Map's input key is `<relative-path>:<byte-offset>`, for example `inputs/sample/input.txt:32`. Its value is a UTF-8 text line without the line ending or the parsed value of a `.jsonl` line. Invalid JSON fails the attempt. Partition map keys with `zlib.crc32(key.encode("utf-8")) % R`. A reducer sorts the pairs by key, groups them and passes an iterator of values to `reduce` for each key.

When enabled and defined, `combine` runs once per key within each map partition. It yields the same key, with values the reducer can consume alongside uncombined values. The job author chooses a combiner that preserves the result when runs are combined in groups, such as summing word counts. Treat a changed combiner key as an attempt failure.

### Split boundary rule

The coordinator sorts input files by relative path and divides their concatenated byte lengths `S` into `M` ranges with bounds `[i * S // M, (i + 1) * S // M)`. Intersect each range with each file and translate the intersection into file-local byte offsets. A zero-width range yields an empty `segments` list. A dataset with an empty input file is valid.

A worker owns a line when its **first byte** is in `[start, end)`. Open each segment in binary mode. If `start > 0` and byte `start - 1` is something other than `\n`, seek to `start` and discard the partial line. Otherwise start reading at `start`. Start each new read only when the current offset is below `end`, and read the full line through its newline or EOF. Decode after reading. For text, remove the trailing LF or CRLF and keep other whitespace.

## Scheduling and publication

- Assign maps first. Open the reduce phase after all map commits. Mark the job succeeded after all reduce commits. Offer unstarted work and retries before speculative attempts.
- Send one heartbeat every 2 seconds, even during task execution. After 10 seconds without contact, expire that worker's active attempt. Requeue a task if it has no live attempt and still has a try left. An expired attempt's late report is stale. Committed maps survive worker failure because their files are on the shared mount.
- Allow up to three started attempts per logical task, including any backup. Fail the job with the latest relevant error when a task has used all tries and has no live attempt. A still-running sibling can finish after the other attempt fails.
- With speculation enabled, after at least 75% of the current phase's tasks commit, an idle worker can back up a running attempt older than 1.5 times the median duration of that phase's committed attempts. Count from assignment to commit for the winner's duration. Start at most one backup per logical task and stay within the three-attempt budget.
- Under one coordinator lock, check the attempt is still live and belongs to the reporting worker, publish its files, then record the winner. Rename a map's staging directory into `map/m-<i>`. Rename a reduce attempt's `part.jsonl` into `outputs/<output-name>/part-<j>.jsonl`. Both moves stay on the shared filesystem. A rename error is an attempt failure. Later reports for the task leave the committed files alone.

Log coordinator events as JSON lines to stdout for the benchmarks. Event names are `assigned`, `expired` and `committed`. Each event includes `event`, `job_id`, `task_id`, `attempt_id`, `worker_id`, `speculative` (boolean) and `elapsed_s` (seconds since job acceptance).

## Combining the branches

1. Develop and run the isolated tests on the three branches. Submit small PRs targeting `main`. Use merge commits for the first round so all lanes retain the same shared documentation commit.
2. Land Noland's client/deployment and unit-test setup early so CI can run on the implementation PRs. Land the coordinator and worker PRs in either order once their isolated tests pass. Each teammate gives and receives review comments. Ayaan reviews Anton, Anton reviews Noland, and Noland reviews Ayaan.
3. With all three components together, run the Compose end-to-end checks. Compare the final word count for several M/R combinations, including with the combiner disabled. Exercise a killed worker and a delayed worker. Noland runs the final checks, saves the benchmark commands, measurements and plots, and opens the README and evaluation PR.

The assignment calls for final code on `main` by November 16, 2026, and a November 17–20 demo.
