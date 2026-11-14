# Ayaan Padela · Worker and task execution

Work on `ayaan-worker`. Build the process that receives task assignments and turns them into staged files. Own `worker.py`, `task_runtime.py`, `examples/`, `tests/test_worker.py`, and `tests/test_task_runtime.py`. The [shared contract](architecture.md) defines the exact HTTP messages, offsets, and paths. A fake coordinator and a temporary shared directory are enough to build and test this branch.

## 1. Build the task runner

In `task_runtime.py`, expose a small entry point such as `run_task(shared_dir, assignment)` that returns after closing every output file or raises an exception. Keep HTTP in `worker.py`. Load `jobs/<job_id>/job.py` with `importlib`, validate the required functions are callable, and create `jobs/<job_id>/tmp/<task_id>-<attempt_id>/` for output.

- **Map.** Walk the assignment's `segments` in order. Use binary offsets, check the byte before `start`, and consume only lines whose first byte lies in the segment. Read through the newline or EOF, then decode. Build the record key from the relative path and the line's byte offset. Pass text lines or parsed JSON values to `map`. Validate each emitted pair, compute the CRC32 partition, and write `r-<j>.jsonl` for every partition, even an empty one.
- **Reduce.** For partition `j`, open `jobs/<job_id>/map/m-<i>/r-<j>.jsonl` for each `i` in `range(maps)`. A missing committed file is an error. Sort rows by key in memory, group them, and call `reduce` with a values iterator for each key. Write all output into the attempt's `part.jsonl`, including an empty file when there are no rows.
- **Combine.** After basic map and reduce work, use the assignment's `combine` flag and the module's optional callable `combine`. Group each map partition by key, call it once per key, and write its output in the same partition. Raise on a changed key. Word count's combiner and reducer can use the same sum function.

The coordinator moves the staged files after completion. Your tests can simulate this with a directory rename before feeding a reduce assignment to the runner.

## 2. Add the worker process

In `worker.py`, read `SHARED_DIR`, `COORDINATOR_URL` and `TASK_DELAY_SECONDS`. Generate and log the process's `worker_id` as specified in the shared contract. Use requests with a finite timeout. Run a heartbeat loop independently of task execution, posting every 2 seconds with its own HTTP calls. Keep the main loop simple.

1. Claim a task. Wait briefly (about 250 ms) on `204` or a temporary connection error, then poll again.
2. Apply the optional delay while the heartbeat keeps running. Execute the assignment synchronously.
3. Report `success` after closing output, or report `failure` with a formatted traceback. Retry a transient completion failure with the same report before claiming again.
4. On a `200` completion response, remove any remaining attempt scratch directory. A lost claim response can be retried using the same worker ID.

## 3. Supply the example and prove the branch

Put a complete map, reduce, and combine example in `examples/wordcount.py`. Use `examples/input.txt` with exactly the two lines below. Lowercase tokens and split on whitespace. The expected combined output across the parts is `hello: 2` and `world: 1`.

```text
Hello world
hello
```

- Test a split that starts exactly at a line boundary, one that starts inside a multibyte UTF-8 character, and a last line without a newline. Across the segments, assert each line is read once with its correct byte offset. Cover an empty segment and `.jsonl` records, including a malformed record.
- Build at least two map outputs with overlapping keys. Verify stable partition numbers across processes, that all partition files exist, and that one reducer sees only its partition. Compare all final rows with and without the combiner. Include a combiner that changes its key and assert the attempt fails.
- Against a fake HTTP peer, exercise `204` then a claim, a heartbeat while the task runs, and a success report with the exact IDs. Cover a failure report and a completion retry without a second task execution.

Install the test dependencies with `python3 -m pip install requests pytest`. Run `python3 -m pytest -q tests/test_worker.py tests/test_task_runtime.py`. First PR the basic runner and worker loop, then the combiner and its equivalence test. The shared contract and the example paths give Noland everything needed to wire up the demo.
