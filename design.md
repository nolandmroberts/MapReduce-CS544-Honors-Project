# CS 544 Honors Project 1: MapReduce Design

November 6, 2026

## Project overview

We plan to build a simple MapReduce system in Python that runs with Docker Compose. It will have one coordinator (the "boss") and four worker containers. Each worker will be limited to one CPU, so running tasks in parallel should be faster than using just one worker.

A command-line client will run on the host machine, outside Docker. Users will be able to upload data, submit their own map and reduce functions, choose the number of map and reduce tasks, and check job results.

## How the system will work

1. The client uploads the input data and the user's Python file to shared storage.
2. The client sends the coordinator a job request with the input location, output location, code file, and map/reduce task counts.
3. The coordinator splits the input into the requested number of map tasks.
4. Workers request tasks from the coordinator. Each map task reads its input split, calls the user's map function, and writes intermediate key/value pairs.
5. The intermediate data is divided into partitions so that every value for a given key goes to the same reducer.
6. Once all map tasks finish, the coordinator schedules the reduce tasks. Reducers group values by key, call the user's reduce function, and write the final output files.
7. The client can check the job status and find the results in shared storage.

The coordinator will track which tasks are waiting, running, or finished.

## Client interface

We plan to support commands like these:

```bash
docker compose up --build -d

python3 client.py upload examples/input.jsonl --name sample

python3 client.py submit \
  --input sample \
  --output wordcount-results \
  --code examples/wordcount.py \
  --maps 8 \
  --reduces 4

python3 client.py status <job-id>
python3 client.py results <job-id>
```

The submitted Python file will define a map function and a reduce function. For example, a word-count mapper will output a pair such as ("hello", 1) for each word. The reducer will add up all values for each word. We will include a complete working example and sample data in the final README.

`submit` also accepts `--no-speculation` and `--no-combine`, which turn off those features for a single job so the benchmarks can compare runs with and without them.

## Job file interface

A job file is a plain Python module. It must define `map` and `reduce`, and it may define `combine`:

```python
def map(key, value):
    # key:   where the record came from, e.g. "books.txt:1042" (file name and byte offset)
    # value: one input record (a line of text, or the parsed JSON value of a .jsonl line)
    for word in value.split():
        yield word.lower(), 1

def reduce(key, values):
    # values: an iterator over every value emitted for this key
    yield key, sum(values)

def combine(key, values):  # optional; same contract as reduce
    yield key, sum(values)
```

- All three functions are generators that yield `(key, value)` pairs. Keys must be strings and values must be JSON-serializable.
- Input files can be `.txt`, where each line is one record passed as a string, or `.jsonl`, where each line is parsed as JSON before being passed to `map`.
- A worker loads the job file from shared storage with `importlib`. If user code raises an exception, that attempt fails and the traceback is saved. If the same task fails 3 times, the job is marked failed and `status` shows the error.

## Language and communication

We will use Python 3 for the client, coordinator, workers, and user-defined jobs. The coordinator will expose an HTTP API using Flask. The client and workers will send HTTP requests with JSON messages.

Workers will ask the coordinator for tasks rather than waiting for the coordinator to contact them. They will also send regular heartbeats and report when tasks finish. This keeps communication fairly simple and gives the coordinator a way to detect failed workers.

The coordinator will be reachable from the host on port 8080.

## Deployment

`docker-compose.yml` defines a `coordinator` service and a single `worker` service limited to `cpus: 1`, with 4 replicas by default. For the parallelism benchmark we change the worker count with `docker compose up -d --scale worker=N`.

## Shared storage and file formats

Because the project runs on one VM, we will use a shared folder instead of HDFS. Docker Compose will mount the host's `./shared` directory into the coordinator and all four workers. The client can access the same folder directly.

Input can be plain text or JSON Lines (JSONL). We will use JSONL for intermediate data and output, with each line storing a `[key, value]` pair. This format is easy to read, create, and debug.

The shared folder will be laid out like this:

```
shared/
  inputs/<name>/...                      uploaded input files
  jobs/<job-id>/job.py                   copy of the user's code
  jobs/<job-id>/tmp/<task>-<attempt>/    output of one attempt, before it is committed
  jobs/<job-id>/map/m-<i>/r-<j>.jsonl    committed intermediate data
  outputs/<name>/part-<j>.jsonl          final output, one file per reduce task
```

**Input splits.** The coordinator lists every file under the input path and divides the total size into M byte ranges of about the same size, where M is the requested number of map tasks. A range can cover parts of several files. To avoid cutting a record in half, a map task skips the partial line at the start of each file segment (unless the segment starts at the beginning of the file) and reads past the end of its range to finish its last line. This way every line is read by exactly one map task. If M is larger than the number of lines, the extra map tasks simply produce empty output.

**Partitioning.** Each map task writes a separate intermediate file for each reduce partition. The partition is `zlib.crc32(key.encode()) % R`. We cannot use Python's built-in `hash()`, because it is randomized differently in each process, so two workers would disagree about where a key belongs.

**Reduce.** Reduce task `j` reads `r-<j>.jsonl` from every map task, sorts the pairs by key in memory, groups them, and calls `reduce` once per key. Sorting in memory is fine for the data sizes we will demo (up to a few hundred MB). External sorting is out of scope.

**Committing results.** Each attempt writes into its own `tmp/<task>-<attempt>/` folder and then reports success. If the task has not been committed yet, the coordinator renames that folder to the task's final location. Since everything is on one filesystem, the rename is atomic. Output from any later attempt is deleted. Retries and duplicate attempts therefore can never double-count results.

## Special features

**1. Speculative execution.** If one task is much slower than the others, the coordinator can start a second copy on an idle worker. The first successful attempt counts, and the other result is ignored. Specifically, once at least 75% of the tasks in the current phase (map or reduce) have finished, a running task gets one backup attempt if it has been running longer than 1.5 times the median duration of the finished tasks in that phase. Each task gets at most one backup. We plan to compare jobs with this feature enabled and disabled (`--no-speculation`).

**2. Worker failure recovery.** Workers send a heartbeat every 2 seconds. If a worker has not sent one for 10 seconds, the coordinator marks it dead and puts its running tasks back in the queue so another worker can run them. Unlike in the original MapReduce, finished map tasks do not need to be re-run, because their output is on shared storage rather than on the dead worker's disk. If a worker that was marked dead comes back and reports a result, the result is accepted only if that task has not been committed yet.

**3. Combiners.** If the job file defines `combine`, a map task keeps its output in memory per partition, groups it by key, and runs `combine` before writing the intermediate files. For word count, this replaces many `("the", 1)` pairs with a single `("the", n)` pair per map task, which shrinks the data the reducers have to read. The `--no-combine` flag turns this off for comparison.

We will keep these features limited to task attempts and retries. Recovering from a coordinator crash is outside the scope of this project.

## Testing

We will use pytest for unit tests and run end-to-end tests with Docker Compose. Important tests include:

- Word count on a small dataset, checked against known correct counts.
- Different numbers of map and reduce tasks, making sure the results stay the same.
- Empty input, repeated keys, and multiple input files or splits.
- Correct partitioning, so identical keys always reach the same reducer.
- Invalid job options and errors in user-provided functions.
- Killing a worker while a job is running, then checking that the task is retried.
- Slowing down one worker, then checking that speculation does not create duplicate results.
- Input splits: across all splits, every line is read exactly once, including lines that cross a split boundary and files that end without a newline.
- Partitioning is stable across processes: the same key maps to the same partition in two separate Python interpreters.
- Running with and without the combiner gives the same output.

We will also add GitHub Actions to run tests on pull requests.

## Performance evaluation

We will measure total job time, map time, and reduce time using the same VM and four one-CPU worker containers.

**Parallelism:** Run the same job with 1, 2, and 4 active workers. Plot total runtime and speedup against the number of workers.

**Speculative execution:** Add an artificial delay to one worker and run the same job with speculation on and off. Repeat each test several times and compare median completion time. We will plot runtime against the injected delay and show a task timeline to explain when the duplicate attempt helped.

**Failure recovery:** Stop a worker during a job, record how long it takes to reassign the task, and verify that the final result is correct.

**Combiners:** Run word count on the example data with and without the combiner. Plot the total size of the intermediate files and the total job time for each run.

We will save the benchmark commands and raw timing data so the experiments can be repeated.

## Development and documentation

We will build the basic map/reduce workflow first, then add retries, speculation, combiners, tests, and benchmarks. Work will be done on feature branches through pull requests. Each pull request needs at least one review comment from a teammate who did not write it, and that teammate is the one who merges it.

### Team responsibilities

| Area | Owner |
| --- | --- |
| Client CLI and coordinator HTTP API (jobs, task queue, status) | TBD |
| Worker runtime (input splits, running map/reduce, partitioning, combiners) | TBD |
| Fault tolerance (heartbeats, retries, speculation) | TBD |
| Docker Compose, GitHub Actions CI, README | TBD |
| Benchmarks and plots | TBD |

Everyone writes tests for the parts they own.

The final README will explain how to build and start the containers, install client dependencies, upload example data, write a map/reduce job, submit it, and view the results.

Our target is to have the code, tests, documentation, and performance results merged by November 16, followed by a demo with Tyler during November 17-20.

This document describes our planned design. Some implementation details may change as we build and test the system.
