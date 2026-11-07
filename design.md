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

## Language and communication

We will use Python 3 for the client, coordinator, workers, and user-defined jobs. The coordinator will expose an HTTP API using Flask. The client and workers will send HTTP requests with JSON messages.

Workers will ask the coordinator for tasks rather than waiting for the coordinator to contact them. They will also send regular heartbeats and report when tasks finish. This keeps communication fairly simple and gives the coordinator a way to detect failed workers.

The coordinator will be reachable from the host on port 8080.

## Shared storage and file formats

Because the project runs on one VM, we will use a shared folder instead of HDFS. Docker Compose will mount the host's `./shared` directory into the coordinator and all four workers. The client can access the same folder directly.

We will use JSON Lines (JSONL) for input, intermediate data, and output. Each line will store a key/value pair. This format is easy to read, create, and debug.

Each map task will write a separate intermediate file for each reduce partition. We will use a stable hash of the key to choose the partition. Reducers will read only the files for their partition.

Task attempts will write to separate temporary files. The coordinator will choose one successful attempt for each task, so retries or duplicate tasks cannot accidentally double-count results.

## Special features

**1. Speculative execution.** If one task is much slower than the others, the coordinator can start a second copy on an idle worker. The first successful attempt counts, and the other result is ignored. We plan to compare jobs with this feature enabled and disabled.

**2. Worker failure recovery.** If a worker stops sending heartbeats, the coordinator will put its unfinished task back in the queue so another worker can run it. This should let jobs finish even if one worker crashes.

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

We will also add GitHub Actions to run tests on pull requests.

## Performance evaluation

We will measure total job time, map time, and reduce time using the same VM and four one-CPU worker containers.

**Parallelism:** Run the same job with 1, 2, and 4 active workers. Plot total runtime and speedup against the number of workers.

**Speculative execution:** Add an artificial delay to one worker and run the same job with speculation on and off. Repeat each test several times and compare median completion time. We will plot runtime against the injected delay and show a task timeline to explain when the duplicate attempt helped.

**Failure recovery:** Stop a worker during a job, record how long it takes to reassign the task, and verify that the final result is correct.

We will save the benchmark commands and raw timing data so the experiments can be repeated.

## Development and documentation

We will build the basic map/reduce workflow first, then add retries, speculation, tests, and benchmarks. Work will be done on feature branches through pull requests, with reviews before merging to main.

The final README will explain how to build and start the containers, install client dependencies, upload example data, write a map/reduce job, submit it, and view the results.

Our target is to have the code, tests, documentation, and performance results merged by November 16, followed by a demo with Tyler during November 17-20.

This document describes our planned design. Some implementation details may change as we build and test the system.
