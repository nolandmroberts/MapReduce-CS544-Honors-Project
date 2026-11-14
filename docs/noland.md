# Noland Roberts · Client, deployment and evaluation

Work on `noland-client-integration`. Own `client.py`, `Dockerfile`, `docker-compose.yml`, `requirements.txt`, `.gitignore`, `.dockerignore`, `.github/workflows/tests.yml`, `README.md`, `tests/test_client.py`, `tests/test_end_to_end.py`, and `benchmarks/`. The [shared contract](architecture.md) defines service names, file paths, response fields, and log events. Implement the host CLI and its tests against a fake coordinator while the other branches are in progress.

## 1. Build the host CLI

Use `argparse` and requests with a finite timeout. Read the shared directory and coordinator URL from the environment with the host defaults in the contract.

| Command | Behavior |
| --- | --- |
| `upload PATH --name NAME` | Make `inputs/NAME/` in the mount and copy one `.txt` or `.jsonl` file, or a directory tree of those files. Validate the name, require at least one regular data file, reject symlinks and refuse to overwrite an upload. Stage the copy in `inputs/` before publishing it so a partial upload is invisible. Print the name. |
| `submit --input NAME --output NAME --code FILE --maps M --reduces R` | Read the Python file as UTF-8 and send it as `code` to `POST /jobs`. Send `speculation` and `combine` as `true` by default. `--no-speculation` and `--no-combine` set their corresponding flags to `false`. Print only the job ID on success so scripts can capture it. |
| `status JOB_ID` | GET the job, then print its JSON. |
| `results JOB_ID` | GET the job. On `succeeded`, print the local path under the mount using `output` from the response. For running or failed jobs, report the state and exit nonzero. |

Write user errors to stderr and exit nonzero on connection errors or HTTP errors. Test upload into `tmp_path`, duplicate names, submit JSON including both flags, the printed job ID, and results before and after success. A fake HTTP response fixture is sufficient.

## 2. Set up Docker and CI

- Use one Python 3.12 image for both services with the repo copied in and Flask, requests, and pytest in `requirements.txt`. Set the service commands and environment from the shared contract. Ignore local shared data in Git and exclude it from the Docker build context.
- Mount `./shared:/shared` into both services and publish `127.0.0.1:8080:8080` on the coordinator. Set `cpus: 1` and `scale: 4` on `worker`. Leave worker container names to Compose so `docker compose up -d --scale worker=N` can change the replica count. [Compose scaling options](https://docs.docker.com/reference/cli/docker/compose/up/)
- Add GitHub Actions on `pull_request` to set up Python, install requirements, and run `python -m pytest -q`. Keep Docker end-to-end tests opt-in with `RUN_COMPOSE_TESTS=1` so the unit suite works before both service entry points land. Land the CI setup with at least one CLI test so it has a test to run.

## 3. Write a runnable README and smoke test

Include the host dependency install, Compose start, example job source and data, and these commands. Ayaan owns both example files. Use a fresh output name on reruns.

```bash
python3 -m pip install -r requirements.txt
docker compose up --build -d
python3 client.py upload examples/input.txt --name sample
JOB=$(python3 client.py submit --input sample --output wordcount-results \
  --code examples/wordcount.py --maps 8 --reduces 4)
python3 client.py status "$JOB"
# Once status is succeeded, open the JSONL part files in the printed directory.
python3 client.py results "$JOB"
```

`tests/test_end_to_end.py` should use the running Compose services and unique names per run. Use a deadline while polling status. Assert that the worker service starts four containers, and that word count produces `hello: 2` and `world: 1` across the parts for `1/1` and `8/4` map/reduce counts. Repeat with `--no-combine`. Compare decoded rows across files without relying on part order. Run this after the coordinator and worker merge.

## 4. Evaluate the system on the course VM

Under `benchmarks/`, keep the run commands, input generation or input description, CSV of raw results, and plots. Record job ID, worker count, M/R counts, flags, injected delay, phase and total seconds from `status`, and bytes across committed `map/` files. Use the same input and counts within each comparison, several repetitions, and unique output names. Verify output against the same expected result on every run.

| Experiment | Run and report |
| --- | --- |
| Parallelism | Scale to 1, 2 and 4 workers. Use enough input or CPU work to make a single run measurable. Plot median total time and speedup relative to one worker. |
| Speculation | Start three normal workers and add one delayed replica with `docker compose run -d --no-deps -e TASK_DELAY_SECONDS=5 worker`. Wait for the worker startup logs before submitting and use at least eight maps and four reducers. Compare the same delay with speculation enabled and disabled, and capture an `assigned`/`committed` timeline showing whether a backup helped. Remove the delayed container before the next configuration. |
| Worker loss | Submit a job with `--no-speculation`. While a task is active, match its `assigned` event and `worker_id` to a container's startup log and kill that container. Record the elapsed time until reassignment, compare `expired` and replacement `assigned`, and check the final result. Restore the fourth worker for the next run. |
| Combiner | Run the same repeated-key word count with and without `--no-combine`. Plot intermediate bytes and total time for both. |

Run `python3 -m pytest -q tests/test_client.py` during independent development. PR the CLI, Docker and CI setup first. After all service code lands, run `RUN_COMPOSE_TESTS=1 python3 -m pytest -q tests/test_end_to_end.py`, then finish the README and benchmark PR with actual measurements.
