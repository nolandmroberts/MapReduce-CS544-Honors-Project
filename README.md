# CS 544 Honors MapReduce

The coordinator and four one-CPU workers run on a single VM with Docker Compose. Run the Python client on the VM outside the containers. The client, coordinator, and workers share the `shared/` directory.

## Build and run word count

Use Docker with the `docker compose` command and Python 3.12 or newer. On Ubuntu, install `python3-venv` if the virtual environment command asks for it. From the repository root, install the client dependencies and build the image used by both services.

```bash
python3 -m venv .venv
source .venv/bin/activate
python -m pip install -r requirements.txt
docker compose up --build -d
docker compose ps
```

When the coordinator is listening on port 8080, upload the example data and submit the example job.

```bash
python client.py upload examples/input.txt --name sample
job_id=$(python client.py submit --input sample --output "wordcount-$(date +%s)" \
  --code examples/wordcount.py --maps 8 --reduces 4)
python client.py status "$job_id"
```

Once `status` shows `succeeded`, read the reducer output. The part files contain `["hello", 2]` and `["world", 1]` across all partitions.

```bash
results_dir=$(python client.py results "$job_id")
cat "$results_dir"/part-*.jsonl
```

Reuse the uploaded `sample` dataset for later runs and pick a fresh output name for each job. Run `docker compose down` to stop the services.

## Write a job

Use [the word-count job](examples/wordcount.py) as a template. Define `map(key, value)` and `reduce(key, values)` in a Python file. Both yield `(key, value)` pairs with string keys and JSON-serializable values. The mapper receives a line of text from a `.txt` file or a decoded `.jsonl` value. The reducer gets an iterator of values for one key.

An optional `combine(key, values)` can aggregate values within a map before writing them. Keep the key the same and emit values the reducer can merge. Use `--no-combine` and `--no-speculation` on `submit` to compare behavior with those features off. See the [implementation contract](docs/architecture.md) for the API, storage layout, and retry rules.

## Tests and benchmarks

```bash
python -m pytest -q
```

With the four Compose workers running, install the plotting dependency and run the full suite.

```bash
python -m pip install -r benchmarks/requirements.txt
RUN_COMPOSE_TESTS=1 python -m pytest -q
```

The [benchmark guide](benchmarks/README.md) gives the commands for measuring worker scaling, speculation, recovery after a worker dies, and the combiner.
