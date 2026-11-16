# Benchmarks

These scripts measure the four things our design doc promises to show:

| Experiment | Question | Plot |
| --- | --- | --- |
| Parallelism | Does a job get faster with 1 → 2 → 4 one-CPU workers? | `parallelism.png` |
| Speculation | With one slow worker, does starting backup copies of slow tasks help? | `speculation.png`, `speculation_timeline.png` |
| Worker loss | If a worker dies mid-task, how long until its task is detected and reassigned? | `worker_loss.png`, `worker_loss_timeline.png` |
| Combiner | How much less data reaches the reducers with a combiner, and is the job faster? | `combiner_bytes.png`, `combiner_time.png` |

Every run is checked against a reference answer computed locally, so a run that
is fast but wrong is recorded as incorrect and left out of the plots.

## Files

| File | Purpose |
| --- | --- |
| `generate_input.py` | Writes a deterministic word-count input file |
| `jobs/wordcount.py` | Plain word count (combiner experiment; also the reference answer) |
| `jobs/wordcount_cpu.py` | Same result, with extra hashing per word so the job is CPU-bound |
| `run_benchmarks.py` | Runs one experiment and appends a row per job to `results/<experiment>.csv` |
| `plot.py` | Reads `results/` and writes PNGs to `plots/` |
| `results/timelines/<job_id>.jsonl` | Coordinator `assigned` / `expired` / `committed` events for one job |

## Running them (on the course VM)

Run everything from the repository root.

```bash
python3 -m pip install -r requirements.txt -r benchmarks/requirements.txt
docker compose up --build -d

# 1. Inputs: one large-vocabulary file and one with many repeated words
python3 benchmarks/generate_input.py benchmarks/data/words.txt --lines 300000
python3 benchmarks/generate_input.py benchmarks/data/repeated.txt --lines 1000000 --vocab 50 --skew 1.2
python3 client.py upload benchmarks/data/words.txt --name words
python3 client.py upload benchmarks/data/repeated.txt --name repeated

# 2. Experiments (each prints a summary of median times when it finishes)
python3 benchmarks/run_benchmarks.py parallelism --input words --code benchmarks/jobs/wordcount_cpu.py
python3 benchmarks/run_benchmarks.py speculation --input words --code benchmarks/jobs/wordcount_cpu.py --delays 2 5
python3 benchmarks/run_benchmarks.py worker-loss --input words --code benchmarks/jobs/wordcount_cpu.py
python3 benchmarks/run_benchmarks.py combiner --input repeated --code benchmarks/jobs/wordcount.py

# 3. Plots
python3 benchmarks/plot.py
```

Useful options: `--reps N` (default 3), `--maps M --reduces R` (default 8 and 4),
`--workers 1 2 4` (parallelism), `--delays ...` (speculation, seconds added
to each task on the slow worker).

Pick the input size so that a 1-worker parallelism run takes at least ~30
seconds; otherwise startup overhead hides the speedup. The worker-loss
experiment also needs tasks that run longer than a few seconds, or the job may
finish before a worker can be killed (the script warns when that happens).

## How each experiment works

- **Parallelism:** scales the `worker` service to each count with
  `docker compose up -d --scale worker=N`, waits for the workers to start, then
  runs the job. Configurations are interleaved within each repetition so slow
  drift on the VM affects all of them equally.
- **Speculation:** runs three normal workers plus one started with
  `docker compose run -d --no-deps -e TASK_DELAY_SECONDS=<delay> worker`, then runs
  the same job with speculation on and off. The slow worker is removed afterwards.
  The timeline plot uses the run that started the most backup attempts.
- **Worker loss:** runs with `--no-speculation`, watches the coordinator log for a
  task assignment, matches its `worker_id` to a container using the worker's
  startup log line, and `docker kill`s that container. From the coordinator's
  `expired` and `assigned` events it records how long detection and
  reassignment took after the kill. The killed worker is restarted for the next run.
- **Combiner:** runs the same job with the combiner on and off and records the
  size of the committed intermediate files under `shared/jobs/<job_id>/map/`.

## CSV columns

`timestamp, experiment, rep, job_id, workers, maps, reduces, speculation,
combine, delay_s, state, correct, map_s, reduce_s, total_s, wall_s,
intermediate_bytes, kill_s, expired_s, reassigned_s`

`map_s`, `reduce_s` and `total_s` come from the coordinator's job status.
`wall_s` is measured by the script from submission until it saw the job finish.
`kill_s`, `expired_s` and `reassigned_s` are only filled in for worker loss
(`expired_s` and `reassigned_s` are seconds after the kill).
