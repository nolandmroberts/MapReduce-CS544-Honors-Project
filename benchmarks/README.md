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

## Results from November 16, 2026

All 30 measured jobs produced the expected word counts. The table reports medians from three runs per configuration.

| Experiment | Result | Evidence |
| --- | --- | --- |
| Parallelism | 28.90 s with one worker, 14.77 s with two, and 8.00 s with four. Four workers were 3.61× faster than one. | [Plot](plots/parallelism.png) · [CSV](results/parallelism.csv) |
| Speculation | With a 2 s delay, job time fell from 5.28 s to 3.41 s. With a 5 s delay, it fell from 11.24 s to 3.35 s. | [Plot](plots/speculation.png) · [Timeline](plots/speculation_timeline.png) · [CSV](results/speculation.csv) |
| Worker loss | Reassignment took about 9.94 s after the kill, consistent with the 10 s heartbeat timeout. Every retried job produced the expected counts. | [Plot](plots/worker_loss.png) · [Timeline](plots/worker_loss_timeline.png) · [CSV](results/worker-loss.csv) |
| Combiner | Intermediate data fell from 25.5 MB to 7.9 kB on the repeated-word input. Job time fell from 4.08 s to 1.39 s. | [Size plot](plots/combiner_bytes.png) · [Time plot](plots/combiner_time.png) · [CSV](results/combiner.csv) |

## Files

| File | Purpose |
| --- | --- |
| `generate_input.py` | Writes a deterministic word-count input file |
| `jobs/wordcount.py` | Plain word count (combiner experiment; also the reference answer) |
| `jobs/wordcount_cpu.py` | Same result, with extra hashing per word so the job is CPU-bound |
| `run_benchmarks.py` | Runs one experiment and appends a row per job to `results/<experiment>.csv` |
| `plot.py` | Reads `results/` and writes PNGs to `plots/` |
| `results/timelines/<job_id>.jsonl` | Coordinator `assigned` / `expired` / `committed` events for one job |

## Reproduce the evaluation

Run from the repository root on a four-CPU Linux VM. The checked-in runs used an arm64 Ubuntu 26.04 VM with four virtual CPUs and 4 GiB of memory. They used Docker 29.10.0, Compose 5.7.1, eight maps, four reducers, and three repetitions per configuration.

```bash
python3 -m pip install -r requirements.txt -r benchmarks/requirements.txt
docker compose up --build -d

# 1. Generate and upload the same deterministic inputs as the recorded run.
python3 benchmarks/generate_input.py benchmarks/data/words.txt --lines 300000
python3 benchmarks/generate_input.py benchmarks/data/straggler.txt --lines 80000
python3 benchmarks/generate_input.py benchmarks/data/repeated.txt --lines 150000 --vocab 50 --skew 1.2
python3 client.py upload benchmarks/data/words.txt --name words
python3 client.py upload benchmarks/data/straggler.txt --name straggler
python3 client.py upload benchmarks/data/repeated.txt --name repeated

# 2. Run the experiments and compare their median times.
python3 benchmarks/run_benchmarks.py parallelism --input words --code benchmarks/jobs/wordcount_cpu.py --settle 1
python3 benchmarks/run_benchmarks.py speculation --input straggler --code benchmarks/jobs/wordcount_cpu.py --delays 2 5 --settle 1
python3 benchmarks/run_benchmarks.py worker-loss --input words --code benchmarks/jobs/wordcount_cpu.py --settle 1
python3 benchmarks/run_benchmarks.py combiner --input repeated --code benchmarks/jobs/wordcount.py --settle 1

# 3. Plots
python3 benchmarks/plot.py
```

Useful options: `--reps N` (default 3), `--maps M --reduces R` (default 8 and 4),
`--workers 1 2 4` (parallelism), `--delays ...` (speculation, seconds added
to each task on the slow worker).

The runner appends to the CSVs in `benchmarks/results/`. Use a separate checkout with a fresh `results/` directory to keep a new evaluation separate from these runs.

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
  the same job with speculation on and off. Each measurement gets a fresh slow
  worker so an unfinished attempt from one run cannot influence the next.
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
