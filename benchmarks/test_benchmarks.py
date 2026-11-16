"""Unit tests for the benchmark tooling. None of these need Docker."""

import csv
import json
from pathlib import Path

import pytest

import generate_input
import run_benchmarks as rb

JOBS = Path(__file__).resolve().parent / "jobs"


@pytest.fixture
def shared(tmp_path):
    """A fake shared directory with one upload containing a .txt and a .jsonl file."""
    upload = tmp_path / "shared" / "inputs" / "sample"
    (upload / "more").mkdir(parents=True)
    (upload / "a.txt").write_bytes(b"hello world\r\nhello\n")
    (upload / "more" / "b.jsonl").write_text('"world again"\n', encoding="utf-8")
    return tmp_path / "shared"


# ---------------------------------------------------------------------------
# input generation


def test_generate_is_deterministic(tmp_path):
    first, second = tmp_path / "one.txt", tmp_path / "two.txt"
    assert generate_input.generate(first, lines=200, words_per_line=5, vocab=30) == 1000
    generate_input.generate(second, lines=200, words_per_line=5, vocab=30)

    assert first.read_bytes() == second.read_bytes()
    lines = first.read_text(encoding="utf-8").splitlines()
    assert len(lines) == 200
    assert all(len(line.split()) == 5 for line in lines)


def test_higher_skew_repeats_the_top_word_more(tmp_path):
    def top_share(skew):
        path = tmp_path / f"skew-{skew}.txt"
        generate_input.generate(path, lines=500, words_per_line=10, vocab=100, skew=skew)
        words = path.read_text(encoding="utf-8").split()
        return words.count("word00000") / len(words)

    assert top_share(2.0) > top_share(0.5)


# ---------------------------------------------------------------------------
# reference result and output checking


def test_input_records_match_the_worker_contract(shared):
    records = list(rb.iter_input_records(shared / "inputs" / "sample"))

    assert records == [
        ("inputs/sample/a.txt:0", "hello world"),
        ("inputs/sample/a.txt:13", "hello"),
        ("inputs/sample/more/b.jsonl:0", "world again"),
    ]


def test_reference_word_count(shared):
    expected = {"hello": 2, "world": 2, "again": 1}

    assert rb.reference_result(JOBS / "wordcount.py", shared / "inputs" / "sample") == expected
    assert rb.reference_result(JOBS / "wordcount_cpu.py", shared / "inputs" / "sample") == expected


def test_reference_is_cached(shared, tmp_path, monkeypatch):
    calls = []
    real = rb.reference_result
    monkeypatch.setattr(rb, "reference_result", lambda *a: calls.append(a) or real(*a))
    cache = tmp_path / "cache"
    input_dir = shared / "inputs" / "sample"

    first = rb.cached_reference(JOBS / "wordcount.py", input_dir, cache)
    second = rb.cached_reference(JOBS / "wordcount.py", input_dir, cache)

    assert first == second == {"hello": 2, "world": 2, "again": 1}
    assert len(calls) == 1


def write_parts(directory, parts):
    directory.mkdir(parents=True)
    for j, rows in enumerate(parts):
        (directory / f"part-{j}.jsonl").write_text(
            "".join(json.dumps(row) + "\n" for row in rows), encoding="utf-8")


def test_output_check_accepts_matching_output(tmp_path):
    write_parts(tmp_path / "out", [[["hello", 2]], [], [["world", 1]]])

    assert rb.output_is_correct(tmp_path / "out", {"hello": 2, "world": 1})


def test_output_check_rejects_wrong_count(tmp_path):
    write_parts(tmp_path / "out", [[["hello", 3]], [["world", 1]]])

    assert not rb.output_is_correct(tmp_path / "out", {"hello": 2, "world": 1})


def test_output_check_rejects_key_in_two_parts(tmp_path):
    write_parts(tmp_path / "out", [[["hello", 2]], [["hello", 2], ["world", 1]]])

    assert rb.read_job_output(tmp_path / "out")[1] == 1
    assert not rb.output_is_correct(tmp_path / "out", {"hello": 2, "world": 1})


def test_directory_bytes(tmp_path):
    (tmp_path / "map" / "m-0").mkdir(parents=True)
    (tmp_path / "map" / "m-0" / "r-0.jsonl").write_bytes(b"x" * 10)
    (tmp_path / "map" / "m-0" / "r-1.jsonl").write_bytes(b"x" * 5)

    assert rb.directory_bytes(tmp_path / "map") == 15
    assert rb.directory_bytes(tmp_path / "missing") == 0


# ---------------------------------------------------------------------------
# coordinator log events


def event(name, task, attempt, worker, elapsed, job="job-1", speculative=False):
    return {"event": name, "job_id": job, "task_id": task, "attempt_id": attempt,
            "worker_id": worker, "speculative": speculative, "elapsed_s": elapsed}


def as_log(*events):
    lines = [" * Running on http://0.0.0.0:8080", "not json {", '{"no_event": true}']
    lines += [json.dumps(e) for e in events]
    return "\n".join(lines)


def test_parse_events_skips_other_lines_and_jobs():
    log = as_log(event("assigned", "m-0", "a1", "w1", 0.1),
                 event("assigned", "m-0", "a9", "w1", 0.1, job="other"))

    assert [e["attempt_id"] for e in rb.parse_events(log, "job-1")] == ["a1"]
    assert len(rb.parse_events(log)) == 2


def test_active_assignments():
    events = [
        event("assigned", "m-0", "a1", "w1", 0.1),
        event("assigned", "m-1", "a2", "w2", 0.2),
        event("committed", "m-0", "a1", "w1", 1.0),
        event("assigned", "m-2", "a3", "w1", 1.1),
    ]

    assert [e["attempt_id"] for e in rb.active_assignments(events)] == ["a2", "a3"]


def test_recovery_times():
    events = [
        event("assigned", "m-1", "a2", "w2", 0.2),
        event("expired", "m-1", "a2", "w2", 12.5),
        event("assigned", "m-1", "a5", "w3", 12.6),
        event("committed", "m-1", "a5", "w3", 14.0),
    ]

    expired, reassigned = rb.recovery_times(events, "a2", kill_s=2.0)

    assert expired == pytest.approx(10.5)
    assert reassigned == pytest.approx(10.6)


def test_find_worker_id_uses_latest_startup_line():
    log = '{"worker_id": "old"}\nTraceback...\n{"worker_id": "new"}\n'

    assert rb.find_worker_id(log) == "new"
    assert rb.find_worker_id("no id here") is None


def test_append_row_writes_header_once(tmp_path):
    path = tmp_path / "results" / "x.csv"
    rb.append_row(path, {"experiment": "x", "rep": 1, "total_s": 1.5})
    rb.append_row(path, {"experiment": "x", "rep": 2, "total_s": 2.5})

    with open(path, newline="") as f:
        rows = list(csv.DictReader(f))
    assert [r["rep"] for r in rows] == ["1", "2"]
    assert list(rows[0]) == rb.CSV_FIELDS


# ---------------------------------------------------------------------------
# plotting


def fake_results(results_dir):
    def row(experiment, rep, total, workers=4, speculation=True, combine=True, delay=0,
            job=None, inter=1000, correct=True, **extra):
        return {"experiment": experiment, "rep": rep, "job_id": job or f"{experiment}-{rep}-{total}",
                "workers": workers, "maps": 8, "reduces": 4, "speculation": speculation,
                "combine": combine, "delay_s": delay, "state": "succeeded", "correct": correct,
                "map_s": total * 0.8, "reduce_s": total * 0.2, "total_s": total, "wall_s": total + 0.3,
                "intermediate_bytes": inter, **extra}

    for rep in (1, 2, 3):
        for workers, total in ((1, 40.0), (2, 21.0), (4, 11.0)):
            rb.append_row(results_dir / "parallelism.csv", row("parallelism", rep, total + rep, workers))
        rb.append_row(results_dir / "speculation.csv",
                      row("speculation", rep, 14.0 + rep, delay=5, job=f"spec-{rep}"))
        rb.append_row(results_dir / "speculation.csv",
                      row("speculation", rep, 30.0 + rep, speculation=False, delay=5))
        rb.append_row(results_dir / "combiner.csv", row("combiner", rep, 8.0 + rep, inter=2_000_000))
        rb.append_row(results_dir / "combiner.csv",
                      row("combiner", rep, 12.0 + rep, combine=False, inter=60_000_000))
        rb.append_row(results_dir / "worker-loss.csv",
                      row("worker-loss", rep, 25.0, speculation=False, job=f"loss-{rep}",
                          kill_s=3.0, expired_s=10.2 + rep / 10, reassigned_s=10.4 + rep / 10))
    # A wrong result must be ignored by the plots.
    rb.append_row(results_dir / "parallelism.csv", row("parallelism", 4, 1.0, 1, correct=False))

    timelines = results_dir / "timelines"
    timelines.mkdir()
    spec = [event("assigned", f"m-{i}", f"a{i}", f"w{i % 4}", i * 0.5) for i in range(4)]
    spec += [event("committed", f"m-{i}", f"a{i}", f"w{i % 4}", 3 + i) for i in range(3)]
    spec += [event("assigned", "m-3", "b3", "w0", 6.5, speculative=True),
             event("committed", "m-3", "b3", "w0", 9.0)]
    (timelines / "spec-1.jsonl").write_text("".join(json.dumps(e) + "\n" for e in spec))
    loss = [event("assigned", "m-0", "a0", "w0", 0.1), event("assigned", "m-1", "a1", "w1", 0.1),
            event("committed", "m-0", "a0", "w0", 4.0), event("expired", "m-1", "a1", "w1", 13.2),
            event("assigned", "m-1", "a2", "w0", 13.4), event("committed", "m-1", "a2", "w0", 18.0)]
    (timelines / "loss-1.jsonl").write_text("".join(json.dumps(e) + "\n" for e in loss))


def test_plots_are_written(tmp_path):
    pytest.importorskip("matplotlib")
    import plot

    results, out = tmp_path / "results", tmp_path / "plots"
    fake_results(results)

    assert plot.main(["--results", str(results), "--out", str(out)]) == 0

    assert sorted(p.name for p in out.glob("*.png")) == [
        "combiner_bytes.png", "combiner_time.png", "parallelism.png", "speculation.png",
        "speculation_timeline.png", "worker_loss.png", "worker_loss_timeline.png",
    ]


def test_plots_ignore_incorrect_runs(tmp_path):
    pytest.importorskip("matplotlib")
    import plot

    results = tmp_path / "results"
    fake_results(results)
    rows = plot.read_rows(results / "parallelism.csv")

    assert len(rows) == 9
    assert all(r["total_s"] > 1.0 for r in rows)


def test_plot_with_no_results(tmp_path, capsys):
    pytest.importorskip("matplotlib")
    import plot

    assert plot.main(["--results", str(tmp_path / "none"), "--out", str(tmp_path / "plots")]) == 0
    assert "no results found" in capsys.readouterr().out
