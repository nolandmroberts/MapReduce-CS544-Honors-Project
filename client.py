"""Host command-line client for the MapReduce system.

Commands:
  upload PATH --name NAME   copy input data into the shared directory
  submit ...                submit a job to the coordinator and print its ID
  status JOB_ID             print a job's status as JSON
  results JOB_ID            print the local output directory of a finished job

Settings come from the environment:
  SHARED_DIR       shared directory on the host (default: ./shared)
  COORDINATOR_URL  coordinator address (default: http://127.0.0.1:8080)
"""

import argparse
import json
import os
import re
import shutil
import sys
import uuid
from urllib.parse import quote

import requests

DEFAULT_SHARED_DIR = "./shared"
DEFAULT_COORDINATOR_URL = "http://127.0.0.1:8080"
REQUEST_TIMEOUT_SECONDS = 10
DATA_SUFFIXES = (".txt", ".jsonl")
NAME_PATTERN = re.compile(r"[A-Za-z0-9][A-Za-z0-9._-]*")


class ClientError(Exception):
    """An error to report to the user as a single line on stderr."""


def shared_dir():
    return os.environ.get("SHARED_DIR", DEFAULT_SHARED_DIR)


def coordinator_url():
    return os.environ.get("COORDINATOR_URL", DEFAULT_COORDINATOR_URL).rstrip("/")


def check_name(name, kind):
    if not NAME_PATTERN.fullmatch(name):
        raise ClientError(
            f"invalid {kind} name {name!r}: use letters, digits, '.', '_' or '-', "
            "starting with a letter or digit"
        )


def positive_int(text):
    try:
        value = int(text)
    except ValueError:
        raise argparse.ArgumentTypeError(f"{text!r} is not an integer")
    if value < 1:
        raise argparse.ArgumentTypeError(f"{value} is not a positive integer")
    return value


# ---------------------------------------------------------------------------
# upload


def collect_data_files(source):
    """Return (absolute path, path relative to the upload root) for each data file.

    Rejects symlinks anywhere in the source, files that are not .txt or .jsonl,
    and sources that contain no data files.
    """
    if os.path.islink(source):
        raise ClientError(f"refusing to upload symlink {source}")
    if os.path.isfile(source):
        if not source.endswith(DATA_SUFFIXES):
            raise ClientError(f"{source} is not a .txt or .jsonl file")
        return [(source, os.path.basename(source))]
    if not os.path.isdir(source):
        raise ClientError(f"{source} is not a regular file or directory")

    files = []
    for root, dirs, names in os.walk(source):
        for entry in dirs + names:
            path = os.path.join(root, entry)
            if os.path.islink(path):
                raise ClientError(f"refusing to upload symlink {path}")
        dirs.sort()
        for name in sorted(names):
            path = os.path.join(root, name)
            if not os.path.isfile(path):
                raise ClientError(f"{path} is not a regular file")
            if not name.endswith(DATA_SUFFIXES):
                raise ClientError(f"{path} is not a .txt or .jsonl file")
            files.append((path, os.path.relpath(path, source)))
    if not files:
        raise ClientError(f"{source} contains no .txt or .jsonl files")
    return files


def upload(source, name):
    check_name(name, "upload")
    files = collect_data_files(source)

    inputs_dir = os.path.join(shared_dir(), "inputs")
    target = os.path.join(inputs_dir, name)
    if os.path.lexists(target):
        raise ClientError(f"upload {name!r} already exists; choose another name")

    # Copy into a hidden staging directory first, then rename it into place, so
    # nobody can see a partially copied upload. The leading "." means the
    # staging name can never match a valid upload name.
    os.makedirs(inputs_dir, exist_ok=True)
    staging = os.path.join(inputs_dir, f".staging-{name}-{uuid.uuid4().hex}")
    os.mkdir(staging)
    try:
        for path, relative in files:
            destination = os.path.join(staging, relative)
            os.makedirs(os.path.dirname(destination), exist_ok=True)
            shutil.copyfile(path, destination)
        if os.path.lexists(target):
            raise ClientError(f"upload {name!r} already exists; choose another name")
        os.rename(staging, target)
    except BaseException:
        shutil.rmtree(staging, ignore_errors=True)
        raise
    print(name)


# ---------------------------------------------------------------------------
# coordinator requests


def request_json(method, path, body=None):
    url = coordinator_url() + path
    try:
        response = requests.request(method, url, json=body, timeout=REQUEST_TIMEOUT_SECONDS)
    except requests.RequestException as error:
        raise ClientError(f"could not reach the coordinator at {coordinator_url()}: {error}")

    try:
        data = response.json()
    except ValueError:
        data = None

    if response.status_code >= 400:
        message = data.get("error") if isinstance(data, dict) else None
        raise ClientError(f"coordinator returned HTTP {response.status_code}: {message or response.text}")
    if not isinstance(data, dict):
        raise ClientError(f"coordinator returned an unexpected response: {response.text}")
    return data


def get_job(job_id):
    return request_json("GET", "/jobs/" + quote(job_id, safe=""))


def submit(args):
    check_name(args.input, "input")
    check_name(args.output, "output")
    try:
        with open(args.code, encoding="utf-8") as f:
            code = f.read()
    except (OSError, UnicodeDecodeError) as error:
        raise ClientError(f"could not read job file {args.code}: {error}")

    job = request_json(
        "POST",
        "/jobs",
        {
            "input": args.input,
            "output": args.output,
            "code": code,
            "maps": args.maps,
            "reduces": args.reduces,
            "speculation": args.speculation,
            "combine": args.combine,
        },
    )
    if "job_id" not in job:
        raise ClientError(f"coordinator response has no job_id: {job}")
    print(job["job_id"])


def status(job_id):
    print(json.dumps(get_job(job_id), indent=2))


def results(job_id):
    job = get_job(job_id)
    state = job.get("state")
    if state == "succeeded":
        if not job.get("output"):
            raise ClientError(f"coordinator response has no output path: {job}")
        print(os.path.join(shared_dir(), job["output"]))
    elif state == "failed":
        raise ClientError(f"job {job_id} failed: {job.get('error')}")
    else:
        raise ClientError(f"job {job_id} has not finished (state: {state})")


# ---------------------------------------------------------------------------
# command line


def build_parser():
    parser = argparse.ArgumentParser(prog="client.py", description="MapReduce client")
    commands = parser.add_subparsers(dest="command", required=True)

    p = commands.add_parser("upload", help="copy input data into shared storage")
    p.add_argument("path", help="a .txt or .jsonl file, or a directory of them")
    p.add_argument("--name", required=True, help="name for the upload")

    p = commands.add_parser("submit", help="submit a job and print its ID")
    p.add_argument("--input", required=True, help="name of an upload")
    p.add_argument("--output", required=True, help="name for the output (must be new)")
    p.add_argument("--code", required=True, help="Python file defining map and reduce")
    p.add_argument("--maps", required=True, type=positive_int, help="number of map tasks")
    p.add_argument("--reduces", required=True, type=positive_int, help="number of reduce tasks")
    p.add_argument("--no-speculation", dest="speculation", action="store_false",
                   help="turn off speculative execution for this job")
    p.add_argument("--no-combine", dest="combine", action="store_false",
                   help="turn off the combiner for this job")

    p = commands.add_parser("status", help="print a job's status as JSON")
    p.add_argument("job_id")

    p = commands.add_parser("results", help="print the output directory of a finished job")
    p.add_argument("job_id")
    return parser


def main(argv=None):
    args = build_parser().parse_args(argv)
    try:
        if args.command == "upload":
            upload(args.path, args.name)
        elif args.command == "submit":
            submit(args)
        elif args.command == "status":
            status(args.job_id)
        elif args.command == "results":
            results(args.job_id)
    except (ClientError, OSError) as error:
        print(f"error: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
