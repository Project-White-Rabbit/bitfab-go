from __future__ import annotations

import argparse
import base64
import contextlib
import json
import os
import re
import shlex
import signal
import subprocess
import sys
import tempfile
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

CONFIG = ".bitfab/cloud.json"
PREFIX = "bitfab-replay/"
DEFAULT_SECRET_PREFIX = "BITFAB_CLOUD_"
API_VERSION = "2026-03-10"
ITEM_ERROR_FIELDS = (
    "error",
    "traceError",
    "trace_error",
    "replayError",
    "replay_error",
)
UUID = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
SHA = re.compile(r"^[0-9a-f]{40}$")
PIPELINE = re.compile(r"^[\w.][\w.-]*$")
HELP = """GitHub cloud replay (requires git, gh login, and Python 3.10+).
  --cloud PIPELINE --trace-ids UUID[,UUID] [--registry PATH]
    [--max-concurrency 1..32] [--cloud-request-id UUID]
    [--cloud-include PATH ...] [--cloud-dry-run] [--cloud-detach]
  --cloud-status UUID | --cloud-watch UUID | --cloud-cancel UUID
  --cloud-cleanup UUID
  --cloud-init --config SPEC | --cloud-secrets --env-file FILE [NAME ...]
PIPELINE may be any pipeline in the registry, unless .bitfab/cloud.json names one.
Snapshot tracked working files without changing HEAD, the index, or local files.
New files require explicit --cloud-include. Credentials and ignored files are refused.
By default wait for completion and remove the remote snapshot branch. Detached runs
continue on GitHub; watch/status/cleanup can recover them using the printed UUID.
"""


def command(args, *, cwd=None, env=None, timeout=60, input=None):
    result = subprocess.run(
        args,
        cwd=cwd,
        env=env,
        input=input,
        text=True,
        check=False,
        capture_output=True,
        timeout=timeout,
    )
    if result.returncode:
        # Child errors can contain credential-bearing URLs or application output.
        raise RuntimeError(
            f"{args[0]} {args[1]} failed (exit {result.returncode}); check authentication and permissions"
        )
    return result.stdout.strip()


def git(root, *args, env=None, input=None):
    return command(["git", "-C", str(root), *args], env=env, input=input)


def root_directory():
    return Path(command(["git", "rev-parse", "--show-toplevel"])).resolve()


def relative_path(value):
    if not isinstance(value, str) or not value or "\\" in value or "\x00" in value:
        raise ValueError("Expected a repository-relative path")
    if Path(value).is_absolute() or ".." in Path(value).parts or value.startswith("-"):
        raise ValueError("Paths must stay inside the repository")
    return value


def within(root, value):
    path = (root / relative_path(value)).resolve()
    path.relative_to(root)
    return path


def repository(root):
    remote = git(root, "remote", "get-url", "origin")
    match = re.fullmatch(
        r"(?:git@github\.com:|https://github\.com/)([\w.-]+/[\w.-]+?)(?:\.git)?", remote
    )
    if match is None:
        raise ValueError(
            "origin must be a github.com repository without embedded credentials"
        )
    # A pushurl can silently send the snapshot somewhere other than origin's fetch URL.
    push = git(root, "remote", "get-url", "--push", "origin")
    other = re.fullmatch(
        r"(?:git@github\.com:|https://github\.com/)([\w.-]+/[\w.-]+?)(?:\.git)?", push
    )
    if other is None or other[1].lower() != match[1].lower():
        raise ValueError("origin fetch and push repositories must match")
    return match[1]


def configuration(root):
    path = within(root, CONFIG)
    if not path.is_file():
        raise ValueError(
            f"Run bitfab:setup cloud first. Expected {CONFIG} inside this repository."
        )
    config = json.loads(path.read_text())
    validate_config(config)
    return config


ENV_ASSIGNMENT = re.compile(r"[ \t]*(?:export[ \t]+)?([A-Za-z_][A-Za-z0-9_]*)[ \t]*=")

ENV_ESCAPES = {"n": "\n", "r": "\r", "t": "\t", '"': '"', "\\": "\\"}


def read_environment_value(text, assignment_end, name):
    start = assignment_end
    while start < len(text) and text[start] in " \t":
        start += 1
    if start < len(text) and text[start] in ("'", '"'):
        quote = text[start]
        pieces = []
        position = start + 1
        while position < len(text):
            character = text[position]
            if character == "\\" and quote == '"' and position + 1 < len(text):
                following = text[position + 1]
                pieces.append(ENV_ESCAPES.get(following, character + following))
                position += 2
                continue
            if character == quote:
                return "".join(pieces), position + 1
            pieces.append(character)
            position += 1
        raise ValueError(f"{name} opens a quote that the file never closes")
    end = text.find("\n", assignment_end)
    end = len(text) if end == -1 else end
    raw = text[assignment_end:end]
    comment = re.search(r"\s#", raw)
    return (raw if comment is None else raw[: comment.start()]).strip(), end


def parse_environment_file(text):
    values = {}
    position = 0
    while position < len(text):
        end = text.find("\n", position)
        end = len(text) if end == -1 else end
        line = text[position:end]
        match = ENV_ASSIGNMENT.match(line)
        if match is None or line.lstrip().startswith("#"):
            position = end + 1
            continue
        value, consumed = read_environment_value(text, position + match.end(), match[1])
        values[match[1]] = value
        newline = text.find("\n", consumed)
        position = len(text) if newline == -1 else newline + 1
    return values


def configure_secrets(argv):
    parser = argparse.ArgumentParser(
        prog="bitfab-replay --cloud-secrets",
        description="Copy named values from local environment files into GitHub Actions secrets. Values are piped to gh on standard input and never printed, logged, or passed as arguments.",
    )
    parser.add_argument(
        "names",
        nargs="*",
        help="Environment variable names to copy; defaults to the secrets recorded by setup",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        required=True,
        help="Repository-relative environment file to read; repeatable, earliest definition wins",
    )
    parser.add_argument(
        "--environment",
        help="Set GitHub Environment secrets instead of repository ones",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which names were found without writing anything to GitHub",
    )
    args = parser.parse_args(argv)
    root = root_directory()
    config = configuration(root)
    prefix = config.get("secretPrefix", "")
    names = args.names or config.get("secrets") or ["BITFAB_API_KEY"]
    if not all(re.fullmatch(r"[A-Z_][A-Z0-9_]*", name) for name in names):
        raise ValueError("Secret names must be uppercase environment variable names")
    values = {}
    for path in args.env_file:
        for key, value in parse_environment_file(
            within(root, path).read_text()
        ).items():
            values.setdefault(key, value)
    repo = repository(root)
    assigned = []
    missing = []
    empty = []
    for name in names:
        value = values.get(name)
        if value is None:
            missing.append(name)
            continue
        if not value:
            empty.append(name)
            continue
        target = prefix + name
        if not args.dry_run:
            command(
                [
                    "gh",
                    "secret",
                    "set",
                    target,
                    "--repo",
                    repo,
                    *(["--env", args.environment] if args.environment else []),
                ],
                input=value,
            )
        assigned.append(target)
    return {
        "repository": repo,
        "environment": args.environment,
        "dryRun": args.dry_run,
        "set": assigned,
        "missing": missing,
        "empty": empty,
        "next": "Create the missing secrets by hand; empty local values were skipped because an empty secret overrides a working default with nothing; a wrong value only surfaces when the first real replay runs",
    }


def validate_config(config):
    if config.get("version") != 1 or config.get("provider") != "github":
        raise ValueError("Run bitfab:setup cloud to configure the GitHub provider")
    if not re.fullmatch(r"[\w-]+\.ya?ml", config.get("workflow", "")):
        raise ValueError("Invalid workflow filename")
    relative_path(config["workingDirectory"])
    if config.get("registry") is not None:
        relative_path(config["registry"])
    pipeline = config.get("pipeline")
    if pipeline is not None and not (
        isinstance(pipeline, str) and PIPELINE.fullmatch(pipeline)
    ):
        raise ValueError("Invalid pipeline name")
    args = config.get("command")
    if (
        not isinstance(args, list)
        or not args
        or not all(isinstance(v, str) and v and "\x00" not in v for v in args)
    ):
        raise ValueError("command must be a nonempty JSON argument array")
    if any(v.startswith("--cloud") for v in args):
        raise ValueError(
            "The runner command must execute locally, not recursively dispatch"
        )
    if config.get("pushTriggersReviewed") is not True:
        raise ValueError(
            "Setup must review push-triggered CI/deployments and set pushTriggersReviewed=true"
        )
    prefix = config.get("secretPrefix", "")
    if not isinstance(prefix, str) or (
        prefix and not re.fullmatch(r"[A-Z][A-Z0-9_]*_", prefix)
    ):
        raise ValueError(
            "secretPrefix must be uppercase and end with an underscore, such as BITFAB_CLOUD_"
        )
    names = config.get("secrets", [])
    if not isinstance(names, list) or not all(
        isinstance(name, str) and re.fullmatch(r"[A-Z_][A-Z0-9_]*", name)
        for name in names
    ):
        raise ValueError("secrets must be uppercase environment variable names")


def check_pipeline(config, pipeline):
    if not isinstance(pipeline, str) or not PIPELINE.fullmatch(pipeline):
        raise ValueError("Invalid pipeline name")
    allowed = config.get("pipeline")
    if allowed is not None and pipeline != allowed:
        raise ValueError("Pipeline does not match .bitfab/cloud.json")


def gh(repo, *args, payload=None):
    return command(
        [
            "gh",
            "api",
            "--hostname",
            "github.com",
            "-H",
            f"X-GitHub-Api-Version: {API_VERSION}",
            "--jq",
            "tojson",
            *args,
            *(["--input", "-"] if payload is not None else []),
        ],
        input=json.dumps(payload) if payload is not None else None,
    )


def api(repo, suffix, *, method="GET", payload=None):
    result = gh(
        repo,
        f"repos/{repo}" + (f"/{suffix}" if suffix else ""),
        "--method",
        method,
        payload=payload,
    )
    return json.loads(result) if result else None


def parse(argv):
    flags = [value.split("=", 1)[0] for value in argv if value.startswith("--")]
    if any(flags.count(flag) > 1 for flag in flags if flag != "--cloud-include"):
        raise ValueError("Duplicate cloud option")
    parser = argparse.ArgumentParser(
        description=HELP,
        formatter_class=argparse.RawDescriptionHelpFormatter,
        allow_abbrev=False,
    )
    parser.add_argument("pipeline", nargs="?")
    parser.add_argument("--registry")
    parser.add_argument("--cloud", action="store_true")
    for name in ("status", "watch", "cancel", "cleanup"):
        parser.add_argument(f"--cloud-{name}")
    parser.add_argument("--trace-ids")
    parser.add_argument("--max-concurrency", type=int, default=1)
    parser.add_argument("--cloud-request-id")
    parser.add_argument("--cloud-include", action="append", default=[])
    parser.add_argument("--cloud-dry-run", action="store_true")
    parser.add_argument("--cloud-detach", action="store_true")
    args = parser.parse_args(argv)
    operations = [
        name
        for name in ("status", "watch", "cancel", "cleanup")
        if getattr(args, "cloud_" + name)
    ]
    if operations:
        if len(operations) != 1 or len(argv) != 2:
            raise ValueError("Cloud lifecycle commands take only their execution UUID")
        operation = operations[0]
        execution_id = getattr(args, "cloud_" + operation)
    else:
        if not args.cloud or not args.pipeline or not args.trace_ids:
            raise ValueError(HELP)
        operation = "submit"
        execution_id = args.cloud_request_id or str(uuid.uuid4())
        traces = args.trace_ids.split(",")
        if not 1 <= len(traces) <= 100 or not all(UUID.fullmatch(t) for t in traces):
            raise ValueError("Supply 1..100 explicit trace UUIDs")
        if not 1 <= args.max_concurrency <= 32:
            raise ValueError("--max-concurrency must be 1..32")
    if not UUID.fullmatch(execution_id):
        raise ValueError("Execution ID must be a UUID")
    return args, operation, execution_id


def sensitive(path):
    parts = Path(path).parts
    name = Path(path).name.lower()
    return (
        any(part in (".git", ".ssh", ".aws") for part in parts)
        or name == ".env"
        or (
            name.startswith(".env.")
            and name not in (".env.example", ".env.sample", ".env.template")
        )
        or name in (".npmrc", ".pypirc", ".netrc", "id_rsa", "id_ed25519")
        or name.endswith((".pem", ".key", ".p12", ".pfx", ".local.json"))
        or name
        in (
            "credentials",
            "credentials.json",
            "secrets.json",
            "secrets.yml",
            "secrets.yaml",
        )
    )


def snapshot(root, config, args, execution_id):
    if git(root, "ls-files", "-u"):
        raise ValueError("Resolve merge conflicts before snapshotting")
    head = git(root, "rev-parse", "HEAD")
    includes = []
    for value in args.cloud_include:
        path = within(root, value)
        if not path.is_file() or path.is_symlink() or (root / value).is_symlink():
            raise ValueError("--cloud-include requires individual regular files")
        # Never force-add ignored content, even when explicitly requested.
        ignored = subprocess.run(
            ["git", "-C", str(root), "check-ignore", "--quiet", "--", value],
            check=False,
            timeout=15,
        ).returncode
        if ignored not in (0, 1):
            raise ValueError("Unable to determine whether the included file is ignored")
        if ignored == 0 or sensitive(value):
            raise ValueError(f"Refusing ignored or credential-like file: {value}")
        includes.append(value)
    with tempfile.TemporaryDirectory(prefix="bitfab-cloud-index-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        git(root, "read-tree", head, env=env)
        git(root, "add", "-u", "--", ".", env=env)
        if includes:
            git(root, "--literal-pathspecs", "add", "--", *includes, env=env)
        entries = git(root, "ls-files", "--stage", "-z", env=env).split("\x00")
        for entry in filter(None, entries):
            metadata, path = entry.split("\t", 1)
            if metadata.startswith("160000"):
                raise ValueError(
                    "Submodules/gitlinks require explicit setup support before cloud snapshots"
                )
            if sensitive(path):
                raise ValueError(
                    f"Refusing credential-like tracked file in snapshot: {path}"
                )
        required = [CONFIG, f".github/workflows/{config['workflow']}"]
        if config.get("registry") is not None:
            required.append(config["registry"])
        tracked = set(
            git(
                root, "--literal-pathspecs", "ls-files", "-z", "--", *required, env=env
            ).split("\x00")
        )
        untracked = [path for path in required if path not in tracked]
        if untracked:
            raise ValueError(
                "Not in the snapshot: "
                + ", ".join(untracked)
                + ". Commit these files, or pass --cloud-include for each one"
            )
        tree = git(root, "write-tree", env=env)
        files = git(root, "diff", "--name-only", head, tree).splitlines()
        if args.cloud_dry_run:
            new_files = set(
                git(root, "ls-files", "--others", "--exclude-standard").splitlines()
            )
            new_files.update(
                git(
                    root, "diff", "--cached", "--name-only", "--diff-filter=A"
                ).splitlines()
            )
            return {
                "baseSha": head,
                "files": files,
                "includedFiles": includes,
                "omittedNewFiles": sorted(new_files - set(includes)),
            }
        # commit-tree adds an unreachable object, never touching HEAD or the real index.
        sha = git(
            root,
            "commit-tree",
            tree,
            "-p",
            head,
            input=f"Bitfab replay {execution_id}\n",
        )
        return {"baseSha": head, "sha": sha, "files": files}


def state_directory(root):
    path = Path(git(root, "rev-parse", "--absolute-git-dir")) / "bitfab-cloud"
    path.mkdir(mode=0o700, exist_ok=True)
    return path


def save(path, record):
    with tempfile.NamedTemporaryFile(mode="w", dir=path.parent, delete=False) as file:
        json.dump(record, file)
        file.write("\n")
        name = file.name
    os.replace(name, path)


@contextlib.contextmanager
def execution_lock(directory, execution_id):
    import fcntl

    with (directory / f"{execution_id}.lock").open("a") as lock:
        try:
            fcntl.flock(lock, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            raise ValueError(
                "Another command is operating on this cloud execution"
            ) from error
        yield


def find_run(record):
    query = urlencode(
        {"event": "workflow_dispatch", "branch": record["branch"], "per_page": 100}
    )
    runs = api(
        record["repository"], f"actions/workflows/{record['workflow']}/runs?{query}"
    )
    matches = [
        run
        for run in runs["workflow_runs"]
        if run["head_sha"] == record["sha"]
        and run.get("display_title") == f"Bitfab replay {record['id']}"
    ]
    if len(matches) > 1:
        raise ValueError(
            "Multiple matching GitHub runs; inspect Actions before cleanup"
        )
    return matches[0] if matches else None


def status(record, *, fetch_result=True):
    if record["state"] == "prepared":
        return record
    run = (
        api(record["repository"], f"actions/runs/{record['runId']}")
        if record.get("runId")
        else find_run(record)
    )
    if run is None:
        record["state"] = "dispatch_unknown"
        return record
    if run["head_sha"] != record["sha"] or run["head_branch"] != record["branch"]:
        raise ValueError("GitHub run does not match the recorded execution")
    if run.get("display_title") != f"Bitfab replay {record['id']}":
        if run["status"] == "completed":
            raise ValueError("GitHub run does not match the recorded execution")
        record.update(runId=run["id"], url=run["html_url"], state=run["status"])
        return record
    record.update(
        runId=run["id"],
        url=run["html_url"],
        state=run["status"],
        conclusion=run.get("conclusion"),
    )
    if (
        fetch_result
        and record["state"] == "completed"
        and record["conclusion"] == "success"
        and not record.get("testRunId")
    ):
        with tempfile.TemporaryDirectory(prefix="bitfab-cloud-result-") as directory:
            command(
                [
                    "gh",
                    "run",
                    "download",
                    str(record["runId"]),
                    "--repo",
                    record["repository"],
                    "--name",
                    "bitfab-replay-" + record["id"],
                    "--dir",
                    directory,
                ]
            )
            artifact = Path(directory) / "bitfab-cloud-result.json"
            if artifact.is_symlink() or artifact.stat().st_size > 4096:
                raise ValueError("Invalid replay result artifact")
            result = json.loads(artifact.read_text())
            if (
                result.get("executionId") != record["id"]
                or result.get("commitSha") != record["sha"]
                or not UUID.fullmatch(result.get("testRunId", ""))
            ):
                raise ValueError("Replay result artifact does not match this execution")
            counts = replay_counts(result)
            record["testRunId"] = result["testRunId"]
            if counts is not None:
                record["replayed"], record["errored"] = counts
    return record


def cleanup(root, record):
    if record.get("cleaned"):
        return
    if record.get("state") not in ("completed", "prepared"):
        raise ValueError(
            "Cleanup requires a confirmed completed GitHub run; cancel and wait first"
        )
    if record["branch"] != PREFIX + record["id"] or not SHA.fullmatch(record["sha"]):
        raise ValueError("Refusing cleanup of an unowned branch")
    ref = "refs/heads/" + record["branch"]
    remote = git(root, "ls-remote", "--heads", "origin", ref)
    if remote:
        if remote.split()[0] != record["sha"]:
            raise ValueError(
                "Snapshot branch moved; refusing to delete someone else's changes"
            )
        git(
            root,
            "push",
            f"--force-with-lease={ref}:{record['sha']}",
            "origin",
            f":{ref}",
        )
    record["cleaned"] = True


def preflight(repo, config):
    info = api(repo, "")
    workflow = api(repo, f"actions/workflows/{config['workflow']}")
    if workflow.get("state") != "active":
        raise ValueError("Replay workflow must be active")
    api(
        repo,
        f"contents/.github/workflows/{config['workflow']}?"
        + urlencode({"ref": info["default_branch"]}),
    )


def run_cli(argv):
    args, operation, execution_id = parse(argv)
    if os.name != "posix" or sys.version_info < (3, 10):
        raise ValueError("Cloud replay currently requires macOS/Linux and Python 3.10+")
    root = root_directory()
    config = configuration(root) if operation == "submit" else None
    repo = repository(root)
    directory = state_directory(root)
    path = directory / f"{execution_id}.json"
    with execution_lock(directory, execution_id):
        if operation == "submit":
            within(root, config["workingDirectory"])
            check_pipeline(config, args.pipeline)
            if args.registry is not None:
                registry = Path(args.registry).resolve().relative_to(root).as_posix()
                if registry != config.get("registry"):
                    raise ValueError("Registry does not match .bitfab/cloud.json")
            request = {
                "id": execution_id,
                "pipeline": args.pipeline,
                "traceIds": args.trace_ids.split(","),
                "maxConcurrency": args.max_concurrency,
            }
            if args.cloud_dry_run:
                return {
                    "dryRun": True,
                    "repository": repo,
                    **snapshot(root, config, args, execution_id),
                }
            if path.exists():
                record = json.loads(path.read_text())
                if (
                    record["request"] != request
                    or record["repository"].lower() != repo.lower()
                ):
                    raise ValueError(
                        "Execution ID already belongs to a different request"
                    )
                # Never redispatch an uncertain request, even after a lost response.
                status(record)
                if record["state"] == "prepared":
                    raise ValueError(
                        "Submission stopped before dispatch. Use --cloud-cleanup, then submit a new execution UUID"
                    )
            else:
                command(["gh", "auth", "status", "--hostname", "github.com"])
                preflight(repo, config)
                source = snapshot(root, config, args, execution_id)
                record = {
                    "id": execution_id,
                    "repository": repo,
                    "workflow": config["workflow"],
                    "branch": PREFIX + execution_id,
                    "request": request,
                    "state": "prepared",
                    **source,
                }
                save(path, record)
                print(
                    f"Cloud execution {execution_id}. Recover with --cloud-status {execution_id}",
                    file=sys.stderr,
                    flush=True,
                )
                ref = "refs/heads/" + record["branch"]
                # Empty lease asserts that the temporary remote branch does not exist.
                git(
                    root,
                    "push",
                    f"--force-with-lease={ref}:",
                    "origin",
                    f"{record['sha']}:{ref}",
                )
                record["state"] = "dispatch_unknown"
                save(path, record)
                encoded = base64.b64encode(json.dumps(request).encode()).decode()
                response = api(
                    repo,
                    f"actions/workflows/{config['workflow']}/dispatches",
                    method="POST",
                    payload={
                        "ref": record["branch"],
                        "inputs": {"execution_id": execution_id, "request": encoded},
                    },
                )
                if isinstance(response, dict) and response.get("workflow_run_id"):
                    record["runId"] = response["workflow_run_id"]
                    record["url"] = response.get("html_url")
                record["state"] = "queued"
        else:
            if not path.exists():
                raise ValueError(
                    "No local execution record for this UUID in this worktree"
                )
            record = json.loads(path.read_text())
            if record["repository"].lower() != repo.lower():
                raise ValueError("origin no longer matches the execution repository")
            status(record, fetch_result=operation != "cleanup")
            if operation == "cancel" and record["state"] != "completed":
                if not record.get("runId"):
                    raise ValueError(
                        "Run not yet found; inspect Actions and retry status before cancelling"
                    )
                api(repo, f"actions/runs/{record['runId']}/cancel", method="POST")
                record["state"] = "cancel_requested"
        save(path, record)
        if record["state"] == "prepared" and operation != "cleanup":
            save(path, record)
            return record
        if operation == "watch" or (operation == "submit" and not args.cloud_detach):
            deadline = time.monotonic() + 40 * 60
            while record["state"] != "completed" and time.monotonic() < deadline:
                status(record)
                save(path, record)
                if record["state"] != "completed":
                    time.sleep(5)
            if record["state"] != "completed":
                raise ValueError(
                    "Watch timed out; the job may still run. Resume with --cloud-watch "
                    + execution_id
                )
        if record["state"] == "completed" or (
            operation == "cleanup" and record["state"] == "prepared"
        ):
            cleanup(root, record)
        elif operation == "cleanup":
            raise ValueError("Execution is not completed; no branch was deleted")
        save(path, record)
        return record


def execute():
    if os.environ.get("GITHUB_RUN_ATTEMPT") != "1":
        raise ValueError("Submit a new replay instead of rerunning an Actions job")
    request = json.loads(
        base64.b64decode(os.environ["BITFAB_CLOUD_REQUEST"], validate=True)
    )
    root = root_directory()
    if git(root, "rev-parse", "HEAD") != os.environ["GITHUB_SHA"]:
        raise ValueError("Runner checkout does not match the dispatched snapshot SHA")
    config = configuration(root)
    check_pipeline(config, request["pipeline"])
    if request["id"] != os.environ["BITFAB_EXECUTION_ID"]:
        raise ValueError("Replay request does not match the configured execution")
    parse(
        [
            "--cloud",
            request["pipeline"],
            "--trace-ids",
            ",".join(request["traceIds"]),
            "--max-concurrency",
            str(request["maxConcurrency"]),
            "--cloud-request-id",
            request["id"],
        ]
    )
    if not os.environ.get("BITFAB_API_KEY"):
        raise ValueError("Configure the BITFAB_API_KEY GitHub secret")
    args = [
        *config["command"],
        request["pipeline"],
        "--trace-ids",
        ",".join(request["traceIds"]),
        "--max-concurrency",
        str(request["maxConcurrency"]),
        "--no-code-change",
    ]
    with tempfile.TemporaryFile() as output:
        with subprocess.Popen(
            args,
            cwd=within(root, config["workingDirectory"]),
            stdout=output,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        ) as child:
            try:
                deadline = time.monotonic() + 25 * 60
                while child.poll() is None:
                    if os.fstat(output.fileno()).st_size > 16 * 1024 * 1024:
                        raise ValueError("Replay output exceeded 16 MiB")
                    if time.monotonic() >= deadline:
                        raise subprocess.TimeoutExpired(args, 25 * 60)
                    time.sleep(0.1)
                code = child.returncode
            except BaseException:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGTERM)
                with contextlib.suppress(subprocess.TimeoutExpired):
                    child.wait(timeout=10)
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGKILL)
                child.wait()
                raise
        if output.tell() > 16 * 1024 * 1024:
            raise ValueError("Replay output exceeded 16 MiB")
        output.seek(0)
        text = output.read().decode()
    if code:
        raise ValueError(f"Replay command exited {code}; inspect the job logs")
    decoder = json.JSONDecoder()
    result = None
    for offset in [0, *[i + 1 for i, value in enumerate(text) if value == "\n"]]:
        if text.startswith("{", offset):
            try:
                value, end = decoder.raw_decode(text, offset)
                if not text[end:].strip() and isinstance(value, dict):
                    result = value
            except json.JSONDecodeError:
                pass
    test_run = (
        None if result is None else result.get("testRunId", result.get("test_run_id"))
    )
    if not isinstance(test_run, str) or not UUID.fullmatch(test_run):
        raise ValueError("Replay did not return a valid persisted test run UUID")
    items = result.get("items")
    if not isinstance(items, list):
        raise ValueError("Replay did not return its replayed items")
    summary = {
        "executionId": request["id"],
        "commitSha": os.environ["GITHUB_SHA"],
        "testRunId": test_run,
        "replayed": len(items),
        "errored": sum(1 for item in items if item_errored(item)),
    }
    (Path(os.environ["RUNNER_TEMP"]) / "bitfab-cloud-result.json").write_text(
        json.dumps(summary) + "\n"
    )
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as file:
        file.write(
            f"### Bitfab replay\n\nTest run: `{test_run}`\n\nCommit: `{os.environ['GITHUB_SHA']}`\n\n"
            f"Replayed: {summary['replayed']}, errored: {summary['errored']}\n"
        )
    return summary


def item_errored(item):
    return isinstance(item, dict) and any(
        item.get(field) is not None for field in ITEM_ERROR_FIELDS
    )


def replay_counts(result):
    replayed, errored = result.get("replayed"), result.get("errored")
    if replayed is None and errored is None:
        return None
    if (
        not all(
            isinstance(value, int) and not isinstance(value, bool) and value >= 0
            for value in (replayed, errored)
        )
        or errored > replayed
    ):
        raise ValueError("Replay result artifact has invalid item counts")
    return replayed, errored


def report_errored_items(record):
    replayed, errored = record.get("replayed"), record.get("errored")
    if not errored:
        return 0
    if errored == replayed:
        print(
            f"Cloud replay: every replayed trace errored ({errored} of {replayed}), so nothing ran successfully. Open test run {record.get('testRunId')} to see why.",
            file=sys.stderr,
        )
        return 1
    print(
        f"Cloud replay: {errored} of {replayed} replayed traces errored. Open test run {record.get('testRunId')} to see why.",
        file=sys.stderr,
    )
    return 0


def initialize(argv):
    parser = argparse.ArgumentParser(
        prog="bitfab-replay --cloud-init",
        description="Install direct GitHub cloud replay from a reviewed JSON setup specification. Existing files are never overwritten.",
    )
    parser.add_argument(
        "--config",
        required=True,
        help="JSON file with cloud config, cliCommand, setupSteps, optional pipeline (omit or null to allow every registry pipeline), secrets, variables, environment, services, runsOn",
    )
    args = parser.parse_args(argv)
    spec = json.loads(Path(args.config).read_text())
    config = {
        key: spec[key]
        for key in (
            "version",
            "provider",
            "workflow",
            "workingDirectory",
            "registry",
            "command",
            "pushTriggersReviewed",
        )
    }
    config["pipeline"] = spec.get("pipeline")
    secrets = spec.get("secrets", ["BITFAB_API_KEY"])
    variables = spec.get("variables", [])
    secret_prefix = spec.get("secretPrefix")
    if secret_prefix is None:
        secret_prefix = DEFAULT_SECRET_PREFIX
    config["secrets"] = secrets
    config["secretPrefix"] = secret_prefix
    validate_config(config)
    if set(secrets) & set(variables):
        raise ValueError("Secret names and variable names must not overlap")
    if "BITFAB_API_KEY" not in secrets or not all(
        re.fullmatch(r"[A-Z_][A-Z0-9_]*", name) for name in [*secrets, *variables]
    ):
        raise ValueError(
            "Supply uppercase secret/variable names, including BITFAB_API_KEY; never values"
        )
    cli_command = spec.get("cliCommand")
    if (
        not isinstance(cli_command, list)
        or not cli_command
        or not all(isinstance(v, str) and v and "\x00" not in v for v in cli_command)
        or any(v.startswith("--cloud") for v in cli_command)
    ):
        raise ValueError(
            "cliCommand must be the argument array that starts the SDK's bitfab-replay command from workingDirectory"
        )
    steps = spec.get("setupSteps")
    if (
        not isinstance(steps, list)
        or not steps
        or not all(isinstance(step, dict) for step in steps)
    ):
        raise ValueError("setupSteps must contain reviewed GitHub Actions setup steps")
    env = {key: "${{ secrets." + secret_prefix + key + " }}" for key in secrets}
    env.update({key: "${{ vars." + key + " }}" for key in variables})
    env.update(
        BITFAB_CLOUD_REQUEST="${{ inputs.request }}",
        BITFAB_EXECUTION_ID="${{ inputs.execution_id }}",
        BITFAB_COMMIT_SHA="${{ github.sha }}",
    )
    job = {
        "runs-on": spec.get("runsOn", "ubuntu-24.04"),
        "timeout-minutes": 35,
        "steps": [
            {
                "name": "Check out replay snapshot",
                "uses": "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
                "with": {"ref": "${{ github.sha }}", "persist-credentials": False},
            },
            *steps,
            {
                "name": "Replay",
                "working-directory": config["workingDirectory"],
                "run": shlex.join([*cli_command, "--cloud-execute"]),
                "env": env,
            },
            {
                "name": "Save replay identity",
                "if": "always()",
                "uses": "actions/upload-artifact@ea165f8d65b6e75b540449e92b4886f43607fa02",
                "with": {
                    "name": "bitfab-replay-${{ inputs.execution_id }}",
                    "path": "${{ runner.temp }}/bitfab-cloud-result.json",
                    "if-no-files-found": "ignore",
                    "retention-days": 7,
                },
            },
        ],
    }
    for key in ("environment", "services"):
        if key in spec:
            job[key] = spec[key]
    workflow = {
        "name": "Bitfab cloud replay",
        "run-name": "Bitfab replay ${{ inputs.execution_id }}",
        "on": {
            "workflow_dispatch": {
                "inputs": {
                    "execution_id": {"required": True, "type": "string"},
                    "request": {"required": True, "type": "string"},
                }
            }
        },
        "permissions": {"contents": "read"},
        "jobs": {"replay": job},
    }
    root = root_directory()
    outputs = {
        CONFIG: json.dumps(config, indent=2) + "\n",
        f".github/workflows/{config['workflow']}": json.dumps(workflow, indent=2)
        + "\n",
    }
    for name in outputs:
        target = within(root, name)
        if target.exists():
            raise ValueError(
                f"Refusing to overwrite {name}; review and edit the existing setup"
            )
    for name, content in outputs.items():
        target = within(root, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("x") as file:
            file.write(content)
    return {
        "files": list(outputs),
        "requiredSecrets": [secret_prefix + name for name in secrets],
        "requiredVariables": variables,
        "next": "Configure secrets securely, review push triggers, merge the workflow to the default branch, then run --cloud-dry-run",
    }


def main():
    try:
        if sys.argv[1:2] == ["--cloud-init"]:
            result = initialize(sys.argv[2:])
        elif sys.argv[1:2] == ["--cloud-secrets"]:
            result = configure_secrets(sys.argv[2:])
        elif sys.argv[1:] == ["--cloud-execute"]:
            result = execute()
        else:
            result = run_cli(sys.argv[1:])
        print(json.dumps(result, indent=2))
        if result.get("state") == "completed" and result.get("conclusion") != "success":
            return 1
        if result.get("state") == "completed":
            return report_errored_items(result)
        return 0
    except KeyboardInterrupt:
        print(
            "Detached. The GitHub job continues; use the printed execution UUID to resume or cancel.",
            file=sys.stderr,
        )
        return 130
    except (
        ValueError,
        RuntimeError,
        OSError,
        subprocess.SubprocessError,
        KeyError,
    ) as error:
        print(f"Cloud replay: {error}", file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
