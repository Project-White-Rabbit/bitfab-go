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
import threading
import time
import uuid
from pathlib import Path
from urllib.parse import urlencode

CONFIG = ".bitfab/cloud.json"
PREFIX = "bitfab-replay/"
DEFAULT_SECRET_PREFIX = "BITFAB_CLOUD_"
API_VERSION = "2026-03-10"
# The SDK finds runs by this title and feeds them through these inputs. Never change them.
RUN_NAME = "Bitfab replay ${{ inputs.execution_id }}"
RUNNER_ENV = {
    "BITFAB_CLOUD_REQUEST": "${{ inputs.request }}",
    "BITFAB_EXECUTION_ID": "${{ inputs.execution_id }}",
    "BITFAB_COMMIT_SHA": "${{ github.sha }}",
}
# The runner reports its result as a job annotation with this title, so the workflow
# needs no upload step. Older workflows still upload RESULT_FILE as an artifact.
RESULT_TITLE = "Bitfab replay result"
RESULT_FILE = "bitfab-cloud-result.json"
# Steps older setups generated that the SDK now owns.
OLD_UPLOAD_STEP = "Save replay identity"
OLD_JOB_TIMEOUT = 35
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
# Every SDK's replay prints this to stderr as soon as the server creates the experiment.
EXPERIMENT_LINE = re.compile(rb"^\[replay\] Experiment ([0-9a-f-]{36}):")
ITEM_ID_FIELDS = ("originalTraceId", "original_trace_id", "traceId", "trace_id")
ITEM_ERROR_LINES = 20
ITEM_ERROR_LENGTH = 500
OUTPUT_TAIL_LENGTH = 4000
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
CLOUD_VALUE_FLAGS = (
    "--registry",
    "--trace-ids",
    "--max-concurrency",
    "--cloud-status",
    "--cloud-watch",
    "--cloud-cancel",
    "--cloud-cleanup",
    "--cloud-request-id",
    "--cloud-include",
    "--cloud-timeout",
)
CLOUD_SWITCHES = (
    "--cloud",
    "--cloud-dry-run",
    "--cloud-detach",
    "--cloud-check",
    "--fail-on-error",
    "-h",
    "--help",
)
RUNNER_OWNED_FLAGS = {
    "--dry-run": "use --cloud-dry-run to review the snapshot or --cloud-check to resolve traces on the runner",
    "--seed": "seed locally",
    "--cases": "seed locally",
    "--from-trace": "seed locally",
    "--run": "seed locally",
}
PATH_FLAGS = ("--params", "--code-change")
SELECTION_FLAGS = ("--dataset-ids", "--dataset-id", "--resume")
HELP = """GitHub cloud replay (requires git, gh login, and Python 3.10+).
  --cloud PIPELINE --trace-ids UUID[,UUID] [--registry PATH]
    [--max-concurrency 1..32] [--cloud-request-id UUID]
    [--cloud-include PATH ...] [--cloud-dry-run] [--cloud-detach]
    [--cloud-timeout MINUTES] [--cloud-check] [--fail-on-error] [REPLAY OPTIONS]
  Every other replay option after the pipeline, such as --name, --dataset-ids,
    --attempts, --mock, or --resume, is passed to the replay on the runner as
    given. --params and --code-change files must be in the snapshot.
    --registry comes from .bitfab/cloud.json; --dry-run and seeding stay local.
    Select traces with --trace-ids (1..100 UUIDs), --dataset-ids, or --resume.
  --fail-on-error exits 1 locally when any replayed item errored.
  --cloud-check runs on GitHub without replaying anything: it checks that every
    configured secret has a value, runs cloud.json's checkCommand when set, and
    resolves the traces with the replay's --dry-run to load the registry.
  --cloud-status UUID | --cloud-watch UUID | --cloud-cancel UUID
  --cloud-cleanup UUID
  --cloud-init [--config SPEC]   (creates a setup, or brings an existing one up to date)
  --cloud-secrets --env-file FILE [NAME ...]
PIPELINE may be any pipeline in the registry, unless .bitfab/cloud.json names one.
Snapshot tracked working files without changing HEAD, the index, or local files.
New files require explicit --cloud-include. Credentials and ignored files are refused.
By default wait for completion and remove the remote snapshot branch. Detached runs
continue on GitHub; watch/status/cleanup can recover them using the printed UUID.
"""


class CommandError(RuntimeError):
    def __init__(self, message, http_status=None):
        super().__init__(message)
        self.http_status = http_status


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
        status = re.search(r"\bHTTP (\d{3})\b", result.stderr or "")
        http_status = int(status[1]) if status else None
        raise CommandError(
            f"{args[0]} {args[1]} failed (exit {result.returncode}"
            + (f", HTTP {http_status}" if http_status else "")
            + "); check authentication and permissions",
            http_status,
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
    targets = secret_targets(config)
    names = args.names or list(targets) or ["BITFAB_API_KEY"]
    if not all(ENV_NAME.fullmatch(name) for name in names):
        raise ValueError("Secret names must be environment variable names")
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
        target = targets.get(name, config.get("secretPrefix", "") + name)
        renamed = "secret" in config.get("env", {}).get(name, {})
        value = values.get(target, values.get(name)) if renamed else values.get(name)
        if value is None:
            missing.append(name)
            continue
        if not value:
            empty.append(name)
            continue
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


def secret_targets(config):
    prefix = config.get("secretPrefix", "")
    targets = {name: prefix + name for name in config.get("secrets", [])}
    for name, source in config.get("env", {}).items():
        if "secret" in source:
            targets[name] = source["secret"]
    return targets


def runner_env(config):
    env = {
        name: "${{ secrets." + target + " }}"
        for name, target in secret_targets(config).items()
    }
    for name, source in config.get("env", {}).items():
        if "variable" in source:
            env[name] = "${{ vars." + source["variable"] + " }}"
    return env


def validate_env(mapping, secrets):
    if not isinstance(mapping, dict):
        raise ValueError(
            'env must map environment variable names to {"secret": NAME} or {"variable": NAME}'
        )
    for name, source in mapping.items():
        if not ENV_NAME.fullmatch(name) or name in RUNNER_ENV:
            raise ValueError(f"env cannot set {name}")
        if name in secrets:
            raise ValueError(f"{name} is in both secrets and env; list it once")
        if (
            not isinstance(source, dict)
            or len(source) != 1
            or next(iter(source)) not in ("secret", "variable")
            or not isinstance(next(iter(source.values())), str)
            or not ENV_NAME.fullmatch(next(iter(source.values())))
        ):
            raise ValueError(
                f'env.{name} must be {{"secret": "GITHUB_SECRET_NAME"}} or {{"variable": "GITHUB_VARIABLE_NAME"}}'
            )


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
    if "cliCommand" in config:
        validate_cli_command(config["cliCommand"])
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
    validate_env(config.get("env", {}), names)
    if "checkCommand" in config:
        check = config["checkCommand"]
        if (
            not isinstance(check, list)
            or not check
            or not all(isinstance(v, str) and v and "\x00" not in v for v in check)
            or any(v.startswith("--cloud") for v in check)
        ):
            raise ValueError(
                "checkCommand must be a nonempty JSON argument array that runs locally"
            )


def validate_cli_command(cli_command):
    if (
        not isinstance(cli_command, list)
        or not cli_command
        or not all(isinstance(v, str) and v and "\x00" not in v for v in cli_command)
        or any(v.startswith("--cloud") for v in cli_command)
    ):
        raise ValueError(
            "cliCommand must be the argument array that starts the SDK's bitfab-replay command from workingDirectory"
        )


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


def split_replay_options(argv):
    cloud, options = [], []
    index = 0
    while index < len(argv):
        token = argv[index]
        name = token.split("=", 1)[0]
        if name in CLOUD_VALUE_FLAGS:
            step = 1 if "=" in token else 2
            cloud += argv[index : index + step]
            index += step
        elif name in CLOUD_SWITCHES:
            cloud.append(token)
            index += 1
        elif token.startswith("-"):
            if name in RUNNER_OWNED_FLAGS:
                raise ValueError(
                    f"{name} is not sent to the runner; {RUNNER_OWNED_FLAGS[name]}"
                )
            options += token.split("=", 1) if token.startswith("--") else [token]
            index += 1
            following = argv[index] if index < len(argv) else None
            if (
                "=" not in token
                and following is not None
                and not following.startswith("-")
            ):
                options.append(following)
                index += 1
        elif options:
            raise ValueError(
                "Put the pipeline right after --cloud, before replay options"
            )
        else:
            cloud.append(token)
            index += 1
    for value in options:
        if not value or "\x00" in value or "\n" in value or len(value) > 1000:
            raise ValueError(
                "Replay options must be single-line values of at most 1000 characters"
            )
    if sum(len(value) for value in options) > 16000:
        raise ValueError("Replay options exceed 16000 characters")
    return cloud, options


def parse(argv):
    argv, options = split_replay_options(argv)
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
    parser.add_argument("--cloud-timeout", type=int)
    parser.add_argument("--cloud-check", action="store_true")
    parser.add_argument("--fail-on-error", action="store_true")
    args = parser.parse_args(argv)
    args.options = options
    operations = [
        name
        for name in ("status", "watch", "cancel", "cleanup")
        if getattr(args, "cloud_" + name)
    ]
    if operations:
        if len(operations) != 1 or len(argv) != 2 or options:
            raise ValueError("Cloud lifecycle commands take only their execution UUID")
        operation = operations[0]
        execution_id = getattr(args, "cloud_" + operation)
    else:
        if not args.cloud or not args.pipeline:
            raise ValueError(HELP)
        if not args.trace_ids and not any(flag in options for flag in SELECTION_FLAGS):
            raise ValueError(
                "Select traces with --trace-ids, --dataset-ids, or --resume"
            )
        operation = "submit"
        execution_id = args.cloud_request_id or str(uuid.uuid4())
        if args.trace_ids is not None:
            traces = args.trace_ids.split(",")
            if not 1 <= len(traces) <= 100 or not all(
                UUID.fullmatch(t) for t in traces
            ):
                raise ValueError("Supply 1..100 explicit trace UUIDs")
        if not 1 <= args.max_concurrency <= 32:
            raise ValueError("--max-concurrency must be 1..32")
        # Self-hosted runners allow jobs of up to 5 days; GitHub-hosted ones stop at 6 hours.
        if args.cloud_timeout is not None and not 1 <= args.cloud_timeout <= 7200:
            raise ValueError("--cloud-timeout must be 1..7200 minutes")
    if not UUID.fullmatch(execution_id):
        raise ValueError("Execution ID must be a UUID")
    return args, operation, execution_id


def option_paths(options):
    return [value for flag, value in zip(options, options[1:]) if flag in PATH_FLAGS]


def map_option_paths(options, convert):
    mapped = list(options)
    for index, flag in enumerate(options[:-1]):
        if flag in PATH_FLAGS:
            mapped[index + 1] = convert(options[index + 1])
    return mapped


def repository_path(root, value):
    try:
        path = Path(value).resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError(f"{value} must be inside the repository") from error
    if sensitive(path):
        raise ValueError(f"Refusing credential-like file: {value}")
    return path


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


def snapshot(root, config, args, execution_id, files=()):
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
        required = [CONFIG, f".github/workflows/{config['workflow']}", *files]
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
        and not record.get("testRunId")
        and not record.get("check")
        and not record.get("resultChecked")
    ):
        success = record["conclusion"] == "success"
        try:
            result = read_result(record)
        except RuntimeError:
            # A run that failed before its replay started reports no result.
            if success:
                raise
            record["resultChecked"] = True
            return record
        if (
            result.get("executionId") != record["id"]
            or result.get("commitSha") != record["sha"]
        ):
            raise ValueError("Replay result does not match this execution")
        if record["request"].get("check"):
            if result.get("check") != "passed" or not isinstance(
                result.get("resolved"), int
            ):
                raise ValueError("Cloud check result does not match this execution")
            record["check"] = "passed"
            record["resolved"] = result["resolved"]
            return record
        if not UUID.fullmatch(result.get("testRunId", "")):
            raise ValueError("Replay result does not match this execution")
        counts = replay_counts(result)
        record["testRunId"] = result["testRunId"]
        if isinstance(result.get("stoppedEarly"), str):
            record["stoppedEarly"] = result["stoppedEarly"]
        if counts is not None:
            record["replayed"], record["errored"] = counts
    return record


def read_result(record):
    try:
        result = read_result_annotation(record)
    except RuntimeError:
        # Older workflows still upload the result, so a failed Checks API read
        # must not hide it.
        result = None
    return result or read_result_artifact(record)


def read_result_annotation(record):
    repo = record["repository"]
    jobs = api(repo, f"actions/runs/{record['runId']}/jobs?per_page=100") or {}
    for job in jobs.get("jobs", []):
        check_run = str(job.get("check_run_url", "")).rsplit("/", 1)[-1]
        if not check_run.isdigit():
            continue
        for note in api(repo, f"check-runs/{check_run}/annotations?per_page=100") or []:
            if (
                note.get("title") == RESULT_TITLE
                and len(note.get("message", "")) <= 4096
            ):
                try:
                    return json.loads(base64.b64decode(note["message"], validate=True))
                except ValueError as error:
                    raise ValueError("Invalid replay result annotation") from error
    return None


def read_result_artifact(record):
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
        artifact = Path(directory) / RESULT_FILE
        if artifact.is_symlink() or artifact.stat().st_size > 4096:
            raise ValueError("Invalid replay result artifact")
        return json.loads(artifact.read_text())


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


def http_status(error):
    status = getattr(error, "http_status", None)
    return f" (HTTP {status})" if status else ""


def preflight(repo, config):
    try:
        command(["gh", "auth", "status", "--hostname", "github.com"])
    except RuntimeError as error:
        raise ValueError(
            "gh is not logged in to github.com; run gh auth login"
        ) from error
    try:
        api(repo, "")
    except RuntimeError as error:
        raise ValueError(
            f"The gh account cannot read {repo}{http_status(error)}; log in with an account that has write access to it"
        ) from error
    path = f".github/workflows/{config['workflow']}"
    try:
        workflow = api(repo, f"actions/workflows/{config['workflow']}")
    except RuntimeError as error:
        raise ValueError(
            f"GitHub Actions has not registered {path}{http_status(error)}; merging it to the default branch once registers it, and after that each replay runs the copy in its own snapshot"
        ) from error
    if workflow.get("state") != "active":
        raise ValueError(
            f"{path} is {workflow.get('state')}; enable it in the repository's Actions tab"
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
            options = map_option_paths(
                args.options, lambda value: repository_path(root, value)
            )
            request = {
                "id": execution_id,
                "pipeline": args.pipeline,
                "maxConcurrency": args.max_concurrency,
            }
            if args.trace_ids is not None:
                request["traceIds"] = args.trace_ids.split(",")
            if options:
                request["options"] = options
            if args.cloud_check:
                request["check"] = True
            if args.cloud_timeout is not None:
                request["timeoutMinutes"] = args.cloud_timeout
            if args.cloud_dry_run:
                return {
                    "dryRun": True,
                    "repository": repo,
                    **snapshot(root, config, args, execution_id, option_paths(options)),
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
                preflight(repo, config)
                source = snapshot(
                    root, config, args, execution_id, option_paths(options)
                )
                record = {
                    "id": execution_id,
                    "repository": repo,
                    "workflow": config["workflow"],
                    "branch": PREFIX + execution_id,
                    "request": request,
                    "state": "prepared",
                    **source,
                }
                if args.fail_on_error:
                    record["failOnError"] = True
                save(path, record)
                print(
                    f"Cloud execution {execution_id}. Recover with --cloud-status {execution_id}",
                    file=sys.stderr,
                    flush=True,
                )
                ref = "refs/heads/" + record["branch"]
                try:
                    # Empty lease asserts that the temporary remote branch does not exist.
                    git(
                        root,
                        "push",
                        f"--force-with-lease={ref}:",
                        "origin",
                        f"{record['sha']}:{ref}",
                    )
                except RuntimeError as error:
                    raise ValueError(
                        f"Pushing the snapshot branch {record['branch']} to origin failed; check that git can push to {repo}"
                    ) from error
                record["state"] = "dispatch_unknown"
                save(path, record)
                encoded = base64.b64encode(json.dumps(request).encode()).decode()
                try:
                    response = api(
                        repo,
                        f"actions/workflows/{config['workflow']}/dispatches",
                        method="POST",
                        payload={
                            "ref": record["branch"],
                            "inputs": {
                                "execution_id": execution_id,
                                "request": encoded,
                            },
                        },
                    )
                except RuntimeError as error:
                    raise ValueError(
                        f"Dispatching {config['workflow']} on {record['branch']} failed{http_status(error)}; the gh account needs write access to Actions, and the registered workflow must accept workflow_dispatch. Check the Actions tab, then resume with --cloud-status {execution_id}"
                    ) from error
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
            # Once GitHub shows the run, its job timeout bounds the wait and Ctrl-C detaches.
            # A dispatch GitHub never shows as a run would otherwise be waited on forever.
            started = time.monotonic()
            while record["state"] != "completed":
                status(record)
                save(path, record)
                if (
                    record["state"] == "dispatch_unknown"
                    and time.monotonic() - started >= 10 * 60
                ):
                    raise ValueError(
                        "GitHub has shown no run for this dispatch after 10 minutes. Check Actions, then resume with --cloud-watch "
                        + execution_id
                        + " or remove the snapshot branch with --cloud-cleanup "
                        + execution_id
                    )
                if record["state"] != "completed":
                    time.sleep(5)
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
    timeout = request.get("timeoutMinutes")
    traces = (
        ["--trace-ids", ",".join(request["traceIds"])] if "traceIds" in request else []
    )
    options = request.get("options", [])
    parsed, _, _ = parse(
        [
            "--cloud",
            request["pipeline"],
            *traces,
            "--max-concurrency",
            str(request["maxConcurrency"]),
            "--cloud-request-id",
            request["id"],
            *([] if timeout is None else ["--cloud-timeout", str(timeout)]),
            *(["--cloud-check"] if request.get("check") else []),
            *options,
        ]
    )
    if parsed.options != options:
        raise ValueError("Replay options were not in the form the SDK sends")
    check_secrets(config)
    args = [
        *config["command"],
        request["pipeline"],
        *traces,
        "--max-concurrency",
        str(request["maxConcurrency"]),
        *map_option_paths(options, lambda value: snapshot_file(root, value)),
        *(
            []
            if "--code-change" in options or "--no-code-change" in options
            else ["--no-code-change"]
        ),
    ]
    if request.get("check"):
        summary = run_check(root, config, request, args)
        write_result(summary)
        return summary
    experiment = {}
    # GitHub cancels and times out a job with SIGINT then SIGTERM; turn SIGTERM into the
    # same interrupt so the replay is stopped cleanly and its experiment is still reported.
    previous = signal.signal(signal.SIGTERM, raise_interrupt)
    try:
        summary = run_replay(root, config, request, args, timeout, experiment)
    except BaseException as error:
        if UUID.fullmatch(experiment.get("id", "")):
            write_result(
                {
                    "executionId": request["id"],
                    "commitSha": os.environ["GITHUB_SHA"],
                    "testRunId": experiment["id"],
                    "stoppedEarly": (str(error) or type(error).__name__)[:300],
                }
            )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous)
    write_result(summary)
    return summary


def snapshot_file(root, value):
    path = within(root, value)
    if sensitive(value) or not path.is_file():
        raise ValueError(f"{value} is not a file in the replay snapshot")
    return str(path)


def check_secrets(config):
    targets = {"BITFAB_API_KEY": "BITFAB_API_KEY", **secret_targets(config)}
    empty = [name for name in targets if not os.environ.get(name)]
    if empty:
        raise ValueError(
            "These replay environment variables are empty on the runner: "
            + ", ".join(f"{name} (secret {targets[name]})" for name in empty)
            + ". GitHub passes a secret that does not exist as an empty string. Create each one under Settings, Secrets and variables, Actions, in the repository or in the job's Environment"
        )


def run_check(root, config, request, args):
    directory = within(root, config["workingDirectory"])
    if config.get("checkCommand"):
        print(f"Running checkCommand: {shlex.join(config['checkCommand'])}", flush=True)
        code = subprocess.run(
            config["checkCommand"], cwd=directory, stdin=subprocess.DEVNULL, check=False
        ).returncode
        if code:
            raise ValueError(f"checkCommand exited {code}; its output is above")
    result = run_command(root, config, [*args, "--dry-run"], None, {})
    items = result.get("items")
    if not isinstance(items, list):
        raise ValueError("The replay dry run did not return its resolved items")
    errors = item_errors(items)
    report_item_errors(errors)
    if errors:
        raise ValueError(
            f"{len(errors)} of {len(items)} traces failed to resolve; the errors are above"
        )
    return {
        "executionId": request["id"],
        "commitSha": os.environ["GITHUB_SHA"],
        "check": "passed",
        "resolved": len(items),
    }


def item_errors(items):
    errors = []
    for item in items:
        if not item_errored(item):
            continue
        error = next(
            item[field] for field in ITEM_ERROR_FIELDS if item.get(field) is not None
        )
        if isinstance(error, dict):
            error = error.get("message") or json.dumps(error)
        trace = next(
            (
                item[field]
                for field in ITEM_ID_FIELDS
                if isinstance(item.get(field), str)
            ),
            "unknown trace",
        )
        text = " ".join(str(error).split())
        if len(text) > ITEM_ERROR_LENGTH:
            text = text[:ITEM_ERROR_LENGTH] + "..."
        errors.append((trace, text))
    return errors


def report_item_errors(errors):
    if not errors:
        return
    shown = errors[:ITEM_ERROR_LINES]
    lines = [f"trace {trace}: {text}" for trace, text in shown]
    if len(errors) > len(shown):
        lines.append(f"and {len(errors) - len(shown)} more errored items")
    print("Errored items:", file=sys.stderr)
    for line in lines:
        print("  " + line, file=sys.stderr)
    sys.stderr.flush()
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as file:
            file.write(
                "\n#### Errored items\n\n"
                + "".join(f"- `{line.replace('`', chr(39))}`\n" for line in lines)
            )


def raise_interrupt(signum, frame):
    raise KeyboardInterrupt


def forward_stderr(stream, experiment):
    for line in iter(stream.readline, b""):
        sys.stderr.buffer.write(line)
        sys.stderr.flush()
        if "id" not in experiment:
            match = EXPERIMENT_LINE.match(line)
            if match:
                experiment["id"] = match[1].decode()


def write_result(summary):
    # Base64, because GitHub masks every line of a multi-line secret in logs and
    # annotations, so a JSON credential alone turns each { and } into ***.
    message = base64.b64encode(json.dumps(summary).encode()).decode()
    print(f"::notice title={RESULT_TITLE}::{message}", flush=True)
    if "check" in summary:
        with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as file:
            file.write(
                f"### Bitfab cloud check\n\nPassed: every secret has a value and {summary['resolved']} traces resolved. Commit: `{summary['commitSha']}`\n"
            )
        return
    lines = [
        f"Test run: `{summary['testRunId']}`",
        f"Commit: `{summary['commitSha']}`",
    ]
    if "stoppedEarly" in summary:
        lines.append(
            f"Stopped early: {summary['stoppedEarly']}. Traces that finished are saved in this test run."
        )
    else:
        lines.append(f"Replayed: {summary['replayed']}, errored: {summary['errored']}")
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as file:
        file.write("### Bitfab replay\n\n" + "\n\n".join(lines) + "\n")


def run_replay(root, config, request, args, timeout, experiment):
    result = run_command(root, config, args, timeout, experiment)
    test_run = result.get("testRunId", result.get("test_run_id"))
    if not isinstance(test_run, str) or not UUID.fullmatch(test_run):
        raise ValueError("Replay did not return a valid persisted test run UUID")
    items = result.get("items")
    if not isinstance(items, list):
        raise ValueError("Replay did not return its replayed items")
    replayed = [item for item in items if not carried_over(item)]
    errors = item_errors(replayed)
    report_item_errors(errors)
    return {
        "executionId": request["id"],
        "commitSha": os.environ["GITHUB_SHA"],
        "testRunId": test_run,
        "replayed": len(replayed),
        "errored": len(errors),
    }


def carried_over(item):
    return isinstance(item, dict) and (
        item.get("carriedOver") is True or item.get("carried_over") is True
    )


def print_output_tail(text):
    tail = text[-OUTPUT_TAIL_LENGTH:].strip()
    if tail:
        print(
            "Last replay output:\n"
            + "\n".join("  " + line for line in tail.splitlines()),
            file=sys.stderr,
            flush=True,
        )


def run_command(root, config, args, timeout, experiment):
    with tempfile.TemporaryFile() as output:
        with subprocess.Popen(
            args,
            cwd=within(root, config["workingDirectory"]),
            stdout=output,
            stderr=subprocess.PIPE,
            stdin=subprocess.DEVNULL,
            start_new_session=True,
        ) as child:
            reader = threading.Thread(
                target=forward_stderr, args=(child.stderr, experiment), daemon=True
            )
            reader.start()
            try:
                # Without --cloud-timeout the job's own timeout is the only limit.
                deadline = None if timeout is None else time.monotonic() + timeout * 60
                while child.poll() is None:
                    if os.fstat(output.fileno()).st_size > 16 * 1024 * 1024:
                        raise ValueError("Replay output exceeded 16 MiB")
                    if deadline is not None and time.monotonic() >= deadline:
                        raise ValueError(
                            f"Replay stopped at the {timeout}-minute --cloud-timeout; traces that finished are saved in Bitfab as an interrupted experiment"
                        )
                    time.sleep(0.1)
                code = child.returncode
            except BaseException:
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGTERM)
                # Time for the SDK to mark the experiment interrupted before the hard kill.
                with contextlib.suppress(subprocess.TimeoutExpired):
                    child.wait(timeout=30)
                with contextlib.suppress(ProcessLookupError):
                    os.killpg(child.pid, signal.SIGKILL)
                child.wait()
                raise
            finally:
                reader.join(timeout=5)
        if output.tell() > 16 * 1024 * 1024:
            raise ValueError("Replay output exceeded 16 MiB")
        output.seek(0)
        text = output.read().decode(errors="replace")
    if code:
        print_output_tail(text)
        raise ValueError(f"Replay command exited {code}; its output is above")
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
    if result is None:
        print_output_tail(text)
        raise ValueError("Replay did not print a JSON result; its output is above")
    return result


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


def replay_step(config, cli_command, env):
    return {
        "name": "Replay",
        "working-directory": config["workingDirectory"],
        "run": shlex.join([*cli_command, "--cloud-execute"]),
        "env": {**env, **RUNNER_ENV},
    }


def workflow_document(job_options, setup_steps, replay):
    """Only settings belong here; everything Bitfab may need to change runs inside the SDK."""
    job = {
        "runs-on": job_options.get("runsOn", "ubuntu-24.04"),
        "steps": [
            {
                "name": "Check out replay snapshot",
                "uses": "actions/checkout@11d5960a326750d5838078e36cf38b85af677262",
                "with": {"ref": "${{ github.sha }}", "persist-credentials": False},
            },
            *setup_steps,
            replay,
        ],
    }
    for key in ("environment", "services"):
        if key in job_options:
            job[key] = job_options[key]
    return {
        "name": "Bitfab cloud replay",
        "run-name": RUN_NAME,
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


def write_new_files(root, outputs):
    for name in outputs:
        if within(root, name).exists():
            raise ValueError(
                f"Refusing to overwrite {name}; review and edit the existing setup"
            )
    for name, content in outputs.items():
        target = within(root, name)
        target.parent.mkdir(parents=True, exist_ok=True)
        with target.open("x") as file:
            file.write(content)


def initialize(argv):
    parser = argparse.ArgumentParser(
        prog="bitfab-replay --cloud-init",
        description="Install direct GitHub cloud replay from a reviewed JSON setup specification, or bring an existing setup up to date in place. Safe to run again.",
    )
    parser.add_argument(
        "--config",
        help='JSON file with cloud config, cliCommand, setupSteps, optional pipeline (omit or null to allow every registry pipeline), secrets, variables, env (runner variable names mapped to {"secret": NAME} or {"variable": NAME} for renamed secrets and repository variables), checkCommand, environment, services, runsOn. Required for a new setup; for an existing one only cliCommand is read, and only when the workflow does not already show it',
    )
    args = parser.parse_args(argv)
    spec = json.loads(Path(args.config).read_text()) if args.config else None
    root = root_directory()
    if within(root, CONFIG).exists():
        return update_existing(root, spec)
    if spec is None:
        raise ValueError(f"No {CONFIG} yet; pass --config with a setup specification")
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
    if not isinstance(variables, list):
        raise ValueError("variables must be a list of names")
    env = dict(spec.get("env") or {})
    for name in variables:
        env.setdefault(name, {"variable": name})
    if env:
        config["env"] = env
    if spec.get("checkCommand") is not None:
        config["checkCommand"] = spec["checkCommand"]
    validate_config(config)
    validate_cli_command(spec.get("cliCommand"))
    if set(secrets) & set(variables):
        raise ValueError("Secret names and variable names must not overlap")
    if "BITFAB_API_KEY" not in secrets or not all(
        re.fullmatch(r"[A-Z_][A-Z0-9_]*", name) for name in [*secrets, *variables]
    ):
        raise ValueError(
            "Supply uppercase secret/variable names, including BITFAB_API_KEY; never values"
        )
    steps = spec.get("setupSteps")
    if (
        not isinstance(steps, list)
        or not steps
        or not all(isinstance(step, dict) for step in steps)
    ):
        raise ValueError("setupSteps must contain reviewed GitHub Actions setup steps")
    workflow = workflow_document(
        spec, steps, replay_step(config, spec["cliCommand"], runner_env(config))
    )
    outputs = {
        CONFIG: json.dumps(config, indent=2) + "\n",
        f".github/workflows/{config['workflow']}": json.dumps(workflow, indent=2)
        + "\n",
    }
    write_new_files(root, outputs)
    return {
        "files": list(outputs),
        "requiredSecrets": sorted(set(secret_targets(config).values())),
        "requiredVariables": sorted(
            source["variable"]
            for source in config.get("env", {}).values()
            if "variable" in source
        ),
        "next": "Configure secrets securely, review push triggers, get the workflow registered with GitHub Actions (merging it to the default branch once does that), run --cloud-dry-run, then --cloud-check",
    }


def replace_file(target, content):
    with tempfile.NamedTemporaryFile(mode="w", dir=target.parent, delete=False) as file:
        file.write(content)
        temporary = file.name
    os.replace(temporary, target)


def update_existing(root, spec):
    """Existing files win: only the Replay step and steps older setups generated change."""
    config = json.loads(within(root, CONFIG).read_text())
    validate_config(config)
    name = f".github/workflows/{config['workflow']}"
    workflow_path = within(root, name)
    text = workflow_path.read_text() if workflow_path.is_file() else ""
    try:
        workflow = json.loads(text)
        job = workflow["jobs"]["replay"]
        steps = job["steps"]
    except (ValueError, KeyError, TypeError) as error:
        # A workflow the customer rewrote as YAML is current when nothing older remains.
        if (
            "--cloud-execute" in text
            and OLD_UPLOAD_STEP not in text
            and ".bitfab/cloudReplay.py" not in text
            and not re.search(rf"timeout-minutes:\s*{OLD_JOB_TIMEOUT}\b", text)
        ):
            return {"files": [], "updated": False, "next": "Already up to date"}
        raise ValueError(
            f"{name} is missing or was not generated by --cloud-init; edit its Replay step by hand to run the SDK's bitfab-replay command with --cloud-execute"
        ) from error
    replay = [
        index
        for index, step in enumerate(steps)
        if isinstance(step, dict) and step.get("name") == "Replay"
    ]
    if len(replay) != 1:
        raise ValueError(f"{name} must contain exactly one step named Replay")
    step = steps[replay[0]]
    words = shlex.split(step.get("run", ""))
    if spec is not None and spec.get("cliCommand") is not None:
        cli_command = spec["cliCommand"]
    elif words[-1:] == ["--cloud-execute"]:
        cli_command = words[:-1]
    elif config.get("cliCommand") is not None:
        cli_command = config["cliCommand"]
    else:
        raise ValueError(
            "The Replay step does not run the SDK command; pass --config with a file whose cliCommand starts bitfab-replay from workingDirectory"
        )
    validate_cli_command(cli_command)
    env = {
        key: value
        for key, value in (step.get("env") or {}).items()
        if key not in RUNNER_ENV
    }
    declared = runner_env(config)
    conflicts = sorted(
        name for name in declared if name in env and env[name] != declared[name]
    )
    undeclared = sorted(name for name in env if name not in declared)
    env = {**declared, **env}
    extra = {
        key: value
        for key, value in step.items()
        if key not in ("name", "uses", "with", "run", "working-directory", "env")
    }
    before = json.dumps([config, workflow], sort_keys=True)
    # Edit in place so the customer's checkout options, extra steps, and job
    # settings survive; only the Replay step and our old result upload change.
    job["steps"] = [
        {**replay_step(config, cli_command, env), **extra}
        if index == replay[0]
        else entry
        for index, entry in enumerate(steps)
        if not (
            isinstance(entry, dict)
            and entry.get("name") == OLD_UPLOAD_STEP
            and str(entry.get("uses", "")).startswith("actions/upload-artifact@")
        )
    ]
    # The limit older setups generated; a value the customer chose stays.
    if job.get("timeout-minutes") == OLD_JOB_TIMEOUT:
        del job["timeout-minutes"]
    config.pop("cliCommand", None)
    # Setups before the SDK carried the script left a copy that nothing reads now.
    old_script = ".bitfab/cloudReplay.py"
    removable = [old_script] if within(root, old_script).exists() else []
    mismatches = {}
    if conflicts:
        mismatches["conflicts"] = conflicts
    if undeclared:
        mismatches["undeclared"] = undeclared
    if mismatches:
        mismatches["mismatchNext"] = (
            "The Replay step sets these differently from, or in addition to, .bitfab/cloud.json. "
            'Describe each one in cloud.json "env" as {"secret": NAME} or {"variable": NAME} '
            "so --cloud-secrets and the runner's empty-secret check see it; the step was left as it is"
        )
    if json.dumps([config, workflow], sort_keys=True) == before:
        return {
            "files": [],
            "updated": False,
            "removable": removable,
            **mismatches,
            "next": "Already up to date",
        }
    outputs = {
        CONFIG: json.dumps(config, indent=2) + "\n",
        name: json.dumps(workflow, indent=2) + "\n",
    }
    for output, content in outputs.items():
        replace_file(within(root, output), content)
    return {
        "files": list(outputs),
        "updated": True,
        "removable": removable,
        **mismatches,
        "next": "Review the diff, then run --cloud-dry-run; replays use the workflow in their own snapshot, so the change applies without merging",
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
            if result.get("request", {}).get("check"):
                print(
                    f"Cloud check failed. The Replay step log names what is missing: {result.get('url')}",
                    file=sys.stderr,
                )
            elif result.get("testRunId"):
                print(
                    f"Cloud replay stopped early ({result.get('stoppedEarly', result.get('conclusion'))}). Traces that finished are saved in test run {result['testRunId']}.",
                    file=sys.stderr,
                )
            return 1
        if result.get("state") == "completed":
            code = report_errored_items(result)
            if result.get("failOnError") and result.get("errored"):
                print(
                    "Cloud replay: exiting 1 because of --fail-on-error",
                    file=sys.stderr,
                )
                return 1
            return code
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
