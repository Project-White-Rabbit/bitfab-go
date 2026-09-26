from __future__ import annotations

import argparse
import base64
import contextlib
import functools
import json
import os
import re
import shlex
import shutil
import signal
import ssl
import subprocess
import sys
import tempfile
import threading
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path
from urllib.parse import urlencode

WORKFLOW = "bitfab-replay.yml"
WORKFLOW_NAMES = (WORKFLOW, "bitfab-replay.yaml")
WORKFLOW_DIRECTORY = ".github/workflows"
OLD_CONFIG = ".bitfab/cloud.json"
OLD_SCRIPT = ".bitfab/cloudReplay.py"
OLD_RUNNER_ENV = ("BITFAB_CLOUD_REQUEST", "BITFAB_EXECUTION_ID", "BITFAB_COMMIT_SHA")
OLD_UPLOAD_STEP = "Save replay identity"
OLD_JOB_TIMEOUT = 35
PREFIX = "bitfab-replay/"
DEFAULT_SECRET_PREFIX = "BITFAB_CLOUD_"
API_VERSION = "2026-03-10"
RUN_NAME = "Bitfab replay ${{ inputs.execution_id }}"
RESULT_TITLE = "Bitfab replay result"
REPLAY_COMMAND_ENV = "BITFAB_REPLAY_COMMAND"
SDK_LANGUAGE_ENV = "BITFAB_SDK_LANGUAGE"
CHECK_COMMAND_ENV = "BITFAB_REPLAY_CHECK"
REQUEST_VERSION = 2
ITEM_ERROR_FIELDS = (
    "error",
    "traceError",
    "trace_error",
    "replayError",
    "replay_error",
)
ITEM_ID_FIELDS = ("originalTraceId", "original_trace_id", "traceId", "trace_id")
ITEM_ERROR_LINES = 20
ITEM_ERROR_LENGTH = 500
OUTPUT_TAIL_LENGTH = 4000
UUID = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
SHA = re.compile(r"^[0-9a-f]{40}$")
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
EXPERIMENT_LINE = re.compile(rb"^\[replay\] Experiment ([0-9a-f-]{36}):")
SECRET_REFERENCE = re.compile(
    r"[\"']?([A-Za-z_][A-Za-z0-9_]*)[\"']?[ \t]*:[ \t]*[\"']?\$\{\{\s*secrets(?:\.([A-Za-z_][A-Za-z0-9_]*)|\[\s*[\"']([A-Za-z_][A-Za-z0-9_]*)[\"']\s*\])\s*\}\}"
)
ENVIRONMENT_SETTING = re.compile(
    r"^[ \t]*\"?environment\"?[ \t]*:[ \t]*[\"']?([\w.-]+)[\"']?[ \t]*,?[ \t]*$",
    re.MULTILINE,
)
PUSH_TRIGGER = re.compile(
    r"^[ \t]*\"?(?:on\"?[ \t]*:.*\bpush\b|push\"?[ \t]*:)", re.MULTILINE
)
LIFECYCLE_FLAGS = (
    "--cloud-status",
    "--cloud-watch",
    "--cloud-cancel",
    "--cloud-cleanup",
)
CLOUD_VALUE_FLAGS = (
    *LIFECYCLE_FLAGS,
    "--cloud-include",
    "--cloud-request-id",
    "--cloud-timeout",
)
CLOUD_SWITCHES = (
    "--cloud",
    "--cloud-dry-run",
    "--cloud-detach",
    "--cloud-check",
    "--fail-on-error",
    "--dry-run",
)
CHECK_SWITCHES = ("--cloud-check", "--dry-run")
SEED_FLAGS = ("--seed", "--cases", "--from-trace", "--run")
PATH_FLAGS = ("--registry", "--params", "--code-change")
CODE_CHANGE_FLAGS = ("--code-change", "--no-code-change")
SELECTION_FLAGS = ("--trace-ids", "--dataset-ids", "--dataset-id", "--resume")
ACTIONS = {
    "checkout": "actions/checkout@3d3c42e5aac5ba805825da76410c181273ba90b1",
    "node": "actions/setup-node@820762786026740c76f36085b0efc47a31fe5020",
    "pnpm": "pnpm/action-setup@ea17c68df8912ef543352723c149a84f56e3d413",
    "bun": "oven-sh/setup-bun@0c5077e51419868618aeaa5fe8019c62421857d6",
    "python": "actions/setup-python@5fda3b95a4ea91299a34e894583c3862153e4b97",
    "uv": "astral-sh/setup-uv@c18668ad3cf93ea998bef934396af7bb5c839dc7",
    "ruby": "ruby/setup-ruby@14594264cd68ce8a2345dd349bc3d138a4ef85c8",
    "go": "actions/setup-go@b7ad1dad31e06c5925ef5d2fc7ad053ef454303e",
}
NO_GITHUB = (
    "Cloud replay needs access to GitHub. Any one of these works: "
    "log in with the GitHub CLI (gh auth login); "
    "set GH_TOKEN to a token that can push to the repository and run its Actions; "
    "or push to github.com over https once, so git stores a credential for it"
)
HELP = """Cloud replay: run the replay you run locally on GitHub Actions by adding --cloud.

  bitfab-replay --registry scripts/replay.ts classify --trace-ids UUID --cloud

Every replay option works as it does locally and is passed to the runner as given.
The runner starts the same bitfab-replay from the same directory, on a snapshot of
your working tree. New files need --cloud-include PATH. Credentials and ignored files
are refused. Requires git and Python 3.10+ on macOS or Linux, with a github.com origin.

Options added by --cloud:
  --dry-run              Check the runner without replaying: every secret the workflow
                         maps has a value, BITFAB_REPLAY_CHECK passes when set, and the
                         traces resolve. --cloud-check is the same.
  --cloud-dry-run        Show what the snapshot would contain and push nothing.
  --cloud-include PATH   Add a new file to the snapshot (repeatable).
  --cloud-detach         Return after dispatch instead of waiting.
  --cloud-timeout MIN    Stop the replay after MIN minutes (1..7200).
  --cloud-request-id ID  Recover a submission whose response was lost.
  --fail-on-error        Exit 1 when any replayed item errored.

Follow a replay by the execution UUID it prints:
  --cloud-status ID | --cloud-watch ID | --cloud-cancel ID | --cloud-cleanup ID

Set up once:
  --cloud-init [--secret NAME ...] [--environment NAME] [--run COMMAND]
               [--runs-on LABEL] [--secret-prefix PREFIX] [--check COMMAND]
      Writes .github/workflows/bitfab-replay.yml, the only file cloud replay keeps
      in the repository. Run it again to update a setup made by an older SDK.
  --cloud-secrets --env-file FILE [NAME ...] [--environment NAME] [--dry-run]
      Copy local values into the GitHub secrets the workflow reads.

GitHub access comes from the GitHub CLI when it is logged in, otherwise from GH_TOKEN
or GITHUB_TOKEN, otherwise from the github.com credential git already stores.
Only --cloud-secrets needs the GitHub CLI itself.
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


def repository_path(root, path):
    try:
        relative = Path(path).resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError(f"{path} must be inside the repository") from error
    if sensitive(relative):
        raise ValueError(f"Refusing credential-like file: {relative}")
    return relative


def working_directory(root):
    try:
        relative = Path.cwd().resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError("Run cloud replay from inside the repository") from error
    return relative or "."


def repository(root):
    remote = git(root, "remote", "get-url", "origin")
    match = re.fullmatch(
        r"(?:git@github\.com:|https://github\.com/)([\w.-]+/[\w.-]+?)(?:\.git)?", remote
    )
    if match is None:
        raise ValueError(
            "origin must be a github.com repository without embedded credentials"
        )
    push = git(root, "remote", "get-url", "--push", "origin")
    other = re.fullmatch(
        r"(?:git@github\.com:|https://github\.com/)([\w.-]+/[\w.-]+?)(?:\.git)?", push
    )
    if other is None or other[1].lower() != match[1].lower():
        raise ValueError("origin fetch and push repositories must match")
    return match[1]


def find_workflow(root):
    for name in WORKFLOW_NAMES:
        if within(root, f"{WORKFLOW_DIRECTORY}/{name}").is_file():
            return name
    return None


def setup_workflow(root):
    if within(root, OLD_CONFIG).exists():
        raise ValueError(
            f"This cloud replay setup was made by an older SDK. Run bitfab-replay --cloud-init once to move it into the workflow and remove {OLD_CONFIG}"
        )
    name = find_workflow(root)
    if name is None:
        raise ValueError(
            f"No {WORKFLOW_DIRECTORY}/{WORKFLOW} yet. Run bitfab-replay --cloud-init, or ask your coding agent for bitfab:setup cloud"
        )
    return name


def workflow_secrets(text):
    return {
        name: dotted or bracketed
        for name, dotted, bracketed in SECRET_REFERENCE.findall(text)
    }


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
        description="Copy named values from local environment files into the GitHub Actions secrets the cloud replay workflow reads. Values are piped to gh on standard input and never printed, logged, or passed as arguments.",
    )
    parser.add_argument(
        "names",
        nargs="*",
        help="Environment variable names to copy; defaults to every secret the workflow maps",
    )
    parser.add_argument(
        "--env-file",
        action="append",
        required=True,
        help="Repository-relative environment file to read; repeatable, earliest definition wins",
    )
    parser.add_argument(
        "--environment",
        help="GitHub Environment to set the secrets in; defaults to the workflow's environment",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which names were found without writing anything to GitHub",
    )
    args = parser.parse_args(argv)
    root = root_directory()
    workflow = setup_workflow(root)
    text = within(root, f"{WORKFLOW_DIRECTORY}/{workflow}").read_text()
    targets = workflow_secrets(text)
    names = args.names or list(targets)
    unknown = [name for name in names if name not in targets]
    if unknown:
        raise ValueError(
            f"The workflow does not read {', '.join(unknown)}. Add each one to the Replay step's env as NAME: ${{{{ secrets.{DEFAULT_SECRET_PREFIX}NAME }}}} first"
        )
    configured = ENVIRONMENT_SETTING.search(text)
    environment = args.environment or (configured[1] if configured else None)
    values = {}
    for path in args.env_file:
        for key, value in parse_environment_file(
            within(root, path).read_text()
        ).items():
            values.setdefault(key, value)
    repo = repository(root)
    found, missing, empty = [], [], []
    for name in names:
        value = values.get(name, values.get(targets[name]))
        if value is None:
            missing.append(name)
        elif not value:
            empty.append(name)
        else:
            found.append(name)
    if found and not args.dry_run and shutil.which("gh") is None:
        where = (
            f"https://github.com/{repo}/settings/environments"
            if environment
            else f"https://github.com/{repo}/settings/secrets/actions"
        )
        raise ValueError(
            f"Copying secrets needs the GitHub CLI, which encrypts each value for GitHub. Install it (https://cli.github.com), or create {', '.join(targets[name] for name in found)} by hand at {where}"
        )
    for name in found:
        if not args.dry_run:
            command(
                [
                    "gh",
                    "secret",
                    "set",
                    targets[name],
                    "--repo",
                    repo,
                    *(["--env", environment] if environment else []),
                ],
                input=values.get(name, values.get(targets[name])),
            )
    return {
        "repository": repo,
        "environment": environment,
        "dryRun": args.dry_run,
        "set": [targets[name] for name in found],
        "missing": missing,
        "empty": empty,
        "next": "Create the missing secrets by hand; empty local values were skipped because an empty secret overrides a working default with nothing; a wrong value only surfaces when the first real replay runs",
    }


@functools.cache
def github_access():
    if shutil.which("gh") is not None:
        with contextlib.suppress(RuntimeError, OSError, subprocess.SubprocessError):
            command(["gh", "auth", "status", "--hostname", "github.com"])
            return {"source": "the GitHub CLI", "token": None}
    for name in ("GH_TOKEN", "GITHUB_TOKEN"):
        if os.environ.get(name):
            return {"source": name, "token": os.environ[name]}
    token = stored_git_credential()
    if token:
        return {"source": "the github.com credential git stores", "token": token}
    raise ValueError(NO_GITHUB)


def stored_git_credential():
    try:
        result = subprocess.run(
            ["git", "credential", "fill"],
            input="protocol=https\nhost=github.com\n\n",
            text=True,
            capture_output=True,
            check=False,
            timeout=15,
            env={
                **os.environ,
                "GIT_TERMINAL_PROMPT": "0",
                "GCM_INTERACTIVE": "never",
                "GIT_ASKPASS": "",
                "SSH_ASKPASS": "",
            },
        )
    except (OSError, subprocess.SubprocessError):
        return None
    if result.returncode:
        return None
    for line in result.stdout.splitlines():
        if line.startswith("password="):
            return line.removeprefix("password=") or None
    return None


def api(repo, suffix, *, method="GET", payload=None):
    path = f"repos/{repo}" + (f"/{suffix}" if suffix else "")
    access = github_access()
    data = None if payload is None else json.dumps(payload)
    if access["token"] is None:
        result = command(
            [
                "gh",
                "api",
                "--hostname",
                "github.com",
                "-H",
                f"X-GitHub-Api-Version: {API_VERSION}",
                "--jq",
                "tojson",
                path,
                "--method",
                method,
                *(["--input", "-"] if data is not None else []),
            ],
            input=data,
        )
        return json.loads(result) if result else None
    request = urllib.request.Request(
        f"https://api.github.com/{path}",
        method=method,
        data=None if data is None else data.encode(),
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {access['token']}",
            "X-GitHub-Api-Version": API_VERSION,
            "User-Agent": "bitfab-cloud-replay",
            **({"Content-Type": "application/json"} if data is not None else {}),
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=60) as response:
            body = response.read()
    except urllib.error.HTTPError as error:
        raise CommandError(
            f"GitHub {method} {path.split('?')[0]} failed (HTTP {error.code}) using {access['source']}",
            error.code,
        ) from None
    except (urllib.error.URLError, OSError) as error:
        reason = getattr(error, "reason", error)
        if isinstance(reason, ssl.SSLCertVerificationError):
            raise CommandError(
                "Python cannot verify GitHub's certificate because it has no certificate store. "
                "On a python.org install, run Install Certificates.command from its Applications folder, "
                "set SSL_CERT_FILE to a certificate bundle, or log in with the GitHub CLI (gh auth login), which cloud replay uses instead"
            ) from None
        raise CommandError(f"Could not reach api.github.com: {reason}") from None
    return json.loads(body) if body.strip() else None


def split_arguments(argv):
    cloud, includes, switches, replay = {}, [], set(), []
    index = 0
    while index < len(argv):
        token = argv[index]
        name = token.split("=", 1)[0]
        if name in CLOUD_VALUE_FLAGS:
            if "=" in token:
                value = token.split("=", 1)[1]
                index += 1
            elif index + 1 < len(argv):
                value = argv[index + 1]
                index += 2
            else:
                raise ValueError(f"{name} needs a value")
            if name == "--cloud-include":
                includes.append(value)
            elif name in cloud:
                raise ValueError(f"{name} was given twice")
            else:
                cloud[name] = value
        elif token in CLOUD_SWITCHES:
            switches.add(token)
            index += 1
        elif name in SEED_FLAGS:
            raise ValueError(f"{name} seeds traces; run it locally, without --cloud")
        else:
            replay.append(token)
            index += 1
    return cloud, includes, switches, replay


def parse(argv):
    cloud, includes, switches, replay = split_arguments(argv)
    lifecycle = [flag for flag in LIFECYCLE_FLAGS if flag in cloud]
    if lifecycle:
        if len(lifecycle) != 1 or len(cloud) != 1 or includes or switches or replay:
            raise ValueError("Cloud lifecycle commands take only their execution UUID")
        operation = lifecycle[0].removeprefix("--cloud-")
        execution_id = cloud[lifecycle[0]]
    else:
        if not replay:
            raise ValueError(
                "Add --cloud to the replay command you run locally, such as bitfab-replay --registry scripts/replay.ts classify --trace-ids UUID --cloud"
            )
        if not any(token.split("=", 1)[0] in SELECTION_FLAGS for token in replay):
            raise ValueError(
                "Select traces with --trace-ids, --dataset-ids, or --resume"
            )
        timeout = cloud.get("--cloud-timeout")
        if timeout is not None and not (
            timeout.isdigit() and 1 <= int(timeout) <= 7200
        ):
            raise ValueError("--cloud-timeout must be 1..7200 minutes")
        operation = "submit"
        execution_id = cloud.get("--cloud-request-id") or str(uuid.uuid4())
    if not UUID.fullmatch(execution_id):
        raise ValueError("Execution ID must be a UUID")
    return {
        "operation": operation,
        "id": execution_id,
        "cloud": cloud,
        "includes": includes,
        "switches": switches,
        "replay": replay,
    }


def validate_arguments(args):
    if not isinstance(args, list) or not all(isinstance(value, str) for value in args):
        raise ValueError("Replay arguments must be a list of strings")
    for value in args:
        if "\x00" in value or "\n" in value or len(value) > 1000:
            raise ValueError(
                "Replay options must be single-line values of at most 1000 characters"
            )
        if value.startswith("--cloud"):
            raise ValueError("The runner replays locally and never dispatches again")
    if sum(len(value) for value in args) > 16000:
        raise ValueError("Replay options exceed 16000 characters")


def replay_request(root, parsed):
    cwd = Path.cwd().resolve()
    args, files = [], []

    def snapshot_path(value):
        path = cwd / value
        files.append(repository_path(root, path))
        return (
            os.path.relpath(path.resolve(), cwd) if Path(value).is_absolute() else value
        )

    replay = parsed["replay"]
    index = 0
    while index < len(replay):
        token = replay[index]
        name, separator, value = token.partition("=")
        if name in PATH_FLAGS and separator:
            args.append(f"{name}={snapshot_path(value)}")
        elif name in PATH_FLAGS and index + 1 < len(replay):
            args += [token, snapshot_path(replay[index + 1])]
            index += 1
        else:
            args.append(token)
        index += 1
    validate_arguments(args)
    request = {
        "version": REQUEST_VERSION,
        "id": parsed["id"],
        "cwd": working_directory(root),
        "args": args,
    }
    if "--cloud-timeout" in parsed["cloud"]:
        request["timeoutMinutes"] = int(parsed["cloud"]["--cloud-timeout"])
    if parsed["switches"] & set(CHECK_SWITCHES):
        request["check"] = True
    return request, files


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


def snapshot(root, workflow, includes, files, execution_id, *, dry_run=False):
    if git(root, "ls-files", "-u"):
        raise ValueError("Resolve merge conflicts before snapshotting")
    head = git(root, "rev-parse", "HEAD")
    included = []
    for value in includes:
        path = within(root, value)
        if not path.is_file() or path.is_symlink() or (root / value).is_symlink():
            raise ValueError("--cloud-include requires individual regular files")
        ignored = subprocess.run(
            ["git", "-C", str(root), "check-ignore", "--quiet", "--", value],
            check=False,
            timeout=15,
        ).returncode
        if ignored not in (0, 1):
            raise ValueError("Unable to determine whether the included file is ignored")
        if ignored == 0 or sensitive(value):
            raise ValueError(f"Refusing ignored or credential-like file: {value}")
        included.append(value)
    with tempfile.TemporaryDirectory(prefix="bitfab-cloud-index-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        git(root, "read-tree", head, env=env)
        git(root, "add", "-u", "--", ".", env=env)
        if included:
            git(root, "--literal-pathspecs", "add", "--", *included, env=env)
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
        required = [f"{WORKFLOW_DIRECTORY}/{workflow}", *files]
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
        changed = git(root, "diff", "--name-only", head, tree).splitlines()
        if dry_run:
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
                "files": changed,
                "includedFiles": included,
                "omittedNewFiles": sorted(new_files - set(included)),
            }
        sha = git(
            root,
            "commit-tree",
            tree,
            "-p",
            head,
            input=f"Bitfab replay {execution_id}\n",
        )
        return {"baseSha": head, "sha": sha, "files": changed}


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
            if success:
                raise
            result = None
        if result is None:
            if success:
                raise ValueError("The GitHub job succeeded but left no replay result")
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


def preflight(repo, workflow):
    access = github_access()
    try:
        details = api(repo, "")
    except RuntimeError as error:
        raise ValueError(
            f"GitHub access from {access['source']} cannot read {repo}{http_status(error)}; use an account or token with write access to it"
        ) from error
    if (details or {}).get("permissions", {}).get("push") is False:
        raise ValueError(
            f"GitHub access from {access['source']} cannot push to {repo}; use an account or token with write access to it"
        )
    path = f"{WORKFLOW_DIRECTORY}/{workflow}"
    try:
        registered = api(repo, f"actions/workflows/{workflow}")
    except RuntimeError as error:
        raise ValueError(
            f"GitHub Actions has not registered {path}{http_status(error)}; merging it to the default branch once registers it, and after that each replay runs the copy in its own snapshot"
        ) from error
    if registered.get("state") != "active":
        raise ValueError(
            f"{path} is {registered.get('state')}; enable it in the repository's Actions tab"
        )


def run_cli(argv):
    parsed = parse(argv)
    operation, execution_id = parsed["operation"], parsed["id"]
    if os.name != "posix" or sys.version_info < (3, 10):
        raise ValueError("Cloud replay currently requires macOS/Linux and Python 3.10+")
    root = root_directory()
    workflow = setup_workflow(root) if operation == "submit" else None
    repo = repository(root)
    directory = state_directory(root)
    path = directory / f"{execution_id}.json"
    with execution_lock(directory, execution_id):
        if operation == "submit":
            request, files = replay_request(root, parsed)
            if "--cloud-dry-run" in parsed["switches"]:
                return {
                    "dryRun": True,
                    "repository": repo,
                    **snapshot(
                        root,
                        workflow,
                        parsed["includes"],
                        files,
                        execution_id,
                        dry_run=True,
                    ),
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
                status(record)
                if record["state"] == "prepared":
                    raise ValueError(
                        "Submission stopped before dispatch. Use --cloud-cleanup, then submit a new execution UUID"
                    )
            else:
                preflight(repo, workflow)
                if request.get("check"):
                    print(
                        "Cloud dry run: dispatching a GitHub run that checks secrets and resolves the traces without replaying. It receives the workflow's secrets. Use --cloud-dry-run to only review the snapshot.",
                        file=sys.stderr,
                        flush=True,
                    )
                source = snapshot(
                    root, workflow, parsed["includes"], files, execution_id
                )
                record = {
                    "id": execution_id,
                    "repository": repo,
                    "workflow": workflow,
                    "branch": PREFIX + execution_id,
                    "request": request,
                    "state": "prepared",
                    **source,
                }
                if "--fail-on-error" in parsed["switches"]:
                    record["failOnError"] = True
                save(path, record)
                print(
                    f"Cloud execution {execution_id}. Recover with --cloud-status {execution_id}",
                    file=sys.stderr,
                    flush=True,
                )
                ref = "refs/heads/" + record["branch"]
                try:
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
                        f"actions/workflows/{workflow}/dispatches",
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
                        f"Dispatching {workflow} on {record['branch']} failed{http_status(error)}; the GitHub account needs write access to Actions, and the registered workflow must accept workflow_dispatch. Check the Actions tab, then resume with --cloud-status {execution_id}"
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
            return record
        if operation == "watch" or (
            operation == "submit" and "--cloud-detach" not in parsed["switches"]
        ):
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


def replay_command():
    try:
        value = json.loads(os.environ.get(REPLAY_COMMAND_ENV, ""))
    except ValueError:
        value = None
    if (
        not isinstance(value, list)
        or not value
        or not all(isinstance(part, str) and part for part in value)
    ):
        raise ValueError(
            "Start the Replay step with the SDK's bitfab-replay command and --cloud-execute, so the runner knows how to start the replay"
        )
    return value


def dispatched_request():
    event = json.loads(Path(os.environ["GITHUB_EVENT_PATH"]).read_text())
    inputs = event.get("inputs") or {}
    request = json.loads(base64.b64decode(inputs.get("request", ""), validate=True))
    if request.get("id") != inputs.get("execution_id"):
        raise ValueError("Replay request does not match the dispatched execution")
    version = request.get("version", 0)
    if version < REQUEST_VERSION or "args" not in request:
        raise ValueError(
            "This replay was submitted by an older SDK than the one the snapshot installs; update the SDK you run locally to match the lockfile"
        )
    if version > REQUEST_VERSION:
        raise ValueError(
            "This replay was submitted by a newer SDK than the one the snapshot installs; update the SDK in the lockfile to match the one you run locally"
        )
    validate_arguments(request["args"])
    return request


def execute():
    if os.environ.get("GITHUB_RUN_ATTEMPT") != "1":
        raise ValueError("Submit a new replay instead of rerunning an Actions job")
    request = dispatched_request()
    root = root_directory()
    commit = os.environ["GITHUB_SHA"]
    if git(root, "rev-parse", "HEAD") != commit:
        raise ValueError("Runner checkout does not match the dispatched snapshot SHA")
    directory = within(root, request["cwd"])
    check_secrets(root)
    names = {value.split("=", 1)[0] for value in request["args"]}
    args = [
        *replay_command(),
        *request["args"],
        *([] if names & set(CODE_CHANGE_FLAGS) else ["--no-code-change"]),
    ]
    identity = {"executionId": request["id"], "commitSha": commit}
    if request.get("check"):
        summary = {**identity, **run_check(directory, args)}
        write_result(summary)
        return summary
    experiment = {}
    previous = signal.signal(signal.SIGTERM, raise_interrupt)
    try:
        summary = {
            **identity,
            **run_replay(directory, args, request.get("timeoutMinutes"), experiment),
        }
    except BaseException as error:
        if UUID.fullmatch(experiment.get("id", "")):
            write_result(
                {
                    **identity,
                    "testRunId": experiment["id"],
                    "stoppedEarly": (str(error) or type(error).__name__)[:300],
                }
            )
        raise
    finally:
        signal.signal(signal.SIGTERM, previous)
    write_result(summary)
    return summary


def running_workflow(root):
    reference = os.environ.get("GITHUB_WORKFLOW_REF", "")
    match = re.search(r"(\.github/workflows/[^@]+)@", reference)
    if match:
        return within(root, match[1])
    return within(root, f"{WORKFLOW_DIRECTORY}/{find_workflow(root) or WORKFLOW}")


def check_secrets(root):
    path = running_workflow(root)
    targets = workflow_secrets(path.read_text()) if path.is_file() else {}
    empty = [name for name in targets if os.environ.get(name) == ""]
    if empty:
        raise ValueError(
            "These replay environment variables are empty on the runner: "
            + ", ".join(f"{name} (secret {targets[name]})" for name in empty)
            + ". GitHub passes a secret that does not exist as an empty string. Create each one under Settings, Secrets and variables, Actions, in the repository or in the job's Environment"
        )


def run_check(directory, args):
    check = os.environ.get(CHECK_COMMAND_ENV, "").strip()
    if check:
        print(f"Running {CHECK_COMMAND_ENV}: {check}", flush=True)
        code = subprocess.run(
            shlex.split(check), cwd=directory, stdin=subprocess.DEVNULL, check=False
        ).returncode
        if code:
            raise ValueError(f"{CHECK_COMMAND_ENV} exited {code}; its output is above")
    result = run_command(directory, [*args, "--dry-run"], None, {})
    items = result.get("items")
    if not isinstance(items, list):
        raise ValueError("The replay dry run did not return its resolved items")
    errors = item_errors(items)
    report_item_errors(errors)
    if errors:
        raise ValueError(
            f"{len(errors)} of {len(items)} traces failed to resolve; the errors are above"
        )
    return {"check": "passed", "resolved": len(items)}


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
    message = base64.b64encode(json.dumps(summary).encode()).decode()
    print(f"::notice title={RESULT_TITLE}::{message}", flush=True)
    if "check" in summary:
        heading = "Bitfab cloud check"
        lines = [
            f"Passed: every secret has a value and {summary['resolved']} traces resolved. Commit: `{summary['commitSha']}`"
        ]
    else:
        heading = "Bitfab replay"
        lines = [
            f"Test run: `{summary['testRunId']}`",
            f"Commit: `{summary['commitSha']}`",
            f"Stopped early: {summary['stoppedEarly']}. Traces that finished are saved in this test run."
            if "stoppedEarly" in summary
            else f"Replayed: {summary['replayed']}, errored: {summary['errored']}",
        ]
    with Path(os.environ["GITHUB_STEP_SUMMARY"]).open("a") as file:
        file.write(f"### {heading}\n\n" + "\n\n".join(lines) + "\n")


def run_replay(directory, args, timeout, experiment):
    result = run_command(directory, args, timeout, experiment)
    test_run = result.get("testRunId", result.get("test_run_id"))
    if not isinstance(test_run, str) or not UUID.fullmatch(test_run):
        raise ValueError("Replay did not return a valid persisted test run UUID")
    items = result.get("items")
    if not isinstance(items, list):
        raise ValueError("Replay did not return its replayed items")
    replayed = [item for item in items if not carried_over(item)]
    errors = item_errors(replayed)
    report_item_errors(errors)
    return {"testRunId": test_run, "replayed": len(replayed), "errored": len(errors)}


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


def run_command(directory, args, timeout, experiment):
    with tempfile.TemporaryFile() as output:
        with subprocess.Popen(
            args,
            cwd=directory,
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
        raise ValueError("Replay result has invalid item counts")
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


PLAIN_SCALAR = re.compile(r"[A-Za-z0-9_./][A-Za-z0-9_ ./@*+=,()-]*")
YAML_WORDS = {"true", "false", "yes", "no", "on", "off", "null", "~", "y", "n"}


def yaml_scalar(value):
    if isinstance(value, (dict, list)):
        return "{}" if isinstance(value, dict) else "[]"
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    text = str(value)
    if (
        PLAIN_SCALAR.fullmatch(text)
        and text == text.strip()
        and text.lower() not in YAML_WORDS
        and not re.fullmatch(r"[\d._+-]+", text)
    ):
        return text
    return json.dumps(text)


def yaml_key(key):
    return "on" if key == "on" else yaml_scalar(str(key))


def yaml_lines(value, indent=0):
    pad = "  " * indent
    lines = []
    if isinstance(value, dict):
        for key, item in value.items():
            if isinstance(item, (dict, list)) and item:
                lines.append(f"{pad}{yaml_key(key)}:")
                lines += yaml_lines(item, indent + 1)
            else:
                lines.append(f"{pad}{yaml_key(key)}: {yaml_scalar(item)}")
    else:
        for item in value:
            if isinstance(item, dict) and item:
                nested = yaml_lines(item, indent + 1)
                lines.append(f"{pad}- {nested[0].lstrip()}")
                lines += nested[1:]
            else:
                lines.append(f"{pad}- {yaml_scalar(item)}")
    return lines


def to_yaml(document):
    return "\n".join(yaml_lines(document)) + "\n"


def find_upward(root, start, names):
    directory = start
    while True:
        for name in names:
            if (directory / name).is_file():
                return directory, name
        if directory == root:
            return None, None
        directory = directory.parent


def step_directory(root, directory):
    relative = directory.relative_to(root).as_posix()
    return {} if relative in ("", ".") else {"working-directory": relative}


def detect_language(root, start):
    markers = {
        "package.json": "typescript",
        "pyproject.toml": "python",
        "requirements.txt": "python",
        "Gemfile": "ruby",
        "go.mod": "go",
    }
    _, found = find_upward(root, start, list(markers))
    if found is None:
        raise ValueError(
            "Could not tell which Bitfab SDK this project uses; run --cloud-init through your SDK's bitfab-replay command"
        )
    return markers[found]


def version_input(root, start, key, names, default):
    directory, name = find_upward(root, start, names)
    if name is None:
        return {f"{key}-version": default}
    return {f"{key}-version-file": (directory / name).relative_to(root).as_posix()}


def typescript_project(root, start):
    directory, lockfile = find_upward(
        root,
        start,
        ["pnpm-lock.yaml", "bun.lock", "bun.lockb", "yarn.lock", "package-lock.json"],
    )
    manager = {
        "pnpm-lock.yaml": "pnpm",
        "bun.lock": "bun",
        "bun.lockb": "bun",
        "yarn.lock": "yarn",
    }.get(lockfile, "npm")
    install = {
        "pnpm": "pnpm install --frozen-lockfile",
        "bun": "bun install --frozen-lockfile",
        "yarn": "yarn install --frozen-lockfile",
        "npm": "npm ci" if lockfile else "npm install",
    }[manager]
    node = {
        "name": "Set up Node.js",
        "uses": ACTIONS["node"],
        "with": {
            **version_input(root, start, "node", [".nvmrc", ".node-version"], "lts/*"),
            **(
                {"cache": manager}
                if manager in ("pnpm", "yarn", "npm") and lockfile
                else {}
            ),
        },
    }
    steps = []
    if manager == "pnpm":
        steps.append({"name": "Install pnpm", "uses": ACTIONS["pnpm"]})
    if manager == "bun":
        steps.append({"name": "Install Bun", "uses": ACTIONS["bun"]})
    steps += [
        node,
        {
            "name": "Install dependencies",
            **step_directory(root, directory or start),
            "run": install,
        },
    ]
    run = {
        "pnpm": "pnpm exec bitfab-replay",
        "bun": "bunx bitfab-replay",
        "yarn": "yarn bitfab-replay",
        "npm": "npx --no-install bitfab-replay",
    }[manager]
    return steps, run


def python_project(root, start):
    directory, lockfile = find_upward(
        root, start, ["uv.lock", "poetry.lock", "requirements.txt", "pyproject.toml"]
    )
    location = step_directory(root, directory or start)
    python = {
        "name": "Set up Python",
        "uses": ACTIONS["python"],
        "with": version_input(root, start, "python", [".python-version"], "3.12"),
    }
    if lockfile == "uv.lock":
        return [
            {"name": "Install uv", "uses": ACTIONS["uv"]},
            {"name": "Install dependencies", **location, "run": "uv sync --frozen"},
        ], "uv run bitfab-replay"
    if lockfile == "poetry.lock":
        return [
            python,
            {
                "name": "Install dependencies",
                **location,
                "run": "pipx install poetry && poetry install --no-interaction",
            },
        ], "poetry run bitfab-replay"
    install = (
        "pip install -r requirements.txt"
        if lockfile == "requirements.txt"
        else "pip install ."
    )
    return [
        python,
        {"name": "Install dependencies", **location, "run": install},
    ], "bitfab-replay"


def ruby_project(root, start):
    directory, _ = find_upward(root, start, ["Gemfile"])
    location = step_directory(root, directory or start)
    return [
        {
            "name": "Set up Ruby",
            "uses": ACTIONS["ruby"],
            "with": {"bundler-cache": True, **location},
        }
    ], "bundle exec bitfab-replay"


def go_project(root, start):
    directory, name = find_upward(root, start, ["go.mod"])
    version = (
        {"go-version-file": (directory / name).relative_to(root).as_posix()}
        if name
        else {"go-version": "stable"}
    )
    return [{"name": "Set up Go", "uses": ACTIONS["go"], "with": version}], None


PROJECTS = {
    "typescript": typescript_project,
    "python": python_project,
    "ruby": ruby_project,
    "go": go_project,
}


def push_triggers(root, workflow):
    found = []
    directory = root / WORKFLOW_DIRECTORY
    for path in sorted(directory.glob("*.y*ml")) if directory.is_dir() else []:
        text = path.read_text(errors="replace")
        if path.name != workflow and PUSH_TRIGGER.search(text) and PREFIX not in text:
            found.append(path.relative_to(root).as_posix())
    for path in (root / "vercel.json", Path.cwd() / "vercel.json"):
        relative = path.resolve().relative_to(root).as_posix()
        if (
            path.is_file()
            and PREFIX not in path.read_text(errors="replace")
            and relative not in found
        ):
            found.append(relative)
    return found


def reference(kind, name):
    return "${{ " + kind + "." + name + " }}"


def secret_env(names, prefix):
    return {name: reference("secrets", prefix + name) for name in names}


def replay_step(run, directory, env):
    return {
        "name": "Replay",
        **({} if directory == "." else {"working-directory": directory}),
        "run": f"{run} --cloud-execute",
        "env": env,
    }


def workflow_document(steps, replay, *, runs_on="ubuntu-24.04", environment=None):
    job = {"runs-on": runs_on}
    if environment:
        job["environment"] = environment
    job["steps"] = [
        {
            "name": "Check out the replay snapshot",
            "uses": ACTIONS["checkout"],
            "with": {"persist-credentials": False},
        },
        *steps,
        replay,
    ]
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


def initialize(argv):
    parser = argparse.ArgumentParser(
        prog="bitfab-replay --cloud-init",
        description="Write .github/workflows/bitfab-replay.yml, the only file cloud replay keeps in the repository. It detects the runtime, package manager, and install command from the directory you run it in. Run it again to update a setup made by an older SDK; a current setup is left alone, so edit the workflow directly to change it.",
    )
    parser.add_argument(
        "--secret",
        action="append",
        default=[],
        metavar="NAME",
        help="Environment variable the replay reads from a GitHub secret, such as OPENAI_API_KEY; repeatable. BITFAB_API_KEY is always included",
    )
    parser.add_argument(
        "--secret-prefix",
        default=DEFAULT_SECRET_PREFIX,
        help=f"Prefix of the GitHub secret each value is read from (default {DEFAULT_SECRET_PREFIX}, so OPENAI_API_KEY reads {DEFAULT_SECRET_PREFIX}OPENAI_API_KEY); pass an empty string to reuse secrets under their own names",
    )
    parser.add_argument(
        "--environment",
        help="GitHub Environment that holds the secrets, such as bitfab-replay; add required reviewers to it so no run gets credentials until someone approves",
    )
    parser.add_argument(
        "--run",
        help='Command that starts the SDK\'s bitfab-replay on the runner, such as "pnpm exec bitfab-replay"; detected for TypeScript, Python, and Ruby, required for Go (such as "go run ./cmd/registry")',
    )
    parser.add_argument("--runs-on", default="ubuntu-24.04", help="Runner label")
    parser.add_argument(
        "--check",
        help='Command the runner also runs during a cloud dry run, such as "node scripts/checkBucket.js"',
    )
    args = parser.parse_args(argv)
    root = root_directory()
    if within(root, OLD_CONFIG).exists():
        return migrate(root, args)
    existing = find_workflow(root)
    if existing is not None:
        return {
            "files": [],
            "updated": False,
            "workflow": f"{WORKFLOW_DIRECTORY}/{existing}",
            "next": "Already set up. Edit the workflow directly to change its install steps, secrets, runner, or Environment",
        }
    prefix = args.secret_prefix
    if prefix and not re.fullmatch(r"[A-Z][A-Z0-9_]*_", prefix):
        raise ValueError(
            "--secret-prefix must be uppercase and end with an underscore, such as BITFAB_CLOUD_"
        )
    names = list(dict.fromkeys(["BITFAB_API_KEY", *args.secret]))
    if not all(re.fullmatch(r"[A-Z_][A-Z0-9_]*", name) for name in names):
        raise ValueError("Secret names must be uppercase environment variable names")
    start = Path.cwd().resolve()
    directory = working_directory(root)
    language = os.environ.get(SDK_LANGUAGE_ENV) or detect_language(root, start)
    steps, run = PROJECTS[language](root, start)
    run = args.run or run
    if not run:
        raise ValueError(
            'Pass --run with the command that starts your registry program on the runner, such as --run "go run ./cmd/registry"'
        )
    env = secret_env(names, prefix)
    if args.check:
        env[CHECK_COMMAND_ENV] = args.check
    workflow = workflow_document(
        steps,
        replay_step(run, directory, env),
        runs_on=args.runs_on,
        environment=args.environment,
    )
    path = f"{WORKFLOW_DIRECTORY}/{WORKFLOW}"
    target = within(root, path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as file:
        file.write(to_yaml(workflow))
    return {
        "files": [path],
        "detected": {
            "language": language,
            "install": [step.get("run") or step["uses"] for step in steps],
            "replay": replay_step(run, directory, env)["run"],
        },
        "secrets": sorted(prefix + name for name in names),
        "environment": args.environment,
        "reviewPushTriggers": push_triggers(root, WORKFLOW),
        "next": "Review the install steps against your CI and add anything detection missed, such as a build or code-generation step or a monorepo install filter; if you normally run replay through a wrapper script that sets environment variables, add them to the Replay step's env. Then create the secrets, exclude bitfab-replay/** from the push-triggered CI and deployments listed, merge the workflow to the default branch once so GitHub registers it, then run your replay with --cloud --dry-run",
    }


def replace_file(target, content):
    with tempfile.NamedTemporaryFile(mode="w", dir=target.parent, delete=False) as file:
        file.write(content)
        temporary = file.name
    os.replace(temporary, target)


def remove_old_files(root):
    removed = []
    for old in (OLD_CONFIG, OLD_SCRIPT):
        if within(root, old).exists():
            within(root, old).unlink()
            removed.append(old)
    return removed


def migrate(root, args):
    config = json.loads(within(root, OLD_CONFIG).read_text())
    name = config.get("workflow", WORKFLOW)
    path = f"{WORKFLOW_DIRECTORY}/{name}"
    text = within(root, path).read_text() if within(root, path).is_file() else ""
    try:
        workflow = json.loads(text)
        job = workflow["jobs"]["replay"]
        steps = job["steps"]
    except (ValueError, KeyError, TypeError) as error:
        if "--cloud-execute" in text and not any(
            old in text for old in (OLD_SCRIPT, OLD_UPLOAD_STEP, *OLD_RUNNER_ENV)
        ):
            if config.get("checkCommand") and CHECK_COMMAND_ENV not in text:
                raise ValueError(
                    f"{OLD_CONFIG} has a checkCommand the workflow lacks. Add {CHECK_COMMAND_ENV}: {shlex.join(config['checkCommand'])} to the Replay step's env, then run --cloud-init again"
                ) from error
            return {
                "files": [],
                "removed": remove_old_files(root),
                "updated": True,
                **dropped_command(config),
                "next": "The workflow already holds these settings, so only the old files were removed",
            }
        raise ValueError(
            f"{path} is not the workflow --cloud-init generated. Make its Replay step run the SDK's bitfab-replay command with --cloud-execute and read each secret in its env, then delete {OLD_CONFIG}"
        ) from error
    replay = [
        index
        for index, step in enumerate(steps)
        if isinstance(step, dict) and step.get("name") == "Replay"
    ]
    if len(replay) != 1:
        raise ValueError(f"{path} must contain exactly one step named Replay")
    step = steps[replay[0]]
    words = shlex.split(step.get("run") or "")
    if args.run:
        run = args.run
    elif words[-1:] == ["--cloud-execute"] and OLD_SCRIPT not in step.get("run", ""):
        run = shlex.join(words[:-1])
    elif config.get("cliCommand"):
        run = shlex.join(config["cliCommand"])
    else:
        raise ValueError(
            'The Replay step does not start the SDK\'s bitfab-replay command; pass --run with the command that does, such as --run "npx --no-install bitfab-replay"'
        )
    prefix = config.get("secretPrefix", "")
    env = {
        **secret_env(config.get("secrets", []), prefix),
        **{
            key: value
            for key, value in (step.get("env") or {}).items()
            if key not in OLD_RUNNER_ENV
        },
    }
    for variable, source in (config.get("env") or {}).items():
        kind, target = next(iter(source.items()))
        env.setdefault(
            variable, reference("secrets" if kind == "secret" else "vars", target)
        )
    if config.get("checkCommand"):
        env[CHECK_COMMAND_ENV] = shlex.join(config["checkCommand"])
    kept = {
        key: value
        for key, value in step.items()
        if key not in ("name", "uses", "with", "run", "working-directory", "env")
    }
    job["steps"] = [
        {**replay_step(run, config.get("workingDirectory", "."), env), **kept}
        if index == replay[0]
        else entry
        for index, entry in enumerate(steps)
        if not (
            isinstance(entry, dict)
            and entry.get("name") == OLD_UPLOAD_STEP
            and str(entry.get("uses", "")).startswith("actions/upload-artifact@")
        )
    ]
    if job.get("timeout-minutes") == OLD_JOB_TIMEOUT:
        del job["timeout-minutes"]
    replace_file(within(root, path), to_yaml(workflow))
    result = {
        "files": [path],
        "removed": remove_old_files(root),
        "updated": True,
        "next": "Review the diff. Replays run the workflow in their own snapshot, so the change applies without merging",
    }
    return {**result, **dropped_command(config)}


def dropped_command(config):
    if not config.get("command"):
        return {}
    return {
        "droppedCommand": config["command"],
        "droppedCommandNext": "The runner now starts the same bitfab-replay you run locally, from the same directory and with the same arguments, so this command is no longer used. "
        "If it also set environment variables, add them to the Replay step's env",
    }


def main():
    argv = sys.argv[1:]
    try:
        if argv[:1] == ["--cloud-init"]:
            result = initialize(argv[1:])
        elif argv[:1] == ["--cloud-secrets"]:
            result = configure_secrets(argv[1:])
        elif argv == ["--cloud-execute"]:
            result = execute()
        elif "-h" in argv or "--help" in argv:
            print(HELP, end="")
            return 0
        else:
            result = run_cli(argv)
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
