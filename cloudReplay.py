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
import zlib
from pathlib import Path
from urllib.parse import urlencode

WORKFLOW = "bitfab-replay.yml"
WORKFLOW_NAMES = (WORKFLOW, "bitfab-replay.yaml")
WORKFLOW_DIRECTORY = ".github/workflows"
OLD_CONFIG = ".bitfab/cloud.json"
PREFIX = "bitfab-replay/"
DEFAULT_SECRET_PREFIX = "BITFAB_CLOUD_"
API_VERSION = "2026-03-10"
REPLAY_COMMAND_ENV = "BITFAB_REPLAY_COMMAND"
SDK_LANGUAGE_ENV = "BITFAB_SDK_LANGUAGE"
CHECK_COMMAND_ENV = "BITFAB_REPLAY_CHECK"
REQUEST_VERSION = 3
RESULT_LINE = "bitfab-replay-result "
RESULT_CHUNK = 4000
OUTPUT_BEGIN = "bitfab-replay-output-begin"
OUTPUT_END = "bitfab-replay-output-end"
LOG_TAIL_LINES = 40
OUTPUT_LIMIT = 16 * 1024 * 1024
NEW_FILES_LIMIT = 20 * 1024 * 1024
POLL_SECONDS = 5
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
UUID = re.compile(r"^[0-9a-f]{8}(?:-[0-9a-f]{4}){3}-[0-9a-f]{12}$")
SHA = re.compile(r"^[0-9a-f]{40}$")
GITHUB_REMOTE = re.compile(
    r"(?:git@github\.com:|https://github\.com/)([\w.-]+/[\w.-]+?)(?:\.git)?"
)
LOG_TIMESTAMP = re.compile(r"^﻿?\d{4}-\d\d-\d\dT[\d:.]+Z ?")
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
DECLARED_INPUTS = re.compile(r"^[ \t]*\"?execution_id\"?[ \t]*:", re.MULTILINE)
FOLLOW_FLAGS = (
    "--cloud-status",
    "--cloud-watch",
    "--cloud-cancel",
    "--cloud-cleanup",
)
CLOUD_VALUE_FLAGS = (*FOLLOW_FLAGS, "--cloud-request-id", "--cloud-timeout")
CLOUD_SWITCHES = ("--cloud", "--cloud-preview", "--cloud-detach")
RENAMED_FLAGS = {
    "--cloud-dry-run": "--cloud-preview",
    "--cloud-check": "--dry-run",
    "--cloud-include": "nothing: new files that git does not ignore are already in the snapshot",
}
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

Every replay option works as it does locally, including --dry-run and --fail-on-error.
The runner replays a snapshot of your working tree (changed files, plus new files git
does not ignore) from the same directory, and this command prints the replay's output
and exits with its exit code. Credential-like files are refused. Requires git and
Python 3.10+ on macOS or Linux, with a github.com origin.

Options added by --cloud:
  --cloud-preview        List what the snapshot would contain and push nothing.
  --cloud-detach         Return after dispatch instead of waiting.
  --cloud-timeout MIN    Stop the replay after MIN minutes (1..7200).
  --cloud-request-id ID  Recover a submission whose response was lost.

Follow a replay by the execution UUID it prints:
  --cloud-watch ID | --cloud-status ID | --cloud-cancel ID | --cloud-cleanup ID

Set up once:
  --cloud-init [--secret NAME ...] [--environment NAME] [--run COMMAND]
               [--runs-on LABEL] [--secret-prefix PREFIX] [--check COMMAND]
      Writes .github/workflows/bitfab-replay.yml, the only file cloud replay keeps
      in the repository, and lists what is left to do.
  --cloud-secrets --env-file FILE [NAME ...] [--environment NAME] [--dry-run]
      Copies local values into the GitHub secrets the workflow reads.

GitHub access comes from the GitHub CLI when it is logged in, otherwise from GH_TOKEN
or GITHUB_TOKEN, otherwise from the github.com credential git already stores. Without
the GitHub CLI, --cloud-secrets lists the secrets to create by hand and where.
"""


class CommandError(RuntimeError):
    def __init__(self, message, http_status=None):
        super().__init__(message)
        self.http_status = http_status


class KeepRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, *args, **kwargs):
        return None


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


def progress(message):
    print(f"[cloud] {message}", file=sys.stderr, flush=True)


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
        return Path(path).resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError(f"{path} must be inside the repository") from error


def working_directory(root):
    try:
        relative = Path.cwd().resolve().relative_to(root).as_posix()
    except ValueError as error:
        raise ValueError("Run cloud replay from inside the repository") from error
    return relative or "."


def repository(root):
    fetch = GITHUB_REMOTE.fullmatch(git(root, "remote", "get-url", "origin"))
    if fetch is None:
        raise ValueError(
            "origin must be a github.com repository without embedded credentials"
        )
    push = GITHUB_REMOTE.fullmatch(git(root, "remote", "get-url", "--push", "origin"))
    if push is None or push[1].lower() != fetch[1].lower():
        raise ValueError("origin fetch and push repositories must match")
    return fetch[1]


def find_workflow(root):
    for name in WORKFLOW_NAMES:
        if within(root, f"{WORKFLOW_DIRECTORY}/{name}").is_file():
            return name
    return None


def refuse_old_setup(root):
    if within(root, OLD_CONFIG).exists():
        raise ValueError(
            f"This cloud replay setup was made by an older SDK. Delete {OLD_CONFIG} and {WORKFLOW_DIRECTORY}/{WORKFLOW}, then run bitfab-replay --cloud-init"
        )


def setup_workflow(root):
    refuse_old_setup(root)
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


def secrets_page(repo, environment):
    if environment:
        return f"https://github.com/{repo}/settings/environments"
    return f"https://github.com/{repo}/settings/secrets/actions"


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


def ensure_environment(repo, environment):
    try:
        api(repo, f"environments/{environment}")
        return "exists"
    except ValueError:
        return "unchecked"
    except RuntimeError as error:
        if getattr(error, "http_status", None) != 404:
            return "unchecked"
    try:
        api(repo, f"environments/{environment}", method="PUT", payload={})
        return "created"
    except RuntimeError:
        return "missing"


def configure_secrets(argv):
    parser = argparse.ArgumentParser(
        prog="bitfab-replay --cloud-secrets",
        description="Copy named values from local environment files into the GitHub Actions secrets the cloud replay workflow reads. Values are piped to the GitHub CLI on standard input and never printed, logged, or passed as arguments. Without the GitHub CLI, it lists the secrets to create by hand and where.",
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
        help="GitHub Environment to set the secrets in; defaults to the workflow's environment, and is created when it does not exist",
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Report which names were found without writing anything to GitHub",
    )
    args = parser.parse_args(argv)
    root = root_directory()
    text = within(root, f"{WORKFLOW_DIRECTORY}/{setup_workflow(root)}").read_text()
    targets = workflow_secrets(text)
    names = args.names or list(targets)
    unknown = [name for name in names if name not in targets]
    if unknown:
        raise ValueError(
            f"The workflow does not read {', '.join(unknown)}. Add each one to the replay step's env as NAME: ${{{{ secrets.{DEFAULT_SECRET_PREFIX}NAME }}}} first"
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
            missing.append(targets[name])
        elif not value:
            empty.append(targets[name])
        else:
            found.append(name)
    result = {
        "repository": repo,
        "environment": environment,
        "dryRun": args.dry_run,
        "set": [],
        "missing": missing,
        "empty": empty,
    }
    if args.dry_run:
        return {**result, "wouldSet": [targets[name] for name in found]}
    if environment:
        result["environmentStatus"] = ensure_environment(repo, environment)
    if found and shutil.which("gh") is None:
        progress(
            "Setting secrets needs the GitHub CLI (https://cli.github.com), which encrypts each value for GitHub; create the ones listed under createByHand instead"
        )
        return {
            **result,
            "createByHand": [targets[name] for name in found],
            "where": secrets_page(repo, environment),
        }
    token = github_access()["token"]
    for name in found:
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
            env={**os.environ, "GH_TOKEN": token},
        )
    result["set"] = [targets[name] for name in found]
    if missing or empty:
        result["where"] = secrets_page(repo, environment)
    return result


@functools.cache
def github_access():
    if shutil.which("gh") is not None:
        with contextlib.suppress(RuntimeError, OSError, subprocess.SubprocessError):
            token = command(["gh", "auth", "token", "--hostname", "github.com"])
            if token:
                return {"source": "the GitHub CLI", "token": token}
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


def api(repo, suffix, *, method="GET", payload=None, raw=False):
    path = f"repos/{repo}" + (f"/{suffix}" if suffix else "")
    access = github_access()
    data = None if payload is None else json.dumps(payload)
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
        body = fetch(request, follow_without_credentials=raw)
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
                "or set SSL_CERT_FILE to a certificate bundle"
            ) from None
        raise CommandError(f"Could not reach api.github.com: {reason}") from None
    if raw:
        return body.decode(errors="replace").strip()
    return json.loads(body) if body.strip() else None


def fetch(request, *, follow_without_credentials):
    if not follow_without_credentials:
        with urllib.request.urlopen(request, timeout=60) as response:
            return response.read()
    try:
        with urllib.request.build_opener(KeepRedirect).open(
            request, timeout=60
        ) as response:
            return response.read()
    except urllib.error.HTTPError as error:
        location = error.headers.get("Location")
        if error.code not in (301, 302, 303, 307, 308) or not location:
            raise
    with urllib.request.urlopen(location, timeout=120) as response:
        return response.read()


def option_name(token):
    return token.split("=", 1)[0]


def split_arguments(argv):
    cloud, switches, replay = {}, set(), []
    index = 0
    while index < len(argv):
        token = argv[index]
        name = option_name(token)
        if name in RENAMED_FLAGS:
            raise ValueError(f"{name} is gone; use {RENAMED_FLAGS[name]}")
        if name in CLOUD_VALUE_FLAGS:
            if "=" in token:
                value = token.split("=", 1)[1]
                index += 1
            elif index + 1 < len(argv):
                value = argv[index + 1]
                index += 2
            else:
                raise ValueError(f"{name} needs a value")
            if name in cloud:
                raise ValueError(f"{name} was given twice")
            cloud[name] = value
        elif token in CLOUD_SWITCHES:
            switches.add(token)
            index += 1
        elif name in SEED_FLAGS:
            raise ValueError(f"{name} seeds traces; run it locally, without --cloud")
        else:
            replay.append(token)
            index += 1
    return cloud, switches, replay


def parse(argv):
    cloud, switches, replay = split_arguments(argv)
    following = [flag for flag in FOLLOW_FLAGS if flag in cloud]
    if following:
        if len(following) != 1 or len(cloud) != 1 or switches - {"--cloud"} or replay:
            raise ValueError("Cloud follow-up commands take only their execution UUID")
        operation = following[0].removeprefix("--cloud-")
        execution_id = cloud[following[0]]
    else:
        if not replay:
            raise ValueError(
                "Add --cloud to the replay command you run locally, such as bitfab-replay --registry scripts/replay.ts classify --trace-ids UUID --cloud"
            )
        if not any(option_name(token) in SELECTION_FLAGS for token in replay):
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


def dry_run(args):
    return "--dry-run" in args


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


def snapshot(root, workflow, files, request, *, preview=False):
    if git(root, "ls-files", "-u"):
        raise ValueError("Resolve merge conflicts before snapshotting")
    head = git(root, "rev-parse", "HEAD")
    with tempfile.TemporaryDirectory(prefix="bitfab-cloud-index-") as directory:
        env = {**os.environ, "GIT_INDEX_FILE": str(Path(directory) / "index")}
        git(root, "read-tree", head, env=env)
        git(root, "add", "-A", "--", ".", env=env)
        entries = git(root, "ls-files", "--stage", "-z", env=env).split("\x00")
        tracked = set()
        for entry in filter(None, entries):
            metadata, path = entry.split("\t", 1)
            tracked.add(path)
            if metadata.startswith("160000"):
                raise ValueError(
                    f"{path} is a submodule or nested repository, which cloud snapshots do not support"
                )
            if sensitive(path):
                raise ValueError(
                    f"Refusing credential-like file {path}; add it to .gitignore, or git rm --cached it when it is tracked"
                )
        ignored = [
            path
            for path in [f"{WORKFLOW_DIRECTORY}/{workflow}", *files]
            if path not in tracked
        ]
        if ignored:
            raise ValueError(
                "Git ignores "
                + ", ".join(ignored)
                + ", so the snapshot cannot include it; stop ignoring it or move it"
            )
        tree = git(root, "write-tree", env=env)
        changes = [
            line.split("\t", 1)
            for line in git(
                root, "diff", "--name-status", "--no-renames", head, tree
            ).splitlines()
        ]
        new_files = sorted(path for status, path in changes if status == "A")
        changed_files = sorted(path for status, path in changes if status != "A")
        sizes = {path: (root / path).lstat().st_size for path in new_files}
        size = sum(sizes.values())
        if size > NEW_FILES_LIMIT:
            largest = sorted(new_files, key=lambda path: -sizes[path])
            raise ValueError(
                f"New files add {size // (1024 * 1024)} MiB to the snapshot, more than {NEW_FILES_LIMIT // (1024 * 1024)} MiB; add large outputs to .gitignore, such as "
                + ", ".join(largest[:5])
            )
        summary = {
            "baseSha": head,
            "changedFiles": changed_files,
            "newFiles": new_files,
        }
        if preview:
            return summary
        sha = git(
            root,
            "commit-tree",
            tree,
            "-p",
            head,
            input=f"Bitfab replay {request['id']}\n\n{json.dumps(request)}\n",
        )
        return {**summary, "sha": sha}


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
        if run["head_sha"] == record["sha"] and run["head_branch"] == record["branch"]
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
    record.update(
        runId=run["id"],
        url=run["html_url"],
        state=run["status"],
        conclusion=run.get("conclusion"),
    )
    if fetch_result and record["state"] == "completed" and "exitCode" not in record:
        outcome(record)
    return record


def replay_job(record):
    jobs = api(
        record["repository"], f"actions/runs/{record['runId']}/jobs?per_page=100"
    )
    jobs = (jobs or {}).get("jobs", [])
    named = [job for job in jobs if job.get("name") == "replay"]
    return (named or jobs or [None])[0]


def job_log(record):
    job = replay_job(record)
    if job is None:
        return []
    for attempt in range(6):
        try:
            text = api(record["repository"], f"actions/jobs/{job['id']}/logs", raw=True)
            return [LOG_TIMESTAMP.sub("", line) for line in text.splitlines()]
        except RuntimeError:
            if attempt == 5:
                raise
            time.sleep(POLL_SECONDS)
    return []


def read_result(lines):
    chunks = [
        line[len(RESULT_LINE) :] for line in lines if line.startswith(RESULT_LINE)
    ]
    if not chunks:
        return None
    try:
        return json.loads(
            zlib.decompress(base64.b64decode("".join(chunks), validate=True))
        )
    except ValueError as error:
        raise ValueError("The runner left an unreadable replay result") from error


def replay_output(lines):
    if OUTPUT_BEGIN not in lines:
        return None
    start = lines.index(OUTPUT_BEGIN) + 1
    end = lines.index(OUTPUT_END, start) if OUTPUT_END in lines[start:] else len(lines)
    return lines[start:end]


def outcome(record):
    lines = job_log(record)
    result = read_result(lines)
    if result is None:
        record["exitCode"] = 1
        return {"record": record, "lines": lines, "result": None}
    if (
        result.get("executionId") != record["id"]
        or result.get("commitSha") != record["sha"]
    ):
        raise ValueError("Replay result does not match this execution")
    record["exitCode"] = result["exitCode"]
    parsed = last_json(result.get("stdout", "")) or {}
    test_run = result.get("testRunId") or parsed.get(
        "testRunId", parsed.get("test_run_id")
    )
    if isinstance(test_run, str) and UUID.fullmatch(test_run):
        record["testRunId"] = test_run
    if result.get("stoppedEarly"):
        record["stoppedEarly"] = result["stoppedEarly"]
    return {"record": record, "lines": lines, "result": result}


def print_outcome(found):
    record, lines, result = found["record"], found["lines"], found["result"]
    if result is None:
        errors = [
            index for index, line in enumerate(lines) if line.startswith("##[error]")
        ]
        shown = lines[: errors[0] + 1] if errors else lines
        tail = [line for line in shown if line.strip()][-LOG_TAIL_LINES:]
        for line in tail:
            print(line, file=sys.stderr)
        progress(
            f"The run stopped before the replay finished ({record.get('conclusion')}); the end of its log is above. Full log: {record.get('url')}"
        )
        return 1
    for line in replay_output(lines) or []:
        print(line, file=sys.stderr)
    sys.stderr.flush()
    print(result.get("stdout", ""), end="", flush=True)
    if record.get("stoppedEarly"):
        progress(
            f"Replay stopped early: {record['stoppedEarly']}. Traces that finished are saved"
            + (
                f" in experiment {record['testRunId']}"
                if record.get("testRunId")
                else ""
            )
        )
        return result["exitCode"] or 1
    return result["exitCode"]


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


def describe_snapshot(source):
    changed, new = len(source["changedFiles"]), len(source["newFiles"])
    progress(
        f"Snapshot of {source['baseSha'][:12]} with {changed} changed and {new} new files"
        + (
            f" ({', '.join(source['newFiles'][:5])}{', ...' if new > 5 else ''})"
            if new
            else ""
        )
    )


def submit(root, repo, workflow, parsed, path):
    execution_id = parsed["id"]
    request, files = replay_request(root, parsed)
    if "--cloud-preview" in parsed["switches"]:
        return {
            "preview": True,
            "repository": repo,
            **snapshot(root, workflow, files, request, preview=True),
        }
    if path.exists():
        record = json.loads(path.read_text())
        if record["request"] != request or record["repository"].lower() != repo.lower():
            raise ValueError("Execution ID already belongs to a different request")
        status(record, fetch_result=False)
        if record["state"] == "prepared":
            raise ValueError(
                "Submission stopped before dispatch. Use --cloud-cleanup, then submit a new execution UUID"
            )
        return record
    preflight(repo, workflow)
    if dry_run(request["args"]):
        progress(
            "--dry-run dispatches a GitHub run that checks secrets and resolves the traces without replaying; it receives the workflow's secrets"
        )
    source = snapshot(root, workflow, files, request)
    describe_snapshot(source)
    record = {
        "id": execution_id,
        "repository": repo,
        "workflow": workflow,
        "branch": PREFIX + execution_id,
        "request": request,
        "state": "prepared",
        **source,
    }
    save(path, record)
    progress(f"Execution {execution_id}; resume with --cloud-watch {execution_id}")
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
    payload = {"ref": record["branch"]}
    if DECLARED_INPUTS.search(
        within(root, f"{WORKFLOW_DIRECTORY}/{workflow}").read_text()
    ):
        payload["inputs"] = {"execution_id": execution_id, "request": "-"}
    try:
        response = api(
            repo,
            f"actions/workflows/{workflow}/dispatches",
            method="POST",
            payload=payload,
        )
    except RuntimeError as error:
        raise ValueError(
            f"Dispatching {workflow} on {record['branch']} failed{http_status(error)}; the GitHub account needs write access to Actions, and the registered workflow must accept workflow_dispatch. Check the Actions tab, then resume with --cloud-watch {execution_id}"
        ) from error
    if isinstance(response, dict) and response.get("workflow_run_id"):
        record["runId"] = response["workflow_run_id"]
        record["url"] = response.get("html_url")
    record["state"] = "queued"
    return record


def report_steps(record, shown):
    job = replay_job(record)
    for step in (job or {}).get("steps", []):
        if step.get("status") != "queued" and step.get("name") not in shown:
            shown.add(step.get("name"))
            progress(step.get("name"))


def wait(record, path):
    started = time.monotonic()
    announced, steps = None, set()
    while record["state"] != "completed":
        status(record, fetch_result=False)
        save(path, record)
        if record["state"] != announced and record.get("url"):
            announced = record["state"]
            progress(f"{announced.replace('_', ' ').capitalize()}: {record['url']}")
        if record["state"] == "in_progress":
            report_steps(record, steps)
        if (
            record["state"] == "dispatch_unknown"
            and time.monotonic() - started >= 10 * 60
        ):
            raise ValueError(
                f"GitHub has shown no run for this dispatch after 10 minutes. Check Actions, then resume with --cloud-watch {record['id']} or remove the snapshot branch with --cloud-cleanup {record['id']}"
            )
        if record["state"] != "completed":
            time.sleep(POLL_SECONDS)


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
            record = submit(root, repo, workflow, parsed, path)
            if record.get("preview"):
                return record, None
        else:
            if not path.exists():
                raise ValueError(
                    "No local execution record for this UUID in this worktree"
                )
            record = json.loads(path.read_text())
            if record["repository"].lower() != repo.lower():
                raise ValueError("origin no longer matches the execution repository")
            status(record, fetch_result=operation == "status")
            if operation == "cancel" and record["state"] != "completed":
                if not record.get("runId"):
                    raise ValueError(
                        "Run not yet found; inspect Actions and retry status before cancelling"
                    )
                api(repo, f"actions/runs/{record['runId']}/cancel", method="POST")
                record["state"] = "cancel_requested"
        save(path, record)
        attached = operation == "watch" or (
            operation == "submit" and "--cloud-detach" not in parsed["switches"]
        )
        found = None
        if attached and record["state"] != "prepared":
            wait(record, path)
            found = outcome(record)
        if record["state"] == "completed" or (
            operation == "cleanup" and record["state"] == "prepared"
        ):
            cleanup(root, record)
        elif operation == "cleanup":
            raise ValueError("Execution is not completed; no branch was deleted")
        save(path, record)
        return record, found


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
            "Start the replay step with the SDK's bitfab-replay command and --cloud-execute, so the runner knows how to start the replay"
        )
    return value


def snapshot_request(root, commit):
    message = git(root, "log", "-1", "--format=%B", commit)
    _, _, body = message.partition("\n\n")
    try:
        request = json.loads(body)
    except ValueError:
        request = None
    if not isinstance(request, dict) or request.get("version") != REQUEST_VERSION:
        version = request.get("version", 0) if isinstance(request, dict) else 0
        newer = isinstance(version, int) and version > REQUEST_VERSION
        raise ValueError(
            "This replay was submitted by a "
            + ("newer" if newer else "different")
            + " SDK than the one the snapshot installs; match the SDK you run locally to the one in the lockfile"
        )
    if os.environ.get("GITHUB_REF_NAME") != PREFIX + str(request.get("id")):
        raise ValueError(
            "The runner replays only the snapshot branch its request names; start replays with bitfab-replay --cloud"
        )
    validate_arguments(request["args"])
    return request


def execute():
    if os.environ.get("GITHUB_RUN_ATTEMPT") != "1":
        raise ValueError("Submit a new replay instead of rerunning an Actions job")
    root = root_directory()
    commit = os.environ["GITHUB_SHA"]
    if git(root, "rev-parse", "HEAD") != commit:
        raise ValueError("Runner checkout does not match the dispatched snapshot SHA")
    request = snapshot_request(root, commit)
    directory = within(root, request["cwd"])
    check_secrets(root)
    names = {option_name(value) for value in request["args"]}
    args = [
        *replay_command(),
        *request["args"],
        *([] if names & set(CODE_CHANGE_FLAGS) else ["--no-code-change"]),
    ]
    if dry_run(request["args"]):
        run_check_command(directory)
    experiment = {}
    previous = signal.signal(signal.SIGTERM, raise_interrupt)
    print(OUTPUT_BEGIN, flush=True)
    try:
        code, stdout, stopped = run_command(
            directory, args, request.get("timeoutMinutes"), experiment
        )
    finally:
        print(OUTPUT_END, flush=True)
        signal.signal(signal.SIGTERM, previous)
    result = {
        "executionId": request["id"],
        "commitSha": commit,
        "exitCode": code,
        "stdout": stdout,
    }
    if stopped:
        result["stoppedEarly"] = stopped
        if UUID.fullmatch(experiment.get("id", "")):
            result["testRunId"] = experiment["id"]
    write_result(result)
    return result


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


def run_check_command(directory):
    check = os.environ.get(CHECK_COMMAND_ENV, "").strip()
    if not check:
        return
    print(f"Running {CHECK_COMMAND_ENV}: {check}", flush=True)
    code = subprocess.run(
        shlex.split(check), cwd=directory, stdin=subprocess.DEVNULL, check=False
    ).returncode
    if code:
        raise ValueError(f"{CHECK_COMMAND_ENV} exited {code}; its output is above")


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


def item_errored(item):
    return isinstance(item, dict) and any(
        item.get(field) is not None for field in ITEM_ERROR_FIELDS
    )


def carried_over(item):
    return isinstance(item, dict) and (
        item.get("carriedOver") is True or item.get("carried_over") is True
    )


def raise_interrupt(signum, frame):
    raise KeyboardInterrupt


def forward_stderr(stream, experiment):
    for line in iter(stream.readline, b""):
        sys.stdout.write(line.decode(errors="replace"))
        sys.stdout.flush()
        if "id" not in experiment:
            match = EXPERIMENT_LINE.match(line)
            if match:
                experiment["id"] = match[1].decode()


def last_json(text):
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
    return result


def write_result(result):
    encoded = base64.b64encode(zlib.compress(json.dumps(result).encode())).decode()
    for start in range(0, len(encoded), RESULT_CHUNK):
        print(RESULT_LINE + encoded[start : start + RESULT_CHUNK])
    sys.stdout.flush()
    summary = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary:
        with Path(summary).open("a") as file:
            file.write(summary_markdown(result))


def summary_markdown(result):
    parsed = last_json(result["stdout"]) or {}
    test_run = result.get("testRunId") or parsed.get(
        "testRunId", parsed.get("test_run_id")
    )
    items = parsed.get("items") if isinstance(parsed.get("items"), list) else []
    replayed = [item for item in items if not carried_over(item)]
    errors = item_errors(replayed)
    lines = [f"Commit: `{result['commitSha']}`", f"Exit code: {result['exitCode']}"]
    if test_run:
        lines.insert(0, f"Test run: `{test_run}`")
    if result.get("stoppedEarly"):
        lines.append(f"Stopped early: {result['stoppedEarly']}")
    elif items:
        lines.append(f"Replayed: {len(replayed)}, errored: {len(errors)}")
    text = "### Bitfab replay\n\n" + "\n\n".join(lines) + "\n"
    if errors:
        shown = [
            f"trace {trace}: {error}" for trace, error in errors[:ITEM_ERROR_LINES]
        ]
        if len(errors) > len(shown):
            shown.append(f"and {len(errors) - len(shown)} more errored items")
        text += "\n#### Errored items\n\n" + "".join(
            f"- `{line.replace('`', chr(39))}`\n" for line in shown
        )
    return text


def run_command(directory, args, timeout, experiment):
    stopped = None
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
                    if os.fstat(output.fileno()).st_size > OUTPUT_LIMIT:
                        raise ValueError("Replay output exceeded 16 MiB")
                    if deadline is not None and time.monotonic() >= deadline:
                        stopped = f"it reached the {timeout}-minute --cloud-timeout"
                        stop(child)
                        break
                    time.sleep(0.1)
            except KeyboardInterrupt:
                stopped = "the GitHub run was cancelled"
                stop(child)
            except BaseException:
                stop(child)
                raise
            finally:
                reader.join(timeout=5)
            code = child.returncode
        if output.tell() > OUTPUT_LIMIT:
            raise ValueError("Replay output exceeded 16 MiB")
        output.seek(0)
        text = output.read().decode(errors="replace")
    return (code if code >= 0 else 1), text, stopped


def stop(child):
    with contextlib.suppress(ProcessLookupError):
        os.killpg(child.pid, signal.SIGTERM)
    with contextlib.suppress(subprocess.TimeoutExpired):
        child.wait(timeout=30)
    with contextlib.suppress(ProcessLookupError):
        os.killpg(child.pid, signal.SIGKILL)
    child.wait()


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
    steps = []
    if manager == "pnpm":
        steps.append({"uses": ACTIONS["pnpm"]})
    if manager == "bun":
        steps.append({"uses": ACTIONS["bun"]})
    steps += [
        {
            "uses": ACTIONS["node"],
            "with": {
                **version_input(
                    root, start, "node", [".nvmrc", ".node-version"], "lts/*"
                ),
                **(
                    {"cache": manager}
                    if manager in ("pnpm", "yarn", "npm") and lockfile
                    else {}
                ),
            },
        },
        {**step_directory(root, directory or start), "run": install},
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
        "uses": ACTIONS["python"],
        "with": version_input(root, start, "python", [".python-version"], "3.12"),
    }
    if lockfile == "uv.lock":
        return [
            {"uses": ACTIONS["uv"]},
            {**location, "run": "uv sync --frozen"},
        ], "uv run bitfab-replay"
    if lockfile == "poetry.lock":
        return [
            python,
            {
                **location,
                "run": "pipx install poetry && poetry install --no-interaction",
            },
        ], "poetry run bitfab-replay"
    install = (
        "pip install -r requirements.txt"
        if lockfile == "requirements.txt"
        else "pip install ."
    )
    return [python, {**location, "run": install}], "bitfab-replay"


def ruby_project(root, start):
    directory, _ = find_upward(root, start, ["Gemfile"])
    location = step_directory(root, directory or start)
    return [
        {"uses": ACTIONS["ruby"], "with": {"bundler-cache": True, **location}}
    ], "bundle exec bitfab-replay"


def go_project(root, start):
    directory, name = find_upward(root, start, ["go.mod"])
    version = (
        {"go-version-file": (directory / name).relative_to(root).as_posix()}
        if name
        else {"go-version": "stable"}
    )
    return [{"uses": ACTIONS["go"], "with": version}], go_registry_command(root, start)


def go_registry_command(root, start):
    programs = set()
    for directory, folders, names in os.walk(root):
        folders[:] = [
            folder
            for folder in folders
            if not folder.startswith(".")
            and folder not in ("node_modules", "vendor", "testdata")
        ]
        for name in names:
            if not name.endswith(".go") or name.endswith("_test.go"):
                continue
            text = (Path(directory) / name).read_text(errors="replace")
            if "RunCloudReplayCLI(" in text and re.search(
                r"^package main\b", text, re.MULTILINE
            ):
                programs.add(Path(directory))
    if len(programs) != 1:
        return None
    relative = os.path.relpath(programs.pop(), start)
    return f"go run {relative if relative.startswith('..') else './' + relative}"


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


def workflow_document(steps, run, directory, env, *, runs_on, environment):
    job = {"runs-on": runs_on}
    if environment:
        job["environment"] = environment
    job["steps"] = [
        {"uses": ACTIONS["checkout"], "with": {"persist-credentials": False}},
        *steps,
        {
            **({} if directory == "." else {"working-directory": directory}),
            "run": f"{run} --cloud-execute",
            "env": env,
        },
    ]
    return {
        "name": "Bitfab replay",
        "on": "workflow_dispatch",
        "permissions": {"contents": "read"},
        "jobs": {"replay": job},
    }


def initialize(argv):
    parser = argparse.ArgumentParser(
        prog="bitfab-replay --cloud-init",
        description="Write .github/workflows/bitfab-replay.yml, the only file cloud replay keeps in the repository, and list what is left to do. It detects the runtime, package manager, install command, and replay command from the directory you run it in. An existing workflow is left alone; edit it directly to change it.",
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
        help='Command that starts the SDK\'s bitfab-replay on the runner, such as "pnpm exec bitfab-replay"; detected for every SDK, including the Go program that calls RunCloudReplayCLI',
    )
    parser.add_argument("--runs-on", default="ubuntu-24.04", help="Runner label")
    parser.add_argument(
        "--check",
        help='Command the runner also runs on a --cloud --dry-run, such as "node scripts/checkBucket.js"',
    )
    args = parser.parse_args(argv)
    root = root_directory()
    refuse_old_setup(root)
    existing = find_workflow(root)
    if existing is not None:
        return {
            "files": [],
            "workflow": f"{WORKFLOW_DIRECTORY}/{existing}",
            "next": [
                "Already set up. Edit the workflow directly to change its install steps, secrets, runner, or Environment"
            ],
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
    env = {name: "${{ secrets." + prefix + name + " }}" for name in names}
    if args.check:
        env[CHECK_COMMAND_ENV] = args.check
    path = f"{WORKFLOW_DIRECTORY}/{WORKFLOW}"
    target = within(root, path)
    target.parent.mkdir(parents=True, exist_ok=True)
    with target.open("x") as file:
        file.write(
            to_yaml(
                workflow_document(
                    steps,
                    run,
                    directory,
                    env,
                    runs_on=args.runs_on,
                    environment=args.environment,
                )
            )
        )
    secrets = sorted(prefix + name for name in names)
    triggers = push_triggers(root, WORKFLOW)
    try:
        where = secrets_page(repository(root), args.environment)
    except (ValueError, RuntimeError):
        where = "the repository's Settings, Secrets and variables, Actions"
    return {
        "files": [path],
        "detected": {
            "language": language,
            "install": [step.get("run") or step["uses"] for step in steps],
            "replay": f"{run} --cloud-execute",
        },
        "secrets": secrets,
        "reviewPushTriggers": triggers,
        "next": [
            "Compare the install steps with your CI and add anything detection missed, such as a build or code-generation step",
            f"Create the secrets {', '.join(secrets)} at {where}, or copy them from a local file with bitfab-replay --cloud-secrets --env-file .env",
            *(
                [
                    f"Keep bitfab-replay/** branches from triggering {', '.join(triggers)}"
                ]
                if triggers
                else []
            ),
            f"Commit {path} and merge it to the default branch once, so GitHub registers it",
            "Add --cloud --dry-run to your replay command to check the runner, then replay with --cloud",
        ],
    }


def print_json(value):
    print(json.dumps(value, indent=2), flush=True)


def main():
    argv = sys.argv[1:]
    try:
        if argv[:1] == ["--cloud-init"]:
            print_json(initialize(argv[1:]))
            return 0
        if argv[:1] == ["--cloud-secrets"]:
            print_json(configure_secrets(argv[1:]))
            return 0
        if argv == ["--cloud-execute"]:
            result = execute()
            return result["exitCode"] or (1 if result.get("stoppedEarly") else 0)
        if "-h" in argv or "--help" in argv:
            print(HELP, end="")
            return 0
        record, found = run_cli(argv)
        if found is not None:
            return print_outcome(found)
        print_json(record)
        return record.get("exitCode", 0) if record.get("state") == "completed" else 0
    except KeyboardInterrupt:
        progress(
            "Detached. The GitHub run continues; follow it with the printed execution UUID"
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
