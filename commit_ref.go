package bitfab

import (
	"context"
	"net/url"
	"os"
	"sort"
	"strings"
	"sync"
	"time"
)

type CommitRef struct {
	SHA     string  `json:"sha"`
	Branch  *string `json:"branch"`
	Dirty   *bool   `json:"dirty"`
	Remote  *string `json:"remote"`
	RootSHA *string `json:"root_sha"`
}

const (
	explicitCommitSHAEnv = "BITFAB_COMMIT_SHA"
	disableCommitRefEnv  = "BITFAB_DISABLE_COMMIT_REF"
	commitRefGitTimeout  = 2 * time.Second
)

type envLookup func(name string) string

type remoteReader func(env envLookup) *string

type commitRefPlatform struct {
	shaEnv    string
	branchEnv string
	remote    remoteReader
}

var vercelProviderHosts = map[string]string{
	"github":    "github.com",
	"gitlab":    "gitlab.com",
	"bitbucket": "bitbucket.org",
}

func readCommitRefEnv(env envLookup, name string) string {
	if name == "" {
		return ""
	}
	return strings.TrimSpace(env(name))
}

func remoteFromEnv(names ...string) remoteReader {
	return func(env envLookup) *string {
		parts := make([]string, 0, len(names))
		for _, name := range names {
			value := readCommitRefEnv(env, name)
			if value == "" {
				return nil
			}
			parts = append(parts, value)
		}
		return normalizeRemote(strings.Join(parts, "/"))
	}
}

func vercelRemote(env envLookup) *string {
	host := vercelProviderHosts[strings.ToLower(readCommitRefEnv(env, "VERCEL_GIT_PROVIDER"))]
	owner := readCommitRefEnv(env, "VERCEL_GIT_REPO_OWNER")
	slug := readCommitRefEnv(env, "VERCEL_GIT_REPO_SLUG")
	if host == "" || owner == "" || slug == "" {
		return nil
	}
	return commitRefString(host + "/" + owner + "/" + slug)
}

func noRemote(envLookup) *string {
	return nil
}

var commitRefPlatforms = []commitRefPlatform{
	{"VERCEL_GIT_COMMIT_SHA", "VERCEL_GIT_COMMIT_REF", vercelRemote},
	{"GITHUB_SHA", "GITHUB_REF_NAME", remoteFromEnv("GITHUB_SERVER_URL", "GITHUB_REPOSITORY")},
	{"RAILWAY_GIT_COMMIT_SHA", "RAILWAY_GIT_BRANCH", noRemote},
	{"RENDER_GIT_COMMIT", "RENDER_GIT_BRANCH", noRemote},
	{"SOURCE_VERSION", "", noRemote},
	{"CF_PAGES_COMMIT_SHA", "CF_PAGES_BRANCH", noRemote},
	{"CI_COMMIT_SHA", "CI_COMMIT_REF_NAME", remoteFromEnv("CI_REPOSITORY_URL")},
	{"BUILD_SOURCEVERSION", "BUILD_SOURCEBRANCHNAME", remoteFromEnv("BUILD_REPOSITORY_URI")},
	{"CIRCLE_SHA1", "CIRCLE_BRANCH", remoteFromEnv("CIRCLE_REPOSITORY_URL")},
}

func commitRefString(value string) *string {
	return &value
}

func commitRefBool(value bool) *bool {
	return &value
}

func normalizeRemote(raw string) *string {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	host, path := "", value
	if strings.Contains(value, "://") {
		parsed, err := url.Parse(value)
		if err != nil {
			return nil
		}
		host = strings.ToLower(parsed.Hostname())
		path = parsed.Path
	} else if head, tail, found := strings.Cut(value, ":"); found {
		host = head[strings.LastIndex(head, "@")+1:]
		path = tail
	}
	path = strings.Trim(path, "/")
	if strings.HasSuffix(path, ".git") {
		path = strings.TrimRight(strings.TrimSuffix(path, ".git"), "/")
	}
	switch {
	case host == "" && path == "":
		return nil
	case host == "":
		return commitRefString(path)
	case path == "":
		return commitRefString(host)
	}
	return commitRefString(host + "/" + path)
}

func resolveCommitRefFromEnv(env envLookup) *CommitRef {
	explicit := readCommitRefEnv(env, explicitCommitSHAEnv)
	for _, platform := range commitRefPlatforms {
		sha := readCommitRefEnv(env, platform.shaEnv)
		if sha == "" {
			continue
		}
		if explicit != "" {
			sha = explicit
		}
		ref := &CommitRef{SHA: sha, Remote: platform.remote(env)}
		if branch := readCommitRefEnv(env, platform.branchEnv); branch != "" {
			ref.Branch = commitRefString(branch)
		}
		return ref
	}
	if explicit == "" {
		return nil
	}
	return &CommitRef{SHA: explicit}
}

var runCommitRefGit = func(cwd string, args ...string) (string, bool) {
	output, ok := runGit(context.Background(), cwd, commitRefGitTimeout, args...)
	return strings.TrimSpace(output), ok
}

func resolveCommitRefFromGit(cwd string) *CommitRef {
	sha, ok := runCommitRefGit(cwd, "rev-parse", "HEAD")
	if !ok || sha == "" {
		return nil
	}
	ref := &CommitRef{SHA: sha}
	if branch, ok := runCommitRefGit(cwd, "symbolic-ref", "--short", "-q", "HEAD"); ok && branch != "" {
		ref.Branch = commitRefString(branch)
	}
	if status, ok := runCommitRefGit(cwd, "status", "--porcelain"); ok {
		ref.Dirty = commitRefBool(status != "")
	}
	if remote, ok := runCommitRefGit(cwd, "remote", "get-url", "origin"); ok {
		ref.Remote = normalizeRemote(remote)
	}
	if roots, ok := runCommitRefGit(cwd, "rev-list", "--max-parents=0", "HEAD"); ok && roots != "" {
		lines := strings.Fields(roots)
		sort.Strings(lines)
		ref.RootSHA = commitRefString(lines[0])
	}
	return ref
}

var commitRefState struct {
	mu         sync.Mutex
	resolved   bool
	ref        *CommitRef
	gitStarted bool
	pending    sync.WaitGroup
}

func startCommitRefResolution() {
	commitRefState.mu.Lock()
	defer commitRefState.mu.Unlock()
	if commitRefState.resolved || commitRefState.gitStarted {
		return
	}
	if readCommitRefEnv(os.Getenv, disableCommitRefEnv) != "" {
		commitRefState.resolved = true
		return
	}
	if fromEnv := resolveCommitRefFromEnv(os.Getenv); fromEnv != nil {
		commitRefState.ref = fromEnv
		commitRefState.resolved = true
		return
	}
	commitRefState.gitStarted = true
	cwd, err := os.Getwd()
	if err != nil {
		commitRefState.resolved = true
		return
	}
	commitRefState.pending.Add(1)
	go func() {
		defer commitRefState.pending.Done()
		ref := resolveCommitRefFromGit(cwd)
		commitRefState.mu.Lock()
		defer commitRefState.mu.Unlock()
		if !commitRefState.resolved {
			commitRefState.ref = ref
			commitRefState.resolved = true
		}
	}()
}

func currentCommitRef() *CommitRef {
	startCommitRefResolution()
	commitRefState.mu.Lock()
	defer commitRefState.mu.Unlock()
	return commitRefState.ref
}
