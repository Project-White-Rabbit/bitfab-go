package bitfab

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testCommitSHA         = "0123456789abcdef0123456789abcdef01234567"
	testPlatformCommitSHA = "fedcba9876543210fedcba9876543210fedcba98"
	testCommitToken       = "ghs_live_token_that_must_never_ship"
)

func commitRefEnvNames() []string {
	names := []string{explicitCommitSHAEnv, disableCommitRefEnv}
	for _, platform := range commitRefPlatforms {
		names = append(names, platform.shaEnv)
		if platform.branchEnv != "" {
			names = append(names, platform.branchEnv)
		}
	}
	return append(names,
		"GITHUB_SERVER_URL", "GITHUB_REPOSITORY", "CI_REPOSITORY_URL", "BUILD_REPOSITORY_URI",
		"CIRCLE_REPOSITORY_URL", "VERCEL_GIT_PROVIDER", "VERCEL_GIT_REPO_OWNER", "VERCEL_GIT_REPO_SLUG",
	)
}

func resetCommitRef() {
	commitRefState.pending.Wait()
	commitRefState.mu.Lock()
	defer commitRefState.mu.Unlock()
	commitRefState.resolved = false
	commitRefState.ref = nil
	commitRefState.gitStarted = false
}

func scrubCommitRefEnv(t *testing.T) {
	t.Helper()
	for _, name := range commitRefEnvNames() {
		t.Setenv(name, "")
	}
	resetCommitRef()
	t.Cleanup(resetCommitRef)
}

func mapEnv(values map[string]string) envLookup {
	return func(name string) string {
		return values[name]
	}
}

func testGit(t *testing.T, dir string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-c", "commit.gpgsign=false"}, args...)...)
	command.Dir = dir
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v", args, err)
	}
	return strings.TrimSpace(string(output))
}

func initTestRepo(t *testing.T) (string, string) {
	t.Helper()
	dir := t.TempDir()
	testGit(t, dir, "init", "-q", "-b", "main")
	testGit(t, dir, "config", "user.email", "test@example.com")
	testGit(t, dir, "config", "user.name", "Test")
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("a\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, dir, "add", ".")
	testGit(t, dir, "commit", "-q", "-m", "init")
	return dir, testGit(t, dir, "rev-parse", "HEAD")
}

func stubCommitRefGit(t *testing.T, calls *atomic.Int32) {
	t.Helper()
	original := runCommitRefGit
	runCommitRefGit = func(string, ...string) (string, bool) {
		calls.Add(1)
		return "", false
	}
	t.Cleanup(func() { runCommitRefGit = original })
}

func stringValue(value *string) string {
	if value == nil {
		return "<nil>"
	}
	return *value
}

func TestNormalizeRemote(t *testing.T) {
	cases := map[string]string{
		"https://x-access-token:" + testCommitToken + "@github.com/org/repo.git": "github.com/org/repo",
		"git@github.com:org/repo.git":                                            "github.com/org/repo",
		"https://github.com/org/repo":                                            "github.com/org/repo",
		"ssh://git@github.com:22/org/repo.git":                                   "github.com/org/repo",
	}
	for raw, want := range cases {
		if got := stringValue(normalizeRemote(raw)); got != want {
			t.Errorf("normalizeRemote(%q) = %q, want %q", raw, got, want)
		}
	}
	if normalizeRemote("   ") != nil {
		t.Error("blank remote should be nil")
	}
}

func TestResolveCommitRefFromEnv_ExplicitBeatsEveryPlatform(t *testing.T) {
	values := map[string]string{explicitCommitSHAEnv: testCommitSHA}
	for _, platform := range commitRefPlatforms {
		values[platform.shaEnv] = testPlatformCommitSHA
	}
	if ref := resolveCommitRefFromEnv(mapEnv(values)); ref == nil || ref.SHA != testCommitSHA {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestResolveCommitRefFromEnv_PlatformCarriesBranchAndRemote(t *testing.T) {
	ref := resolveCommitRefFromEnv(mapEnv(map[string]string{
		"GITHUB_SHA":        testPlatformCommitSHA,
		"GITHUB_REF_NAME":   "release",
		"GITHUB_SERVER_URL": "https://github.com",
		"GITHUB_REPOSITORY": "org/repo",
	}))
	if ref == nil || ref.SHA != testPlatformCommitSHA || stringValue(ref.Branch) != "release" || stringValue(ref.Remote) != "github.com/org/repo" {
		t.Fatalf("ref = %#v", ref)
	}
	if ref.Dirty != nil || ref.RootSHA != nil {
		t.Fatalf("dirty and root sha must be unknown from env: %#v", ref)
	}
}

func TestResolveCommitRefFromEnv_VercelRemote(t *testing.T) {
	ref := resolveCommitRefFromEnv(mapEnv(map[string]string{
		"VERCEL_GIT_COMMIT_SHA": testPlatformCommitSHA,
		"VERCEL_GIT_PROVIDER":   "github",
		"VERCEL_GIT_REPO_OWNER": "org",
		"VERCEL_GIT_REPO_SLUG":  "repo",
	}))
	if ref == nil || stringValue(ref.Remote) != "github.com/org/repo" {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestResolveCommitRefFromEnv_CredentialNeverReachesRef(t *testing.T) {
	ref := resolveCommitRefFromEnv(mapEnv(map[string]string{
		"CI_COMMIT_SHA":     testPlatformCommitSHA,
		"CI_REPOSITORY_URL": "https://gitlab-ci-token:" + testCommitToken + "@gitlab.com/org/repo.git",
	}))
	encoded, err := json.Marshal(ref)
	if err != nil {
		t.Fatal(err)
	}
	if stringValue(ref.Remote) != "gitlab.com/org/repo" || strings.Contains(string(encoded), testCommitToken) {
		t.Fatalf("ref leaked credential: %s", encoded)
	}
}

func TestResolveCommitRefFromEnv_NothingSet(t *testing.T) {
	if resolveCommitRefFromEnv(mapEnv(map[string]string{})) != nil {
		t.Fatal("empty env should resolve nothing")
	}
	if resolveCommitRefFromEnv(mapEnv(map[string]string{explicitCommitSHAEnv: "  "})) != nil {
		t.Fatal("blank explicit sha should resolve nothing")
	}
}

func TestResolveCommitRefFromGit_ReadsHeadBranchAndCleanTree(t *testing.T) {
	dir, sha := initTestRepo(t)
	ref := resolveCommitRefFromGit(dir)
	if ref == nil || ref.SHA != sha || stringValue(ref.Branch) != "main" || ref.Remote != nil || stringValue(ref.RootSHA) != sha {
		t.Fatalf("ref = %#v", ref)
	}
	if ref.Dirty == nil || *ref.Dirty {
		t.Fatalf("clean tree should report dirty=false: %#v", ref.Dirty)
	}
}

func TestResolveCommitRefFromGit_DirtyIsARealBool(t *testing.T) {
	dir, _ := initTestRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "a.txt"), []byte("changed\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	ref := resolveCommitRefFromGit(dir)
	if ref == nil || ref.Dirty == nil || !*ref.Dirty {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestResolveCommitRefFromGit_RemoteIsNormalized(t *testing.T) {
	dir, _ := initTestRepo(t)
	testGit(t, dir, "remote", "add", "origin", "git@github.com:org/repo.git")
	if ref := resolveCommitRefFromGit(dir); ref == nil || stringValue(ref.Remote) != "github.com/org/repo" {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestResolveCommitRefFromGit_RootSHAIsFirstCommit(t *testing.T) {
	dir, first := initTestRepo(t)
	if err := os.WriteFile(filepath.Join(dir, "b.txt"), []byte("b\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	testGit(t, dir, "add", ".")
	testGit(t, dir, "commit", "-q", "-m", "second")
	ref := resolveCommitRefFromGit(dir)
	if ref == nil || stringValue(ref.RootSHA) != first || ref.SHA == first {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestResolveCommitRefFromGit_OutsideRepositoryIsNil(t *testing.T) {
	if ref := resolveCommitRefFromGit(t.TempDir()); ref != nil {
		t.Fatalf("ref = %#v", ref)
	}
}

func TestCommitRef_EnvBeatsGitAndNeverSpawnsIt(t *testing.T) {
	scrubCommitRefEnv(t)
	dir, _ := initTestRepo(t)
	t.Chdir(dir)
	t.Setenv("GITHUB_SHA", testPlatformCommitSHA)
	var calls atomic.Int32
	stubCommitRefGit(t, &calls)
	if ref := currentCommitRef(); ref == nil || ref.SHA != testPlatformCommitSHA {
		t.Fatalf("ref = %#v", ref)
	}
	if calls.Load() != 0 {
		t.Fatalf("git ran %d times", calls.Load())
	}
}

func TestCommitRef_GitRunsAtMostOnceIncludingNegativeResult(t *testing.T) {
	scrubCommitRefEnv(t)
	var calls atomic.Int32
	stubCommitRefGit(t, &calls)
	for i := 0; i < 5; i++ {
		currentCommitRef()
	}
	commitRefState.pending.Wait()
	if currentCommitRef() != nil {
		t.Fatal("failed git should resolve to no ref")
	}
	if calls.Load() != 1 {
		t.Fatalf("git ran %d times, want 1", calls.Load())
	}
}

func TestCommitRef_GitResultLandsInBackground(t *testing.T) {
	scrubCommitRefEnv(t)
	dir, sha := initTestRepo(t)
	t.Chdir(dir)
	startCommitRefResolution()
	commitRefState.pending.Wait()
	ref := currentCommitRef()
	if ref == nil || ref.SHA != sha || stringValue(ref.Branch) != "main" || ref.Dirty == nil || *ref.Dirty {
		t.Fatalf("ref = %#v", ref)
	}
}

func commitRefTraceCompletion(t *testing.T) map[string]any {
	t.Helper()
	sink := &carrierSink{}
	server := newCarrierCaptureServer(t, sink)
	defer server.Close()
	client := NewClient("test-key", WithServiceURL(server.URL))
	if _, err := client.Span(context.Background(), "commit-fn", func(context.Context) (any, error) {
		return "done", nil
	}); err != nil {
		t.Fatal(err)
	}
	if !client.FlushTraces(5 * time.Second) {
		t.Fatal("flush reported failure")
	}
	payload := sink.lastTracePayload()
	if payload == nil {
		t.Fatal("no trace payload captured")
	}
	rawTrace, ok := payload["externalTrace"].(map[string]any)
	if !ok {
		t.Fatalf("payload = %#v", payload)
	}
	return rawTrace
}

func TestCommitRef_TraceCarriesCommitRef(t *testing.T) {
	scrubCommitRefEnv(t)
	t.Setenv(explicitCommitSHAEnv, testCommitSHA)
	rawTrace := commitRefTraceCompletion(t)
	ref, ok := rawTrace["commit_ref"].(map[string]any)
	if !ok {
		t.Fatalf("trace omitted commit_ref: %#v", rawTrace)
	}
	if ref["sha"] != testCommitSHA {
		t.Fatalf("sha = %v", ref["sha"])
	}
	for _, key := range []string{"branch", "dirty", "remote", "root_sha"} {
		value, present := ref[key]
		if !present || value != nil {
			t.Fatalf("%s = %#v (present=%v), want explicit null", key, value, present)
		}
	}
}

func TestCommitRef_TraceShipsWithoutRefWhenNothingResolves(t *testing.T) {
	scrubCommitRefEnv(t)
	var calls atomic.Int32
	stubCommitRefGit(t, &calls)
	rawTrace := commitRefTraceCompletion(t)
	if _, present := rawTrace["commit_ref"]; present {
		t.Fatalf("trace carried commit_ref: %#v", rawTrace["commit_ref"])
	}
	if rawTrace["id"] == nil {
		t.Fatalf("trace missing id: %#v", rawTrace)
	}
}

func TestCommitRef_DisabledByEnvShipsNoRefAndNeverSpawnsGit(t *testing.T) {
	scrubCommitRefEnv(t)
	t.Setenv(disableCommitRefEnv, "1")
	t.Setenv(explicitCommitSHAEnv, testCommitSHA)
	var calls atomic.Int32
	stubCommitRefGit(t, &calls)
	if ref := currentCommitRef(); ref != nil {
		t.Fatalf("ref = %#v, want nil when disabled", ref)
	}
	if calls.Load() != 0 {
		t.Fatalf("git ran %d times", calls.Load())
	}
}
