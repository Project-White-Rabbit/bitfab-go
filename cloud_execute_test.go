package bitfab

import (
	"bytes"
	"compress/zlib"
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"regexp"
	"strings"
	"sync"
	"testing"
	"time"
)

const cloudCaseID = "11111111-1111-4111-8111-111111111111"

var cloudCaseUUID = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)

var cloudCaseEnvLine = regexp.MustCompile(`^([A-Za-z_][A-Za-z0-9_]*)=(.*)$`)

const cloudFakeReplay = `for last; do :; done
if [ "$last" = "--print-api-key" ]; then
  printf '@@bitfab:api-key {"apiKey":"bf_local","serviceUrl":"%s"}\n' "$BITFAB_TEST_SERVICE"
  exit 0
fi
mark="$BITFAB_TEST_MARK"
printf '%s\n' "$@" > "$mark/args"
pwd -P > "$mark/cwd"
env > "$mark/env"
git symbolic-ref --short -q HEAD > "$mark/branch" 2>/dev/null
if [ -n "$BITFAB_CODE_CHANGE_PATH" ]; then cp "$BITFAB_CODE_CHANGE_PATH" "$mark/change.json"; fi
cat "$mark/out"
cat "$mark/err" >&2
exit "$(cat "$mark/code")"
`

var cloudDefaultResponses = map[string]map[string]any{
	"GET /token":                           {"status": float64(200), "body": map[string]any{"value": "oidc-token"}},
	"POST /api/sdk/replay/cloudKey/redeem": {"status": float64(200), "body": map[string]any{"apiKey": "bf_temporary"}},
	"DELETE /api/sdk/replay/cloudKey":      {"status": float64(200), "body": map[string]any{}},
}

type cloudCaseAPI struct {
	server   *httptest.Server
	lock     sync.Mutex
	requests []map[string]any
}

func startCloudCaseAPI(t *testing.T, responses map[string]any) *cloudCaseAPI {
	t.Helper()
	answers := map[string]map[string]any{}
	for key, value := range cloudDefaultResponses {
		answers[key] = value
	}
	for key, value := range responses {
		answers[key] = value.(map[string]any)
	}
	api := &cloudCaseAPI{}
	api.server = httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		body, _ := io.ReadAll(request.Body)
		recorded := map[string]any{"method": request.Method, "path": request.URL.Path, "query": request.URL.RawQuery, "authorization": nil}
		if authorization := request.Header.Get("Authorization"); authorization != "" {
			recorded["authorization"] = authorization
		}
		if len(body) > 0 {
			var parsed any
			if err := json.Unmarshal(body, &parsed); err != nil {
				t.Errorf("request body is not JSON: %s", body)
			}
			recorded["body"] = parsed
		}
		api.lock.Lock()
		api.requests = append(api.requests, recorded)
		api.lock.Unlock()
		answer, ok := answers[request.Method+" "+request.URL.Path]
		if !ok {
			answer = map[string]any{"status": float64(404), "body": map[string]any{}}
		}
		payload, _ := json.Marshal(answer["body"])
		if raw, ok := answer["raw"].(string); ok {
			payload = []byte(raw)
		}
		response.Header().Set("Content-Type", "application/json")
		if location, ok := answer["location"].(string); ok && location != "" {
			response.Header().Set("Location", location)
		}
		response.WriteHeader(int(answer["status"].(float64)))
		response.Write(payload)
	}))
	return api
}

func cloudCaseGit(t *testing.T, directory string, args ...string) string {
	t.Helper()
	command := exec.Command("git", append([]string{"-c", "user.name=test", "-c", "user.email=test@example.com"}, args...)...)
	command.Dir = directory
	command.Env = append(os.Environ(), "GIT_CONFIG_NOSYSTEM=1")
	var stderr bytes.Buffer
	command.Stderr = &stderr
	output, err := command.Output()
	if err != nil {
		t.Fatalf("git %v: %v\n%s", args, err, stderr.String())
	}
	return strings.TrimSpace(string(output))
}

func writeCloudCaseFiles(t *testing.T, root string, files any) {
	t.Helper()
	entries, _ := files.(map[string]any)
	for name, content := range entries {
		path := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(path, []byte(content.(string)), 0o644); err != nil {
			t.Fatal(err)
		}
	}
}

func cloudCaseJSON(t *testing.T, value any) string {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return string(encoded)
}

func writeCloudCaseFile(t *testing.T, path, content string) {
	t.Helper()
	if err := os.WriteFile(path, []byte(content), 0o644); err != nil {
		t.Fatal(err)
	}
}

func setUpCloudCase(t *testing.T, testCase map[string]any, directory string) (string, string, map[string]string) {
	t.Helper()
	root := filepath.Join(directory, "repo")
	mark := filepath.Join(directory, "mark")
	for _, path := range []string{root, mark} {
		if err := os.Mkdir(path, 0o755); err != nil {
			t.Fatal(err)
		}
	}
	cloudCaseGit(t, root, "init", "-q", "-b", "main")
	writeCloudCaseFiles(t, root, testCase["files"])
	cloudCaseGit(t, root, "add", "-A")
	cloudCaseGit(t, root, "commit", "-q", "--allow-empty", "-m", "Initial commit")
	base := cloudCaseGit(t, root, "rev-parse", "HEAD")
	env := map[string]string{}
	if snapshot, ok := testCase["snapshot"].(map[string]any); ok {
		var request map[string]any
		replaced := strings.ReplaceAll(cloudCaseJSON(t, snapshot["request"]), `"BASE"`, cloudCaseJSON(t, base))
		if err := json.Unmarshal([]byte(replaced), &request); err != nil {
			t.Fatal(err)
		}
		if _, ok := request["id"]; !ok {
			request["id"] = cloudCaseID
		}
		branch := fmt.Sprintf("bitfab-replay/%v", request["id"])
		cloudCaseGit(t, root, "checkout", "-q", "-b", branch)
		writeCloudCaseFiles(t, root, snapshot["files"])
		removed, _ := snapshot["remove"].([]any)
		for _, name := range removed {
			if err := os.Remove(filepath.Join(root, name.(string))); err != nil {
				t.Fatal(err)
			}
		}
		cloudCaseGit(t, root, "add", "-A")
		cloudCaseGit(t, root, "commit", "-q", "--allow-empty", "-m", "Bitfab replay snapshot\n\n"+cloudCaseJSON(t, request))
		env["GITHUB_REF_NAME"] = branch
		if ref, ok := snapshot["ref"].(string); ok {
			env["GITHUB_REF_NAME"] = ref
		}
	} else {
		if testCase["origin"] == true {
			origin := filepath.Join(directory, "origin.git")
			if output, err := exec.Command("git", "init", "-q", "--bare", "-b", "main", origin).CombinedOutput(); err != nil {
				t.Fatalf("%s: %v", output, err)
			}
			cloudCaseGit(t, root, "remote", "add", "origin", origin)
			cloudCaseGit(t, root, "push", "-q", "origin", "main")
		}
		cloudCaseGit(t, root, "checkout", "-q", "-b", "dev")
		if branchFiles, ok := testCase["branchFiles"].(map[string]any); ok && len(branchFiles) > 0 {
			writeCloudCaseFiles(t, root, branchFiles)
			cloudCaseGit(t, root, "add", "-A")
			cloudCaseGit(t, root, "commit", "-q", "-m", "Change on dev")
		}
		inputs := map[string]any{}
		given, _ := testCase["inputs"].(map[string]any)
		for key, value := range given {
			inputs[key] = value
		}
		if request, ok := inputs["request"]; ok {
			inputs["request"] = cloudCaseJSON(t, request)
		}
		if text, ok := inputs["requestText"]; ok {
			inputs["request"] = text
			delete(inputs, "requestText")
		}
		if request, ok := inputs["requestBase64"]; ok {
			inputs["request"] = base64.StdEncoding.EncodeToString([]byte(cloudCaseJSON(t, request)))
			delete(inputs, "requestBase64")
		}
		event := map[string]any{}
		given, _ = testCase["event"].(map[string]any)
		for key, value := range given {
			event[key] = value
		}
		event["inputs"] = inputs
		eventPath := filepath.Join(directory, "event.json")
		writeCloudCaseFile(t, eventPath, cloudCaseJSON(t, event))
		env["GITHUB_REF_NAME"] = "dev"
		env["GITHUB_EVENT_PATH"] = eventPath
	}
	replay, _ := testCase["replay"].(map[string]any)
	stdout, _ := replay["stdout"].(string)
	stderr, _ := replay["stderr"].(string)
	code := "0"
	if exitCode, ok := replay["exitCode"].(float64); ok {
		code = fmt.Sprint(int(exitCode))
	}
	writeCloudCaseFile(t, filepath.Join(mark, "out"), stdout)
	writeCloudCaseFile(t, filepath.Join(mark, "err"), stderr)
	writeCloudCaseFile(t, filepath.Join(mark, "code"), code)
	script := filepath.Join(directory, "replay.sh")
	writeCloudCaseFile(t, script, cloudFakeReplay)
	for name, value := range map[string]string{
		"PATH":                  os.Getenv("PATH"),
		"HOME":                  directory,
		"GIT_CONFIG_NOSYSTEM":   "1",
		"GITHUB_RUN_ATTEMPT":    "1",
		"GITHUB_SHA":            cloudCaseGit(t, root, "rev-parse", "HEAD"),
		"GITHUB_WORKFLOW_REF":   "acme/app/.github/workflows/bitfab-replay.yml@refs/heads/dev",
		"GITHUB_STEP_SUMMARY":   filepath.Join(directory, "summary.md"),
		"RUNNER_TEMP":           directory,
		"BITFAB_REPLAY_COMMAND": cloudCaseJSON(t, []string{"sh", script}),
		"BITFAB_API_KEY":        "test",
		"BITFAB_TEST_MARK":      mark,
	} {
		env[name] = value
	}
	caseEnv, _ := testCase["env"].(map[string]any)
	for name, value := range caseEnv {
		env[name] = value.(string)
	}
	return root, mark, env
}

func decodeCloudCaseResult(t *testing.T, stdout string) any {
	t.Helper()
	var encoded strings.Builder
	for _, line := range strings.Split(stdout, "\n") {
		if strings.HasPrefix(line, "bitfab-replay-result ") {
			encoded.WriteString(strings.TrimPrefix(line, "bitfab-replay-result "))
		}
	}
	if encoded.Len() == 0 {
		return nil
	}
	compressed, err := base64.StdEncoding.DecodeString(encoded.String())
	if err != nil {
		t.Fatal(err)
	}
	reader, err := zlib.NewReader(bytes.NewReader(compressed))
	if err != nil {
		t.Fatal(err)
	}
	data, err := io.ReadAll(reader)
	if err != nil {
		t.Fatal(err)
	}
	var result any
	if err := json.Unmarshal(data, &result); err != nil {
		t.Fatal(err)
	}
	return result
}

func cloudCaseReplayed(t *testing.T, root, mark string) any {
	t.Helper()
	if _, err := os.Stat(filepath.Join(mark, "args")); err != nil {
		return false
	}
	read := func(name string) string {
		data, err := os.ReadFile(filepath.Join(mark, name))
		if err != nil {
			t.Fatal(err)
		}
		return string(data)
	}
	env := map[string]any{}
	for _, line := range strings.Split(read("env"), "\n") {
		if match := cloudCaseEnvLine.FindStringSubmatch(line); match != nil {
			env[match[1]] = match[2]
		}
	}
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		t.Fatal(err)
	}
	cwd, err := filepath.Rel(realRoot, strings.TrimSpace(read("cwd")))
	if err != nil {
		t.Fatal(err)
	}
	argLines := strings.Split(read("args"), "\n")
	args := []any{}
	for _, arg := range argLines[:len(argLines)-1] {
		args = append(args, arg)
	}
	var codeChange any
	if data, err := os.ReadFile(filepath.Join(mark, "change.json")); err == nil {
		if err := json.Unmarshal(data, &codeChange); err != nil {
			t.Fatal(err)
		}
	}
	return map[string]any{"args": args, "cwd": cwd, "env": env, "branch": strings.TrimSpace(read("branch")), "codeChange": codeChange}
}

func cloudCaseContains(actual, expected any) bool {
	wanted, ok := expected.(map[string]any)
	if !ok {
		return reflect.DeepEqual(actual, expected)
	}
	got, ok := actual.(map[string]any)
	if !ok {
		return false
	}
	for key, value := range wanted {
		present, ok := got[key]
		if !ok || !cloudCaseContains(present, value) {
			return false
		}
	}
	return true
}

func cloudCaseStrings(value any) []string {
	items, _ := value.([]any)
	texts := make([]string, 0, len(items))
	for _, item := range items {
		texts = append(texts, item.(string))
	}
	return texts
}

func TestSharedCloudRunnerCases(t *testing.T) {
	data, err := os.ReadFile(filepath.Join("testdata", "cloud_runner_cases.json"))
	if err != nil {
		t.Fatal(err)
	}
	var file struct {
		Cases []map[string]any `json:"cases"`
	}
	if err := json.Unmarshal(data, &file); err != nil {
		t.Fatal(err)
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, testCase := range file.Cases {
		name := testCase["name"].(string)
		t.Run(name, func(t *testing.T) {
			t.Parallel()
			directory, err := filepath.EvalSymlinks(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			root, mark, env := setUpCloudCase(t, testCase, directory)
			var api *cloudCaseAPI
			if responses, ok := testCase["api"].(map[string]any); ok {
				api = startCloudCaseAPI(t, responses)
				env["BITFAB_TEST_SERVICE"] = api.server.URL
				env["ACTIONS_ID_TOKEN_REQUEST_URL"] = api.server.URL + "/token"
				env["ACTIONS_ID_TOKEN_REQUEST_TOKEN"] = "bearer-test"
			}
			env[cloudExecuteProgramEnv] = "1"
			command := exec.Command(executable)
			command.Dir = root
			for key, value := range env {
				command.Env = append(command.Env, key+"="+value)
			}
			var stdoutBuffer, stderrBuffer bytes.Buffer
			command.Stdout, command.Stderr = &stdoutBuffer, &stderrBuffer
			runErr := command.Run()
			if api != nil {
				api.server.Close()
			}
			var exit *exec.ExitError
			if runErr != nil && !errors.As(runErr, &exit) {
				t.Fatal(runErr)
			}
			stdout, stderr := stdoutBuffer.String(), stderrBuffer.String()
			context := name + "\n" + stdout + "\n" + stderr
			expected := testCase["expect"].(map[string]any)
			if code := command.ProcessState.ExitCode(); float64(code) != expected["exitCode"].(float64) {
				t.Fatalf("exit %d, want %v\n%s", code, expected["exitCode"], context)
			}
			for _, text := range cloudCaseStrings(expected["stdout"]) {
				if !strings.Contains(stdout, text) {
					t.Errorf("stdout lacks %q\n%s", text, context)
				}
			}
			for _, text := range cloudCaseStrings(expected["notStdout"]) {
				if strings.Contains(stdout, text) {
					t.Errorf("stdout has %q\n%s", text, context)
				}
			}
			for _, text := range cloudCaseStrings(expected["stderr"]) {
				if !strings.Contains(stderr, text) {
					t.Errorf("stderr lacks %q\n%s", text, context)
				}
			}
			result := decodeCloudCaseResult(t, stdout)
			if result != nil {
				fields := result.(map[string]any)
				if id, _ := fields["executionId"].(string); !cloudCaseUUID.MatchString(id) {
					t.Errorf("execution id %v is not a UUID\n%s", fields["executionId"], context)
				}
				if fields["commitSha"] != env["GITHUB_SHA"] {
					t.Errorf("commit %v, want %s\n%s", fields["commitSha"], env["GITHUB_SHA"], context)
				}
			}
			if wanted, ok := expected["result"]; ok && !cloudCaseContains(result, wanted) {
				t.Errorf("result %v does not contain %v\n%s", result, wanted, context)
			}
			if wanted, ok := expected["resultStdoutJson"]; ok {
				var parsed any
				text, _ := result.(map[string]any)["stdout"].(string)
				if err := json.Unmarshal([]byte(text), &parsed); err != nil || !reflect.DeepEqual(parsed, wanted) {
					t.Errorf("result stdout %q, want %v\n%s", text, wanted, context)
				}
			}
			if wanted, ok := expected["requests"].([]any); ok {
				if api == nil || len(api.requests) != len(wanted) {
					t.Fatalf("requests %v, want %v\n%s", api, wanted, context)
				}
				for index, request := range wanted {
					if !cloudCaseContains(api.requests[index], request) {
						t.Errorf("request %d %v does not contain %v\n%s", index, api.requests[index], request, context)
					}
				}
			}
			summary := ""
			if data, err := os.ReadFile(env["GITHUB_STEP_SUMMARY"]); err == nil {
				summary = string(data)
			}
			for _, text := range cloudCaseStrings(expected["summary"]) {
				if !strings.Contains(summary, text) {
					t.Errorf("summary lacks %q\n%s\n%s", text, summary, context)
				}
			}
			actual := cloudCaseReplayed(t, root, mark)
			if wanted, ok := expected["replayed"]; ok {
				if wanted == false {
					if actual != false {
						t.Errorf("replayed %v, want no replay\n%s", actual, context)
					}
				} else {
					fields := map[string]any{}
					for key, value := range wanted.(map[string]any) {
						fields[key] = value
					}
					wantedEnv, _ := fields["env"].(map[string]any)
					delete(fields, "env")
					if !cloudCaseContains(actual, fields) {
						t.Fatalf("replayed %v does not contain %v\n%s", actual, fields, context)
					}
					actualEnv := actual.(map[string]any)["env"].(map[string]any)
					for envName, value := range wantedEnv {
						if actualEnv[envName] != value {
							t.Errorf("replay env %s = %v, want %v\n%s", envName, actualEnv[envName], value, context)
						}
					}
				}
			}
		})
	}
}

func TestCloudExecuteStopsAReplayPastItsTimeoutWithItsWholeProcessGroup(t *testing.T) {
	directory := t.TempDir()
	marker := filepath.Join(directory, "grandchild-alive")
	script := "(sleep 3; touch " + marker + ") & echo '[replay] Experiment 33333333-3333-4333-8333-333333333333: running' >&2; sleep 30"
	var output bytes.Buffer
	runner := &cloudRunner{ctx: context.Background(), env: map[string]string{}, stdout: &output, stderr: io.Discard, runInterrupts: make(chan struct{}, 1)}
	for _, entry := range os.Environ() {
		if name, value, ok := strings.Cut(entry, "="); ok && name != "" {
			runner.env[name] = value
		}
	}
	started := time.Now()
	code, stdout, omitted, stopped, experiment, err := runner.runReplay(directory, []string{"sh", "-c", script}, &cloudTimeout{minutes: 0.02, label: "0.02"}, true, nil)
	if err != nil {
		t.Fatal(err)
	}
	if elapsed := time.Since(started); elapsed > 10*time.Second {
		t.Fatalf("took %s to stop", elapsed)
	}
	if stopped != "it reached the 0.02-minute --cloud-timeout" {
		t.Fatalf("stopped %q", stopped)
	}
	if experiment != "33333333-3333-4333-8333-333333333333" {
		t.Fatalf("experiment %q", experiment)
	}
	if code == 0 || stdout != "" || omitted != 0 {
		t.Fatalf("code %d stdout %q omitted %d", code, stdout, omitted)
	}
	time.Sleep(3500 * time.Millisecond)
	if _, err := os.Stat(marker); err == nil {
		t.Fatal("the replay's background child outlived the timeout")
	}
}

func TestCloudJSONPrintsNumbersAndKeysLikeJavaScript(t *testing.T) {
	for _, check := range []struct {
		number float64
		want   string
	}{
		{1, "1"}, {1.5, "1.5"}, {-2.25, "-2.25"}, {math.Copysign(0, -1), "0"}, {1e20, "100000000000000000000"},
		{1e21, "1e+21"}, {1.5e21, "1.5e+21"}, {0.000001, "0.000001"}, {1e-7, "1e-7"}, {1.25e-8, "1.25e-8"},
		{9007199254740993, "9007199254740992"}, {123.456, "123.456"}, {5e-324, "5e-324"},
	} {
		if got := cloudJSNumber(check.number); got != check.want {
			t.Errorf("cloudJSNumber(%v) = %q, want %q", check.number, got, check.want)
		}
	}
	parsed, err := parseCloudJSON(`{"x": 1.0, "2": "a", "10": [1e400, -0.0], "1": "b", "4294967295": 0, "01": 3}`)
	if err != nil {
		t.Fatal(err)
	}
	if got, want := compactCloudJSON(parsed), `{"1":"b","2":"a","10":[null,0],"x":1,"4294967295":0,"01":3}`; got != want {
		t.Fatalf("compactCloudJSON = %s, want %s", got, want)
	}
}

func TestCloudCoercesValuesLikeJavaScript(t *testing.T) {
	for _, check := range []struct {
		value any
		want  float64
	}{
		{"5", 5}, {" 5 ", 5}, {"", 0}, {true, 1}, {false, 0}, {nil, 0}, {"0x10", 16}, {"1e3", 1000}, {".5", 0.5},
		{[]any{}, 0}, {[]any{"7"}, 7}, {float64(3), 3},
	} {
		if got := cloudJSToNumber(check.value); got != check.want {
			t.Errorf("cloudJSToNumber(%#v) = %v, want %v", check.value, got, check.want)
		}
	}
	for _, value := range []any{"abc", "5 minutes", newCloudObject(), []any{1.0, 2.0}} {
		if got := cloudJSToNumber(value); !math.IsNaN(got) {
			t.Errorf("cloudJSToNumber(%#v) = %v, want NaN", value, got)
		}
	}
	for _, value := range []any{[]any{}, newCloudObject(), "x", 1.0} {
		if !cloudTruthy(value) {
			t.Errorf("cloudTruthy(%#v) is false", value)
		}
	}
	for _, value := range []any{nil, cloudMissing{}, false, "", 0.0, math.NaN()} {
		if cloudTruthy(value) {
			t.Errorf("cloudTruthy(%#v) is true", value)
		}
	}
	if got := cloudJSString([]any{1.0, nil, "a", []any{2.0, 3.0}}); got != "1,,a,2,3" {
		t.Errorf("cloudJSString = %q", got)
	}
}

func TestCloudDecodesBrokenUTF8LikeTheWHATWGDecoder(t *testing.T) {
	for _, check := range []struct {
		input string
		want  string
	}{
		{"x\xe2\x82y", "x�y"}, {"\xff\xfe", "��"}, {"\xed\xa0\x80", "���"},
		{"\xf0\x9f\x98", "�"}, {"\xf0\x9f\x98\x80", "\U0001F600"}, {"a\xc3", "a�"}, {"\xe0\x80\x80", "���"},
	} {
		if got := validCloudText([]byte(check.input)); got != check.want {
			t.Errorf("validCloudText(%q) = %q, want %q", check.input, got, check.want)
		}
	}
}

func TestCloudCountsTextInUTF16UnitsLikeJavaScript(t *testing.T) {
	if got := cloudUTF16Length("a\U0001F600é"); got != 4 {
		t.Fatalf("length %d", got)
	}
	if got := cloudUTF16Prefix("a\U0001F600b", 2); got != "a�" {
		t.Fatalf("prefix %q", got)
	}
	if got := cloudUTF16Prefix("a\U0001F600b", 3); got != "a\U0001F600" {
		t.Fatalf("prefix %q", got)
	}
}
