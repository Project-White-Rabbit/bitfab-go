package instrument

import (
	"archive/zip"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestParseListOptionsIncludePackages(t *testing.T) {
	args := []string{"-include-packages", "example.com/a, example.com/b", "-tags=x", "--include-packages=example.com/c", "-modfile=custom.mod", "./cmd/app"}
	result, err := ParseListOptions("build", args)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(result.IncludePackages, []string{"example.com/a", "example.com/b", "example.com/c"}) {
		t.Fatalf("libraries=%#v", result.IncludePackages)
	}
	if !reflect.DeepEqual(result.BuildFlags, []string{"-tags=x", "-modfile=custom.mod"}) || result.ModFile != "custom.mod" {
		t.Fatalf("options=%#v", result)
	}
	if got := result.GoArgs(args); !reflect.DeepEqual(got, []string{"-tags=x", "-modfile=custom.mod", "./cmd/app"}) {
		t.Fatalf("go args=%#v", got)
	}
	build := &Build{Overlay: "o.json", ModFile: "generated.mod", options: result}
	if got := build.Args("build", args); !reflect.DeepEqual(got, []string{"build", "-overlay", "o.json", "-modfile=generated.mod", "-tags=x", "./cmd/app"}) {
		t.Fatalf("build args=%#v", got)
	}
	if _, err := ParseListOptions("build", []string{"-include-packages"}); err == nil {
		t.Fatal("missing value accepted")
	}
}

func listed(importPath, module string, main bool, deps ...string) listedPackage {
	pkg := listedPackage{ImportPath: importPath, Deps: deps}
	if module != "" {
		pkg.Module = &struct {
			Path, Version, Dir, GoVersion string
			Main                          bool
		}{Path: module, Main: main}
	}
	return pkg
}

func TestSelectPackagesIncludesOnlyListedModules(t *testing.T) {
	packages := []listedPackage{
		listed("example.com/app", "example.com/app", true),
		listed("example.com/app [example.com/app.test]", "example.com/app", true),
		listed("example.com/app.test", "example.com/app", true),
		listed("example.com/payments", "example.com/payments", false),
		listed("example.com/payments/fees", "example.com/payments", false),
		listed("example.com/payments/v2", "example.com/payments/v2", false),
		listed("example.com/other", "example.com/other", false),
		listed("fmt", "", false),
		listed(SDKImport, SDKImport, false, "github.com/google/uuid"),
		listed(SDKImport+"/baml", SDKImport, false),
		listed("github.com/google/uuid", "github.com/google/uuid", false),
	}
	selected, err := selectPackages(packages, map[string]bool{"example.com/payments": true})
	if err != nil {
		t.Fatal(err)
	}
	var paths []string
	for _, pkg := range selected {
		paths = append(paths, pkg.ImportPath)
	}
	want := []string{"example.com/app", "example.com/app [example.com/app.test]", "example.com/payments", "example.com/payments/fees"}
	if !reflect.DeepEqual(paths, want) {
		t.Fatalf("selected=%#v", paths)
	}
	if _, err := selectPackages(packages, map[string]bool{"github.com/google/uuid": true}); err == nil || !strings.Contains(err.Error(), "import cycle") {
		t.Fatalf("SDK dependency accepted: %v", err)
	}
	if _, err := selectPackages(packages, map[string]bool{"fmt": true}); err == nil {
		t.Fatal("standard library accepted")
	}
	if _, err := includedLibrarySet([]string{SDKImport}); err == nil {
		t.Fatal("SDK accepted")
	}
	if _, err := includedLibrarySet([]string{SDKImport + "/baml"}); err == nil {
		t.Fatal("SDK package accepted")
	}
}

func writeTree(t *testing.T, root string, files map[string]string) {
	t.Helper()
	for name, content := range files {
		target := filepath.Join(root, name)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(content), 0600); err != nil {
			t.Fatal(err)
		}
	}
}

func writeProxyModule(t *testing.T, proxy, module, version string, files map[string]string) {
	t.Helper()
	dir := filepath.Join(proxy, module, "@v")
	if err := os.MkdirAll(dir, 0700); err != nil {
		t.Fatal(err)
	}
	writeTree(t, dir, map[string]string{
		"list":            version + "\n",
		version + ".info": `{"Version":"` + version + `"}`,
		version + ".mod":  files["go.mod"],
	})
	archive, err := os.Create(filepath.Join(dir, version+".zip"))
	if err != nil {
		t.Fatal(err)
	}
	writer := zip.NewWriter(archive)
	for name, content := range files {
		entry, err := writer.Create(module + "@" + version + "/" + name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := entry.Write([]byte(content)); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := archive.Close(); err != nil {
		t.Fatal(err)
	}
}

func goEnvValue(t *testing.T, name string) string {
	t.Helper()
	out, err := exec.Command("go", "env", name).Output()
	if err != nil {
		t.Fatal(err)
	}
	return strings.TrimSpace(string(out))
}

func TestPrepareBuildTracesListedLibrariesFromReplaceAndModuleCache(t *testing.T) {
	sdk, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	realCache := goEnvValue(t, "GOMODCACHE")
	base, err := filepath.EvalSymlinks(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	proxy := filepath.Join(base, "proxy")
	writeProxyModule(t, proxy, "example.com/cached", "v1.0.0", map[string]string{
		"go.mod":    "module example.com/cached\n\ngo 1.16\n",
		"cached.go": "// +build !bitfab_excluded\n\npackage cached\n\nfunc Lookup(id int) int { return id * 2 }\n",
	})
	writeTree(t, filepath.Join(base, "payments"), map[string]string{
		"go.mod":       "module example.com/payments\n\ngo 1.26.0\n",
		"payments.go":  "package payments\n\nimport \"example.com/payments/fees\"\n\nfunc Charge(amount int) int { return fees.Fee(amount) + amount }\n",
		"fees/fees.go": "package fees\n\nfunc Fee(amount int) int { return amount / 10 }\n",
	})
	writeTree(t, filepath.Join(base, "app"), map[string]string{
		"go.mod":  "module example.com/app\n\ngo 1.26.0\n\nrequire (\n\t" + SDKImport + " v0.0.0\n\texample.com/payments v0.0.0\n\texample.com/cached v1.0.0\n)\n\nreplace " + SDKImport + " => " + sdk + "\n\nreplace example.com/payments => ../payments\n",
		"main.go": libraryApplication,
	})
	t.Setenv("GOMODCACHE", filepath.Join(base, "modcache"))
	t.Setenv("GOPROXY", "file://"+filepath.ToSlash(proxy)+",file://"+filepath.ToSlash(filepath.Join(realCache, "cache", "download")))
	t.Setenv("GOSUMDB", "off")
	t.Setenv("GOFLAGS", "-modcacherw")
	t.Setenv("GOWORK", "off")
	app := filepath.Join(base, "app")
	tidy := exec.Command("go", "mod", "tidy")
	tidy.Dir = app
	if out, err := tidy.CombinedOutput(); err != nil {
		t.Fatalf("tidy: %v\n%s", err, out)
	}
	before, err := os.ReadFile(filepath.Join(app, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	runArgs := []string{"-include-packages=example.com/payments,example.com/cached", "."}
	options, err := ParseListOptions("run", runArgs)
	if err != nil {
		t.Fatal(err)
	}
	build, err := PrepareBuild(app, options)
	if err != nil {
		t.Fatal(err)
	}
	defer build.Cleanup()
	if build.ModFile == "" {
		t.Fatal("module cache library did not get a generated go.mod")
	}
	run := exec.Command("go", build.Args("run", runArgs)...)
	run.Dir = app
	run.Env = append(os.Environ(), build.Env...)
	out, err := run.CombinedOutput()
	if err != nil || !strings.Contains(string(out), "library capture passed") {
		t.Fatalf("run: %v\n%s", err, out)
	}
	after, err := os.ReadFile(filepath.Join(app, "go.mod"))
	if err != nil {
		t.Fatal(err)
	}
	if string(before) != string(after) {
		t.Fatal("go.mod changed")
	}
	if _, _, err := OverlayWithOptions(app, options); err == nil {
		t.Fatal("OverlayWithOptions accepted a module cache library")
	}
	options.IncludePackages = []string{"example.com/payments"}
	overlay, cleanup, err := OverlayWithOptions(app, options)
	if err != nil {
		t.Fatal(err)
	}
	cleanup()
	if overlay == "" {
		t.Fatal("no overlay")
	}
	options.IncludePackages = []string{SDKImport}
	if _, err := PrepareBuild(app, options); err == nil {
		t.Fatal("SDK accepted")
	}
	writeTree(t, base, map[string]string{"go.work": "go 1.26.0\n\nuse ./app\n"})
	t.Setenv("GOWORK", filepath.Join(base, "go.work"))
	options.IncludePackages = []string{"example.com/payments", "example.com/cached"}
	workspace, err := PrepareBuild(app, options)
	if err != nil {
		t.Fatal(err)
	}
	defer workspace.Cleanup()
	if workspace.ModFile != "" || len(workspace.Env) != 1 || !strings.HasPrefix(workspace.Env[0], "GOWORK=") {
		t.Fatalf("workspace build=%#v", workspace)
	}
	run = exec.Command("go", workspace.Args("run", runArgs)...)
	run.Dir = app
	run.Env = append(os.Environ(), workspace.Env...)
	if out, err := run.CombinedOutput(); err != nil || !strings.Contains(string(out), "library capture passed") {
		t.Fatalf("workspace run: %v\n%s", err, out)
	}
}

const libraryApplication = `package main

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync"

	bitfab "github.com/Project-White-Rabbit/bitfab-go"
	"example.com/cached"
	"example.com/payments"
)

func helper(x int) int { return x + 1 }

func checkout(ctx context.Context) (any, error) {
	return payments.Charge(cached.Lookup(helper(50))), nil
}

func main() {
	var mu sync.Mutex
	names := map[string]map[string]int{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var reader io.Reader = r.Body
		if r.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(r.Body)
			if err != nil {
				panic(err)
			}
			defer gz.Close()
			reader = gz
		}
		var envelope map[string]any
		_ = json.NewDecoder(reader).Decode(&envelope)
		mu.Lock()
		defer mu.Unlock()
		resources, _ := envelope["resourceSpans"].([]any)
		for _, resource := range resources {
			for _, scope := range resource.(map[string]any)["scopeSpans"].([]any) {
				for _, span := range scope.(map[string]any)["spans"].([]any) {
					for _, attr := range span.(map[string]any)["attributes"].([]any) {
						a := attr.(map[string]any)
						if a["key"] != "bitfab.payload" {
							continue
						}
						var payload map[string]any
						_ = json.Unmarshal([]byte(a["value"].(map[string]any)["stringValue"].(string)), &payload)
						raw, ok := payload["rawSpan"].(map[string]any)
						if !ok {
							continue
						}
						key := raw["trace_id"].(string)
						if names[key] == nil {
							names[key] = map[string]int{}
						}
						names[key][raw["span_data"].(map[string]any)["name"].(string)]++
					}
				}
			}
		}
		_, _ = w.Write([]byte("{}"))
	}))
	defer server.Close()
	client := bitfab.NewClient("test", bitfab.WithServiceURL(server.URL), bitfab.WithSimulationPlan(false), bitfab.WithIncludePackages("example.com/cached"))
	if _, err := client.Trace(context.Background(), "named", checkout, bitfab.TraceOptions{Name: "named", IncludePackages: []string{"example.com/payments"}}); err != nil {
		panic(err)
	}
	if _, err := client.Trace(context.Background(), "unnamed", checkout, bitfab.TraceOptions{Name: "unnamed"}); err != nil {
		panic(err)
	}
	if !client.FlushTraces(5e9) {
		panic("flush failed")
	}
	mu.Lock()
	defer mu.Unlock()
	var named, unnamed map[string]int
	for _, counts := range names {
		if counts["named"] == 1 {
			named = counts
		}
		if counts["unnamed"] == 1 {
			unnamed = counts
		}
	}
	if named["helper"] != 1 || named["Charge"] != 1 || named["Fee"] != 1 || named["Lookup"] != 1 {
		panic(fmt.Sprint("named root missed library calls: ", named))
	}
	if unnamed["helper"] != 1 || unnamed["Charge"] != 0 || unnamed["Fee"] != 0 || unnamed["Lookup"] != 1 {
		panic(fmt.Sprint("unnamed root recorded the wrong library calls: ", unnamed))
	}
	fmt.Println("library capture passed")
}
`
