package instrument

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestOverlaySelectsTaggedPackageWithoutUnrelatedTests(t *testing.T) {
	dir := t.TempDir()
	sdk, err := filepath.Abs("..")
	if err != nil {
		t.Fatal(err)
	}
	sources := map[string]string{
		"go.mod":                 "module example.com/selection\n\ngo 1.25.0\n\nrequire " + SDKImport + " v0.0.0\nreplace " + SDKImport + " => " + sdk + "\n",
		"broken/unrelated.go":    "package broken\nthis is not Go\n",
		"cmd/app/main.go":        "package main\nimport bitfab \"" + SDKImport + "\"\nfunc main(){ _=bitfab.Version; helper() }\n",
		"cmd/app/default.go":     "//go:build !custom\n\npackage main\nfunc helper() int{return 1}\n",
		"custom_replacement.txt": "//go:build custom\n\npackage main\nfunc helper() int{return 99}\n",
		"cmd/app/custom.go":      "//go:build custom\n\npackage main\nfunc helper() int{return 2}\n",
		"cmd/app/broken_test.go": "package main\nthis test intentionally does not compile\n",
	}
	for path, source := range sources {
		target := filepath.Join(dir, path)
		if err := os.MkdirAll(filepath.Dir(target), 0700); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(target, []byte(source), 0600); err != nil {
			t.Fatal(err)
		}
	}
	overlayInput := filepath.Join(dir, "source-overlay.json")
	inputJSON, _ := json.Marshal(map[string]any{"Replace": map[string]string{filepath.Join(dir, "cmd/app/custom.go"): filepath.Join(dir, "custom_replacement.txt")}})
	if err := os.WriteFile(overlayInput, inputJSON, 0600); err != nil {
		t.Fatal(err)
	}
	options, err := ParseListOptions("run", []string{"-tags=custom", "-mod=mod", "-overlay=" + overlayInput, "./cmd/app", "program-argument"})
	if err != nil {
		t.Fatal(err)
	}
	overlay, cleanup, err := OverlayWithOptions(dir, options)
	if err != nil {
		t.Fatal(err)
	}
	defer cleanup()
	var mapping struct{ Replace map[string]string }
	data, err := os.ReadFile(overlay)
	if err != nil {
		t.Fatal(err)
	}
	if err = json.Unmarshal(data, &mapping); err != nil {
		t.Fatal(err)
	}
	customFound := false
	for source, generated := range mapping.Replace {
		if strings.HasSuffix(source, "default.go") || strings.Contains(source, "broken") {
			t.Fatalf("selected unrelated source: %s", source)
		}
		if strings.HasSuffix(source, "custom.go") {
			customFound = true
			content, _ := os.ReadFile(generated)
			if !strings.Contains(string(content), "return 99") {
				t.Fatal("user overlay was not preserved")
			}
			if !strings.Contains(string(content), "EnterAutoNode") {
				t.Fatal("tagged helper was not instrumented")
			}
		}
	}
	if !customFound {
		t.Fatal("tagged helper missing from overlay")
	}
	command := exec.Command("go", "run", "-tags=custom", "-mod=mod", "-overlay", overlay, "./cmd/app")
	command.Dir = dir
	if output, err := command.CombinedOutput(); err != nil {
		t.Fatalf("tagged run: %v\n%s", err, output)
	}
}
