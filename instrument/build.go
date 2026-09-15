package instrument

import (
	"bytes"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/importer"
	"go/parser"
	"go/token"
	"go/types"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

type listedPackage struct {
	Dir, ImportPath, Name, Export      string
	GoFiles, CgoFiles, CompiledGoFiles []string
	ImportMap                          map[string]string
	Module                             *struct{ Main bool }
}

// Overlay compiles unchanged first-party packages for type information, then
// writes instrumented sources and a Go overlay into a temporary directory.
// The caller must invoke cleanup after the Go command finishes.
func Overlay(dir string) (overlay string, cleanup func(), err error) {
	return OverlayWithOptions(dir, ListOptions{Patterns: []string{"./..."}, IncludeTests: true})
}

// OverlayWithOptions uses the same package selection and build flags as the Go
// command that will consume the generated sources.
func OverlayWithOptions(dir string, options ListOptions) (overlay string, cleanup func(), err error) {
	cmd := exec.Command("go", options.Args()...)
	cmd.Dir = options.directory(dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		return "", nil, fmt.Errorf("list first-party packages: %w\n%s", err, stderr.String())
	}
	var packages []listedPackage
	exports := map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var pkg listedPackage
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			return "", nil, err
		}
		packages = append(packages, pkg)
		exports[pkg.ImportPath] = pkg.Export
	}
	temp, err := os.MkdirTemp("", "bitfab-instrument-*")
	if err != nil {
		return "", nil, err
	}
	cleanup = func() { _ = os.RemoveAll(temp) }
	succeeded := false
	defer func() {
		if !succeeded {
			cleanup()
		}
	}()
	replacements := map[string]string{}
	sourceOverlays := map[string]string{}
	if options.OverlayPath != "" {
		filename := options.OverlayPath
		if !filepath.IsAbs(filename) {
			filename = filepath.Join(options.directory(dir), filename)
		}
		data, readErr := os.ReadFile(filename)
		if readErr != nil {
			return "", nil, readErr
		}
		var existing struct{ Replace map[string]string }
		if parseErr := json.Unmarshal(data, &existing); parseErr != nil {
			return "", nil, parseErr
		}
		for source, target := range existing.Replace {
			if !filepath.IsAbs(source) {
				source = filepath.Join(options.directory(dir), source)
			}
			if target != "" && !filepath.IsAbs(target) {
				target = filepath.Join(options.directory(dir), target)
			}
			sourceOverlays[source] = target
			replacements[source] = target
		}
	}
	parseSource := func(fset *token.FileSet, name string) (*ast.File, error) {
		if replacement, ok := sourceOverlays[name]; ok {
			data, err := os.ReadFile(replacement)
			if err != nil {
				return nil, err
			}
			return parser.ParseFile(fset, name, data, parser.ParseComments)
		}
		return parser.ParseFile(fset, name, nil, parser.ParseComments)
	}
	nextFile := 0
	for _, pkg := range packages {
		importPath := strings.Split(pkg.ImportPath, " [")[0]
		if pkg.Module == nil || !pkg.Module.Main || importPath == SDKImport || strings.HasPrefix(importPath, SDKImport+"/") || strings.HasSuffix(importPath, ".test") {
			continue
		}
		fset := token.NewFileSet()
		var files []*ast.File
		compiledFiles := pkg.CompiledGoFiles
		if len(compiledFiles) == 0 {
			compiledFiles = pkg.GoFiles
		}
		for _, name := range compiledFiles {
			if !filepath.IsAbs(name) {
				name = filepath.Join(pkg.Dir, name)
			}
			file, err := parseSource(fset, name)
			if err != nil {
				return "", nil, err
			}
			files = append(files, file)
		}
		if len(files) == 0 {
			continue
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}
		arch := os.Getenv("GOARCH")
		if arch == "" {
			arch = runtime.GOARCH
		}
		config := types.Config{Sizes: types.SizesFor("gc", arch), Importer: importer.ForCompiler(fset, "gc", func(path string) (io.ReadCloser, error) {
			if mapped := pkg.ImportMap[path]; mapped != "" {
				path = mapped
			}
			return os.Open(exports[path])
		})}
		if _, err := config.Check(pkg.ImportPath, fset, files, info); err != nil {
			return "", nil, fmt.Errorf("type-check %s: %w", pkg.ImportPath, err)
		}
		compiledGoCalls := map[string][]*ast.GoStmt{}
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				if call, ok := n.(*ast.GoStmt); ok {
					name := canonicalSourcePath(fset.Position(call.Pos()).Filename)
					compiledGoCalls[name] = append(compiledGoCalls[name], call)
				}
				return true
			})
		}
		for _, name := range append(append([]string{}, pkg.GoFiles...), pkg.CgoFiles...) {
			if !filepath.IsAbs(name) {
				name = filepath.Join(pkg.Dir, name)
			}
			file, err := parseSource(fset, name)
			if err != nil {
				return "", nil, err
			}
			if ast.IsGenerated(file) {
				continue
			}
			mappedInfo, err := mapCompiledCallTypes(file, compiledGoCalls[canonicalSourcePath(name)], info)
			if err != nil {
				return "", nil, fmt.Errorf("map compiler types for %s: %w", name, err)
			}
			transformed, err := Transform(fset, file, importPath, mappedInfo)
			if err != nil {
				return "", nil, fmt.Errorf("instrument %s: %w", name, err)
			}
			target := filepath.Join(temp, fmt.Sprintf("%d.go", nextFile))
			nextFile++
			if err := os.WriteFile(target, transformed, 0600); err != nil {
				return "", nil, err
			}
			replacements[name] = target
		}
	}
	overlay = filepath.Join(temp, "overlay.json")
	encoded, err := json.Marshal(struct{ Replace map[string]string }{replacements})
	if err != nil {
		return "", nil, err
	}
	if err = os.WriteFile(overlay, encoded, 0600); err != nil {
		return "", nil, err
	}
	succeeded = true
	return overlay, cleanup, nil
}

func canonicalSourcePath(name string) string {
	if resolved, err := filepath.EvalSymlinks(name); err == nil {
		return resolved
	}
	return filepath.Clean(name)
}

// Cgo rewrites C expressions before Go type-checking. Match the unchanged Go
// launch order back to the original source so we never fake C types or emit
// compiler-generated cgo source into the user's files.
func mapCompiledCallTypes(file *ast.File, compiled []*ast.GoStmt, info *types.Info) (*types.Info, error) {
	mapped := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}}
	var calls []*ast.GoStmt
	ast.Inspect(file, func(n ast.Node) bool {
		if call, ok := n.(*ast.GoStmt); ok {
			calls = append(calls, call)
		}
		return true
	})
	if len(calls) != len(compiled) {
		return nil, fmt.Errorf("source has %d goroutine launches, compiler has %d", len(calls), len(compiled))
	}
	for i, call := range calls {
		other := compiled[i].Call
		mapped.Types[call.Call.Fun] = info.Types[other.Fun]
		if id, ok := call.Call.Fun.(*ast.Ident); ok {
			if compiledID, ok := other.Fun.(*ast.Ident); ok {
				mapped.Uses[id] = info.Uses[compiledID]
			}
		}
		if len(call.Call.Args) == 1 && len(other.Args) == 1 {
			mapped.Types[call.Call.Args[0]] = info.Types[other.Args[0]]
		}
	}
	return mapped, nil
}
