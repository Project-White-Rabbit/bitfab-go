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
	Deps                               []string
	Module                             *struct {
		Path, Version, Dir, GoVersion string
		Main                          bool
	}
}

// Build is an instrumented Go build prepared by PrepareBuild.
type Build struct {
	// Overlay is the -overlay file that holds the instrumented sources.
	Overlay string
	// ModFile, when set, is a -modfile that points each traced dependency from
	// the module cache at an instrumented copy, since Go refuses overlays of
	// files in the module cache.
	ModFile string
	// Env holds extra environment entries for the Go command, such as a GOWORK
	// workspace file with the same replacements as ModFile.
	Env []string
	// Cleanup removes the generated files once the Go command finishes.
	Cleanup func()
	options ListOptions
}

// Args returns the arguments for the Go command that consumes the build,
// starting with command.
func (b *Build) Args(command string, args []string) []string {
	result := []string{command, "-overlay", b.Overlay}
	if b.ModFile != "" {
		result = append(result, "-modfile="+b.ModFile)
	}
	for i, arg := range args {
		if b.options.overlayIndices[i] || b.ModFile != "" && b.options.modFileIndices[i] {
			continue
		}
		result = append(result, arg)
	}
	return result
}

// Overlay compiles unchanged first-party packages for type information, then
// writes instrumented sources and a Go overlay into a temporary directory.
// The caller must invoke cleanup after the Go command finishes.
func Overlay(dir string) (overlay string, cleanup func(), err error) {
	return OverlayWithOptions(dir, ListOptions{Patterns: []string{"./..."}, IncludeTests: true})
}

// OverlayWithOptions uses the same package selection and build flags as the Go
// command that will consume the generated sources. It returns an error when a
// listed library comes from the module cache; use PrepareBuild for those.
func OverlayWithOptions(dir string, options ListOptions) (overlay string, cleanup func(), err error) {
	build, err := PrepareBuild(dir, options)
	if err != nil {
		return "", nil, err
	}
	if build.ModFile != "" || len(build.Env) > 0 {
		build.Cleanup()
		return "", nil, fmt.Errorf("bitfab-instrument: a listed library comes from the module cache; use PrepareBuild")
	}
	return build.Overlay, build.Cleanup, nil
}

// PrepareBuild writes instrumented sources for the main module and the
// options' IncludePackages, using the same package selection and build flags
// as the Go command that will consume them. The caller must invoke Cleanup
// after the Go command finishes.
func PrepareBuild(dir string, options ListOptions) (build *Build, err error) {
	cmd := exec.Command("go", options.Args()...)
	cmd.Dir = options.directory(dir)
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	data, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("list first-party packages: %w\n%s", err, stderr.String())
	}
	included, err := includedLibrarySet(options.IncludePackages)
	if err != nil {
		return nil, err
	}
	var packages []listedPackage
	exports := map[string]string{}
	decoder := json.NewDecoder(bytes.NewReader(data))
	for {
		var pkg listedPackage
		if err := decoder.Decode(&pkg); err == io.EOF {
			break
		} else if err != nil {
			return nil, err
		}
		packages = append(packages, pkg)
		exports[pkg.ImportPath] = pkg.Export
	}
	selected, err := selectPackages(packages, included)
	if err != nil {
		return nil, err
	}
	temp, err := os.MkdirTemp("", "bitfab-instrument-*")
	if err != nil {
		return nil, err
	}
	cleanup := func() { _ = os.RemoveAll(temp) }
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
			return nil, readErr
		}
		var existing struct{ Replace map[string]string }
		if parseErr := json.Unmarshal(data, &existing); parseErr != nil {
			return nil, parseErr
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
	modCache := ""
	copies := map[string]string{}
	var copiedModules []copiedModule
	nextFile := 0
	for _, pkg := range selected {
		importPath := strings.Split(pkg.ImportPath, " [")[0]
		relocate := func(name string) string { return name }
		if !pkg.Module.Main {
			if modCache == "" {
				if modCache, err = goEnv(options.directory(dir), "GOMODCACHE"); err != nil {
					return nil, err
				}
			}
			if within(canonicalSourcePath(pkg.Module.Dir), canonicalSourcePath(modCache)) {
				copyDir, ok := copies[pkg.Module.Dir]
				if !ok {
					copyDir = filepath.Join(temp, "modules", fmt.Sprint(len(copies)))
					if err := copyModule(pkg.Module.Dir, pkg.Module.Path, copyDir); err != nil {
						return nil, err
					}
					copies[pkg.Module.Dir] = copyDir
					copiedModules = append(copiedModules, copiedModule{path: pkg.Module.Path, version: pkg.Module.Version, dir: copyDir})
				}
				moduleDir := pkg.Module.Dir
				relocate = func(name string) string {
					if relative, err := filepath.Rel(moduleDir, name); err == nil && !strings.HasPrefix(relative, "..") {
						return filepath.Join(copyDir, relative)
					}
					return name
				}
			}
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
				return nil, err
			}
			files = append(files, file)
		}
		if len(files) == 0 {
			continue
		}
		info := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
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
			return nil, fmt.Errorf("type-check %s: %w", pkg.ImportPath, err)
		}
		compiledGoCalls := map[string][]*ast.GoStmt{}
		compiledJevCalls := map[string][]*ast.SelectorExpr{}
		for _, file := range files {
			ast.Inspect(file, func(n ast.Node) bool {
				if call, ok := n.(*ast.GoStmt); ok {
					name := canonicalSourcePath(fset.Position(call.Pos()).Filename)
					compiledGoCalls[name] = append(compiledGoCalls[name], call)
				}
				if selector := jevCallSelector(n); selector != nil {
					name := canonicalSourcePath(fset.Position(selector.Pos()).Filename)
					compiledJevCalls[name] = append(compiledJevCalls[name], selector)
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
				return nil, err
			}
			if ast.IsGenerated(file) {
				continue
			}
			mappedInfo, err := mapCompiledCallTypes(file, compiledGoCalls[canonicalSourcePath(name)], compiledJevCalls[canonicalSourcePath(name)], info)
			if err != nil {
				return nil, fmt.Errorf("map compiler types for %s: %w", name, err)
			}
			transformed, err := Transform(fset, file, importPath, mappedInfo)
			if err != nil {
				return nil, fmt.Errorf("instrument %s: %w", name, err)
			}
			if pkg.Module != nil && !pkg.Module.Main {
				if transformed, err = raiseFileLanguage(transformed, pkg.Module.GoVersion); err != nil {
					return nil, fmt.Errorf("instrument %s: %w", name, err)
				}
			}
			target := filepath.Join(temp, fmt.Sprintf("%d.go", nextFile))
			nextFile++
			if err := os.WriteFile(target, transformed, 0600); err != nil {
				return nil, err
			}
			replacements[relocate(name)] = target
		}
	}
	build = &Build{Cleanup: cleanup, options: options}
	if len(copiedModules) > 0 {
		if err := redirectModules(options.directory(dir), options, temp, copiedModules, build); err != nil {
			return nil, err
		}
	}
	overlay := filepath.Join(temp, "overlay.json")
	encoded, err := json.Marshal(struct{ Replace map[string]string }{replacements})
	if err != nil {
		return nil, err
	}
	if err = os.WriteFile(overlay, encoded, 0600); err != nil {
		return nil, err
	}
	build.Overlay = overlay
	succeeded = true
	return build, nil
}

func isSDKPath(path string) bool {
	return path == SDKImport || strings.HasPrefix(path, SDKImport+"/")
}

func includedLibrarySet(libraries []string) (map[string]bool, error) {
	included := map[string]bool{}
	for _, library := range libraries {
		library = strings.TrimSpace(library)
		if library == "" {
			continue
		}
		if isSDKPath(library) {
			return nil, fmt.Errorf("bitfab-instrument: -include-packages cannot name the Bitfab SDK (%s)", library)
		}
		included[library] = true
	}
	return included, nil
}

func selectPackages(packages []listedPackage, included map[string]bool) ([]listedPackage, error) {
	sdkDeps := map[string]bool{}
	modules := map[string]bool{}
	for _, pkg := range packages {
		if pkg.Module != nil {
			modules[pkg.Module.Path] = true
		}
		if isSDKPath(strings.Split(pkg.ImportPath, " [")[0]) {
			for _, dep := range pkg.Deps {
				sdkDeps[dep] = true
			}
		}
	}
	for library := range included {
		if modules[library] {
			continue
		}
		if first, _, _ := strings.Cut(library, "/"); !strings.Contains(first, ".") {
			return nil, fmt.Errorf("bitfab-instrument: -include-packages names %q, which is not a module in this build; the standard library cannot be traced", library)
		}
		fmt.Fprintf(os.Stderr, "bitfab-instrument: -include-packages names %q, but no package in this build comes from that module; ignoring it\n", library)
	}
	var selected []listedPackage
	for _, pkg := range packages {
		importPath := strings.Split(pkg.ImportPath, " [")[0]
		if pkg.Module == nil || isSDKPath(importPath) || strings.HasSuffix(importPath, ".test") {
			continue
		}
		if pkg.Module.Main {
			selected = append(selected, pkg)
			continue
		}
		if !included[pkg.Module.Path] {
			continue
		}
		if sdkDeps[importPath] {
			return nil, fmt.Errorf("bitfab-instrument: -include-packages names %s, but the Bitfab SDK imports its package %s, so tracing it would create an import cycle", pkg.Module.Path, importPath)
		}
		selected = append(selected, pkg)
	}
	return selected, nil
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
func mapCompiledCallTypes(file *ast.File, compiled []*ast.GoStmt, compiledJev []*ast.SelectorExpr, info *types.Info) (*types.Info, error) {
	mapped := &types.Info{Types: map[ast.Expr]types.TypeAndValue{}, Uses: map[*ast.Ident]types.Object{}, Selections: map[*ast.SelectorExpr]*types.Selection{}}
	var calls []*ast.GoStmt
	var jevCalls []*ast.SelectorExpr
	ast.Inspect(file, func(n ast.Node) bool {
		if call, ok := n.(*ast.GoStmt); ok {
			calls = append(calls, call)
		}
		if selector := jevCallSelector(n); selector != nil {
			jevCalls = append(jevCalls, selector)
		}
		return true
	})
	if len(jevCalls) != len(compiledJev) {
		return nil, fmt.Errorf("source has %d %s calls, compiler has %d", len(jevCalls), jevMethodName, len(compiledJev))
	}
	for i, selector := range jevCalls {
		if selection := info.Selections[compiledJev[i]]; selection != nil {
			mapped.Selections[selector] = selection
		}
	}
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
