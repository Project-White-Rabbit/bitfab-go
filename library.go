package bitfab

import (
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
)

const sdkModulePath = "github.com/Project-White-Rabbit/bitfab-go"

type buildModuleSet struct {
	main    string
	modules map[string]bool
	known   bool
}

var readBuildModules = sync.OnceValue(func() buildModuleSet {
	info, ok := debug.ReadBuildInfo()
	if !ok {
		return buildModuleSet{}
	}
	set := buildModuleSet{main: info.Main.Path, modules: map[string]bool{}, known: true}
	for _, dep := range info.Deps {
		set.modules[dep.Path] = true
	}
	return set
})

var currentBuildModules = readBuildModules

var libraryModuleCache sync.Map

func validateIncludedLibraries(libraries []string) error {
	modules := currentBuildModules()
	for _, library := range libraries {
		if strings.TrimSpace(library) == "" {
			return fmt.Errorf("bitfab: IncludePackages cannot contain an empty module path")
		}
		if library == sdkModulePath || strings.HasPrefix(library, sdkModulePath+"/") {
			return fmt.Errorf("bitfab: IncludePackages cannot name the Bitfab SDK (%s)", library)
		}
		if modules.modules[library] {
			continue
		}
		if first, _, _ := strings.Cut(library, "/"); !strings.Contains(first, ".") {
			return fmt.Errorf("bitfab: IncludePackages names %q, which is not a module this program depends on; the standard library cannot be traced", library)
		}
		if modules.known && library != modules.main {
			warnOnce("include-library-missing:"+library, fmt.Sprintf("IncludePackages names %q, but this program does not depend on that module; ignoring it.", library))
		}
	}
	return nil
}

func symbolPackage(symbol string) string {
	slash := strings.LastIndex(symbol, "/")
	dot := strings.IndexByte(symbol[slash+1:], '.')
	if dot < 0 {
		return strings.ReplaceAll(symbol, "%2e", ".")
	}
	return strings.ReplaceAll(symbol[:slash+1+dot], "%2e", ".")
}

func libraryModule(symbol string) string {
	pkg := symbolPackage(normalizeAutoSymbol(symbol))
	if cached, ok := libraryModuleCache.Load(pkg); ok {
		return cached.(string)
	}
	modules := currentBuildModules()
	module := ""
	for candidate := pkg; candidate != "" && candidate != "."; {
		if candidate == modules.main {
			break
		}
		if modules.modules[candidate] {
			module = candidate
			break
		}
		slash := strings.LastIndex(candidate, "/")
		if slash < 0 {
			break
		}
		candidate = candidate[:slash]
	}
	libraryModuleCache.Store(pkg, module)
	return module
}

func includedLibrarySet(lists ...[]string) map[string]bool {
	set := map[string]bool{}
	for _, list := range lists {
		for _, library := range list {
			set[library] = true
		}
	}
	return set
}

func (c *Client) rootLibraries(rootList []string) map[string]bool {
	if !c.includePackagesSet {
		return nil
	}
	return includedLibrarySet(c.includePackages, rootList)
}

func (root *autoRoot) recordsLibrary(library string) bool {
	return root.libraries == nil || root.libraries[library]
}

func (scope *autoScope) includesLibrary(library string) bool {
	for _, frame := range scope.frames {
		if frame.root.recordsLibrary(library) {
			return true
		}
	}
	return false
}
