package instrument

import (
	"fmt"
	"path/filepath"
	"strings"
)

// ListOptions selects the same source files that the requested Go command builds.
type ListOptions struct {
	OverlayPath    string
	overlayIndices map[int]bool
	Directory      string
	BuildFlags     []string
	Patterns       []string
	IncludeTests   bool
}

// Args returns go list arguments for compiler export data and selected packages.
func (o ListOptions) Args() []string {
	args := []string{"list", "-deps", "-compiled", "-export", "-json"}
	if o.IncludeTests {
		args = append(args, "-test")
	}
	args = append(args, o.BuildFlags...)
	patterns := o.Patterns
	if len(patterns) == 0 {
		patterns = []string{"."}
	}
	return append(args, patterns...)
}

var selectionValueFlags = map[string]bool{
	"asmflags": true, "buildmode": true, "compiler": true, "covermode": true, "coverpkg": true,
	"gccgoflags": true, "gcflags": true, "installsuffix": true, "ldflags": true, "mod": true,
	"modfile": true, "overlay": true, "p": true, "pgo": true, "pkgdir": true, "tags": true, "toolexec": true,
}
var selectionBooleanFlags = map[string]bool{"a": true, "race": true, "msan": true, "asan": true, "cover": true, "trimpath": true, "buildvcs": true}
var commandValueFlags = map[string]bool{
	"o": true, "exec": true, "bench": true, "benchtime": true, "blockprofile": true, "blockprofilerate": true,
	"count": true, "coverprofile": true, "cpu": true, "cpuprofile": true, "fuzz": true, "fuzztime": true, "fuzzminimizetime": true,
	"list": true, "memprofile": true, "memprofilerate": true, "mutexprofile": true, "mutexprofilefraction": true,
	"outputdir": true, "parallel": true, "run": true, "shuffle": true, "skip": true, "timeout": true, "trace": true, "vet": true,
}

// ParseListOptions separates compiler selection from output flags and program arguments.
// The original argument slice remains untouched for the actual Go invocation.
func ParseListOptions(command string, args []string) (ListOptions, error) {
	options := ListOptions{IncludeTests: command == "test"}
	runFiles := false
	for i := 0; i < len(args); i++ {
		arg := args[i]
		if command == "run" && runFiles && len(options.Patterns) > 0 && !strings.HasSuffix(arg, ".go") {
			break
		}
		if arg == "-args" && command == "test" {
			break
		}
		if arg == "--" {
			if command == "run" {
				if i+1 < len(args) {
					options.Patterns = append(options.Patterns, args[i+1])
				}
				break
			}
			options.Patterns = append(options.Patterns, args[i+1:]...)
			break
		}
		if !strings.HasPrefix(arg, "-") || arg == "-" {
			if command == "run" {
				if len(options.Patterns) == 0 {
					runFiles = strings.HasSuffix(arg, ".go")
					options.Patterns = append(options.Patterns, arg)
					if !runFiles {
						break
					}
					continue
				}
				if runFiles && strings.HasSuffix(arg, ".go") {
					options.Patterns = append(options.Patterns, arg)
					continue
				}
				break
			}
			options.Patterns = append(options.Patterns, arg)
			continue
		}
		flagIndex := i
		flag, value, hasValue := strings.Cut(strings.TrimLeft(arg, "-"), "=")
		if flag == "C" {
			if !hasValue {
				if i+1 >= len(args) {
					return options, fmt.Errorf("bitfab-instrument: -C requires a directory")
				}
				i++
				value = args[i]
			}
			options.Directory = value
			continue
		}
		wantsValue := selectionValueFlags[flag] || commandValueFlags[flag]
		if wantsValue && !hasValue {
			if i+1 >= len(args) {
				return options, fmt.Errorf("bitfab-instrument: flag -%s requires a value", flag)
			}
			i++
			value = args[i]
			hasValue = true
		}
		if flag == "overlay" {
			options.OverlayPath = value
			if options.overlayIndices == nil {
				options.overlayIndices = map[int]bool{}
			}
			options.overlayIndices[flagIndex] = true
			if i != flagIndex {
				options.overlayIndices[i] = true
			}
		}
		if selectionValueFlags[flag] || selectionBooleanFlags[flag] {
			forwarded := "-" + flag
			if hasValue {
				forwarded += "=" + value
			}
			options.BuildFlags = append(options.BuildFlags, forwarded)
		}
	}
	if len(options.Patterns) == 0 {
		options.Patterns = []string{"."}
	}
	return options, nil
}

func (o ListOptions) directory(base string) string {
	if o.Directory == "" {
		return base
	}
	if filepath.IsAbs(o.Directory) {
		return o.Directory
	}
	return filepath.Join(base, o.Directory)
}

// GoArgs preserves the caller's invocation except overlays merged into the generated overlay.
func (o ListOptions) GoArgs(args []string) []string {
	result := make([]string, 0, len(args))
	for i, arg := range args {
		if !o.overlayIndices[i] {
			result = append(result, arg)
		}
	}
	return result
}
