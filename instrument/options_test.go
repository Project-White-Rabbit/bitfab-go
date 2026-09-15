package instrument

import (
	"reflect"
	"testing"
)

func TestParseListOptions(t *testing.T) {
	cases := []struct {
		name, command         string
		args, flags, patterns []string
		tests                 bool
	}{
		{"tagged run", "run", []string{"-tags", "custom", "-race", "./cmd/app", "--user-argument"}, []string{"-tags=custom", "-race"}, []string{"./cmd/app"}, false},
		{"run file program flags", "run", []string{"main.go", "-tags=program-argument"}, nil, []string{"main.go"}, false},
		{"run files", "run", []string{"main.go", "helper.go", "input.json"}, nil, []string{"main.go", "helper.go"}, false},
		{"scoped build", "build", []string{"-o", "artifact", "-mod=mod", "./cmd/one", "./cmd/two"}, []string{"-mod=mod"}, []string{"./cmd/one", "./cmd/two"}, false},
		{"selected tests", "test", []string{"-run", "TestWork", "./pkg/...", "-tags=integration", "-count=1", "-args", "data.go"}, []string{"-tags=integration"}, []string{"./pkg/..."}, true},
		{"default directory", "test", []string{"-v", "-race=false"}, []string{"-race=false"}, []string{"."}, true},
		{"compiler flags", "build", []string{"-gcflags", "all=-N -l", "-modfile", "custom.mod", "."}, []string{"-gcflags=all=-N -l", "-modfile=custom.mod"}, []string{"."}, false},
	}
	for _, test := range cases {
		t.Run(test.name, func(t *testing.T) {
			original := append([]string(nil), test.args...)
			result, err := ParseListOptions(test.command, test.args)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(result.BuildFlags, test.flags) || !reflect.DeepEqual(result.Patterns, test.patterns) || result.IncludeTests != test.tests {
				t.Fatalf("options=%#v", result)
			}
			if !reflect.DeepEqual(original, test.args) {
				t.Fatal("mutated original go arguments")
			}
		})
	}
}

func TestListOptionsMissingValue(t *testing.T) {
	if _, err := ParseListOptions("run", []string{"-tags"}); err == nil {
		t.Fatal("accepted missing tag value")
	}
}

func TestListOptionsDirectory(t *testing.T) {
	result, err := ParseListOptions("run", []string{"-C", "app", "."})
	if err != nil {
		t.Fatal(err)
	}
	if result.directory("/work") != "/work/app" {
		t.Fatalf("directory=%q", result.directory("/work"))
	}
}

func TestListOptionsComposesCallerOverlay(t *testing.T) {
	args := []string{"-overlay", "custom.json", "main.go", "-overlay=program-value"}
	result, err := ParseListOptions("run", args)
	if err != nil {
		t.Fatal(err)
	}
	if result.OverlayPath != "custom.json" || !reflect.DeepEqual(result.GoArgs(args), []string{"main.go", "-overlay=program-value"}) {
		t.Fatalf("overlay selection=%#v args=%#v", result, result.GoArgs(args))
	}
}
