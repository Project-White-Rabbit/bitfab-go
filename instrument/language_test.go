package instrument

import (
	"strings"
	"testing"
)

func TestRaiseFileLanguage(t *testing.T) {
	cases := []struct {
		name, module, source, want string
	}{
		{"module new enough", "1.18", "package p\n", "package p\n"},
		{"no go directive", "", "package p\n", "//go:build go1.21\n\npackage p\n"},
		{"old module", "go1.16", "// Copyright\n\npackage p\n", "//go:build go1.21\n\n// Copyright\n\npackage p\n"},
		{"plus build lines", "1.17", "// +build linux darwin\n// +build !cgo\n\npackage p\n", "//go:build (linux || darwin) && !cgo && go1.21\n\n\npackage p\n"},
		{"go build line", "1.17", "//go:build linux\n// +build linux\n\npackage p\n", "//go:build linux && go1.21\n\n\npackage p\n"},
		{"file already new enough", "1.16", "//go:build go1.18\n\npackage p\n", "//go:build go1.18\n\npackage p\n"},
		{"constraint after package ignored", "1.17", "package p\n\n// +build ignore\n", "//go:build go1.21\n\npackage p\n\n// +build ignore\n"},
	}
	for _, tc := range cases {
		got, err := raiseFileLanguage([]byte(tc.source), tc.module)
		if err != nil {
			t.Fatalf("%s: %v", tc.name, err)
		}
		if string(got) != tc.want {
			t.Errorf("%s:\ngot  %q\nwant %q", tc.name, got, tc.want)
		}
	}
	if _, err := raiseFileLanguage([]byte("//go:build (\n\npackage p\n"), "1.16"); err == nil || !strings.Contains(err.Error(), "build constraint") {
		t.Fatalf("bad constraint error = %v", err)
	}
}
