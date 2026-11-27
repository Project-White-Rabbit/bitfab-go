package instrument

import (
	"bytes"
	"fmt"
	"go/build/constraint"
	"go/parser"
	"go/token"
	"go/version"
	"strings"
)

const (
	generatedCodeLanguage = "go1.18"
	raisedFileLanguage    = "go1.21"
	defaultModuleLanguage = "go1.16"
)

func raiseFileLanguage(source []byte, moduleGoVersion string) ([]byte, error) {
	moduleLanguage := defaultModuleLanguage
	if moduleGoVersion != "" {
		moduleLanguage = "go" + strings.TrimPrefix(moduleGoVersion, "go")
	}
	if version.Compare(moduleLanguage, generatedCodeLanguage) >= 0 {
		return source, nil
	}
	fset := token.NewFileSet()
	file, err := parser.ParseFile(fset, "", source, parser.PackageClauseOnly|parser.ParseComments)
	if err != nil {
		return nil, err
	}
	if file.GoVersion != "" && version.Compare(file.GoVersion, generatedCodeLanguage) >= 0 {
		return source, nil
	}
	var goBuild constraint.Expr
	var plusBuild []constraint.Expr
	var removed [][2]int
	for _, group := range file.Comments {
		if group.Pos() >= file.Package {
			break
		}
		for _, comment := range group.List {
			if !constraint.IsGoBuild(comment.Text) && !constraint.IsPlusBuild(comment.Text) {
				continue
			}
			expr, err := constraint.Parse(comment.Text)
			if err != nil {
				return nil, fmt.Errorf("parse build constraint %q: %w", comment.Text, err)
			}
			if constraint.IsGoBuild(comment.Text) {
				goBuild = expr
			} else {
				plusBuild = append(plusBuild, expr)
			}
			start := fset.Position(comment.Pos()).Offset
			end := fset.Position(comment.End()).Offset
			if end < len(source) && source[end] == '\n' {
				end++
			}
			removed = append(removed, [2]int{start, end})
		}
	}
	existing := goBuild
	if existing == nil {
		for _, expr := range plusBuild {
			if existing == nil {
				existing = expr
			} else {
				existing = &constraint.AndExpr{X: existing, Y: expr}
			}
		}
	}
	var raised constraint.Expr = &constraint.TagExpr{Tag: raisedFileLanguage}
	if existing != nil {
		raised = &constraint.AndExpr{X: existing, Y: raised}
	}
	var out bytes.Buffer
	out.WriteString("//go:build " + raised.String() + "\n\n")
	previous := 0
	for _, span := range removed {
		out.Write(source[previous:span[0]])
		previous = span[1]
	}
	out.Write(source[previous:])
	return out.Bytes(), nil
}
