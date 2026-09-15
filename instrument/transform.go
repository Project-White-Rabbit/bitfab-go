// Package instrument rewrites first-party Go functions for Bitfab subtree capture.
package instrument

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/format"
	"go/parser"
	"go/token"
	"go/types"
	"hash/fnv"
	"strconv"
	"strings"
)

const SDKImport = "github.com/Project-White-Rabbit/bitfab-go"

// Transform instruments named functions and methods. info must contain the
// original package's expression types to preserve typed goroutine launches.
func Transform(fset *token.FileSet, file *ast.File, importPath string, info *types.Info) ([]byte, error) {
	if file.Name.Name == "main" {
		importPath = "main"
	}
	alias := "__bitfab"
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if p == SDKImport {
			alias = "bitfab"
			if imp.Name != nil {
				alias = imp.Name.Name
			}
			if alias == "." || alias == "_" {
				return nil, fmt.Errorf("automatic instrumentation requires a named Bitfab import")
			}
		}
	}
	used := map[string]bool{}
	ast.Inspect(file, func(n ast.Node) bool {
		if id, ok := n.(*ast.Ident); ok {
			used[id.Name] = true
		}
		return true
	})
	if alias == "__bitfab" {
		for used[alias] {
			alias += "_"
		}
	}
	fresh := func(base string) string {
		for used[base] {
			base += "_"
		}
		used[base] = true
		return base
	}
	var helperDecls []ast.Decl
	fileHash := fnv.New64a()
	_, _ = fileHash.Write([]byte(fset.Position(file.Package).Filename))
	helperPrefix := fmt.Sprintf("__bitfabGo_%x", fileHash.Sum64())
	goReplacements := map[*ast.GoStmt]ast.Stmt{}
	var transformErr error
	ast.Inspect(file, func(n ast.Node) bool {
		goStmt, ok := n.(*ast.GoStmt)
		if !ok {
			return true
		}
		if selector, ok := goStmt.Call.Fun.(*ast.SelectorExpr); ok {
			if qualifier, ok := selector.X.(*ast.Ident); ok && qualifier.Name == "C" {
				return true
			}
		}
		if info == nil {
			transformErr = fmt.Errorf("goroutine transformation requires Go type information")
			return false
		}
		callType := info.TypeOf(goStmt.Call.Fun)
		if callType == nil {
			return true
		}
		sig, ok := callType.Underlying().(*types.Signature)
		if !ok {
			return true
		}
		if ident, ok := goStmt.Call.Fun.(*ast.Ident); ok {
			if _, builtin := info.Uses[ident].(*types.Builtin); builtin {
				return true
			}
		}
		name := fresh(helperPrefix)
		var tparams, params, args, ftypes, rets []string
		for i := 0; i < sig.Params().Len(); i++ {
			typ := fmt.Sprintf("A%d", i)
			tparams = append(tparams, typ+" any")
			callArg := fmt.Sprintf("a%d", i)
			if sig.Variadic() && i == sig.Params().Len()-1 {
				ftypes = append(ftypes, "..."+typ)
				params = append(params, callArg+" ..."+typ)
				args = append(args, callArg+"...")
			} else {
				ftypes = append(ftypes, typ)
				params = append(params, callArg+" "+typ)
				args = append(args, callArg)
			}
		}
		for i := 0; i < sig.Results().Len(); i++ {
			typ := fmt.Sprintf("R%d", i)
			tparams = append(tparams, typ+" any")
			rets = append(rets, typ)
		}
		tp := ""
		if len(tparams) > 0 {
			tp = "[" + strings.Join(tparams, ",") + "]"
		}
		result := ""
		if len(rets) > 0 {
			result = "(" + strings.Join(rets, ",") + ")"
		}
		params = append([]string{"fn func(" + strings.Join(ftypes, ",") + ")" + result}, params...)
		source := fmt.Sprintf("package p;func %s%s(%s){ snapshot := %s.CaptureAutoContext();go %s.RunAutoContext(snapshot,func(){fn(%s)}) }", name, tp, strings.Join(params, ","), alias, alias, strings.Join(args, ","))
		parsed, err := parser.ParseFile(fset, "generated.go", source, 0)
		if err != nil {
			transformErr = err
			return false
		}
		helperDecls = append(helperDecls, parsed.Decls...)
		original := goStmt.Call.Fun
		originalArgs := goStmt.Call.Args
		goStmt.Call.Fun = ast.NewIdent(name)
		goStmt.Call.Args = append([]ast.Expr{original}, goStmt.Call.Args...)
		if len(originalArgs) == 1 {
			if tuple, ok := info.TypeOf(originalArgs[0]).(*types.Tuple); ok && tuple.Len() > 1 {
				fnName := ast.NewIdent(fresh("__bitfabGoFunction"))
				var values []ast.Expr
				for range tuple.Len() {
					values = append(values, ast.NewIdent(fresh("__bitfabGoArgument")))
				}
				goStmt.Call.Args = append([]ast.Expr{fnName}, values...)
				goReplacements[goStmt] = &ast.BlockStmt{List: []ast.Stmt{
					&ast.AssignStmt{Lhs: []ast.Expr{fnName}, Tok: token.DEFINE, Rhs: []ast.Expr{original}},
					&ast.AssignStmt{Lhs: values, Tok: token.DEFINE, Rhs: originalArgs},
					&ast.ExprStmt{X: goStmt.Call},
				}}
			}
		}
		return true
	})
	if transformErr != nil {
		return nil, transformErr
	}
	// Replace GoStmt nodes with synchronous submissions. Arguments still evaluate
	// on the caller before the generated helper starts the goroutine.
	var replaceGo func(ast.Node)
	replaceGo = func(n ast.Node) {
		ast.Inspect(n, func(node ast.Node) bool {
			switch block := node.(type) {
			case *ast.BlockStmt:
				for i, s := range block.List {
					if g, ok := s.(*ast.GoStmt); ok && strings.HasPrefix(exprName(g.Call.Fun), "__bitfabGo") {
						if replacement := goReplacements[g]; replacement != nil {
							block.List[i] = replacement
						} else {
							block.List[i] = &ast.ExprStmt{X: g.Call}
						}
					}
				}
			case *ast.CaseClause:
				for i, s := range block.Body {
					if g, ok := s.(*ast.GoStmt); ok && strings.HasPrefix(exprName(g.Call.Fun), "__bitfabGo") {
						if replacement := goReplacements[g]; replacement != nil {
							block.Body[i] = replacement
						} else {
							block.Body[i] = &ast.ExprStmt{X: g.Call}
						}
					}
				}
			case *ast.CommClause:
				for i, s := range block.Body {
					if g, ok := s.(*ast.GoStmt); ok && strings.HasPrefix(exprName(g.Call.Fun), "__bitfabGo") {
						if replacement := goReplacements[g]; replacement != nil {
							block.Body[i] = replacement
						} else {
							block.Body[i] = &ast.ExprStmt{X: g.Call}
						}
					}
				}
			}
			return true
		})
	}
	replaceGo(file)
	count := 0
	contextAliases := map[string]bool{}
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if p == "context" {
			a := "context"
			if imp.Name != nil {
				a = imp.Name.Name
			}
			contextAliases[a] = true
		}
	}
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok || fn.Body == nil || fn.Name.Name == "init" {
			continue
		}
		comments := ""
		if fn.Doc != nil {
			for _, comment := range fn.Doc.List {
				comments += comment.Text + "\n"
			}
		}
		if strings.Contains(comments, "bitfab:ignore") || strings.Contains(comments, "go:nosplit") {
			continue
		}
		name := fn.Name.Name
		symbol := importPath + "." + name
		if fn.Recv != nil {
			var buf bytes.Buffer
			_ = format.Node(&buf, fset, fn.Recv.List[0].Type)
			recv := buf.String()
			if strings.HasPrefix(recv, "*") {
				recv = "(" + recv + ")"
			}
			symbol = importPath + "." + recv + "." + name
			name = recv + "." + name
		}
		var inputs, outputs []string
		ctx := "nil"
		if fn.Type.Params != nil {
			for _, field := range fn.Type.Params.List {
				if len(field.Names) == 0 {
					field.Names = []*ast.Ident{ast.NewIdent(fresh("__bitfabArg"))}
				}
				for i, id := range field.Names {
					if id.Name == "_" {
						id = ast.NewIdent(fresh("__bitfabArg"))
						field.Names[i] = id
					}
					isContext := false
					if selector, ok := field.Type.(*ast.SelectorExpr); ok && selector.Sel.Name == "Context" {
						if pkg, ok := selector.X.(*ast.Ident); ok && contextAliases[pkg.Name] {
							isContext = true
						}
					}
					if isContext {
						if ctx == "nil" {
							ctx = id.Name
						}
					} else {
						inputs = append(inputs, id.Name)
					}
				}
			}
		}
		if fn.Type.Results != nil {
			for _, field := range fn.Type.Results.List {
				if len(field.Names) == 0 {
					field.Names = []*ast.Ident{ast.NewIdent(fresh("__bitfabResult"))}
				}
				for i, id := range field.Names {
					if id.Name == "_" {
						id = ast.NewIdent(fresh("__bitfabResult"))
						field.Names[i] = id
					}
					outputs = append(outputs, "&"+id.Name)
				}
			}
		}
		wrapper := false
		if fn.Type.Params != nil && len(fn.Type.Params.List) == 1 {
			_, wrapper = fn.Type.Params.List[0].Type.(*ast.Ellipsis)
		}
		node := fresh("__bitfabNode")
		panicName := fresh("__bitfabPanic")
		assignCtx := ""
		if ctx != "nil" {
			assignCtx = ctx + "=" + node + ".Context();"
		}
		source := fmt.Sprintf("package p;func stub(){if %s.AutoCaptureActive(){if %s := %s.EnterAutoNode(%s,%s.AutoFunctionSymbol(%q),%q,[]any{%s},[]any{%s},%t);%s!=nil{defer func(){%s:=recover();%s.End(%s);if %s!=nil{panic(%s)}}();%s if %s.Mocked(){%s.AssignMock();return}}}}", alias, node, alias, ctx, alias, symbol, name, strings.Join(inputs, ","), strings.Join(outputs, ","), wrapper, node, panicName, node, panicName, panicName, panicName, assignCtx, node, node)
		parsed, err := parser.ParseFile(fset, "generated.go", source, 0)
		if err != nil {
			return nil, err
		}
		prologue := parsed.Decls[0].(*ast.FuncDecl).Body.List
		fn.Body.List = append(prologue, fn.Body.List...)
		count++
	}
	if count == 0 && len(helperDecls) == 0 {
		var out bytes.Buffer
		err := format.Node(&out, fset, file)
		return out.Bytes(), err
	}
	found := false
	for _, imp := range file.Imports {
		p, _ := strconv.Unquote(imp.Path.Value)
		if p == SDKImport {
			found = true
		}
	}
	if !found {
		index := 0
		position := file.Name.End() + 1
		for i, decl := range file.Decls {
			if imports, ok := decl.(*ast.GenDecl); ok && imports.Tok == token.IMPORT {
				index = i + 1
				position = imports.End() + 1
			}
		}
		imp := &ast.ImportSpec{Name: &ast.Ident{Name: alias, NamePos: position}, Path: &ast.BasicLit{Kind: token.STRING, Value: strconv.Quote(SDKImport), ValuePos: position}}
		decl := &ast.GenDecl{Tok: token.IMPORT, TokPos: position, Specs: []ast.Spec{imp}}
		file.Decls = append(file.Decls, nil)
		copy(file.Decls[index+1:], file.Decls[index:])
		file.Decls[index] = decl
		file.Imports = append(file.Imports, imp)
	}
	file.Decls = append(file.Decls, helperDecls...)
	var out bytes.Buffer
	if err := format.Node(&out, fset, file); err != nil {
		return nil, err
	}
	return format.Source(out.Bytes())
}

func exprName(expr ast.Expr) string {
	if id, ok := expr.(*ast.Ident); ok {
		return id.Name
	}
	return ""
}
