package instrument

import (
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"go/types"
	"strings"
)

const jevMethodName = "SystemOne"

func jevCallSelector(node ast.Node) *ast.SelectorExpr {
	call, ok := node.(*ast.CallExpr)
	if !ok {
		return nil
	}
	selector, ok := call.Fun.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != jevMethodName {
		return nil
	}
	return selector
}

func isContextType(typ types.Type) bool {
	named, ok := typ.(*types.Named)
	if !ok || named.Obj().Pkg() == nil {
		return false
	}
	return named.Obj().Pkg().Path() == "context" && named.Obj().Name() == "Context"
}

func jevCallSignature(info *types.Info, call *ast.CallExpr, selector *ast.SelectorExpr) (*types.Signature, string, bool) {
	if info == nil || info.Selections == nil {
		return nil, "", false
	}
	selection := info.Selections[selector]
	if selection == nil || selection.Kind() != types.MethodVal {
		return nil, "", false
	}
	sig, ok := selection.Type().(*types.Signature)
	if !ok || sig.Params().Len() == 0 || sig.Results().Len() != 2 {
		return nil, "", false
	}
	if !isContextType(sig.Params().At(0).Type()) || !types.Identical(sig.Results().At(1).Type(), types.Universe.Lookup("error").Type()) {
		return nil, "", false
	}
	if len(call.Args) == 1 && sig.Params().Len() > 1 {
		return nil, "", false
	}
	return sig, jevCallName(selection), true
}

func jevCallName(selection *types.Selection) string {
	method, ok := selection.Obj().(*types.Func)
	if !ok {
		return jevMethodName
	}
	recv := method.Type().(*types.Signature).Recv()
	if recv == nil {
		return jevMethodName
	}
	typ := recv.Type()
	pointer := false
	if p, ok := typ.(*types.Pointer); ok {
		typ, pointer = p.Elem(), true
	}
	named, ok := typ.(*types.Named)
	if !ok {
		return jevMethodName
	}
	name := named.Obj().Name()
	if pointer {
		name = "(*" + name + ")"
	}
	return name + "." + jevMethodName
}

func isJevMethodDecl(fn *ast.FuncDecl, contextAliases map[string]bool) bool {
	if fn.Recv == nil || fn.Name.Name != jevMethodName || fn.Type.Params == nil || len(fn.Type.Params.List) == 0 || fn.Type.Results == nil {
		return false
	}
	selector, ok := fn.Type.Params.List[0].Type.(*ast.SelectorExpr)
	if !ok || selector.Sel.Name != "Context" {
		return false
	}
	if pkg, ok := selector.X.(*ast.Ident); !ok || !contextAliases[pkg.Name] {
		return false
	}
	results := 0
	for _, field := range fn.Type.Results.List {
		results += max(len(field.Names), 1)
	}
	last := fn.Type.Results.List[len(fn.Type.Results.List)-1].Type
	ident, ok := last.(*ast.Ident)
	return results == 2 && ok && ident.Name == "error"
}

func rewriteJevCalls(fset *token.FileSet, file *ast.File, info *types.Info, alias string, fresh func(string) string, helperPrefix string) ([]ast.Decl, error) {
	var helpers []ast.Decl
	var rewriteErr error
	visit := func(n ast.Node) bool {
		if rewriteErr != nil {
			return false
		}
		selector := jevCallSelector(n)
		if selector == nil {
			return true
		}
		call := n.(*ast.CallExpr)
		sig, name, ok := jevCallSignature(info, call, selector)
		if !ok {
			return true
		}
		helper := fresh(helperPrefix)
		source := jevHelperSource(helper, alias, name, sig)
		parsed, err := parser.ParseFile(fset, "generated.go", source, 0)
		if err != nil {
			rewriteErr = err
			return false
		}
		helpers = append(helpers, parsed.Decls...)
		call.Args = append([]ast.Expr{selector}, call.Args...)
		call.Fun = ast.NewIdent(helper)
		return true
	}
	for _, decl := range file.Decls {
		if fn, ok := decl.(*ast.FuncDecl); ok && fn.Doc != nil && strings.Contains(fn.Doc.Text(), "bitfab:ignore") {
			continue
		}
		ast.Inspect(decl, visit)
	}
	return helpers, rewriteErr
}

func jevHelperSource(helper, alias, name string, sig *types.Signature) string {
	var typeParams, params, inputs, funcParams, callArgs []string
	for i := 0; i < sig.Params().Len(); i++ {
		typ := fmt.Sprintf("A%d", i)
		arg := fmt.Sprintf("a%d", i)
		typeParams = append(typeParams, typ+" any")
		if sig.Variadic() && i == sig.Params().Len()-1 {
			funcParams = append(funcParams, "..."+typ)
			params = append(params, arg+" ..."+typ)
			callArgs = append(callArgs, arg+"...")
		} else {
			funcParams = append(funcParams, typ)
			params = append(params, arg+" "+typ)
			callArgs = append(callArgs, arg)
		}
		if i > 0 {
			inputs = append(inputs, arg)
		}
	}
	typeParams = append(typeParams, "R0 any", "R1 any")
	params = append([]string{"__bitfabCall func(" + strings.Join(funcParams, ",") + ") (R0, R1)"}, params...)
	return fmt.Sprintf(`package p
func %s[%s](%s) (r0 R0, r1 R1) {
	if %s.AutoCaptureActive() {
		if __bitfabNode := %s.EnterAutoLLMCall(__bitfabCall, any(a0), %q, []any{%s}, []any{&r0, &r1}); __bitfabNode != nil {
			defer func() {
				__bitfabPanic := recover()
				__bitfabNode.End(__bitfabPanic)
				if __bitfabPanic != nil {
					panic(__bitfabPanic)
				}
			}()
			if __bitfabCtx, ok := any(__bitfabNode.Context()).(A0); ok {
				a0 = __bitfabCtx
			}
			if __bitfabNode.Mocked() {
				__bitfabNode.AssignMock()
				return
			}
		}
	}
	return __bitfabCall(%s)
}
`, helper, strings.Join(typeParams, ", "), strings.Join(params, ", "), alias, alias, name, strings.Join(inputs, ", "), strings.Join(callArgs, ", "))
}
