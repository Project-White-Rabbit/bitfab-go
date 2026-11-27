package instrument

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type copiedModule struct {
	path, version, dir string
}

func goEnv(dir, name string) (string, error) {
	cmd := exec.Command("go", "env", name)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return "", fmt.Errorf("go env %s: %w\n%s", name, err, stderr.String())
	}
	return strings.TrimSpace(string(out)), nil
}

func within(name, root string) bool {
	if root == "" {
		return false
	}
	relative, err := filepath.Rel(root, name)
	return err == nil && relative != ".." && !strings.HasPrefix(relative, ".."+string(filepath.Separator))
}

func copyModule(source, modulePath, target string) error {
	err := filepath.WalkDir(source, func(name string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		relative, err := filepath.Rel(source, name)
		if err != nil {
			return err
		}
		destination := filepath.Join(target, relative)
		if entry.IsDir() {
			return os.MkdirAll(destination, 0700)
		}
		if !entry.Type().IsRegular() {
			return nil
		}
		data, err := os.ReadFile(name)
		if err != nil {
			return err
		}
		return os.WriteFile(destination, data, 0600)
	})
	if err != nil {
		return fmt.Errorf("copy %s for instrumentation: %w", modulePath, err)
	}
	if _, err := os.Stat(filepath.Join(target, "go.mod")); os.IsNotExist(err) {
		return os.WriteFile(filepath.Join(target, "go.mod"), []byte("module "+modulePath+"\n"), 0600)
	}
	return nil
}

func replaceEdits(modules []copiedModule) []string {
	var edits []string
	for _, module := range modules {
		if module.version != "" {
			edits = append(edits, "-dropreplace="+module.path+"@"+module.version)
		}
		edits = append(edits, "-dropreplace="+module.path, "-replace="+module.path+"="+module.dir)
	}
	return edits
}

func runGo(dir string, args ...string) ([]byte, error) {
	cmd := exec.Command("go", args...)
	cmd.Dir = dir
	var stderr bytes.Buffer
	cmd.Stderr = &stderr
	out, err := cmd.Output()
	if err != nil {
		return nil, fmt.Errorf("go %s: %w\n%s", strings.Join(args, " "), err, stderr.String())
	}
	return out, nil
}

func copyFile(source, target string) error {
	data, err := os.ReadFile(source)
	if os.IsNotExist(err) {
		return nil
	}
	if err != nil {
		return err
	}
	return os.WriteFile(target, data, 0600)
}

func isLocalPath(path string) bool {
	return filepath.IsAbs(path) || path == "." || path == ".." || strings.HasPrefix(path, "./") || strings.HasPrefix(path, "../") || strings.HasPrefix(path, `.\`) || strings.HasPrefix(path, `..\`)
}

func redirectModules(dir string, options ListOptions, temp string, modules []copiedModule, build *Build) error {
	work, err := goEnv(dir, "GOWORK")
	if err != nil {
		return err
	}
	if work != "" && work != "off" {
		return redirectWorkspace(dir, work, temp, modules, build)
	}
	modFile := options.ModFile
	if modFile == "" {
		if modFile, err = goEnv(dir, "GOMOD"); err != nil {
			return err
		}
	} else if !filepath.IsAbs(modFile) {
		modFile = filepath.Join(dir, modFile)
	}
	if modFile == "" || modFile == os.DevNull {
		return fmt.Errorf("bitfab-instrument: -include-packages needs a go.mod")
	}
	target := filepath.Join(temp, "go.mod")
	if err := copyFile(modFile, target); err != nil {
		return err
	}
	if err := copyFile(strings.TrimSuffix(modFile, ".mod")+".sum", filepath.Join(temp, "go.sum")); err != nil {
		return err
	}
	if _, err := runGo(dir, append(append([]string{"mod", "edit"}, replaceEdits(modules)...), target)...); err != nil {
		return err
	}
	build.ModFile = target
	return nil
}

func redirectWorkspace(dir, work, temp string, modules []copiedModule, build *Build) error {
	target := filepath.Join(temp, "go.work")
	if err := copyFile(work, target); err != nil {
		return err
	}
	if err := copyFile(work+".sum", target+".sum"); err != nil {
		return err
	}
	data, err := runGo(dir, "work", "edit", "-json", target)
	if err != nil {
		return err
	}
	var parsed struct {
		Use     []struct{ DiskPath string }
		Replace []struct {
			Old, New struct{ Path, Version string }
		}
	}
	if err := json.Unmarshal(data, &parsed); err != nil {
		return err
	}
	workDir := filepath.Dir(work)
	var edits []string
	for _, use := range parsed.Use {
		if !filepath.IsAbs(use.DiskPath) {
			edits = append(edits, "-dropuse="+use.DiskPath, "-use="+filepath.Join(workDir, use.DiskPath))
		}
	}
	for _, replace := range parsed.Replace {
		if replace.New.Version != "" || !isLocalPath(replace.New.Path) || filepath.IsAbs(replace.New.Path) {
			continue
		}
		old := replace.Old.Path
		if replace.Old.Version != "" {
			old += "@" + replace.Old.Version
		}
		edits = append(edits, "-dropreplace="+old, "-replace="+old+"="+filepath.Join(workDir, replace.New.Path))
	}
	edits = append(edits, replaceEdits(modules)...)
	if _, err := runGo(dir, append(append([]string{"work", "edit"}, edits...), target)...); err != nil {
		return err
	}
	build.Env = append(build.Env, "GOWORK="+target)
	return nil
}
