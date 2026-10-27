package bitfab

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"slices"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxCodeChangeFiles     = 60
	maxCodeChangeFileBytes = 500_000
	maxCodeChangeTotal     = 2_000_000
	maxBranchWalk          = 1000
)

// CodeChangeType names the code change a replay runs against.
type CodeChangeType string

const (
	// CodeChangeWorkingTree attaches the uncommitted changes in the git
	// working tree, tracked edits plus untracked files, vs HEAD.
	CodeChangeWorkingTree CodeChangeType = "working-tree"
	// CodeChangeLastBranch attaches the diff against the branch this one was
	// cut from, such as the branch below it in a stack, else the default
	// branch. Nothing is attached on the default branch itself.
	CodeChangeLastBranch CodeChangeType = "last-branch"
	// CodeChangeDefaultBranch attaches the diff against where HEAD split from
	// the default branch (origin/HEAD, else origin/main, origin/master, main,
	// master).
	CodeChangeDefaultBranch CodeChangeType = "default-branch"
	// CodeChangeSkip attaches no code change, such as for a baseline run.
	CodeChangeSkip CodeChangeType = "skip"
	// CodeChangeManual attaches the files passed in ReplayOptions.CodeChangeFiles.
	CodeChangeManual CodeChangeType = "manual"
)

const codeChangeChoices = `"working-tree" (uncommitted changes vs HEAD), "last-branch" (the diff against the branch this one was cut from), "default-branch" (the diff against the default branch), "skip" (no code change), or "manual" (pass the files yourself)`

var codeChangeTypes = []CodeChangeType{CodeChangeWorkingTree, CodeChangeLastBranch, CodeChangeDefaultBranch, CodeChangeSkip, CodeChangeManual}

func isCodeChangeType(value string) bool {
	return slices.Contains(codeChangeTypes, CodeChangeType(value))
}

type resolvedCodeChange struct {
	Description *string          `json:"description"`
	Files       []CodeChangeFile `json:"files"`
}

type gitCodeChange struct {
	status     byte
	beforePath string
	path       string
}

func validateCodeChange(options *ReplayOptions) error {
	if options == nil || options.CodeChange == "" {
		return fmt.Errorf("bitfab: replay needs a CodeChange naming the code this run tests: %s.", codeChangeChoices)
	}
	codeChange := options.CodeChange
	if !isCodeChangeType(string(codeChange)) {
		return fmt.Errorf("bitfab: Unknown CodeChange %q. Pass %s.", string(codeChange), codeChangeChoices)
	}
	hasFiles := options.CodeChangeFiles != nil
	if codeChange == CodeChangeManual && !hasFiles {
		return fmt.Errorf(`bitfab: CodeChange "manual" needs CodeChangeFiles: the path and before/after contents of each edited file.`)
	}
	if codeChange != CodeChangeManual && hasFiles {
		return fmt.Errorf(`bitfab: CodeChangeFiles only applies with CodeChange "manual" (got %q).`, string(codeChange))
	}
	if codeChange == CodeChangeSkip && options.CodeChangeDescription != "" {
		return fmt.Errorf(`bitfab: CodeChange "skip" attaches no code change, so it cannot take a CodeChangeDescription.`)
	}
	return nil
}

func resolveCodeChange(ctx context.Context, options ReplayOptions) (string, []CodeChangeFile) {
	switch options.CodeChange {
	case CodeChangeSkip:
		return "", nil
	case CodeChangeManual:
		return options.CodeChangeDescription, options.CodeChangeFiles
	}
	captured := captureCodeChange(ctx, options.CodeChange, options.Name)
	if captured == nil || len(captured.Files) == 0 {
		warnNoCodeChangeFound(options.CodeChange)
		return options.CodeChangeDescription, nil
	}
	description := options.CodeChangeDescription
	if description == "" && captured.Description != nil {
		description = *captured.Description
	}
	return description, captured.Files
}

func warnNoCodeChangeFound(codeChange CodeChangeType) {
	defer func() { recover() }()
	log.Printf("Bitfab: CodeChange %q found no changed files, so this replay has no code change attached.", string(codeChange))
}

func captureCodeChange(ctx context.Context, codeChange CodeChangeType, label string) *resolvedCodeChange {
	if fromFile := readCodeChangeOverride(); fromFile != nil {
		return fromFile
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return nil
	}
	return captureCodeChangeFromGit(ctx, workingDirectory, codeChange, label)
}

func readCodeChangeOverride() *resolvedCodeChange {
	path := os.Getenv("BITFAB_CODE_CHANGE_PATH")
	if path == "" {
		return nil
	}
	content, err := os.ReadFile(path)
	if err != nil {
		return nil
	}
	var raw struct {
		Description any             `json:"description"`
		Files       json.RawMessage `json:"files"`
	}
	if err := json.Unmarshal(content, &raw); err != nil {
		return nil
	}
	var description *string
	if value, ok := raw.Description.(string); ok {
		description = &value
	}
	var files []CodeChangeFile
	if len(raw.Files) > 0 && string(raw.Files) != "null" {
		if err := json.Unmarshal(raw.Files, &files); err != nil {
			files = nil
		}
	}
	if description == nil && files == nil {
		return nil
	}
	return &resolvedCodeChange{Description: description, Files: files}
}

func captureCodeChangeFromGit(ctx context.Context, cwd string, codeChange CodeChangeType, label string) *resolvedCodeChange {
	root, ok := runReplayGit(ctx, cwd, "rev-parse", "--show-toplevel")
	root = strings.TrimSpace(root)
	if !ok || root == "" {
		return nil
	}
	base, against, ok := resolveCodeChangeBase(ctx, root, codeChange)
	if !ok {
		return nil
	}
	entries := listCodeChanges(ctx, root, base)
	if len(entries) == 0 {
		return nil
	}

	files := make([]CodeChangeFile, 0, min(len(entries), maxCodeChangeFiles))
	totalBytes := 0
	for _, entry := range entries {
		if len(files) >= maxCodeChangeFiles {
			break
		}
		beforeBytes := int64(0)
		if entry.status != 'A' {
			beforeBytes = gitBlobSize(ctx, root, base, entry.beforePath)
		}
		afterBytes := int64(0)
		if entry.status != 'D' {
			afterBytes = workingFileSize(root, entry.path)
		}
		if beforeBytes > maxCodeChangeFileBytes || afterBytes > maxCodeChangeFileBytes {
			continue
		}
		before := ""
		if entry.status != 'A' {
			before, _ = runReplayGit(ctx, root, "show", base+":"+entry.beforePath)
		}
		after := ""
		if entry.status != 'D' {
			content, err := os.ReadFile(filepath.Join(root, filepath.FromSlash(entry.path)))
			if err == nil {
				after = string(content)
			}
		}
		before = strings.ReplaceAll(before, "\r\n", "\n")
		after = strings.ReplaceAll(after, "\r\n", "\n")
		if before == after {
			continue
		}
		size := len([]byte(before)) + len([]byte(after))
		if totalBytes+size > maxCodeChangeTotal || looksLikeBinary(before) || looksLikeBinary(after) {
			continue
		}
		totalBytes += size
		files = append(files, CodeChangeFile{Path: entry.path, Before: before, After: after})
	}
	if len(files) == 0 {
		return nil
	}

	subject, _ := runReplayGit(ctx, root, "log", "-1", "--format=%s", "HEAD")
	head := strings.TrimSpace(label)
	if head == "" {
		head = strings.TrimSpace(subject)
	}
	if head == "" {
		head = "Working-tree change"
	}
	word := "files"
	if len(files) == 1 {
		word = "file"
	}
	description := head + " (" + strconv.Itoa(len(files)) + " " + word + " changed " + against + ")"
	return &resolvedCodeChange{Description: &description, Files: files}
}

func listCodeChanges(ctx context.Context, root string, base string) []gitCodeChange {
	tracked, _ := runReplayGit(
		ctx,
		root,
		"diff",
		"--name-status",
		"--find-renames",
		"-z",
		base,
		"--",
		":!.bitfab",
	)
	untracked, _ := runReplayGit(
		ctx,
		root,
		"ls-files",
		"--others",
		"--exclude-standard",
		"-z",
		"--",
		":!.bitfab",
	)
	entries := parseGitNameStatus(tracked)
	for _, path := range splitNUL(untracked) {
		entries = append(entries, gitCodeChange{status: 'A', beforePath: path, path: path})
	}
	return entries
}

func defaultBranchCandidates(ctx context.Context, root string) []string {
	candidates := []string{}
	if originHead, ok := runReplayGit(ctx, root, "rev-parse", "--abbrev-ref", "origin/HEAD"); ok {
		if trimmed := strings.TrimSpace(originHead); trimmed != "" && trimmed != "origin/HEAD" {
			candidates = append(candidates, trimmed)
		}
	}
	for _, ref := range []string{"origin/main", "origin/master", "main", "master"} {
		if !slices.Contains(candidates, ref) {
			candidates = append(candidates, ref)
		}
	}
	return candidates
}

func resolveDefaultBranch(ctx context.Context, root string) string {
	for _, ref := range defaultBranchCandidates(ctx, root) {
		if replayGitRefExists(ctx, root, ref) {
			return ref
		}
	}
	return ""
}

func currentBranchRefs(ctx context.Context, root string) map[string]bool {
	refs := map[string]bool{}
	branch, _ := runReplayGit(ctx, root, "symbolic-ref", "--short", "-q", "HEAD")
	branch = strings.TrimSpace(branch)
	if branch == "" {
		return refs
	}
	refs["refs/heads/"+branch] = true
	remotes, _ := runReplayGit(ctx, root, "remote")
	for _, remote := range strings.Fields(remotes) {
		refs["refs/remotes/"+remote+"/"+branch] = true
	}
	return refs
}

func ownBranchRefs(ctx context.Context, root string) map[string]bool {
	own := currentBranchRefs(ctx, root)
	if upstream, ok := runReplayGit(ctx, root, "rev-parse", "--symbolic-full-name", "@{upstream}"); ok && strings.TrimSpace(upstream) != "" {
		own[strings.TrimSpace(upstream)] = true
	}
	return own
}

func isOnDefaultBranch(ctx context.Context, root, defaultBranch string) bool {
	fullName, _ := runReplayGit(ctx, root, "rev-parse", "--symbolic-full-name", defaultBranch)
	fullName = strings.TrimSpace(fullName)
	return fullName != "" && currentBranchRefs(ctx, root)[fullName]
}

func branchTips(ctx context.Context, root string) map[string]string {
	own := ownBranchRefs(ctx, root)
	listing, _ := runReplayGit(ctx, root, "for-each-ref", "--format=%(objectname) %(refname)", "refs/heads", "refs/remotes")
	tips := map[string]string{}
	for _, line := range strings.Split(listing, "\n") {
		sha, ref, found := strings.Cut(strings.TrimSpace(line), " ")
		if !found || own[ref] || strings.HasSuffix(ref, "/HEAD") {
			continue
		}
		if _, seen := tips[sha]; seen {
			continue
		}
		name := strings.TrimPrefix(strings.TrimPrefix(ref, "refs/heads/"), "refs/remotes/")
		tips[sha] = name
	}
	return tips
}

func resolveBranchPoint(ctx context.Context, root string) (string, string, bool) {
	head, ok := runReplayGit(ctx, root, "rev-parse", "HEAD")
	head = strings.TrimSpace(head)
	if !ok || head == "" {
		return "", "", false
	}
	defaultBranch := resolveDefaultBranch(ctx, root)
	if defaultBranch != "" && isOnDefaultBranch(ctx, root, defaultBranch) {
		return "", "", false
	}
	defaultBase := ""
	if defaultBranch != "" {
		output, _ := runReplayGit(ctx, root, "merge-base", "HEAD", defaultBranch)
		defaultBase = strings.TrimSpace(output)
	}
	walk := []string{"rev-list", "--first-parent", "--max-count=" + strconv.Itoa(maxBranchWalk), "HEAD"}
	if defaultBase != "" {
		walk = append(walk, "^"+defaultBase)
	}
	tips := branchTips(ctx, root)
	history, _ := runReplayGit(ctx, root, walk...)
	commits := strings.Fields(history)
	if len(commits) > 0 {
		commits = commits[1:]
	}
	for _, sha := range commits {
		if name, found := tips[sha]; found {
			return sha, "vs " + name, true
		}
	}
	if defaultBase != "" && defaultBase != head {
		return defaultBase, "vs " + defaultBranch, true
	}
	return "", "", false
}

func resolveDefaultBranchPoint(ctx context.Context, root string) (string, string, bool) {
	defaultBranch := resolveDefaultBranch(ctx, root)
	if defaultBranch == "" {
		return "", "", false
	}
	base, ok := runReplayGit(ctx, root, "merge-base", "HEAD", defaultBranch)
	base = strings.TrimSpace(base)
	if !ok || base == "" {
		return "", "", false
	}
	return base, "vs " + defaultBranch, true
}

func resolveCodeChangeBase(ctx context.Context, root string, codeChange CodeChangeType) (string, string, bool) {
	switch codeChange {
	case CodeChangeLastBranch:
		return resolveBranchPoint(ctx, root)
	case CodeChangeDefaultBranch:
		return resolveDefaultBranchPoint(ctx, root)
	}
	if replayGitRefExists(ctx, root, "HEAD") {
		return "HEAD", "uncommitted (vs HEAD)", true
	}
	return "", "", false
}

func replayGitRefExists(ctx context.Context, root string, ref string) bool {
	_, ok := runReplayGit(ctx, root, "rev-parse", "--verify", ref+"^{object}")
	return ok
}

func gitBlobSize(ctx context.Context, root string, ref string, path string) int64 {
	output, ok := runReplayGit(ctx, root, "cat-file", "-s", ref+":"+path)
	if !ok {
		return 0
	}
	size, err := strconv.ParseInt(strings.TrimSpace(output), 10, 64)
	if err != nil {
		return 0
	}
	return size
}

func workingFileSize(root string, path string) int64 {
	info, err := os.Stat(filepath.Join(root, filepath.FromSlash(path)))
	if err != nil {
		return 0
	}
	return info.Size()
}

func runReplayGit(ctx context.Context, cwd string, args ...string) (string, bool) {
	return runGit(ctx, cwd, 30*time.Second, args...)
}

func parseGitNameStatus(raw string) []gitCodeChange {
	parts := splitNUL(raw)
	changes := make([]gitCodeChange, 0, len(parts)/2)
	for index := 0; index+1 < len(parts); {
		status := parts[index][0]
		index++
		beforePath := parts[index]
		index++
		path := beforePath
		if (status == 'R' || status == 'C') && index < len(parts) {
			path = parts[index]
			index++
		}
		changes = append(changes, gitCodeChange{status: status, beforePath: beforePath, path: path})
	}
	return changes
}

func splitNUL(raw string) []string {
	parts := strings.Split(raw, "\x00")
	result := make([]string, 0, len(parts))
	for _, part := range parts {
		if part != "" {
			result = append(result, part)
		}
	}
	return result
}

func looksLikeBinary(value string) bool {
	prefix := value
	if len(prefix) > 8000 {
		prefix = prefix[:8000]
	}
	return strings.ContainsRune(prefix, '\x00') || !utf8.ValidString(prefix)
}
