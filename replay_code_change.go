package bitfab

import (
	"context"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"
)

const (
	maxCodeChangeFiles     = 60
	maxCodeChangeFileBytes = 500_000
	maxCodeChangeTotal     = 2_000_000
)

var codeChangeTrunkCandidates = []string{
	"origin/HEAD",
	"origin/main",
	"origin/master",
	"main",
	"master",
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

func resolveAutoCodeChange(ctx context.Context, label string) *resolvedCodeChange {
	if os.Getenv("BITFAB_DISABLE_CODE_CHANGE_CAPTURE") != "" {
		return nil
	}
	if fromFile := readCodeChangeOverride(); fromFile != nil {
		return fromFile
	}
	workingDirectory, err := os.Getwd()
	if err != nil {
		return nil
	}
	return captureCodeChangeFromGit(ctx, workingDirectory, label)
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

func captureCodeChangeFromGit(ctx context.Context, cwd string, label string) *resolvedCodeChange {
	root, ok := runReplayGit(ctx, cwd, "rev-parse", "--show-toplevel")
	root = strings.TrimSpace(root)
	if !ok || root == "" {
		return nil
	}
	base, fromTrunk, ok := resolveCodeChangeBase(ctx, root)
	if !ok {
		return nil
	}
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
	against := "uncommitted (vs HEAD)"
	if fromTrunk {
		against = "vs trunk"
	}
	description := head + " (" + strconv.Itoa(len(files)) + " " + word + " changed " + against + ")"
	return &resolvedCodeChange{Description: &description, Files: files}
}

func resolveCodeChangeBase(ctx context.Context, root string) (string, bool, bool) {
	if forced := os.Getenv("BITFAB_CODE_CHANGE_BASE"); forced != "" && replayGitRefExists(ctx, root, forced) {
		if base, ok := runReplayGit(ctx, root, "merge-base", "HEAD", forced); ok && strings.TrimSpace(base) != "" {
			return strings.TrimSpace(base), true, true
		}
		if base, ok := runReplayGit(ctx, root, "rev-parse", "--verify", forced); ok && strings.TrimSpace(base) != "" {
			return strings.TrimSpace(base), true, true
		}
		return "", false, false
	}
	for _, candidate := range codeChangeTrunkCandidates {
		if !replayGitRefExists(ctx, root, candidate) {
			continue
		}
		if base, ok := runReplayGit(ctx, root, "merge-base", "HEAD", candidate); ok && strings.TrimSpace(base) != "" {
			return strings.TrimSpace(base), true, true
		}
	}
	if replayGitRefExists(ctx, root, "HEAD") {
		return "HEAD", false, true
	}
	return "", false, false
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
	commandCtx, cancel := context.WithTimeout(ctx, 30*time.Second)
	defer cancel()
	command := exec.CommandContext(commandCtx, "git", args...)
	command.Dir = cwd
	output, err := command.Output()
	if err != nil {
		return "", false
	}
	return string(output), true
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
