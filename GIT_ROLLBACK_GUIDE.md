# Git Rollback Guide

How to undo committed changes, covering the two main strategies.

---

## Strategy 1: `git revert` — Safe Undo (Preferred for shared branches)

Creates a new commit that undoes the changes. History is preserved. Safe to use even after pushing.

### Revert a single commit
```bash
git revert <commit-hash>
```

### Revert multiple commits (most recent first)
```bash
git revert <newer-hash> <older-hash>
```

### Revert a range of commits
```bash
git revert HEAD~2..HEAD   # reverts last 2 commits
```

### Revert without auto-committing (review first)
```bash
git revert --no-commit <commit-hash>
git diff                  # review changes
git commit -m "Revert: <reason>"
```

Then push normally:
```bash
git push origin <branch>
```

---

## Strategy 2: `git reset --hard` — Destructive Undo (Removes commits entirely)

Permanently removes commits from history. **Requires force push if already pushed to remote.**

> **Warning:** Anyone else who has pulled those commits will need to reset their local branch too.

### Step 1 — Find the target commit hash
```bash
git log --oneline
```
Copy the hash of the commit you want to reset **to** (the last good commit).

### Step 2 — Reset locally
```bash
git reset --hard <target-commit-hash>
```

### Step 3 — Force push to remote
```bash
git push origin <branch> --force
```

---

## Quick Reference: Which strategy to use?

| Situation | Strategy |
|-----------|----------|
| Commits are only on your local machine | Either — `reset` is cleaner |
| Commits are pushed but you're the only developer | `reset` + force push |
| Commits are pushed and others have pulled them | `revert` only — do NOT reset |
| You want to keep a history of what was undone | `revert` |
| You want the commits to disappear entirely | `reset` |

---

## Finding a commit hash

```bash
git log --oneline          # short list
git log --oneline -10      # last 10 commits
git log --oneline --graph  # visual branch graph
```

---

## Emergency: Recovering after a bad `git reset`

If you reset too far and lost commits you wanted, use `git reflog` — it tracks every HEAD movement for ~30 days:

```bash
git reflog                           # find the commit you lost
git reset --hard <lost-commit-hash>  # restore it
```
