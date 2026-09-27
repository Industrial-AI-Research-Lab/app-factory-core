"""
Repository Manager

Creates and manages per-environment Git repositories on the host.
"""
from __future__ import annotations

from pathlib import Path
import subprocess
from typing import Optional


class RepoManager:
    def __init__(self, root: str):
        # Sanitize root to avoid Windows escape issues like "C:\\work\repositories" or "C:\\work\r..."
        sanitized = str(root).replace("\r", "").replace("\n", "").strip()
        sanitized = sanitized.replace("\\", "/")
        self.root = Path(sanitized)
        self.root.mkdir(parents=True, exist_ok=True)

    def _run(self, args: list[str], cwd: Optional[Path] = None) -> subprocess.CompletedProcess:
        return subprocess.run(args, cwd=str(cwd) if cwd else None, capture_output=True, text=True, shell=False)

    def create_temp_repo(self, name: str) -> Path:
        """
        Create a temporary repo directory under root with given name and initialize git.
        Returns the repo path.
        """
        repo_path = self.root / name
        repo_path.mkdir(parents=True, exist_ok=True)
        # git init
        self._run(["git", "init", "-b", "main"], cwd=repo_path)
        # Ensure identity is set locally (avoid global pollution)
        self._run(["git", "config", "user.name", "AppFactory Bot"], cwd=repo_path)
        self._run(["git", "config", "user.email", "AppFactory@example.com"], cwd=repo_path)
        # seed a commit
        gitkeep = repo_path / ".gitkeep"
        gitkeep.write_text("", encoding="utf-8")
        self._run(["git", "add", ".gitkeep"], cwd=repo_path)
        self._run(["git", "commit", "-m", "chore: initialize repository"], cwd=repo_path)
        return repo_path

    def rename_repo(self, old_path: Path, new_name: str) -> Path:
        """
        Rename an existing repo directory to new_name under root.
        Returns new repo path.
        """
        new_path = self.root / new_name
        if new_path.exists():
            # If already exists, keep existing to avoid data loss
            return new_path
        old_path.rename(new_path)
        return new_path

    def ensure_repo(self, name: str) -> Path:
        """
        Ensure a repo exists by name; if missing, create and init.
        """
        path = self.root / name
        if (path / ".git").exists():
            return path
        return self.create_temp_repo(name)

    # Snapshot helpers
    def commit_snapshot(self, name: str, label: str = "snapshot") -> str:
        """Record current repo state for snapshots.
        
        IMPORTANT: We do NOT commit files here anymore. Container-use manages
        all file commits via its branch. Committing here causes merge conflicts
        because both our main and container-use branch would add the same files.
        
        Instead, we just return the current HEAD commit (or container-use branch tip
        if available) as a reference point for reverts.
        """
        repo = self.ensure_repo(name)
        
        # Try to get the container-use branch tip (source of truth for files)
        # Container-use branches are named like: container-use/<env-name>
        branches = self._run(["git", "branch", "-a", "--list", "container-use/*"], cwd=repo)
        cu_branches = [b.strip().lstrip("* ") for b in (branches.stdout or "").strip().splitlines() if b.strip()]
        
        if cu_branches:
            # Get the latest commit from the most recent container-use branch
            latest_branch = cu_branches[-1]  # Most recently created
            cu_head = self._run(["git", "rev-parse", latest_branch], cwd=repo)
            if cu_head.returncode == 0 and cu_head.stdout.strip():
                return cu_head.stdout.strip()
        
        # Fallback to main HEAD
        head = self._run(["git", "rev-parse", "HEAD"], cwd=repo)
        return (head.stdout or "").strip()

    def checkout_commit(self, name: str, commit: str) -> None:
        """Hard reset the named repo to the given commit."""
        repo = self.ensure_repo(name)
        # Ensure we are on main branch and have the commit
        self._run(["git", "checkout", "-B", "main"], cwd=repo)
        # Hard reset to commit
        self._run(["git", "reset", "--hard", commit], cwd=repo)

    def list_changed_files(self, name: str, from_commit: str, to_commit: str | None = None) -> list[dict]:
        """Return list of changed files between two commits as [{'path': str, 'status': 'A|M|D'}].
        If to_commit is None, diff against HEAD.
        """
        repo = self.ensure_repo(name)
        to_spec = to_commit or "HEAD"
        res = self._run(["git", "diff", "--name-status", f"{from_commit}..{to_spec}"], cwd=repo)
        out = (res.stdout or "").strip().splitlines()
        items = []
        for line in out:
            try:
                status, path = line.split("\t", 1)
                items.append({"path": path.strip(), "status": status.strip()})
            except Exception:
                continue
        return items
