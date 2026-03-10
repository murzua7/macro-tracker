"""GitHub repository bootstrap helpers."""

from __future__ import annotations

import logging
import subprocess
from pathlib import Path


LOGGER = logging.getLogger(__name__)


def _run_command(command: list[str], cwd: Path) -> None:
    """Run a subprocess command with logging and strict failure handling."""
    LOGGER.info("Running command: %s", " ".join(command))
    subprocess.run(command, cwd=cwd, check=True)


def setup_github_repo(repo_name: str, repo_path: str | Path = ".") -> None:
    """Initialize a local git repo and create a private GitHub remote."""
    path = Path(repo_path).resolve()

    _run_command(["git", "init"], cwd=path)
    _run_command(["git", "checkout", "-B", "main"], cwd=path)
    _run_command(["git", "add", "."], cwd=path)
    _run_command(["git", "commit", "-m", "Initial commit"], cwd=path)
    _run_command(
        [
            "gh",
            "repo",
            "create",
            repo_name,
            "--private",
            "--source=.",
            "--remote=origin",
        ],
        cwd=path,
    )
    _run_command(["git", "push", "-u", "origin", "main"], cwd=path)
