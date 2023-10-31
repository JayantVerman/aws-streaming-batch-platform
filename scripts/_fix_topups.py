#!/usr/bin/env python3
"""Replace the marker text in the last 2 commits with meaningful changes."""
import subprocess
from pathlib import Path

REPO = Path(".")

# Read what's currently in the last 2 commits for these two files
r = subprocess.run(["git", "-C", str(REPO), "log", "-p", "-2",
                    "--", "scripts/setup.sh", "scripts/smoke_test.sh"],
                   capture_output=True, text=True)
print("--- last 2 commit diffs ---")
print(r.stdout[:2000])