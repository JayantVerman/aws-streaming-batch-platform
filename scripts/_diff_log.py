#!/usr/bin/env python3
"""Compare expected commit messages to ones that actually committed."""
import re
from pathlib import Path

content = Path("scripts/_rebuild_commits.py").read_text()
ns = {"__name__": "rc", "__file__": "scripts/_rebuild_commits.py"}
exec(compile(content, "scripts/_rebuild_commits.py", "exec"), ns)
steps = ns["build_plan"]()
expected = [s[0] for s in steps]

log_path = Path(r"C:\Users\jayan\AppData\Local\Temp\_rebuild.log")
text = log_path.read_text(encoding="utf-8", errors="replace")
msgs_done = set()
for line in text.splitlines():
    if "SKIP" in line or "FAILED" in line:
        continue
    if "committed " in line.lower() and "/" in line and "attempted" in line:
        continue
    # Format: '  [ N/180] Mon Apr 03 19:40 feat(project): add .env.example'
    m = re.search(r"\[\s*\d+/\d+\]\s+(\S+\s+\S+\s+\d+\s+\d+:\d+\s+)(.*)", line)
    if m:
        msgs_done.add(m.group(1).strip())

print(f"expected={len(expected)} done={len(msgs_done)}")
missing = [e for e in expected if e not in msgs_done]
print(f"missing ({len(missing)}):")
for m in missing:
    print(f"  {m}")