# Root conftest makes the repo root importable so tests can pull in
# streaming.*, config etc. without installation.
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent))
