"""Thin entrypoint for the generator service."""

import sys
from pathlib import Path

CURRENT_DIR = Path(__file__).resolve().parent
if str(CURRENT_DIR) not in sys.path:
    sys.path.insert(0, str(CURRENT_DIR))

from runtime import run

main = run

__all__ = ["main", "run"]


if __name__ == "__main__":
    sys.exit(run())
