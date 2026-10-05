import subprocess
import sys
import tempfile
from pathlib import Path

from db_condenser import config_reader, subset, subset_utils  # noqa: F401

with tempfile.TemporaryDirectory() as directory:
    for option in ("--help-config", "--example-config"):
        result = subprocess.run(
            [sys.executable, "-m", "db_condenser.direct_subset", option],
            cwd=directory,
            capture_output=True,
            text=True,
            check=True,
        )
        assert result.stdout.strip(), option
        assert not result.stderr, result.stderr
        if option == "--help-config":
            reference = Path(__file__).resolve().parents[1] / "CONFIG.md"
            assert result.stdout == reference.read_text(encoding="utf-8")

print("Smoke test passed")
