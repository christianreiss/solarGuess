import sys
from pathlib import Path

# ensure src package importable
ROOT = Path(__file__).resolve().parents[1]
src = ROOT / "src"
if str(src) not in sys.path:
    sys.path.insert(0, str(src))

# Disable external plugins for reproducibility in isolated test envs
PYTEST_DISABLE_PLUGIN_AUTOLOAD = True
