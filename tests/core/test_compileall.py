import compileall
from pathlib import Path


def test_compileall():
    root = Path(__file__).resolve().parents[1] / "src" / "solarpredict"
    assert compileall.compile_dir(str(root), quiet=1)
