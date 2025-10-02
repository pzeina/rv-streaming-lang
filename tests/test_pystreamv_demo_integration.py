import importlib.util
import os
import sys


def test_pystreamv_demo_builds_formulas_without_errors():
    # Ensure project root on path so we can import local pystreamv.py
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    import pystreamv as psv  # local module under test

    # Load and execute the demo script (filename contains a hyphen, so use importlib)
    demo_path = os.path.join(project_root, "pystreamv-demo.py")
    spec = importlib.util.spec_from_file_location("pystreamv_demo", demo_path)
    assert spec and spec.loader, "Could not create import spec for demo script"
    demo = importlib.util.module_from_spec(spec)
    # Execute the module code
    spec.loader.exec_module(demo)  # type: ignore[union-attr]

    # Validate key objects were created and have expected types
    assert hasattr(demo, "self") and isinstance(demo.self, psv.InputStream)
    assert hasattr(demo, "intruder") and isinstance(demo.intruder, psv.InputStream)

    assert hasattr(demo, "dist") and isinstance(demo.dist, psv.Formula)
    assert hasattr(demo, "stale") and isinstance(demo.stale, psv.ScheduledFormula)

    assert hasattr(demo, "output") and isinstance(demo.output, psv.Formula)

    assert hasattr(demo, "function") and isinstance(demo.function, dict)
    for key in ("output", "id_from", "eos_from"):
        assert key in demo.function # type: ignore
