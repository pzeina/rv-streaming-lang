import importlib.util
import os
import sys
import time

import pytest


def test_compile_demo_script_to_sponge_smoke():
    # Ensure project root on path so we can import local pystreamv.py and demo
    project_root = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
    if project_root not in sys.path:
        sys.path.insert(0, project_root)

    ls = pytest.importorskip("logicsponge.core")

    import pystreamv as psv  # local module under test

    # Load the demo script via importlib (hyphenated filename)
    demo_path = os.path.join(project_root, "pystreamv-demo.py")
    spec = importlib.util.spec_from_file_location("pystreamv_demo", demo_path)
    assert spec and spec.loader, "Could not create import spec for demo script"
    demo = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(demo)  # type: ignore[union-attr]

    # Basic sanity of objects from demo (mirrors existing demo test)
    assert hasattr(demo, "self") and isinstance(demo.self, psv.InputStream)
    assert hasattr(demo, "intruder") and isinstance(demo.intruder, psv.InputStream)
    assert hasattr(demo, "output")

    # Compile demo output to sponge
    root, sources, evaluator = psv.compile_to_sponge(demo.output, {"self": demo.self, "intruder": demo.intruder})

    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di))  # type: ignore
    sponge = root * sink

    try:
        sponge.start()

        # Push a couple of items for both inputs to drive dist.km history and comparison
        # Self position
        sources["self"].push({"lat": 48.8566, "lon": 2.3522})  # Paris
        # Intruder position with id
        sources["intruder"].push({"lat": 51.5074, "lon": -0.1278, "id": 1, "time": 0})  # London

        # Push another pair to enable [-1] and [-2] comparison
        sources["self"].push({"lat": 48.8566, "lon": 2.3522})
        sources["intruder"].push({"lat": 51.5074, "lon": -0.1278, "id": 1, "time": 15})

        # Wait briefly for processing
        deadline = time.time() + 3.0
        while time.time() < deadline and len(outputs) < 1:  # type: ignore
            time.sleep(0.02)

        assert len(outputs) >= 1, "No output produced by compiled demo"  # type: ignore
    finally:
        try:
            sponge.stop()
            try:
                sponge.join()
            except Exception:
                pass
        except Exception:
            pass
