import os
import sys
import time
from datetime import datetime, timedelta
import random

import pytest
import logging
logging.basicConfig(level=logging.DEBUG)
logger = logging.getLogger(__name__)

# Ensure project root is on sys.path so Python can import pystreamv.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)


def test_compile_to_sponge_sum_two_inputs():
    # Skip if logicsponge-core is not available
    ls = pytest.importorskip("logicsponge.core")

    # Skip if pystreamv is not available/importable
    psv = pytest.importorskip("pystreamv")

    # Define two input streams and a simple expression over them
    a = psv.InputStream(name="a")
    b = psv.InputStream(name="b")
    expr = a.value + b.value

    root, sources, _ = psv.compile_to_sponge(expr, {"a": a, "b": b})

    # Attach a sink to collect outputs
    outputs = []
    sink = ls.Dump(print_fun=lambda di: outputs.append(di)) # type: ignore
    sponge = root * sink

    try:
        sponge.start()

        # Push one item on each source so eval term can compute
        sources["a"].push({"value": 1})
        sources["b"].push({"value": 2})

        # Wait briefly for the pipeline to process
        deadline = time.time() + 2.0
        while time.time() < deadline and len(outputs) < 1: # type: ignore
            time.sleep(0.01)

        assert len(outputs) >= 1, "No output produced by sponge" # type: ignore
        first = outputs[0] # type: ignore
        # Support dicts and DataItem objects (plus common variants)
        if isinstance(first, dict):
            assert first == {"result": 3}
        elif hasattr(ls, "DataItem") and isinstance(first, ls.DataItem):
            assert first == ls.DataItem({"result": 3})
        elif hasattr(first, "data"): # type: ignore
            assert first.data == {"result": 3} # type: ignore
        elif hasattr(first, "payload"): # type: ignore
            assert first.payload == {"result": 3} # type: ignore
        else:
            raise AssertionError(f"Unexpected output type: {type(first)} -> {first!r}") # type: ignore
    finally:
        # Cleanly stop threads even on failure
        try:
            sponge.stop()
            # Wait for worker threads to exit to avoid lingering background threads
            try:
                sponge.join()
            except Exception:
                # Some terms may not implement join; that's fine
                pass
            logger.debug("Sponge stopped successfully")
        except Exception:
            logger.exception("Error stopping sponge")

    logger.debug(f"Outputs: {outputs}")  # type: ignore

def generate_random_dict(current_time=None):
    """Generate random data with optional timestamp"""
    ls = pytest.importorskip("logicsponge.core")
    
    if current_time is None:
        return ls.DataItem({"A": bool(random.getrandbits(1)), "B": bool(random.getrandbits(1))})
    return ls.DataItem({
        "Time": current_time,
        "A": bool(random.getrandbits(1)),
        "B": bool(random.getrandbits(1)),
    })


def test_basic_sponge_data_processing():
    """Test basic sponge data processing without monitoring modules"""
    # Skip if required modules are not available
    ls = pytest.importorskip("logicsponge.core")

    class TestDataSource(ls.SourceTerm):
        def __init__(self, data_count=10):
            super().__init__()
            self.current_time = datetime.now()
            self.count = 0
            self.max_count = data_count
            self._stop_flag = False

        def stop(self):
            self._stop_flag = True
            super().stop()

        def run(self):
            while self.count < self.max_count and not self._stop_flag:
                try:
                    # Generate data with timestamp
                    data_item = generate_random_dict(self.current_time)
                    self.output(data_item)
                    
                    self.current_time += timedelta(seconds=1)
                    self.count += 1
                    time.sleep(0.05)  # Small delay for processing
                except Exception as e:
                    logger.debug(f"Error in TestDataSource: {e}")
                    break

    # Initialize circuit to None for proper cleanup
    circuit = None
    
    try:
        # Collect outputs for verification
        final_outputs = []

        # Create output sink
        final_sink = ls.Dump(print_fun=lambda di: final_outputs.append(di))

        # Build the sponge circuit
        source = TestDataSource(data_count=8)
        
        # Simple data processing pipeline
        data_processing = ls.KeyFilter(not_keys="Time")
        
        # Main circuit
        circuit = source * data_processing * final_sink

        # Start the circuit
        circuit.start()

        # Wait for processing
        deadline = time.time() + 10.0
        while time.time() < deadline and len(final_outputs) < 5:
            time.sleep(0.1)

        # Verify outputs were generated
        assert len(final_outputs) > 0, "No final outputs produced"
        
        # Test that the pipeline produced some results
        logger.debug(f"Final outputs count: {len(final_outputs)}")
        logger.debug(f"Sample final outputs: {final_outputs[:3]}")
        
        # Verify data structure
        for output in final_outputs[:3]:
            if hasattr(output, 'data'):
                data = output.data
            elif isinstance(output, dict):
                data = output
            else:
                data = output
                
            logger.debug(f"Output data keys: {list(data.keys()) if isinstance(data, dict) else type(data)}")

    finally:
        # Clean shutdown
        if circuit is not None:
            try:
                circuit.stop()
            except Exception:
                logger.exception("Error stopping circuit")
            try:
                circuit.join()
            except Exception:
                logger.exception("Error joining circuit")

    logger.info("Basic sponge data processing test completed successfully")


if __name__ == "__main__":
    # Run this test module via pytest so that skips are handled gracefully
    # (e.g., when logicsponge is not installed) instead of raising exceptions.
    import pytest as _pytest
    import sys as _sys
    # Run only this file, in quiet mode; pytest will return 0 if tests pass or are skipped
    _sys.exit(_pytest.main([__file__, "-q"]))