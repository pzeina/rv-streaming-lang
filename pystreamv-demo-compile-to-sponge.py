"""
Concise demo: compile a pystreamv model to logicsponge ("sponge") and run a live random source.

Mirrors `pystreamv-demo.py` (self/intruder streams, stale detection, multiplex by id) with minimal code.
Steps:
    - define the model in pystreamv
    - compile to LS FunctionTerms via `compile_to_ls`
    - print the compiled tree (human-readable)
    - run a tiny dynamic sponge that multiplexes by id and prints computed outputs
"""
from __future__ import annotations

from typing import Any

import logicsponge.core as ls
import pystreamv as psv
from pystreamv_ls_compile import (
    compile_to_ls,  # type: ignore
    ls_print_tree,
    simple_intruder_source_map,
    run_random_intruder_sponge,
)




def build_model_and_compile() -> dict[str, Any]:
    # Stream types
    SelfType = type("SelfType", (), {"__annotations__": {"lat": float, "lon": float}})
    IntruderType = type(
        "IntruderType",
        (),
        {"__annotations__": {"lat": float, "lon": float, "id": int}},
    )

    # Inputs
    _self_stream = psv.InputStream(type=SelfType)  # unused here, kept for parity with demo
    intruder = psv.InputStream(type=IntruderType)
    intr_ts = psv.timestamp(intruder)

    # Formulas (stale + H + multiplex)
    stale = ((intr_ts.time.last() - psv.GLOBAL.time) > 10).every(10 * psv.s)
    dist_stream = psv.Formula("distance_stream")  # type: ignore  # placeholder numeric stream
    output = psv.H(5 * psv.s, dist_stream[-1] < dist_stream[-2])  # type: ignore
    mux = psv.multiplex_id(output, id_from=intruder.id, eos_from=stale)

    # Compile pystreamv AST -> pystreamv.FunctionTerm
    stale_ft = stale.compile()  # type: ignore[attr-defined]
    mux_ft = mux.compile()  # type: ignore[attr-defined]

    # Map sources to backend LS placeholder nodes
    source_map = simple_intruder_source_map(IntruderType)

    # Compile to LS tree
    ls_stale = compile_to_ls(stale_ft, ls=ls, source_map=source_map)  # type: ignore[arg-type]
    ls_mux = compile_to_ls(mux_ft, ls=ls, source_map=source_map)  # type: ignore[arg-type]

    ls_print_tree("stale (ls)", ls_stale)
    ls_print_tree("multiplex (ls)", ls_mux)

    # Optional: light verification comment – LS tree contains multiplex/always/index/const
    # (see test_pystreamv_ls_compile.py for a full structural check)

    return {
        "types": {"SelfType": SelfType, "IntruderType": IntruderType},
        "ls": {"stale": ls_stale, "mux": ls_mux},
    }


def run_random_source_demo(runtime_s: float = 10.0):
    # One-liner helper to run a live dynamic sponge with printing
    run_random_intruder_sponge(runtime_s=runtime_s, period_s=0.05)


if __name__ == "__main__":
    build_model_and_compile()
    run_random_source_demo(10.0)
    print("\nDemo complete.")
