from __future__ import annotations

from typing import Any

import logicsponge.core as ls
import pystreamv
from pystreamv_ls_compile import compile_to_ls


def print_tree(label: str, node: Any, indent: int = 0):
    prefix = "  " * indent
    if indent == 0:
        print(f"\n=== {label} ===")
    FT = ls.FunctionTerm
    if isinstance(node, FT):
        name = getattr(node, "name", getattr(node, "function_name", "<?>"))
        print(f"{prefix}{name}:")
        for a in getattr(node, "args", []):
            print_tree(label, a, indent + 1)
    else:
        print(f"{prefix}{node!r}")


def test_compile_stale_and_multiplex():
    # Streams
    SelfType = type('SelfType', (), {'__annotations__': {'lat': float, 'lon': float}})
    IntruderType = type('IntruderType', (), {'__annotations__': {'lat': float, 'lon': float, 'id': int}})
    self_stream = pystreamv.InputStream(type=SelfType)
    intruder = pystreamv.InputStream(type=IntruderType)
    intr_ts = pystreamv.timestamp(intruder)

    # Formulas
    stale = ((intr_ts.time.last() - pystreamv.GLOBAL.time) > 10).every(10 * pystreamv.s)
    mock_dist = pystreamv.Formula('distance_stream')
    output = pystreamv.H(5 * pystreamv.s, mock_dist[-1] < mock_dist[-2])
    mux = pystreamv.multiplex_id(output, id_from=intruder.id, eos_from=stale)

    # Compile pystreamv AST -> pystreamv.FunctionTerm
    stale_ft = stale.compile()
    mux_ft = mux.compile()

    # Map sources to backend (use backend-native FunctionTerm placeholders)
    FT = ls.FunctionTerm
    # Helper to build placeholder FunctionTerms compatible with logicsponge ctor
    def FTn(name: str, args: list[Any] | None = None):
        node = FT(name=name)
        # attach args attribute for downstream tree checks
        node.args = [] if args is None else args
        return node
    source_map = {
        (IntruderType, 'time'): FTn('source_intruder_time'),
        (IntruderType, 'id'): FTn('source_intruder_id'),
    }

    # Compile to LS
    ls_stale = compile_to_ls(stale_ft, ls=ls, source_map=source_map)
    ls_mux = compile_to_ls(mux_ft, ls=ls, source_map=source_map)

    print_tree("stale (ls)", ls_stale)
    print_tree("multiplex (ls)", ls_mux)

    # Basic structure checks
    assert isinstance(ls_stale, FT) and getattr(ls_stale, 'name', None) == 'every'
    assert isinstance(ls_mux, FT) and getattr(ls_mux, 'name', None) == 'multiplex'

    # Check indices exist inside the always condition (output)
    always = getattr(ls_mux, 'args', [None])[0]
    assert isinstance(always, FT) and getattr(always, 'name', None) == 'always'
    cond = getattr(always, 'args', [None, None])[1]
    assert isinstance(cond, FT) and getattr(cond, 'name', None) == 'lt'
    left_idx = getattr(cond, 'args', [None, None])[0]
    right_idx = getattr(cond, 'args', [None, None])[1]
    assert isinstance(left_idx, FT) and getattr(left_idx, 'name', None) == 'index'
    assert isinstance(right_idx, FT) and getattr(right_idx, 'name', None) == 'index'
    l_idx_arg = getattr(left_idx, 'args', [None, None])[1]
    r_idx_arg = getattr(right_idx, 'args', [None, None])[1]
    assert isinstance(l_idx_arg, FT) and getattr(l_idx_arg, 'name', None) == 'const'
    assert isinstance(r_idx_arg, FT) and getattr(r_idx_arg, 'name', None) == 'const'
    l_val = getattr(l_idx_arg, 'value', None)
    r_val = getattr(r_idx_arg, 'value', None)
    if l_val is None and getattr(l_idx_arg, 'args', None):
        l_val = l_idx_arg.args[0]
    if r_val is None and getattr(r_idx_arg, 'args', None):
        r_val = r_idx_arg.args[0]
    assert l_val == -1
    assert r_val == -2


if __name__ == "__main__":
    # Simple manual run
    test_compile_stale_and_multiplex()
    print("\nOK (manual run)")
