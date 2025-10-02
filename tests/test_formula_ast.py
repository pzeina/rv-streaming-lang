import ast
import importlib
import os
import sys
import types

# Ensure project root is on sys.path for importlib to find pystreamv.py
PROJECT_ROOT = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))
if PROJECT_ROOT not in sys.path:
    sys.path.insert(0, PROJECT_ROOT)

psv = importlib.import_module("pystreamv")


def test_arithmetic_and_compare_ast_construction():
    a = psv.Formula(ast.Name(id="a", ctx=ast.Load()))
    b = psv.Formula(ast.Name(id="b", ctx=ast.Load()))

    expr = (a + 2) * (b - 3) / (a ** 2)
    assert hasattr(expr, "ast_node")
    node = expr.ast_node
    assert isinstance(node, ast.BinOp)
    # a quick spot check of structure
    assert isinstance(node.left, ast.BinOp)
    assert isinstance(node.right, ast.BinOp)

    cmp = (a + 1) < (b + 1)
    assert isinstance(cmp.ast_node, ast.Compare)


def test_attribute_chain_via_fieldstream():
    s = psv.InputStream(name="s")
    lat = s.lat
    assert hasattr(lat, "ast_node")
    assert isinstance(lat.ast_node, ast.Attribute)
    assert isinstance(lat.ast_node.value, ast.Name)
    assert lat.ast_node.attr == "lat"


def test_subscript_history_builds_ast():
    x = psv.Formula(ast.Name(id="x", ctx=ast.Load()))
    h1 = x[-1]
    assert isinstance(h1.ast_node, ast.Subscript)


def test_function_lifting_and_call_ast():
    def my_add(u: int, v: int) -> int:
        return u + v

    adder = psv.lift(my_add)
    a = psv.Formula(ast.Name(id="a", ctx=ast.Load()))
    b = psv.Formula(ast.Name(id="b", ctx=ast.Load()))
    call = adder(a, b)
    assert isinstance(call.ast_node, ast.Call)


def test_module_proxy_wraps_callables():
    m = types.SimpleNamespace()
    m.double = lambda x: x * 2  # type: ignore[assignment]
    proxy = psv.wrap_module(m)
    a = psv.Formula(ast.Name(id="a", ctx=ast.Load()))
    call = proxy.double(a)
    assert isinstance(call.ast_node, ast.Call)


def test_evaluator_simple_binops_and_bools():
    a = psv.Formula(ast.Name(id="a", ctx=ast.Load()))
    b = psv.Formula(ast.Name(id="b", ctx=ast.Load()))
    expr = ((a + 2) * 3) > (b + 1)
    ev = psv.FormulaEvaluator(env={"a": 5, "b": 2})
    assert ev.eval(expr) is True


def test_evaluator_attribute_and_dict_access():
    s = psv.InputStream(name="s")
    expr = s.value
    ev = psv.FormulaEvaluator(env={"s": {"value": 42}})
    assert ev.eval(expr) == 42


def test_history_sequence_indexing():
    hs = psv.HistorySequence("k")
    hs.append(1)
    hs.append(2)
    assert hs.get_by_index(-1) == 2


def test_geopy_fallback_distance_km_history():
    # Use fallback: synthesize geopy.distance.geodesic(...).km
    gp = types.ModuleType('geopy')
    dist_mod = types.ModuleType('geopy.distance')

    class _D:
        def __init__(self, key: str):
            self.km = psv.HistorySequence(key)

    def geodesic(a: object, b: object) -> object:
        return _D(f"geodesic({a},{b})")

    dist_mod.geodesic = geodesic  # type: ignore
    gp.distance = dist_mod  # type: ignore
    sys.modules['geopy'] = gp
    sys.modules['geopy.distance'] = dist_mod

    s = psv.InputStream(name="s")
    lat = s.lat
    lon = s.lon

    # Build expression geodesic((lat, lon), (lat, lon)).km[-1]
    proxy = psv.wrap_module(gp)
    expr = proxy.distance.geodesic((lat, lon), (lat, lon)).km[-1]
    # Append a value in the history for that key and then evaluate index
    key = ast.dump(expr.ast_node.value) if isinstance(expr.ast_node, ast.Subscript) else "k"
    psv.HistorySequence.append_to_key(key, 3.14)

    ev = psv.FormulaEvaluator(env={"s": {"lat": 0.0, "lon": 0.0}, "geopy": gp})
    assert ev.eval(expr) == 3.14
