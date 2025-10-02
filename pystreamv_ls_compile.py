from __future__ import annotations

from typing import Any, Callable, Dict, Iterable, List, Mapping, Optional, Tuple

import pystreamv
import logicsponge.core as ls  # use logicsponge-core backend
from logicsponge.core.logicsponge import DataItem


PSV_SourceTerm = getattr(pystreamv, 'SourceTerm', ())
PSV_ConstantTerm = getattr(pystreamv, 'ConstantTerm', ())
PSV_PeriodTerm = getattr(pystreamv, 'PeriodTerm', ())
PSV_UnitTerm = getattr(pystreamv, 'UnitTerm', ())
PSV_ExternalFunctionTerm = getattr(pystreamv, 'ExternalFunctionTerm', ())
PSV_FunctionTerm = getattr(pystreamv, 'FunctionTerm', ())


class SourceResolver:
    """
    Resolve pystreamv.SourceTerm to backend ls source terms.

    mapping key can be:
      - (stream_type_name: str, field: str)
      - (stream_type: type, field: str)
    """
    def __init__(self, mapping: Mapping[Tuple[Any, str], Any]):
        self._mapping = dict(mapping)

    def __call__(self, src: Any) -> Any:
        st = getattr(src, "stream_type", None)
        field = getattr(src, "field", None)
        if st is None or field is None:
            raise ValueError("Invalid SourceTerm, missing stream_type/field")
        # try by class object then by class name
        key_obj = (st, field)
        if key_obj in self._mapping:
            return self._mapping[key_obj]
        key_name = (getattr(st, "__name__", str(st)), field)
        if key_name in self._mapping:
            return self._mapping[key_name]
        raise KeyError(f"No ls source mapping for {key_name}")


def default_make_call(_: Any = None) -> Callable[[str, List[Any]], Any]:
    """Factory for ls calls. Create FunctionTerm(name=...) and attach args attribute."""
    def _make(name: str, args: List[Any]):
        node = ls.FunctionTerm(name=name)
        # Attach args for downstream consumers/tests
        try:
            node.args = list(args)  # type: ignore[attr-defined]
        except Exception:
            pass
        return node
    return _make


def default_make_const(_: Any = None) -> Callable[[Any], Any]:
    """Factory for constants: represent as FunctionTerm 'const' with value and args[0]."""
    def _make(value: Any):
        node = ls.FunctionTerm(name='const')
        # store value in value attribute and in args[0] for compatibility
        try:
            node.value = value  # type: ignore[attr-defined]
        except Exception:
            pass
        try:
            node.args = [value]  # type: ignore[attr-defined]
        except Exception:
            pass
        return node
    return _make


class PystreamvToLSCompiler:
    """
    Compile a pystreamv.FunctionTerm into a backend ls.FunctionTerm (logicsponge.core).
    Provide:
      - resolve_source: callable to convert pystreamv.SourceTerm -> ls term
      - make_const/make_call: optional factories to build backend nodes
    """
    def __init__(
        self,
    resolve_source: Callable[[Any], Any],
        make_const: Optional[Callable[[Any], Any]] = None,
        make_call: Optional[Callable[[str, List[Any]], Any]] = None,
    ):
        self.resolve_source = resolve_source
        self.make_const = make_const or default_make_const()
        self.make_call = make_call or default_make_call()

    def compile(self, node: Any) -> Any:
        return self._compile_node(node)

    # ---- internals ----

    def _compile_node(self, node: Any) -> Any:
        # Leaf specializations
        if isinstance(node, PSV_SourceTerm):
            return self.resolve_source(node)
        if isinstance(node, PSV_ConstantTerm):
            return self.make_const(node.value)
        if isinstance(node, PSV_PeriodTerm):
            # normalize to seconds constant
            seconds = float(node.period.value) * float(node.period.unit.to_seconds)
            return self.make_const(seconds)
        if isinstance(node, PSV_UnitTerm):
            # expose unit scale as constant seconds factor
            return self.make_const(float(node.unit.to_seconds))
        if isinstance(node, PSV_ExternalFunctionTerm):
            # represent external as a call with its repr/name as payload
            return self.make_call("external", [self.make_const(repr(node.function))])

        # Composite terms based on function_name
        fn = getattr(node, "function_name", None)
        args = list(getattr(node, "args", []))

        # Recursively compile children first
        def c(child: Any) -> Any:
            return self._compile_node(child)

        # Operators / nodes mapping
        if fn in ("lt", "gt", "sub", "index", "last", "every", "always", "multiplex",
                  "global_time", "time_unit_second"):
            if fn == "every":
                # every(target, periodSeconds)
                target = c(args[0])
                # args[1] may already be seconds constant from PeriodTerm
                period = c(args[1])
                return self.make_call("every", [target, period])
            if fn == "always":
                # always(durationSeconds, condition)
                duration = c(args[0])
                condition = c(args[1])
                return self.make_call("always", [duration, condition])
            if fn == "last":
                # last(target)
                return self.make_call("last", [c(args[0])])
            if fn == "index":
                # index(target, index) with index possibly a const
                return self.make_call("index", [c(args[0]), c(args[1])])
            if fn in ("lt", "gt", "sub"):
                # binary op
                return self.make_call(fn, [c(args[0]), c(args[1])])
            if fn == "multiplex":
                # multiplex(output, id, eos)
                return self.make_call("multiplex", [c(args[0]), c(args[1]), c(args[2])])
            if fn == "global_time":
                return self.make_call("global_time", [])
            if fn == "time_unit_second":
                return self.make_const(1.0)

        # Fallback: generic call with compiled args
        return self.make_call(fn or "unknown", [self._compile_node(a) for a in args])


def compile_to_ls(
    root: Any,
    *,
    ls: Any = None,  # kept for backward-compatibility, ignored
    source_map: Optional[Mapping[Tuple[Any, str], Any]] = None,
    resolve_source: Optional[Callable[[Any], Any]] = None,
    make_const: Optional[Callable[[Any], Any]] = None,
    make_call: Optional[Callable[[str, List[Any]], Any]] = None,
) -> Any:
    """
    Compile a pystreamv.FunctionTerm to a logicsponge.core FunctionTerm.

    Provide either:
      - resolve_source callable, or
      - source_map mapping {(stream_type|stream_type_name), field} -> ls source term
    """
    if resolve_source is None:
        if source_map is None:
            raise ValueError("Provide resolve_source or source_map")
        resolve_source = SourceResolver(source_map)
    compiler = PystreamvToLSCompiler(resolve_source, make_const, make_call)
    return compiler.compile(root)


# ----------------------------
# Convenience helpers for demos
# ----------------------------

def ls_print_tree(label: str, node: Any, *, indent: int = 0) -> None:
    prefix = "  " * indent
    if indent == 0:
        print(f"\n=== {label} ===")
    FT = ls.FunctionTerm
    if isinstance(node, FT):
        name = getattr(node, "name", getattr(node, "function_name", "<?>"))
        print(f"{prefix}{name}:")
        for a in getattr(node, "args", []):
            ls_print_tree(label, a, indent=indent + 1)
    else:
        print(f"{prefix}{node!r}")


def ls_ft(name: str, args: Optional[list[Any]] = None) -> Any:
    """Create an ls.FunctionTerm with a name and attach args list for inspection."""
    node = ls.FunctionTerm(name=name)
    try:
        node.args = [] if args is None else list(args)  # type: ignore[attr-defined]
    except Exception:
        pass
    return node


def simple_intruder_source_map(intruder_type: Any) -> Mapping[tuple[Any, str], Any]:
    """Return a minimal source map for an "Intruder" stream type: time and id sources."""
    return {
        (intruder_type, "time"): ls_ft("source_intruder_time"),
        (intruder_type, "id"): ls_ft("source_intruder_id"),
    }


class RandomIntruderSource(ls.SourceTerm):
    """A logicsponge SourceTerm producing infinite random intruder items (id, lat, lon, time)."""

    def __init__(self, *, name: str = "intruder", period_s: float = 0.05):
        super().__init__(name=name)  # type: ignore[misc]
        self._period_s = period_s

    def run(self) -> None:  # type: ignore[override]
        import random
        import threading
        import time
        i = 0
        while not getattr(self, "_stop_event", threading.Event()).is_set():
            i += 1
            payload = {
                "id": random.randint(1, 5),
                "lat": random.uniform(-90, 90),
                "lon": random.uniform(-180, 180),
                "time": time.time(),
            }
            di = DataItem(payload)
            di.set_time_to_now()
            if i % 25 == 0:
                print(f"[src] emit #{i}: id={payload['id']} lat={payload['lat']:.4f} lon={payload['lon']:.4f}")
            self.output(di)
            time.sleep(self._period_s)


class ComputeDistanceTerm(ls.FunctionTerm):
    """Compute a simple 'distance' metric from lat/lon and forward id."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[misc]

    def f(self, item: DataItem) -> DataItem:  # type: ignore[override]
        import math
        try:
            lat = float(item.get("lat", 0.0))
            lon = float(item.get("lon", 0.0))
            idv = item.get("id", None)
        except Exception:
            return item
        dist = math.hypot(lat, lon)
        out = DataItem({"id": idv, "dist": dist})
        out.time = item.time
        return out


class CompareLastTwoTerm(ls.FunctionTerm):
    """Keep last two 'dist' values and output whether last < previous as key 'lt'."""

    def __init__(self, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[misc]
        self.state = {"prev": None}

    def f(self, item: DataItem) -> DataItem:  # type: ignore[override]
        dist = item.get("dist", None)
        prev = self.state.get("prev")
        payload: dict[str, Any] = {"id": item.get("id"), "dist": dist}
        if isinstance(dist, (int, float)) and isinstance(prev, (int, float)):
            payload["lt"] = bool(dist < prev)
        self.state["prev"] = dist
        out = DataItem(payload)
        out.time = item.time
        return out


class ComputeStaleTerm(ls.FunctionTerm):
    """Compute stale = (now - item.time) > threshold_s and add it to the item."""

    def __init__(self, threshold_s: float = 10.0, *args: Any, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)  # type: ignore[misc]
        self._th = float(threshold_s)

    def f(self, item: DataItem) -> DataItem:  # type: ignore[override]
        import time as _time
        now = _time.time()
        try:
            stale = (now - float(item.time)) > self._th  # type: ignore[arg-type]
        except Exception:
            stale = False
        try:
            payload = {k: item.get(k) for k in item.keys()}  # type: ignore[attr-defined]
        except Exception:
            payload = {}
        payload["stale"] = stale
        out = DataItem(payload)
        out.time = item.time
        return out


# Annotations so LS can match f() signatures at runtime
ComputeDistanceTerm.f.__annotations__ = {"item": DataItem, "return": DataItem}  # type: ignore[attr-defined]
CompareLastTwoTerm.f.__annotations__ = {"item": DataItem, "return": DataItem}  # type: ignore[attr-defined]
ComputeStaleTerm.f.__annotations__ = {"item": DataItem, "return": DataItem}  # type: ignore[attr-defined]


def run_random_intruder_sponge(*, runtime_s: float = 10.0, period_s: float = 0.05) -> None:
    """Run a simple dynamic sponge that multiplexes by 'id' and prints computed fields."""
    src = RandomIntruderSource(period_s=period_s)

    def spawn_fun(key: Any):
        return (
            ComputeDistanceTerm(name=f"dist_{key}")
            * CompareLastTwoTerm(name=f"cmp_{key}")
            * ComputeStaleTerm(threshold_s=10.0, name=f"stale_{key}")
            * ls.Print(keys=["id", "dist", "lt", "stale"], print_fun=print)
        )

    sponge = ls.DynamicSpawnTerm(filter_key="id", spawn_fun=spawn_fun)
    pipeline = src * sponge
    print("\nStarting sponge with random intruder source...")
    pipeline.start()
    try:
        import time as _time
        _time.sleep(runtime_s)
    finally:
        print("Stopping sponge...")
        pipeline.stop()
        pipeline.join()
    print("Sponge stopped.")