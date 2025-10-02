# pystreamv: Stream formulas → AST → sponge

pystreamv is a tiny DSL to build symbolic formulas over streams. It constructs Python ASTs (without executing), can evaluate them against incoming data, and can compile them into a logicsponge-core pipeline (a “sponge”). It also supports lifting arbitrary Python functions into the DSL, so calls like `geopy.distance.geodesic(a, b).km[-1]` become AST that the evaluator/compiler can interpret.

- File: `pystreamv.py` — core DSL, evaluator, and simple compile-to-sponge
- File: `pystreamv_ls_compile.py` — structural compiler to logicsponge FunctionTerms (LS) + handy helpers for demos
- Optional dep: `geopy` (a minimal fallback is provided if it’s not installed)

If you plan to compile/run with logicsponge-core, install it and follow its runtime usage. You can still build ASTs and use the evaluator without it.

Requirements:
- Python 3.11+
- `pip install -r requirements.txt`
  - geopy==2.3.0 (optional for geodesic examples)
  - logicsponge-core, logicsponge-monitoring (for sponge demos)

## Quick start

```python
from pystreamv import InputStream, wrap_module, FormulaEvaluator, compile_to_sponge

# Define input streams
gps = InputStream(name="gps")
lat = gps.lat
lon = gps.lon

# Lift geopy (or use built-in fallback)
import geopy
geo = wrap_module(geopy)

# Build a formula: last distance in km is < 1
dist_km = geo.distance.geodesic((lat, lon), (lat, lon)).km
expr = (dist_km[-1] < 1.0)

# Evaluate locally (no logicsponge required)
ev = FormulaEvaluator()
env = {"gps": {"lat": 48.85, "lon": 2.35}}  # mock item
ev.env = {k: v for k, v in env.items()}  # evaluator wraps dict inputs automatically in compile mode
result = ev.eval(expr)
print("result:", result)

# Compile to a sponge (requires logicsponge-core)
# root, sources, evaluator = compile_to_sponge(expr, {"gps": gps})
# sources["gps"].push({"lat": 48.85, "lon": 2.35})
# root.run()  # consult logicsponge-core docs for orchestration

# Alternatively: compile to LS FunctionTerms for structure-only use
# (see section below for details and a live demo)
```

## Architecture overview

1) Expressions as AST
- `Formula(ast.expr)` wraps a Python AST expression.
- Operator overloads (`+`, `-`, `*`, `/`, `**`, comparisons, `&`, `|`, `~`) build new AST nodes.
- Indexing `expr[-1]` creates `ast.Subscript` for history access.
- `.every(Period)` or `@` is a scheduling annotation that returns `ScheduledFormula`.

2) Streams and fields
- `InputStream(name=...)` represents a named stream.
- Access fields via attributes: `gps.lat` returns `FieldStream` whose `hold()` is an AST attribute chain (e.g., `Name("gps").attr("lat")`).
- Streams also have `last(n)` which builds a symbolic `ast.Call(Name("last"), ...)` (evaluation semantics are pluggable).

3) Function lifting
- `FunctionRef` wraps a Python callable. If called with any `Formula`/`Stream` args, it builds an `ast.Call`; otherwise it calls the function.
- `ModuleProxy` wraps a module so accessing callables returns `FunctionRef` automatically and nested modules are proxied.
- Helpers:
  - `lift(func) -> FunctionRef`
  - `wrap_module(module) -> ModuleProxy`

4) Evaluation
- `FormulaEvaluator(env: dict)` interprets AST at runtime against `env`.
- Supported AST: `Constant`, `Name`, `Attribute`, `BinOp`, `Compare`, `BoolOp`, `UnaryOp(not)`, `Subscript` (history indexing on `HistorySequence`), `Call` (resolves names/attributes to callables/modules in `env` or `builtins`).
- History support: `HistorySequence(key)` acts like a list with `[-1]`, `[-2]`, ...; the fallback `geodesic(...).km` produces a `HistorySequence` so `km[-1]` works.
- Note: `Stream.last()` currently generates a symbolic call; you can inject `env['last'] = lambda xs, n: xs[-n]` and map `xs` to a suitable history source if you need that behavior.

5) Compile to sponge
- `compile_to_sponge(expr_or_scheduled, inputs: dict[str, InputStream]) -> (root_term, sources, evaluator)`
  - Creates one `ExternalSource` per input name (push-based)
  - Runs them in parallel and feeds a `FunctionTerm` that evaluates the `Formula` with `FormulaEvaluator`
  - Returns the assembled root term, the sources (so you can `push` dicts), and the shared evaluator instance

## API cheat sheet

Core types
- `class Formula(ast_node: ast.expr)`
  - arithmetic: `+ - * / **`
  - compare: `< <= > >= == !=`
  - boolean: `& | ~`
  - indexing: `__getitem__(int|slice|Formula)` -> history/subscript
  - schedule: `every(Period)` and `__matmul__(Period)`

- `class Stream(type=None, name=None)`
  - `hold() -> Formula` returns AST name for the stream
  - `last(n=1) -> Formula` symbolic last-call
  - attribute access returns `FieldStream`

- `class FieldStream(Stream)`
  - `hold() -> Formula` returns AST attribute chain

- `class InputStream(Stream)` — marker for inputs

Scheduling
- `class Unit(name: str, to_seconds: float)` with singletons: `s`, `ms`, `m`, `h`
- `class Period(value: float, unit: Unit)` and `class ScheduledFormula(formula: Formula, period: Period)`

Lifting
- `lift(func) -> FunctionRef`
- `wrap_module(module) -> ModuleProxy`

Evaluation
- `class FormulaEvaluator(env: Optional[dict] = None)`
  - `eval(expr_or_formula)` — evaluates a Formula AST or returns the literal
  - resolves `Name` via `env` or `builtins`
  - supports dict attribute access and `DataItemProxy` convenience used during compilation

Compilation
- `compile_to_sponge(expr_or_scheduled, inputs: dict[name, InputStream])`
  - returns `(root_term, sources: dict[name, ExternalSource], evaluator)`
  - `sources[name].push(dict)` enqueues an item; the FunctionTerm emits a `DataItem` of shape `{"result": value}` unless the result is already a dict

LS structural compile helpers (in `pystreamv_ls_compile`)
- `compile_to_ls(expr_or_scheduled, source_map) -> FunctionTerm`
- `ls_print_tree(label, node)`
- `simple_intruder_source_map(IntruderType)`
- `run_random_intruder_sponge(runtime_s=10.0, period_s=0.05)`

6) Compile to logicsponge (LS) structure
- `pystreamv_ls_compile.compile_to_ls(expr_or_scheduled, source_map)` converts the AST into a tree of `logicsponge.core.FunctionTerm` nodes.
- The compiler models common operators with well-known names: `lt`, `gt`, `sub`, `index`, `last`, `every`, `always`, `multiplex`, `global_time`, and a `const` wrapper.
- Constants are represented as `FunctionTerm(name='const')` with both `.value` and `.args=[value]` for easy inspection.
- Helpers available for demos:
  - `ls_print_tree(label, node)` — pretty-print an LS tree
  - `simple_intruder_source_map(IntruderType)` — map `(Type, field)` to LS placeholder sources used by the compiler
  - `run_random_intruder_sponge(runtime_s=10.0, period_s=0.05)` — start a live dynamic sponge streaming random intruder-like items, computing distance, comparing last two, flagging `stale`, and printing outputs

Utilities
- `class HistorySequence(key: str)` with `.get_by_index(idx)` and class method `append_to_key(key, value)`
- `class DataItemProxy(data: dict)` exposes attributes from dicts (used when compiling)

## How it works under the hood

1) AST building
- All DSL operations construct Python `ast.expr` nodes. No execution happens during construction.
- Example: `a + b` yields `ast.BinOp(left=a.ast_node, op=ast.Add(), right=b.ast_node)`.

2) Function lifting
- When a lifted function is called with any `Formula/Stream` argument, it returns a new `Formula` wrapping `ast.Call` with arguments converted recursively to AST.
- A wrapped module (`ModuleProxy`) automatically yields lifted callables for all functions down the attribute chain.

3) Evaluation
- The evaluator walks the AST nodes and executes them against `env`, mimicking Python for supported nodes. It also supports attribute resolution on `DataItemProxy` and plain dicts.
- History indexing: when the target is a `HistorySequence`, `[-1]` etc. is served from its internal store.
- The fallback `geopy` emulation returns an object whose `.km` is a `HistorySequence`, enabling `geodesic(...).km[-1]`.

4) Compilation to logicsponge
- Creates a push-based `ExternalSource` per input and a `FunctionTerm` (`EvalFormulaTerm`) that evaluates the formula over incoming items.
- The evaluator env is rebuilt per batch: inputs are converted to plain dicts then wrapped into `DataItemProxy` objects.
- Results are emitted as `DataItem` objects. The example uses a single `FunctionTerm`, but you can extend the assembly to more complex pipelines.

## Examples

Distance check
```python
from pystreamv import InputStream, wrap_module, compile_to_sponge
import geopy

gps = InputStream(name="gps")
lat = gps.lat
lon = gps.lon
geo = wrap_module(geopy)
expr = geo.distance.geodesic((lat, lon), (lat, lon)).km[-1] < 1.0

root, sources, evaluator = compile_to_sponge(expr, {"gps": gps})
sources["gps"].push({"lat": 48.85, "lon": 2.35})
# Start/compose with logicsponge runtime to consume outputs
```

Boolean combination
```python
from pystreamv import InputStream

s = InputStream(name="s")
expr = (s.value > 10) & ((s.flag == True) | ~(s.error))  # builds AST only
```

Live demos

- `pystreamv-demo.py` — simple evaluator + geopy fallback demo, no logicsponge required.
- `pystreamv-demo-compile-to-sponge.py` — concise demo that:
  - builds a pystreamv model (self/intruder streams, stale detection, multiplex)
  - compiles it to logicsponge LS FunctionTerms
  - prints the compiled trees (human-readable)
  - runs a live random source through a dynamic per-id sponge and prints outputs

How to run (optional virtualenv recommended):

```bash
# create and activate venv (optional)
python -m venv .venv
source .venv/bin/activate

# install deps
pip install -r requirements.txt

# run the LS compile demo
python -X utf8 -u pystreamv-demo-compile-to-sponge.py

# sample output will include printed LS trees and streaming lines like:
# {'id': 3, 'dist': 142.00, 'lt': False, 'stale': False}
```

Programmatic usage of the LS compiler helpers:

```python
from pystreamv_ls_compile import compile_to_ls, ls_print_tree, simple_intruder_source_map, run_random_intruder_sponge
import pystreamv as psv

# define types and streams
class Intruder: pass
intr = psv.InputStream(type=Intruder)
ts = psv.timestamp(intr)

expr = (ts.last(1) - psv.global_time > 10) @ psv.s(10)  # stale example
ls_node = compile_to_ls(expr, simple_intruder_source_map(Intruder))
ls_print_tree("stale (ls)", ls_node)

# run a live demo sponge for ~10s
run_random_intruder_sponge(runtime_s=10.0, period_s=0.05)
```

## Limitations and next steps

- The `last()` symbolic call is not implemented by default in the evaluator; inject an env function or extend evaluator logic based on your needs.
- The evaluator supports a well-defined subset of Python AST nodes; extend as necessary.
- The `geopy` fallback is minimal and focused on enabling `.km[-1]` distance patterns.

## Development

Requirements:
- Python 3.11+
- Optional: `geopy` for real geodesic distance APIs (fallback included)
- Optional: `logicsponge-core` for compilation target

Install dev dependencies (optional):

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Run tests (after generating them):

```bash
pytest -q
```
