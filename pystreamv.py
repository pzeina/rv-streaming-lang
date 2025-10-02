from __future__ import annotations

import sys
from typing import Any, Dict, Type, List, Optional, Union
import ast


# --- Units / Period / Scheduling -------------------------------------------
class Unit:
    def __init__(self, name: str, to_seconds: float = 1.0):
        self.name = name
        self.to_seconds = to_seconds

    def __rmul__(self, value: Union[int, float]) -> 'Period':
        return Period(float(value), self)

    def __repr__(self) -> str:
        return f"Unit({self.name})"


class Period:
    def __init__(self, value: float, unit: Unit):
        self.value = float(value)
        self.unit = unit

    def to_seconds(self) -> float:
        return self.value * self.unit.to_seconds

    def __repr__(self) -> str:
        return f"Period({self.value}{self.unit.name})"


class ScheduledFormula:
    def __init__(self, formula: Formula, period: Period):
        self.formula = formula
        self.period = period

    def __repr__(self) -> str:
        return f"ScheduledFormula({self.formula}, every={self.period})"


# Common time units
s = Unit('s', 1.0)
ms = Unit('ms', 0.001)
m = Unit('m', 60.0)
h = Unit('h', 3600.0)

class Formula:
    """Base class for formula AST nodes"""
    def __init__(self, op: str, *args: Any):
        self.op = op
        self.args = args
    
    def __lt__(self, other: Any):
        return BinaryOp('<', self, other)
    
    def __gt__(self, other: Any):
        return BinaryOp('>', self, other)
    
    def __sub__(self, other: Any):
        return BinaryOp('-', self, other)
    
    def __getitem__(self, index: int):
        return IndexOp(self, index)
    
    def every(self, period: Union[Period, Any]):
        return TemporalOp('every', self, period)
    
    def last(self):
        return TemporalOp('last', self)

    def to_ast(self) -> 'FormulaAST':
        """Convert this formula to an AST node"""
        return FormulaAST(self)
    
    def compile(self) -> 'FunctionTerm':
        """Compile this formula to a logicsponge circuit"""
        ast_node = self.to_ast()
        return ast_node.compile_to_circuit()

class BinaryOp(Formula):
    def __init__(self, op: str, left: Any, right: Any):
        super().__init__(op, left, right)
        self.left = left
        self.right = right

class IndexOp(Formula):
    def __init__(self, target: Any, index: Any):
        super().__init__('index', target, index)
        self.target = target
        self.index = index

class TemporalOp(Formula):
    def __init__(self, op: str, target: Any, *args: Any):
        # Only store extra args (e.g., period) in Formula.args, not the target
        super().__init__(op, *args)
        self.target = target

class FieldAccess(Formula):
    def __init__(self, stream: 'InputStream', field: str):
        super().__init__('field_access', stream, field)
        self.stream = stream
        self.field = field

class InputStream:
    def __init__(self, type: Type[Any]):
        self.type = type
        self._fields: Dict[str, Type[Any]] = {}
        
        # Handle both class annotations and dict-style type definitions
        if hasattr(type, '__annotations__'):
            for field_name, field_type in type.__annotations__.items():
                self._fields[field_name] = field_type
        elif isinstance(type, dict):
            # Handle dict-style type definitions from demo
            for field_name, field_type in type.items():
                self._fields[field_name] = field_type
        elif hasattr(type, '__dict__'):
            # Handle type() created classes
            for field_name, field_type in type.__dict__.items():
                if not field_name.startswith('_'):
                    self._fields[field_name] = field_type
    
    def __getattr__(self, name: str):
        if name in self._fields:
            return FieldAccess(self, name)
        raise AttributeError(f"Stream has no field '{name}'")

class TimestampedStream(InputStream):
    def __init__(self, original_stream: InputStream):
        super().__init__(original_stream.type)
        self._original = original_stream
        # Copy all fields from original stream
        self._fields.update(original_stream._fields)
        # Add time field
        self._fields['time'] = float
        self.time = FieldAccess(self, 'time')

class GlobalTime:
    def __init__(self):
        self.time = Formula('global_time')

class MultiplexFormula(Formula):
    def __init__(self, output: Any, id_from: Any, eos_from: Any):
        super().__init__('multiplex', output, id_from, eos_from)
        self.output = output
        self.id_from = id_from
        self.eos_from = eos_from

class AlwaysOperator(Formula):
    def __init__(self, duration: Union[Period, Any], condition: Any):
        super().__init__('always', duration, condition)
        self.duration = duration
        self.condition = condition

# DSL functions
def timestamp(stream: InputStream) -> TimestampedStream:
    return TimestampedStream(stream)

def H(duration: Union[Period, Any], condition: Any) -> AlwaysOperator:
    return AlwaysOperator(duration, condition)

def multiplex_id(output: Any, id_from: Any, eos_from: Any) -> MultiplexFormula:
    return MultiplexFormula(output, id_from, eos_from)

class FormulaAST:
    """AST node that can be compiled to logicsponge circuits"""
    def __init__(self, formula: Formula, children: Optional[List['FormulaAST']] = None):
        self.formula = formula
        self.children = children or []
    
    def add_child(self, child: 'FormulaAST'):
        self.children.append(child)
    
    def compile_to_circuit(self) -> 'FunctionTerm':
        """Compile this AST node to a logicsponge FunctionTerm"""
        compiler = CircuitCompiler()
        return compiler.visit(self)

class CircuitCompiler:
    """Compiles Formula AST to logicsponge FunctionTerm circuits"""
    
    def visit(self, node: FormulaAST) -> 'FunctionTerm':
        # Normalize operator symbols to valid visitor method suffixes
        op = node.formula.op
        op_key = {'>': 'gt', '<': 'lt', '-': 'sub'}.get(op, op)
        method_name = f'visit_{op_key}'
        visitor = getattr(self, method_name, self.generic_visit)
        return visitor(node)
    
    def generic_visit(self, node: FormulaAST) -> 'FunctionTerm':
        # For unknown operations, create a generic function term
        args = [self.visit(child) for child in node.children]
        return FunctionTerm(node.formula.op, args)
    
    def visit_field_access(self, node: FormulaAST) -> 'FunctionTerm':
        field_access = node.formula
        stream_term = SourceTerm(field_access.stream.type, field_access.field)
        return stream_term
    
    def visit_binary_op(self, node: FormulaAST) -> 'FunctionTerm':
        binary_op = node.formula
        left_term = self._compile_operand(binary_op.left)
        right_term = self._compile_operand(binary_op.right)
        return FunctionTerm(f'op_{binary_op.op}', [left_term, right_term])
    
    def visit_lt(self, node: FormulaAST) -> 'FunctionTerm':
        binary_op = node.formula
        left_term = self._compile_operand(binary_op.left)
        right_term = self._compile_operand(binary_op.right)
        return FunctionTerm('lt', [left_term, right_term])
    
    def visit_gt(self, node: FormulaAST) -> 'FunctionTerm':
        binary_op = node.formula
        left_term = self._compile_operand(binary_op.left)
        right_term = self._compile_operand(binary_op.right)
        return FunctionTerm('gt', [left_term, right_term])
    
    def visit_sub(self, node: FormulaAST) -> 'FunctionTerm':
        binary_op = node.formula
        left_term = self._compile_operand(binary_op.left)
        right_term = self._compile_operand(binary_op.right)
        return FunctionTerm('sub', [left_term, right_term])
    
    def visit_index(self, node: FormulaAST) -> 'FunctionTerm':
        index_op = node.formula
        target_term = self._compile_operand(index_op.target)
        index_term = self._compile_operand(index_op.index)
        return FunctionTerm('index', [target_term, index_term])
    
    def visit_every(self, node: FormulaAST) -> 'FunctionTerm':
        temporal_op = node.formula
        target_term = self._compile_operand(temporal_op.target)
        # args[0] is the period (no duplication of target anymore)
        period_term = self._compile_operand(temporal_op.args[0])
        return FunctionTerm('every', [target_term, period_term])
    
    def visit_last(self, node: FormulaAST) -> 'FunctionTerm':
        temporal_op = node.formula
        target_term = self._compile_operand(temporal_op.target)
        return FunctionTerm('last', [target_term])
    
    def visit_always(self, node: FormulaAST) -> 'FunctionTerm':
        always_op = node.formula
        duration_term = self._compile_operand(always_op.duration)
        condition_term = self._compile_operand(always_op.condition)
        return FunctionTerm('always', [duration_term, condition_term])
    
    def visit_multiplex(self, node: FormulaAST) -> 'FunctionTerm':
        multiplex_op = node.formula
        output_term = self._compile_operand(multiplex_op.output)
        id_term = self._compile_operand(multiplex_op.id_from)
        eos_term = self._compile_operand(multiplex_op.eos_from)
        return FunctionTerm('multiplex', [output_term, id_term, eos_term])
    
    def visit_global_time(self, node: FormulaAST) -> 'FunctionTerm':
        return FunctionTerm('global_time', [])
    
    def visit_second(self, node: FormulaAST) -> 'FunctionTerm':
        return FunctionTerm('time_unit_second', [])
    
    def _compile_operand(self, operand: Any) -> 'FunctionTerm':
        """Compile an operand which can be a Formula, Period, or standard Python value"""
        if isinstance(operand, Formula):
            ast_node = self._formula_to_ast(operand)
            return self.visit(ast_node)
        elif isinstance(operand, Period):
            return PeriodTerm(operand)
        elif isinstance(operand, Unit):
            return UnitTerm(operand)
        elif isinstance(operand, (int, float, str)):
            return ConstantTerm(operand)
        else:
            # For external functions like geopy.distance, create a function call term
            return ExternalFunctionTerm(operand)
    
    def _formula_to_ast(self, formula: Formula) -> FormulaAST:
        """Convert a Formula to FormulaAST for compilation"""
        ast_node = FormulaAST(formula)
        # Don't add children here - let the visit methods handle operands directly
        return ast_node

# Logicsponge circuit classes (simplified interface)
class FunctionTerm:
    """Represents a function term in logicsponge circuits"""
    def __init__(self, function_name: str, args: List['FunctionTerm']):
        self.function_name = function_name
        self.args = args
    
    def __repr__(self):
        args_str = ', '.join(str(arg) for arg in self.args)
        return f"{self.function_name}({args_str})"

class SourceTerm(FunctionTerm):
    """Represents a source stream in logicsponge circuits"""
    def __init__(self, stream_type: Type[Any], field: str):
        super().__init__('source', [])
        self.stream_type = stream_type
        self.field = field
    
    def __repr__(self):
        return f"Source({self.stream_type.__name__}.{self.field})"

class ConstantTerm(FunctionTerm):
    """Represents a constant value in logicsponge circuits"""
    def __init__(self, value: Any):
        super().__init__('const', [])
        self.value = value
    
    def __repr__(self):
        return f"Const({self.value})"

class ExternalFunctionTerm(FunctionTerm):
    """Represents external function calls in logicsponge circuits"""
    def __init__(self, function: Any):
        super().__init__('external', [])
        self.function = function
    
    def __repr__(self):
        return f"External({self.function})"

class PeriodTerm(FunctionTerm):
    """Represents a time period in logicsponge circuits"""
    def __init__(self, period: Period):
        super().__init__('period', [])
        self.period = period
    
    def __repr__(self):
        return f"Period({self.period.value}, {self.period.unit.name})"


class UnitTerm(FunctionTerm):
    """Represents a time unit in logicsponge circuits"""
    def __init__(self, unit: Unit):
        super().__init__('unit', [])
        self.unit = unit
    
    def __repr__(self):
        return f"Unit({self.unit.name})"

# Global time reference
GLOBAL = GlobalTime()

# DSL compilation function
def compile_formula(formula: Formula) -> FunctionTerm:
    """Compile a formula to logicsponge FunctionTerm circuit"""
    return formula.compile()

def create_module() -> Any:
    """Create a module-like object with all classes and functions"""
    import types
    module = types.ModuleType('pystreamv')
    module.__dict__["Unit"] = Unit
    module.__dict__["Period"] = Period
    module.__dict__["PeriodTerm"] = PeriodTerm
    module.__dict__["ScheduledFormula"] = ScheduledFormula
    module.__dict__["Formula"] = Formula
    module.__dict__["FormulaAST"] = FormulaAST
    module.__dict__["FunctionTerm"] = FunctionTerm
    module.__dict__["InputStream"] = InputStream
    module.__dict__["GLOBAL"] = GLOBAL
    module.__dict__["s"] = s
    module.__dict__["ms"] = ms
    module.__dict__["m"] = m
    module.__dict__["h"] = h
    module.__dict__["timestamp"] = timestamp
    module.__dict__["H"] = H
    module.__dict__["multiplex_id"] = multiplex_id
    module.__dict__["compile_formula"] = compile_formula
    module.__dict__["SourceTerm"] = SourceTerm
    module.__dict__["ConstantTerm"] = ConstantTerm
    module.__dict__["ExternalFunctionTerm"] = ExternalFunctionTerm
    module.__dict__["IndexOp"] = IndexOp
    module.__dict__["BinaryOp"] = BinaryOp
    module.__dict__["TemporalOp"] = TemporalOp
    module.__dict__["FieldAccess"] = FieldAccess
    module.__dict__["TimestampedStream"] = TimestampedStream
    module.__dict__["MultiplexFormula"] = MultiplexFormula
    module.__dict__["AlwaysOperator"] = AlwaysOperator
    module.__dict__["FormulaAST"] = FormulaAST
    module.__dict__["CircuitCompiler"] = CircuitCompiler
    # Export UnitTerm so debug printer can use it
    module.__dict__["UnitTerm"] = UnitTerm
    return module

pystreamv = create_module()
sys.modules['pystreamv'] = pystreamv