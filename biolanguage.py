import ast
import sys
import types
import inspect
from typing import Any, Callable, List, Union, Dict, Optional

class Formula:
    """
    Wrapper class to enable custom logical operations between formulas
    """
    def __init__(self, ast_node):
        # Check that the ast_node is an AST node
        if not isinstance(ast_node, ast.AST):
            raise ValueError("Invalid AST node provided")
        self.ast_node = ast_node
        self.compiler = FormulaCompiler()
        
    def __and__(self, other):
        """
        Overload the & operator to create an AND logical operation
        """
        # Extract AST nodes, handling both Formula and other types
        left_node = self.ast_node.ast_node if isinstance(self.ast_node, Formula) else self.ast_node
        right_node = other.ast_node if isinstance(other, Formula) else other
        
        # Create an AST for the AND operation
        and_call = ast.BoolOp(
            op=ast.And(),
            values=[left_node, right_node]
        )
        
        # Compile and return a new Formula
        return self.compiler.compile(and_call)
    
    def __or__(self, other):
        """
        Overload the | operator to create an OR logical operation
        """
        # Extract AST nodes, handling both Formula and other types
        left_node = self.ast_node.ast_node if isinstance(self.ast_node, Formula) else self.ast_node
        right_node = other.ast_node if isinstance(other, Formula) else other
        
        # Create an AST for the OR operation
        or_call = ast.BoolOp(
            op=ast.Or(),
            values=[left_node, right_node]
        )
        
        # Compile and return a new Formula
        return self.compiler.compile(or_call)
    
    def __invert__(self):
        """
        Overload the ~ operator to create a NOT operation
        """
        # Extract AST node
        operand = self.ast_node.ast_node if isinstance(self.ast_node, Formula) else self.ast_node
        
        # Create an AST for the NOT operation
        not_call = ast.UnaryOp(
            op=ast.Not(),
            operand=operand
        )
        
        # Compile and return a new Formula
        return self.compiler.compile(not_call)

class FormulaCompiler:
    """
    Enhanced singleton class to manage formula compilation and AST storage
    with full formula tree tracking
    """
    _instance = None
    _formula_trees: Dict[str, ast.AST] = {}
    _current_formula_name: Optional[str] = None

    def __new__(cls):
        if not cls._instance:
            cls._instance = super(FormulaCompiler, cls).__new__(cls)
        return cls._instance

    @classmethod
    def compile(cls, formula: Any, name: Optional[str] = None) -> Formula:
        """
        Compile a formula and store its AST
        
        :param formula: Formula to compile (can be lambda, AST, or previous formula reference)
        :param name: Optional name to store the formula under
        :return: Compiled Formula wrapper
        """
        # If no name provided, generate a unique name
        if name is None:
            name = f"formula_autogen_{len(cls._formula_trees) + 1}"
        
        # Handle different input types
        if isinstance(formula, Formula):
            # If already a Formula, extract its AST
            compiled_ast = formula.ast_node
        elif isinstance(formula, ast.AST):
            # If already an AST, store directly
            compiled_ast = formula
        elif callable(formula):
            # If a lambda, convert to AST
            compiled_ast = _lambda_to_ast(formula)
        elif isinstance(formula, str) and formula in cls._formula_trees:
            # If a string referencing a previously compiled formula
            compiled_ast = cls._formula_trees[formula]
        else:
            # For complex nested formulas or references
            raise ValueError(f"Cannot compile formula of type {type(formula)}")
        
        # Store the AST
        cls._formula_trees[name] = compiled_ast
        cls._current_formula_name = name
        
        # Return a Formula wrapper
        return Formula(compiled_ast)

    @classmethod
    def get_formula_tree(cls, name: Optional[str] = None) -> ast.AST:
        """
        Retrieve a formula's AST, with optional resolution of nested references
        
        :param name: Name of the formula to retrieve (defaults to most recently compiled)
        :return: Complete AST for the formula
        """
        if name is None:
            name = cls._current_formula_name
        
        if name not in cls._formula_trees:
            raise KeyError(f"No formula found with name: {name}")
        
        # Unwrap Formula if necessary
        formula_ast = cls._formula_trees[name]
        return formula_ast.ast_node if isinstance(formula_ast, Formula) else formula_ast

    @classmethod
    def get_all_formulas(cls) -> Dict[str, ast.AST]:
        """
        Retrieve all stored formula trees
        
        :return: Dictionary of all stored formula ASTs
        """
        # Ensure we return the raw AST nodes, not the Formula wrappers
        return {
            name: (
                formula_ast.ast_node if isinstance(formula_ast, Formula) 
                else formula_ast
            ) for name, formula_ast in cls._formula_trees.items()
        }

    @classmethod
    def clear_formulas(cls):
        """
        Clear all stored formula trees
        """
        cls._formula_trees.clear()
        cls._current_formula_name = None

        
def _lambda_to_ast(func: Callable) -> ast.AST:
    """
    Convert a lambda function to an AST node
    
    :param func: Lambda function to convert
    :return: AST representation of the lambda
    """
    try:
        # Get the source code of the lambda
        src = inspect.getsource(func).strip()
        
        # Parse the source and extract the lambda node
        parsed = ast.parse(src)
        
        # Find the lambda node
        for node in ast.walk(parsed):
            if isinstance(node, ast.Lambda):
                return node
        
        raise ValueError("Could not extract lambda from source")
    
    except Exception:
        # Fallback for more complex scenarios
        return ast.Lambda(
            args=ast.arguments(
                posonlyargs=[],
                args=[ast.arg(arg='x')],
                vararg=None,
                kwonlyargs=[],
                kw_defaults=[],
                kwarg=None,
                defaults=[]
            ),
            body=ast.Constant(value=func)
        )

def rolling_window(interval: List[float], map_func: Callable, reduce_func: Callable):
    """
    Create AST for rolling window operation
    """
    # Compile the functions
    compiler = FormulaCompiler()
    map_lambda_ast = compiler.compile(map_func)
    reduce_lambda_ast = compiler.compile(reduce_func)
    
    # Convert interval to AST list
    interval_ast = ast.List(
        elts=[
            ast.Constant(value=interval[0]), 
            ast.Constant(value=interval[1])
        ], 
        ctx=ast.Load()
    )
    
    # Create the rolling_window call AST
    rolling_call = ast.Call(
        func=ast.Name(id='rolling_window', ctx=ast.Load()),
        args=[
            interval_ast, 
            # Extract AST nodes to handle nested Formulas
            map_lambda_ast.ast_node if isinstance(map_lambda_ast, Formula) else map_lambda_ast, #### Define function
            reduce_lambda_ast.ast_node if isinstance(reduce_lambda_ast, Formula) else reduce_lambda_ast
        ],
        keywords=[]
    )
    
    # Compile and return the formula
    return compiler.compile(rolling_call)

def since(formula1: Callable, formula2: Callable, interval: List[float]):
    """
    Create AST for 'since' temporal logic operation
    """
    # Compile the formulas
    compiler = FormulaCompiler()
    formula1_ast = compiler.compile(formula1)
    formula2_ast = compiler.compile(formula2)
    
    # Convert interval to AST list
    interval_ast = ast.List(
        elts=[
            ast.Constant(value=interval[0]), 
            ast.Constant(value=interval[1])
        ], 
        ctx=ast.Load()
    )
    
    # Create the since call AST
    since_call = ast.Call(
        func=ast.Name(id='since', ctx=ast.Load()),
        args=[
            # Extract AST nodes to handle nested Formulas
            formula1_ast.ast_node if isinstance(formula1_ast, Formula) else formula1_ast,
            formula2_ast.ast_node if isinstance(formula2_ast, Formula) else formula2_ast,
            interval_ast
        ],
        keywords=[]
    )
    
    # Compile and return the formula
    return compiler.compile(since_call)

def create_biolanguage_module():
    """
    Create a module with biolanguage functions
    """
    module = types.ModuleType('biolanguage')
    
    # Add key functions
    module.__dict__['rolling_window'] = rolling_window
    module.__dict__['since'] = since
    module.__dict__['FormulaCompiler'] = FormulaCompiler
    module.__dict__['Formula'] = Formula
    
    return module

# Create the module and add it to sys.modules
sys.modules['biolanguage'] = create_biolanguage_module()