# Biolanguage: A Temporal Logic Formula Library

## Content of the Directory
- `README.md`: This file, providing an overview of the project
- `biolanguage.py`: Core implementation of the temporal logic formula library
- `usage_example.py`: Example script demonstrating library usage

## Project Overview
Biolanguage is a Python library for creating and manipulating temporal logic formulas using Abstract Syntax Tree (AST) transformations.

## Technical Documentation

### Formula Creation Process

#### Lambda to AST Conversion
The `_lambda_to_ast()` function converts lambda functions to AST nodes:
1. Extract source code of the lambda
2. Parse the source code 
3. Locate the lambda node within the parsed AST
4. Provides a fallback mechanism for complex scenarios

The function essentially "lifts" lambda functions from runtime executable code to a static, analyzable representation that can be manipulated, combined, and transformed.
The fallback mechanism (ast.Lambda(...)) ensures that even in complex scenarios where source code extraction fails, a basic representation can be created, making the library robust and flexible.

#### `Formula` Class: Operator Overloading
Enables custom logical operations through overloaded operators:
- `&` (Logical AND)
- `|` (Logical OR)
- `~` (Logical NOT)

As well as temporal logic functions:
- `since()`
- `rolling_window()`


#### Example of Operator Usage
```python

lambda1 = lambda x: x['value'] > 5
lambda2 = lambda x: x['other_value'] < 10

complex_formula_1 = (lambda1 & lambda2) | (lambda1 & ~lambda2)

complex_formula_2 = (
    biolanguage.since(
        lambda x: x['value'] > 2, 
        lambda x: x['rolling_avg'] > 5,
        [2, 31]
    ) & 
    complex_formula_1
)
```

### `FormulaCompiler`: Compilation Workflow
- Singleton pattern for global formula management
- Supports compilation of:
  - Lambda functions
  - Existing AST nodes
  - Existing `Formula` instances
  - String references to previously compiled formulas

#### Compilation Example
```python
compiler = FormulaCompiler()

# Compile a lambda function
formula1 = compiler.compile(lambda x: x['value'] > 5, 'my_formula')

# Retrieve the formula's AST
ast_tree = compiler.get_formula_tree('my_formula')
```

### Temporal Logic Functions

#### `rolling_window()`
- Creates AST for windowed calculations, and wraps it in a Formula instance
- Parameters:
  1. Interval
  2. Map function
  3. Reduce function

#### `since()`
- Implements temporal 'since' logic
- Tracks conditions across a time interval
- Parameters:
  1. First formula
  2. Second formula
  3. Time interval

## Key Mechanisms
When a function from 

Remarks:
- AST manipulation for logical operations
- Lazy evaluation through AST compilation
- Flexible formula composition
- Preservation of original lambda semantics

## Example Usage
```python
import biolanguage

# Simple formula
formula1 = lambda x: x['value'] > 5

# Complex temporal logic formula
formula2 = biolanguage.since(
    lambda x: x['value'] > 2, 
    lambda x: x['value'] > 3, 
    [2, 31]
)

# Rolling window calculation
formula3 = biolanguage.rolling_window(
    [5, 25], 
    lambda x: x['value'], 
    lambda y: sum(y) / len(y)
)
```

## Running the Example
1. Ensure Python 3.7+ is installed
2. Run the example script:
   ```
   python usage_example.py
   ```
3. To see AST compilation details, use:
   ```
   python usage_example.py --recompile
   ```

## Requirements
- Python 3.7+
- No external dependencies
