import biolanguage
import ast

# Define lambda functions for formulas
def make_formulas():
    # Formula 1: Simple lambda
    def formula1(x):
        return x['value'] > 5

    formula1bis = biolanguage.since(
        formula1, 
        lambda x: x['value'] > 3, 
        [2, 31]
    )

    # Formula 2: Check if value > 2, then check if value > 3
    formula2 = biolanguage.since(
        lambda x: x['value'] > 2, 
        lambda x: x['value'] > 3, 
        [2, 31]
    )

    # Formula 3: Rolling window average calculation
    formula3 = biolanguage.rolling_window(
        [5, 25], 
        lambda x: x['value'], 
        lambda y: sum(y) / len(y)
    )

    # Formula 4: Complex nested formula with logical operations
    formula4 = (
        biolanguage.since(
            lambda x: x['value'] > 2, 
            lambda x: x['value'] > biolanguage.rolling_window(
                [5, 25], 
                lambda x: x['value'], 
                lambda x: sum(x) / len(x)
            ),
            [2, 31]
        ) & formula2 | formula3 & formula1
    )

    return formula1, formula2, formula3, formula4

def main(recompile=False):
    # Create formulas
    formula1, formula2, formula3, formula4 = make_formulas()

    if recompile:
        # Create compiler instance
        compiler = biolanguage.FormulaCompiler()

        # Compile and get AST for each formula
        print("=== Compiling Formulas ===")
        
        # Compile and print AST for Formula 1
        print("\n--- Formula 1 AST ---")
        formula1_compiled = compiler.compile(formula1, 'formula1')
        print(ast.dump(formula1_compiled.ast_node, indent=2))

        # Compile and print AST for Formula 2
        print("\n--- Formula 2 AST ---")
        formula2_compiled = compiler.compile(formula2, 'formula2')
        print(ast.dump(formula2_compiled.ast_node, indent=2))

        # Compile and print AST for Formula 3
        print("\n--- Formula 3 AST ---")
        formula3_compiled = compiler.compile(formula3, 'formula3')
        print(ast.dump(formula3_compiled.ast_node, indent=2))

        # Compile and print AST for Formula 4
        print("\n--- Formula 4 AST ---")
        formula4_compiled = compiler.compile(formula4, 'formula4')
        print(ast.dump(formula4_compiled.ast_node, indent=2))

        # Retrieve and print all stored formulas
        print("\n=== All Stored Formulas ===")
        all_formulas = compiler.get_all_formulas()
        for name, formula_ast in all_formulas.items():
            print(f"{name}:")
            print(ast.dump(formula_ast, indent=2))
            print()
    else:
        print(ast.dump(formula2.ast_node, indent=2))
        print(ast.dump(formula3.ast_node, indent=2))
        print(ast.dump(formula4.ast_node, indent=2))

        # And retrieve all formulas directly
        all_formulas = biolanguage.FormulaCompiler().get_all_formulas()

if __name__ == "__main__":
    main()