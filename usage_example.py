import pystreamv
import ast

# Define lambda functions for formulas
def make_formulas() -> tuple[pystreamv.Formula]:
    # Formula 0: Triggers exists
    formula0 = pystreamv.triggers_exists(
        lambda x: x['value'] > 5,
        lambda x: x['value'] > 3,
        [2, 31]
    )

    # Formula 1: Simple lambda
    def formula1(x):
        return x['value'] > 5

    # formula1bis = pystreamv.since(
    #     formula1, 
    #     lambda x: x['value'] > 3, 
    #     [2, 31]
    # )

    # Formula 2: Check if value > 2, then check if value > 3
    formula2 = pystreamv.since(
        lambda x: x['value'] > 2, 
        lambda x: x['value'] > 3, 
        [2, 31]
    )

    # Formula 3: Rolling window average calculation
    formula3 = pystreamv.rolling_window(
        [5, 25], 
        lambda x: x['value'], 
        lambda y: sum(y) / len(y)
    )

    # Formula 4: Complex nested formula with logical operations
    formula4 = (
        pystreamv.since(
            lambda x: x['value'] > 2, 
            lambda x: x['value'] > pystreamv.rolling_window(
                [5, 25], 
                lambda x: x['value'], 
                lambda x: sum(x) / len(x)
            ),
            [2, 31]
        ) & formula2 | formula3 & formula1
    )

    return formula0, formula1, formula2, formula3, formula4

def main(recompile=False):
    # Create formulas
    formulas = make_formulas()

    if recompile:
        # Create compiler instance
        compiler = pystreamv.FormulaCompiler()

        # Compile and get AST for each formula
        print("=== Compiling Formulas ===")
        
        # Compile and print AST for each formula
        for m in range(len(formulas)):
            print(f"\n--- Formula {m} AST ---")
            formula_compiled = compiler.compile(formulas[m], f'formula{m}')
            print(ast.dump(formula_compiled.ast_node, indent=2))

        # Retrieve and print all stored formulas
        print("\n=== All Stored Formulas ===")
        all_formulas = compiler.get_all_formulas()
        for name, formula_ast in all_formulas.items():
            print(f"{name}:")
            print(ast.dump(formula_ast, indent=2))
            print()
    else:
        for m in range(len(formulas)):
            print(f"\n--- Formula {m} AST ---")
            if hasattr(formulas[m], 'ast_node'):
                print(ast.dump(formulas[m].ast_node, indent=2))
            else:
                print("No AST node found for this formula. Please run with recompile=True to generate ASTs.")

        # And retrieve all formulas directly
        all_formulas = pystreamv.FormulaCompiler().get_all_formulas()

if __name__ == "__main__":
    main()