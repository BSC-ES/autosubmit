import ast
from pathlib import Path


def test_package_contains_no_pass_statements():
    package_root = Path(__file__).parents[2] / "autosubmit"
    pass_statements = []

    for path in sorted(package_root.rglob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        pass_statements.extend(
            f"{path.relative_to(package_root)}:{node.lineno}"
            for node in ast.walk(tree)
            if isinstance(node, ast.Pass)
        )

    assert not pass_statements, "Standalone pass statements found:\n" + "\n".join(
        pass_statements
    )
