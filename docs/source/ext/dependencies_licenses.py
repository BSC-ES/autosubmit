# Copyright 2015-2025 Earth Sciences Department, BSC-CNS
#
# This file is part of Autosubmit.
#
# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

from __future__ import annotations

from importlib.metadata import metadata, PackageNotFoundError
from pathlib import Path
from typing import Callable, Literal

import tomli
from docutils import nodes  # type: ignore
from docutils.nodes import Node  # type: ignore
from packaging.requirements import Requirement
from sphinx.util import logging
from sphinx.util.docutils import SphinxDirective

__version__ = "0.1.0"

logger = logging.getLogger(__name__)

_PROJECT_ROOTDIR: Path = Path(__file__).parents[3]

# Fixed table headers and column widths for typing
TableHeader = Literal["Package", "Version Spec", "License"]
ColumnWidth = Literal[30, 20, 50]


class AutosubmitDependenciesLicensesDirective(SphinxDirective):
    """An Autosubmit directive to print the runtime dependencies and their licenses."""
    has_content: bool = True
    required_arguments: int = 0
    optional_arguments: int = 99
    final_argument_whitespace: bool = False

    option_spec: dict[str, Callable[[str], object]] = {}

    options: dict[str, object]
    arguments: list[str]

    def run(self) -> list[Node]:
        pyproject_path: Path = Path(_PROJECT_ROOTDIR, "pyproject.toml")
        if not pyproject_path.exists():
            logger.warning(f"pyproject.toml not found at {pyproject_path}")
            return []

        with pyproject_path.open("rb") as f:
            data: dict = tomli.load(f)

        deps: list[str] = data.get("project", {}).get("dependencies", [])
        if not deps:
            logger.warning("No runtime dependencies found in pyproject.toml")
            return []

        rows: list[tuple[str, str, str]] = []
        for dep in deps:
            req: Requirement = Requirement(dep)
            name: str = req.name
            try:
                md = metadata(name)

                # 1) Try License‑Expression
                license_expr = md.get("License‑Expression", None)
                if license_expr:
                    license_ = license_expr.strip()
                    logger.info(f'Dependency {dep} license={license_}')
                else:
                    # 2) Try classifiers
                    classifiers = md.get_all("Classifier", [])
                    license_classifiers = [
                        c for c in classifiers
                        if c.startswith("License ::")
                    ]
                    if license_classifiers:
                        # Take the most specific classifier
                        license_ = license_classifiers[0].split("::")[-1].strip()
                        logger.info(f'Dependency {dep} license={license_}')

                    else:
                        # 3) Fallback to the plain License field
                        raw_license = md.get("License", "").strip()
                        license_ = raw_license if raw_license else "UNKNOWN"
                        logger.info(f'Dependency {dep} license={license_}')
            except PackageNotFoundError:
                license_ = "NOT INSTALLED"

            rows.append((name, str(req.specifier) or "-", license_))

        table: nodes.table = nodes.table()
        tgroup: nodes.tgroup = nodes.tgroup(cols=3)
        table += tgroup

        column_widths: list[ColumnWidth] = [30, 20, 50]
        for width in column_widths:
            tgroup += nodes.colspec(colwidth=width)

        thead: nodes.thead = nodes.thead()
        tgroup += thead
        header_row: nodes.row = nodes.row()
        thead += header_row

        headers: list[TableHeader] = ["Package", "Version Spec", "License"]
        for title in headers:
            header_row += nodes.entry("", nodes.paragraph(text=title))

        tbody: nodes.tbody = nodes.tbody()
        tgroup += tbody
        for name, spec, license_ in rows:
            row: nodes.row = nodes.row()
            for text in [name, spec, license_]:
                row += nodes.entry("", nodes.paragraph(text=text))
            tbody += row

        return [table]


def setup(app) -> dict[str, object]:
    app.add_directive(
        "dependencies_licenses",
        AutosubmitDependenciesLicensesDirective
    )
    return {
        "version": __version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
