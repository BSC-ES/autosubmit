# Copyright 2015-2026 Earth Sciences Department, BSC-CNS
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

from collections.abc import Callable
from importlib.metadata import PackageNotFoundError, metadata
from pathlib import Path
from typing import Callable, Literal, Optional

import tomli
from docutils import nodes  # type: ignore
from docutils.nodes import Node  # type: ignore
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import Version
from sphinx.util import logging
from sphinx.util.docutils import SphinxDirective

__version__ = "0.1.0"

logger = logging.getLogger(__name__)

_PROJECT_ROOTDIR: Path = Path(__file__).parents[3]

# Fixed table headers and column widths for typing
TableHeader = Literal["Package", "Version Spec", "License"]
ColumnWidth = Literal[30, 20, 50]

# A ``License`` metadata field longer than this, or spanning several lines, is
# the full licence text rather than a name, and is not usable as a table cell.
_MAX_LICENSE_NAME_LENGTH = 64

# Carries no information beyond what the other classifiers already say.
_UNSPECIFIC_CLASSIFIERS = frozenset({"License :: OSI Approved"})

_PYTHON_SUBSTITUTIONS = """

.. |python_min| replace:: {minimum}
.. |python_max| replace:: {maximum}
.. |python_requires| replace:: {requires}
"""


def _load_pyproject() -> dict:
    """Read ``pyproject.toml`` from the project root, or return an empty dict."""
    pyproject_path: Path = Path(_PROJECT_ROOTDIR, "pyproject.toml")
    if not pyproject_path.exists():
        logger.warning(f"pyproject.toml not found at {pyproject_path}")
        return {}

    with pyproject_path.open("rb") as f:
        data: dict = tomli.load(f)
    return data


def _license_from_metadata(md) -> str:
    """Resolve a package's licence name from its installed metadata.

    Tries, in order of decreasing reliability: the PEP 639 ``License-Expression``
    field, the most specific ``License ::`` trove classifier, and finally the
    legacy free-form ``License`` field.
    """
    # 1) PEP 639 License-Expression (Metadata 2.4+).
    license_expression: Optional[str] = md.get("License-Expression", None)
    if license_expression and license_expression.strip():
        return license_expression.strip()

    # 2) Trove classifiers. Prefer the most specific one, i.e. the one with the
    #    most "::" separators, rather than whichever happens to come first.
    license_classifiers = [
        c for c in md.get_all("Classifier", [])
        if c.startswith("License ::") and c.strip() not in _UNSPECIFIC_CLASSIFIERS
    ]
    if license_classifiers:
        most_specific = max(license_classifiers, key=lambda c: c.count("::"))
        return most_specific.split("::")[-1].strip()

    # 3) Legacy free-form License field. Some packages put the entire licence
    #    text here, which would blow up the table, so only accept short values.
    raw_license: str = (md.get("License") or "").strip()
    if raw_license and "\n" not in raw_license and len(raw_license) <= _MAX_LICENSE_NAME_LENGTH:
        return raw_license

    return "UNKNOWN"


def _python_bounds(requires_python: str) -> tuple[str, str]:
    """Return the (minimum, maximum) supported ``MAJOR.MINOR`` Python versions.

    An exclusive upper bound such as ``<3.13`` is reported as the last version
    it admits, ``3.12``.
    """
    minimum: str = ""
    maximum: str = ""

    for specifier in SpecifierSet(requires_python):
        version = Version(specifier.version)
        if specifier.operator in (">=", "=="):
            minimum = f"{version.major}.{version.minor}"
        elif specifier.operator == "<=":
            maximum = f"{version.major}.{version.minor}"
        elif specifier.operator == "<" and version.minor > 0:
            maximum = f"{version.major}.{version.minor - 1}"

    return minimum, maximum


def inject_python_substitutions(app, config) -> None:
    """Define ``|python_min|``, ``|python_max|`` and ``|python_requires|``.

    The values come from ``project.requires-python`` in ``pyproject.toml``, so
    the docs cannot drift from the packaging metadata.
    """
    requires_python: str = _load_pyproject().get("project", {}).get("requires-python", "")
    if not requires_python:
        logger.warning("No requires-python found in pyproject.toml")
        return

    minimum, maximum = _python_bounds(requires_python)
    if not minimum or not maximum:
        logger.warning(
            f"Could not derive both Python bounds from requires-python={requires_python!r}"
        )

    config.rst_epilog = (config.rst_epilog or "") + _PYTHON_SUBSTITUTIONS.format(
        minimum=minimum or requires_python,
        maximum=maximum or requires_python,
        requires=requires_python,
    )


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
        deps: list[str] = _load_pyproject().get("project", {}).get("dependencies", [])
        if not deps:
            logger.warning("No runtime dependencies found in pyproject.toml")
            return []

        rows: list[tuple[str, str, str]] = []
        seen: set[str] = set()
        for dep in deps:
            req: Requirement = Requirement(dep)
            name: str = req.name

            # pyproject.toml may list the same package twice; render it once.
            canonical_name: str = canonicalize_name(name)
            if canonical_name in seen:
                logger.info(f"Skipping duplicate dependency {name}")
                continue
            seen.add(canonical_name)

            try:
                license_ = _license_from_metadata(metadata(name))
            except PackageNotFoundError:
                license_ = "NOT INSTALLED"
            logger.info(f'Dependency {dep} license={license_}')

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
    app.connect("config-inited", inject_python_substitutions)
    return {
        "version": __version__,
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
