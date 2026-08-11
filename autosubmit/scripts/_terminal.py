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

"""Code for handling terminal interactions."""

from os import environ
from subprocess import PIPE, Popen
from sys import stdout
from textwrap import wrap

__all__ = ["get_width", "print_contents", "supports_color"]


def get_width(default=80):
    """Return the terminal width or `default` if it is not determinable."""
    # stty can have different install locs so don't use absolute path
    proc = Popen(["stty", "size"], stdout=PIPE, stderr=PIPE)  # nosec
    if proc.wait():
        return default
    try:
        return int(proc.communicate()[0].split()[1]) or default
    except (IndexError, ValueError):
        return default


def print_contents(
    contents,
    padding=5,
    char=".",
    indent=0,
    title_width=None,
):
    if title_width is None:
        title_width = max(len(title) for title, _ in contents)

    width = get_width(default=0)
    width = max(width, title_width + 20 - indent - padding)
    desc_width = width - title_width - padding - 2 - indent

    indent_str = " " * indent

    for title, desc in contents:
        desc_lines = wrap(desc or "", desc_width) or [""]

        print(
            f"{indent_str}"
            f"{title} "
            f"{char * (padding + title_width - len(title))} "
            f"{desc_lines[0]}"
        )

        for line in desc_lines[1:]:
            print(f"{indent_str}{' ' * title_width}{' ' * (padding + 2)}{line}")


def supports_color() -> bool:
    """Return whether stdout appears to support ANSI colours."""
    return (
        stdout.isatty() and environ.get("TERM") != "dumb" and "NO_COLOR" not in environ
    )
