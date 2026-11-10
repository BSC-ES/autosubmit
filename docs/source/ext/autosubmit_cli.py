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

"""Sphinx extension for the Autosubmit command-line reference."""

from argparse import SUPPRESS
from collections import defaultdict
from collections.abc import Callable
from textwrap import dedent
from typing import Any, cast

from docutils import nodes
from docutils.parsers.rst import Directive

# noinspection PyProtectedMember
from autosubmit.scripts._args import CommandGroup

# noinspection PyProtectedMember
from autosubmit.scripts._entry_points import get_commands, iter_commands


def _make_paragraph(text: str) -> nodes.paragraph:
    """Create a paragraph node from plain text."""
    paragraph = nodes.paragraph()
    paragraph += nodes.Text(text)
    return paragraph


def _make_literal_block(text: str) -> nodes.literal_block:
    """Create a literal/code block."""
    return nodes.literal_block(text, text)


def _make_option_list(parser: Any) -> nodes.definition_list:
    """Render an argparse parser as a definition list."""
    result = nodes.definition_list()
    result["classes"].append("as-options")

    for action in parser._actions:
        if action.help == SUPPRESS or action.dest == "help":
            continue

        if action.option_strings:
            names = ", ".join(action.option_strings)

            if action.nargs == 0:
                option_text = names
            else:
                metavar = action.metavar or action.dest
                option_text = f"{names} {metavar}"

            term_class = "as-opt"
        else:
            metavar = action.metavar or action.dest
            option_text = metavar
            term_class = "as-arg"

        term = nodes.term()
        name_node = nodes.literal(option_text, option_text)
        name_node["classes"].append(term_class)
        term += name_node

        if not action.option_strings:
            badge = nodes.inline(text="required")
            badge["classes"].append("as-badge")
            term += badge

        definition = nodes.definition()
        definition += _make_paragraph(action.help or "")

        meta = nodes.paragraph()
        meta["classes"].append("as-option-meta")

        if action.choices:
            label = nodes.inline(text="choices")
            label["classes"].append("as-meta-label")
            meta += label

            for choice in action.choices:
                meta += nodes.literal(str(choice), str(choice))

        if action.default not in (None, False, SUPPRESS):
            label = nodes.inline(text="default")
            label["classes"].append("as-meta-label")
            meta += label
            meta += nodes.literal(str(action.default), str(action.default))

        if len(meta.children):
            definition += meta

        item = nodes.definition_list_item()
        item += term
        item += definition

        result += item

    return result


def _find_cli_function(module: Any, command: str) -> Any | None:
    """Find the CLI function registered by a command module.

    Autosubmit's entry-point iterator returns the command module, while
    the ``command_group`` attribute is attached to the function decorated
    with ``@cli_function`` inside that module.
    """
    candidate = getattr(module, command, None)

    if candidate is not None:
        return candidate

    # Fall back to looking through module globals. For commands whose
    # Python function name differs from their entry-point name.
    for value in vars(module).values():
        if callable(value) and hasattr(value, "command_group"):
            return value

    return None


def _make_command(
    command: str,
    cli_function: Any,
    description: str,
    examples: str | None,
    document: nodes.document,
) -> nodes.section:
    """Create the documentation section for one command."""
    section = nodes.section()
    section["ids"] = [f"autosubmit-{command}"]
    section["names"] = [nodes.fully_normalize_name(f"autosubmit {command}")]
    section["classes"].append("as-command")
    document.note_implicit_target(section)

    title = nodes.title()
    title += nodes.literal(
        f"autosubmit {command}",
        f"autosubmit {command}",
    )
    section += title

    if description:
        section += _make_paragraph(description)

    build_parser = cast(Callable, getattr(cli_function, "build_parser", None))

    if build_parser is not None:
        parser = build_parser()
        parser.prog = f"autosubmit {command}"

        usage = parser.format_usage().replace("usage: ", "", 1).strip()
        usage_block = _make_literal_block(usage)
        usage_block["classes"].append("as-usage")
        section += usage_block

        section += nodes.rubric(text="Options")
        section += _make_option_list(parser)

    if examples:
        section += nodes.rubric(text="Examples")

        examples_block = _make_literal_block(dedent(examples))
        examples_block["classes"].append("as-examples")
        section += examples_block

    return section


class AutosubmitCommandsDirective(Directive):
    """Generate the Autosubmit CLI command reference."""

    has_content = False
    required_arguments = 0
    optional_arguments = 0

    def run(self) -> list[nodes.Node]:
        """Generate the command reference."""
        document = self.state.document
        sections: list[nodes.Node] = []

        commands = get_commands()
        grouped_commands = defaultdict(list)

        for command, doc, module, entry_point in iter_commands(commands):
            cli_function = _find_cli_function(module, command)

            if cli_function is None:
                # Keep the command visible even if we cannot find its
                # decorated CLI function.
                cli_function = module

            group = getattr(
                entry_point,
                "command_group",
                CommandGroup.GENERAL,
            )

            grouped_commands[group].append(
                (
                    command,
                    cli_function,
                    doc.description,
                    doc.examples,
                )
            )

        for group in CommandGroup:
            group_commands = grouped_commands.get(group)

            if not group_commands:
                continue

            group_section = nodes.section()
            group_section["ids"] = [f"autosubmit-{group.name.lower()}"]
            group_section["names"] = [nodes.fully_normalize_name(group.value)]
            group_section["classes"].extend(["autosubmit-cli", "as-command-group"])
            document.note_implicit_target(group_section)

            title = nodes.title()
            title += nodes.Text(group.value)
            group_section += title

            for command, cli_function, description, examples in sorted(
                group_commands,
                key=lambda item: item[0],
            ):
                group_section += _make_command(
                    command,
                    cli_function,
                    description,
                    examples,
                    document,
                )

            sections.append(group_section)

        return sections


def setup(app: Any) -> dict[str, Any]:
    app.add_directive(
        "autosubmit-commands",
        AutosubmitCommandsDirective,
    )

    return {
        "version": "1.0",
        "parallel_read_safe": True,
        "parallel_write_safe": True,
    }
