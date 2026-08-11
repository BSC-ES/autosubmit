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

"""The ``@cli_function`` decorator code."""

from argparse import ArgumentParser
from collections.abc import Callable
from functools import wraps
from typing import TYPE_CHECKING

from autosubmit.log import setup_log_files
from autosubmit.scripts._args import (
    CommandGroup,
    add_common_arguments,
    exit_from_error,
    get_expid_mode,
    normalise_return_value,
    parse_expids,
)
from autosubmit.scripts._validation import (
    validate_host_prohibited_commands,
    validate_required_files,
)

if TYPE_CHECKING:
    from autosubmit.profiler.profiler import Profiler
    from autosubmit.scripts._args import AutosubmitOptions, OptionsT
    from autosubmit.scripts._validation import Validator

__all__ = ["cli_function"]


def cli_function(
    *,
    args_parser: Callable[[], ArgumentParser],
    options_type: type["AutosubmitOptions"],
    validators: "Validator[OptionsT] | list[Validator[OptionsT]] | None" = None,
    group: "CommandGroup" = CommandGroup.GENERAL,
) -> Callable:
    """Decorator for Autosubmit CLI functions.

    The decorator is responsible for:

    * creating the sub-command parser;
    * adding common arguments such as ``expid``;
    * parsing command-line arguments;
    * validating options;
    * handling command exceptions;
    * converting command return values into process exit codes.

    :param args_parser: The argparse parser for the sub-command.
    :param options_type: The type of the options for the sub-command.
    :param validators: A validation function for the sub-command.
    :param group: CLI command group. Default is general.
    :raises ValueError: If ``options_type`` or ``args_parser`` are not provided.
    """
    if options_type is None:
        raise ValueError("options_type must be provided")
    if args_parser is None:
        raise ValueError("args_parser must be provided")

    def _create_parser() -> ArgumentParser:
        """Create the argparse parser for the sub-command.

        Combines the arguments passed to the function, with the common default
        arguments. This is needed to reuse this logic for the manpages generation.
        """
        parser = args_parser()

        # The sub-command parser knows its own name ("clean", "run", ...),
        # but the actual CLI command is "autosubmit <sub-command>".
        subcommand = parser.prog
        parser.prog = f"autosubmit {subcommand}"

        # Add arguments that are common to all Autosubmit sub-commands.
        expid_mode = get_expid_mode(options_type)
        add_common_arguments(parser, expid_mode=expid_mode, options_type=options_type)

        return parser

    def decorator(func):
        """Apply the decorator.

        :param func: The function to decorate.
        """

        @wraps(func)
        def _cli_function(*args, **kwargs):
            """Run the decorator.

            Ensures every autosubmit sub-command prints the Autosubmit version used.

            If ``expid`` is ``True``, the sub-command requires an experiment ID, and
            we check that the argparse arguments do not contain one yet and add the
            experiment ID to the arguments.

            The function also adds other arguments common to all sub-commands.

            If a validator function is provided, we call it to validate the arguments.
            The validator function may fail the sub-command with an appropriate error message.

            The ``args_parser`` is used to call the function.
            """

            parser = _create_parser()
            subcommand = parser.prog.removeprefix("autosubmit ")

            opts = parser.parse_args(args, namespace=options_type())

            # Validate command-line arguments.
            # NOTE: Here we insert the basic validators that check that the Autosubmit basic
            #       files exist, and that only allowed commands are used.
            validators_list: list[Validator] = [
                validate_required_files,
                validate_host_prohibited_commands,
            ]

            if validators is not None:
                validators_list.extend(
                    [validators] if callable(validators) else validators
                )

            for validator in validators_list:
                validator(subcommand, opts)

            expids = parse_expids(getattr(opts, "expid", None))

            # NOTE: the log file level is set earlier, in ``autosubmit.py``.
            setup_log_files(subcommand, expids)

            from autosubmit.scripts._initialise import initialise_command

            initialise_command(subcommand, opts)

            # TODO: Autosubmit._check_folders(expid, as_conf) (we already knows they are the owner!)

            # Run the command with a profiler if requested.
            profiler: "Profiler | None" = None

            try:
                if opts.profile:
                    from autosubmit.profiler.profiler import Profiler

                    profile_expid = getattr(opts, "expid", None) or "no-expid"
                    trace = getattr(opts, "profile_trace", False)
                    max_iterations = getattr(opts, "profile_max_iterations", 0)

                    profiler = Profiler(
                        profile_expid,
                        trace_enabled=trace,
                        max_checkpoints=max_iterations,
                    )
                    profiler.start()
                    profiler.iteration_checkpoint(0, 0)

                    opts._profiler = profiler

                return_value = func(opts, **kwargs)

                return normalise_return_value(return_value)
            except KeyboardInterrupt:
                return 1
            except Exception as e:
                # TODO: We can centralise catching exceptions for command line
                #       here, and handle --verbose/--debug like in Cylc later.
                return exit_from_error(e)
            finally:
                if profiler is not None:
                    profiler.stop()

        _cli_function.command_group = group
        _cli_function.options_type = options_type
        # Export the build_parser function for manpages generation.
        _cli_function.build_parser = _create_parser

        return _cli_function

    return decorator
