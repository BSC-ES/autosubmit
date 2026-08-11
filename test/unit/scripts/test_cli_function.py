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

"""Unit tests for the ``autosubmit.scripts._cli_function`` module."""

from argparse import ArgumentParser

import pytest

# noinspection PyProtectedMember
from autosubmit.scripts._args import CommandGroup

# noinspection PyProtectedMember
from autosubmit.scripts._cli_function import cli_function


class Options:
    """Minimal options object used by the tests."""

    def __init__(self):
        self.expid = None
        self.profile = False


@pytest.fixture
def cli_environment(mocker):
    """Neutralise external CLI setup for decorator execution tests."""
    mocker.patch(
        "autosubmit.scripts._cli_function.validate_required_files",
    )
    mocker.patch(
        "autosubmit.scripts._cli_function.validate_host_prohibited_commands",
    )
    mocker.patch(
        "autosubmit.scripts._cli_function.parse_expids",
        return_value=[],
    )
    mocker.patch(
        "autosubmit.scripts._cli_function.setup_log_files",
    )
    mocker.patch(
        "autosubmit.scripts._initialise.initialise_command",
    )


@pytest.fixture
def cli_factory(mocker, cli_environment):
    """Create a decorated CLI command with isolated dependencies."""

    def factory(
        command=None,
        *,
        parser=None,
        options_type=Options,
        validators=None,
        group=CommandGroup.GENERAL,
    ):
        if parser is None:
            parser = ArgumentParser(prog="run")

        if command is None:
            command = mocker.Mock(return_value=0)

        decorated = cli_function(
            args_parser=lambda: parser,
            options_type=options_type,
            validators=validators,
            group=group,
        )(command)

        return decorated, command, parser

    return factory


def test_cli_function_requires_options_type():
    """Test that options_type is required."""
    parser_factory = lambda: ArgumentParser(prog="run")

    with pytest.raises(ValueError, match="options_type must be provided"):
        cli_function(
            args_parser=parser_factory,
            options_type=None,
        )


def test_cli_function_requires_args_parser():
    """Test that args_parser is required."""
    with pytest.raises(ValueError, match="args_parser must be provided"):
        cli_function(
            args_parser=None,
            options_type=Options,
        )


def test_cli_function_sets_metadata(mocker):
    """Test metadata added to the decorated function."""
    parser = ArgumentParser(prog="run")

    get_expid_mode = mocker.patch(
        "autosubmit.scripts._cli_function.get_expid_mode",
        return_value="required",
    )
    add_common_arguments = mocker.patch(
        "autosubmit.scripts._cli_function.add_common_arguments",
    )

    def command(opts):
        return 0

    decorated = cli_function(
        args_parser=lambda: parser,
        options_type=Options,
        group=CommandGroup.EXPERIMENT,
    )(command)

    assert decorated.__name__ == "command"
    assert decorated.command_group == CommandGroup.EXPERIMENT
    assert decorated.options_type is Options
    assert callable(decorated.build_parser)

    decorated.build_parser()

    get_expid_mode.assert_called_once_with(Options)
    add_common_arguments.assert_called_once_with(
        parser,
        expid_mode="required",
        options_type=Options,
    )
    assert parser.prog == "autosubmit run"


def test_cli_function_build_parser_updates_program_name(mocker):
    """Test that the parser program name includes autosubmit."""
    parser = ArgumentParser(prog="clean")

    mocker.patch(
        "autosubmit.scripts._cli_function.get_expid_mode",
        return_value="optional",
    )
    mocker.patch(
        "autosubmit.scripts._cli_function.add_common_arguments",
    )

    decorated = cli_function(
        args_parser=lambda: parser,
        options_type=Options,
    )(lambda opts: 0)

    result = decorated.build_parser()

    assert result is parser
    assert result.prog == "autosubmit clean"


def test_cli_function_normal_execution(mocker, cli_factory):
    """Test successful command execution."""
    parse_expids = mocker.patch(
        "autosubmit.scripts._cli_function.parse_expids",
        return_value=["a000"],
    )
    setup_log_files = mocker.patch(
        "autosubmit.scripts._cli_function.setup_log_files",
    )
    initialise_command = mocker.patch(
        "autosubmit.scripts._initialise.initialise_command",
    )
    normalise = mocker.patch(
        "autosubmit.scripts._cli_function.normalise_return_value",
        return_value=0,
    )

    command = mocker.Mock(return_value=123)

    decorated, _, _ = cli_factory(command)

    assert decorated() == 0

    parse_expids.assert_called_once_with(None)
    setup_log_files.assert_called_once_with("run", ["a000"])
    initialise_command.assert_called_once()
    command.assert_called_once_with(mocker.ANY)
    normalise.assert_called_once_with(123)


def test_cli_function_single_custom_validator(mocker, cli_factory):
    """Test that a single custom validator is executed."""
    validator = mocker.Mock()
    command = mocker.Mock(return_value=0)

    decorated, _, _ = cli_factory(
        command,
        validators=validator,
    )

    decorated()

    validator.assert_called_once_with("run", mocker.ANY)
    command.assert_called_once_with(mocker.ANY)


def test_cli_function_multiple_custom_validators(mocker, cli_factory):
    """Test that multiple custom validators are executed."""
    validator_1 = mocker.Mock()
    validator_2 = mocker.Mock()
    command = mocker.Mock(return_value=0)

    decorated, _, _ = cli_factory(
        command,
        validators=[validator_1, validator_2],
    )

    decorated()

    validator_1.assert_called_once_with("run", mocker.ANY)
    validator_2.assert_called_once_with("run", mocker.ANY)
    command.assert_called_once_with(mocker.ANY)


def test_cli_function_passes_subcommand_to_validators(mocker, cli_factory):
    """Test that validators receive the actual sub-command name."""
    parser = ArgumentParser(prog="inspect")
    validator = mocker.Mock()

    decorated, _, _ = cli_factory(
        validators=validator,
        parser=parser,
    )

    decorated()

    validator.assert_called_once_with("inspect", mocker.ANY)


def test_cli_function_passes_kwargs_to_command(mocker, cli_factory):
    """Test that keyword arguments are passed to the command."""
    normalise = mocker.patch(
        "autosubmit.scripts._cli_function.normalise_return_value",
        return_value=0,
    )

    command = mocker.Mock(return_value=123)

    decorated, _, _ = cli_factory(command)

    assert decorated(foo="bar") == 0

    opts = command.call_args.args[0]

    assert isinstance(opts, Options)
    assert command.call_args.kwargs == {"foo": "bar"}
    normalise.assert_called_once_with(123)


def test_cli_function_keyboard_interrupt_returns_one(cli_factory):
    """Test that KeyboardInterrupt returns exit code 1."""
    command = cli_factory()[1]
    command.side_effect = KeyboardInterrupt

    decorated = cli_factory(command)[0]

    assert decorated() == 1


def test_cli_function_exception_uses_exit_from_error(mocker, cli_factory):
    """Test that command exceptions are handled by exit_from_error."""
    error = RuntimeError("boom")

    exit_from_error = mocker.patch(
        "autosubmit.scripts._cli_function.exit_from_error",
        return_value=42,
    )

    command = mocker.Mock(side_effect=error)

    decorated, _, _ = cli_factory(command)

    assert decorated() == 42
    exit_from_error.assert_called_once_with(error)


def test_cli_function_profile(mocker, cli_factory):
    """Test profiler setup and teardown."""
    mocker.patch(
        "autosubmit.scripts._cli_function.parse_expids",
        return_value=["a000"],
    )

    profiler = mocker.Mock()

    profiler_class = mocker.patch(
        "autosubmit.profiler.profiler.Profiler",
        return_value=profiler,
    )

    parser = ArgumentParser(prog="run")

    def parse_args(args, namespace):
        namespace.expid = "a000"
        namespace.profile = True
        namespace.profile_trace = True
        namespace.profile_max_iterations = 10
        return namespace

    parser.parse_args = parse_args

    command = mocker.Mock(return_value=0)

    decorated, _, _ = cli_factory(
        command,
        parser=parser,
    )

    assert decorated() == 0

    profiler_class.assert_called_once_with(
        "a000",
        trace_enabled=True,
        max_checkpoints=10,
    )
    profiler.start.assert_called_once_with()
    profiler.iteration_checkpoint.assert_called_once_with(0, 0)
    profiler.stop.assert_called_once_with()

    opts = command.call_args.args[0]
    assert opts._profiler is profiler


def test_cli_function_profile_uses_defaults(mocker, cli_factory):
    """Test profiler defaults when optional profile attributes are absent."""

    class ProfileOptions:
        def __init__(self):
            self.expid = None
            self.profile = True

    profiler = mocker.Mock()

    profiler_class = mocker.patch(
        "autosubmit.profiler.profiler.Profiler",
        return_value=profiler,
    )

    command = mocker.Mock(return_value=0)

    decorated, _, _ = cli_factory(
        command,
        options_type=ProfileOptions,
    )

    decorated()

    profiler_class.assert_called_once_with(
        "no-expid",
        trace_enabled=False,
        max_checkpoints=0,
    )


def test_cli_function_profile_stops_after_exception(mocker, cli_factory):
    """Test that the profiler is stopped when the command raises."""

    class ProfileOptions:
        def __init__(self):
            self.expid = None
            self.profile = True

    mocker.patch(
        "autosubmit.scripts._cli_function.exit_from_error",
        return_value=1,
    )

    profiler = mocker.Mock()

    mocker.patch(
        "autosubmit.profiler.profiler.Profiler",
        return_value=profiler,
    )

    command = mocker.Mock(side_effect=RuntimeError("boom"))

    decorated, _, _ = cli_factory(
        command,
        options_type=ProfileOptions,
    )

    assert decorated() == 1
    profiler.stop.assert_called_once_with()


def test_cli_function_without_profile_does_not_create_profiler(
    mocker,
    cli_factory,
):
    """Test that the profiler is not created unless requested."""
    profiler_class = mocker.patch(
        "autosubmit.profiler.profiler.Profiler",
    )

    decorated, _, _ = cli_factory()

    decorated()

    profiler_class.assert_not_called()
