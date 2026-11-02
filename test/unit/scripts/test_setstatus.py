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

"""Unit tests for the ``autosubmit.scripts.setstatus`` module."""

from autosubmit.scripts.setstatus import SetstatusOptions, main


def test_setstatus_parser_has_updated_list_flag():
    """Verify that the setstatus parser accepts -ul and --updated-list."""
    parser = main.build_parser()
    options_short = parser.parse_args(["a000", "-t", "SUSPENDED", "-ul"])
    assert options_short.updated_list is True

    options_long = parser.parse_args(["a000", "-t", "SUSPENDED", "--updated-list"])
    assert options_long.updated_list is True

    options_default = parser.parse_args(["a000", "-t", "SUSPENDED"])
    assert options_default.updated_list is False


def test_setstatus_options_acquires_lock_property():
    """Verify that acquires_lock is False when updated_list is True."""
    opts_normal = SetstatusOptions()
    opts_normal.updated_list = False
    assert opts_normal.acquires_lock is True

    opts_updated = SetstatusOptions()
    opts_updated.updated_list = True
    assert opts_updated.acquires_lock is False


def test_main_forwards_updated_list_to_set_status(mocker):
    """Verify that main passes updated_list to set_status."""
    mocker.patch("autosubmit.scripts._cli_function.validate_required_files")
    mocker.patch("autosubmit.scripts._cli_function.validate_host_prohibited_commands")
    mocker.patch("autosubmit.scripts._validation.experiment_exists", return_value=True)
    mocker.patch("autosubmit.scripts._validation.check_ownership")
    mocker.patch("autosubmit.scripts._cli_function.setup_log_files")
    mocker.patch("autosubmit.scripts._initialise.initialise_command")

    mock_set_status = mocker.patch("autosubmit.job.manage.set_status")

    main("a000", "-t", "SUSPENDED", "-ul")

    assert mock_set_status.called
    kwargs = mock_set_status.call_args
    # updated_list should be passed either as positional argument or kwarg
    assert (
        kwargs.kwargs.get("updated_list") is True
        or (len(kwargs.args) >= 17 and kwargs.args[16] is True)
    )



