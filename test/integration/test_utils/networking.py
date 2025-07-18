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

"""Networking utilities for integration tests."""

from socket import socket


def get_free_port() -> int:
    """Get a free TCP port to use in a test (e.g. for a Docker container).

    Creating a Socket in Python, and binding to port 0, triggers underlying
    code that finds a free TCP port in the operating system.

    On Windows, for instance, there is a similar call in ``winsock.h``, which
    returns a value between 49152 and 65535, for a port that's not in use.
    This range lists what are called "ephemeral ports", or dynamic or private
    ports, used for temporary connections initiated by applications.

    On Linux, you can see these ports too:

        $ cat /proc/sys/net/ipv4/ip_local_port_range
        32768	60999

    Here, we ask for one of these ports and give it to the current test.

    Having a central place where these ports are allocated may be also helpful
    to troubleshoot brittle tests when there are issues like port already
    allocated.
    """
    with socket() as s:
        s.bind(('', 0))
        return s.getsockname()[1]
