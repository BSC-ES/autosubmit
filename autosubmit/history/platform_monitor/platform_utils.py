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

from contextlib import suppress
from datetime import datetime
from time import mktime

SLURM_DATETIME_FORMAT = "%Y-%m-%dT%H:%M:%S"

def parse_output_number(string_number):
    """Parses number in format 1.0 K 1.0 M 1.0 G.

    :param string_number: String representation of number
    :type string_number: str
    :return: number in a float format
    :rtype: float
    """    
    number = 0.0
    if string_number:
        last_letter = string_number.strip()[-1]
        multiplier = 1.0
        if last_letter == "G":
            multiplier = 1000000000.0 # Billion
            number = float(string_number[:-1])
        elif last_letter == "M":
            multiplier = 1000000.0 # A Million
            number = float(string_number[:-1])
        elif last_letter == "K":
            multiplier = 1000.0 # A Thousand
            number = float(string_number[:-1])            
        else:
            number = float(string_number)
        try:
            number = float(number) * multiplier
        except (ValueError, TypeError):
            number = 0.0
    return number

def try_parse_time_to_timestamp(input_):
    """Receives a string in the format "%Y-%m-%dT%H:%M:%S" and tries to parse it to timestamp."""
    with suppress(ValueError, TypeError):
        return int(mktime(datetime.strptime(input_, SLURM_DATETIME_FORMAT).timetuple()))
    return 0


def read_example(example_name):
    import importlib.resources as pkg_resources
    from autosubmit.history.platform_monitor import output_examples
    return pkg_resources.read_text(output_examples, example_name)
