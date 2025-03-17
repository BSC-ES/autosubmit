#!/usr/bin/env python3

# Copyright 2015-2020 Earth Sciences Department, BSC-CNS

# This file is part of Autosubmit.

# Autosubmit is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.

# Autosubmit is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.

# You should have received a copy of the GNU General Public License
# along with Autosubmit.  If not, see <http://www.gnu.org/licenses/>.

"""
Module containing functions to manage autosubmit's experiments.
"""
import string
import autosubmit.database.db_common as db_common
from log.log import Log,AutosubmitCritical
Log.get_logger("Autosubmit")

def new_experiment(description, version, test=False, operational=False, evaluation=False):
    """
    Stores a new experiment on the database and generates its identifier

    :param description: description of the experiment
    :type description: str
    :param version: version of the experiment
    :type version: str
    :param test: if True, the experiment is a test experiment
    :type test: bool
    :param operational: if True, the experiment is an operational experiment
    :type operational: bool
    :return: experiment id for the new experiment
    :rtype: str
    """
    if test:
        last_exp_name = db_common.last_name_used(True)
    elif operational:
        last_exp_name = db_common.last_name_used(False, True)
    elif evaluation:
        last_exp_name = db_common.last_name_used(False, False, True)
    else:
        last_exp_name = db_common.last_name_used()
    if last_exp_name == '':
        return ''
    if last_exp_name == 'empty':
        if test:
            # test identifier restricted also to 4 characters.
            new_name = 't000'
        elif operational:
            # operational identifier restricted also to 4 characters.
            new_name = 'o000'
        elif evaluation:
            # evaluation identifier restricted also to 4 characters.
            new_name = 'e000'
        else:
            new_name = 'a000'
    else:
        new_name = last_exp_name
        if new_name == '':
            return ''
    while db_common.check_experiment_exists(new_name, False):
        new_name = next_experiment_id(new_name)
        if new_name == '':
            return ''
    if not db_common.save_experiment(new_name, description, version):
        return ''
    Log.info('The new experiment "{0}" has been registered.', new_name)
    return new_name


def copy_experiment(experiment_id, description, version, test=False, operational=False, evaluation=False):
    """
    Creates a new experiment by copying an existing experiment

    :param version: experiment's associated autosubmit version
    :type version: str
    :param experiment_id: identifier of experiment to copy
    :type experiment_id: str
    :param description: experiment's description
    :type description: str
    :param test: specifies if it is a test experiment
    :type test: bool
    :param operational: specifies if it is an operational experiment
    :type operational: bool
    :param evaluation: specifies if it is an evaluation experiment
    :type evaluation: bool
    :return: experiment id for the new experiment
    :rtype: str
    """
    if not db_common.check_experiment_exists(experiment_id):
        return ''
    new_name = new_experiment(description, version, test, operational, evaluation)
    return new_name


def next_experiment_id(current_id):
    """
    Get next experiment identifier

    :param current_id: previous experiment identifier
    :type current_id: str
    :return: new experiment identifier
    :rtype: str
    """
    if not is_valid_experiment_id(current_id):
        return ''
    # Convert the name to base 36 in number add 1 and then encode it
    next_id = base36encode(base36decode(current_id) + 1)
    return next_id if is_valid_experiment_id(next_id) else ''


def is_valid_experiment_id(name):
    """
    Checks if it is a valid experiment identifier

    :param name: experiment identifier to check
    :type name: str
    :return: name if is valid, terminates program otherwise
    :rtype: str
    """
    name = name.lower()
    if len(name) < 4 or not name.isalnum():
        raise AutosubmitCritical("Incorrect experiment, it must have exactly 4 alphanumeric chars")
    return True


def base36encode(number, alphabet=string.digits + string.ascii_lowercase):
    """
    Convert positive integer to a base36 string.

    :param number: number to convert
    :type number: int
    :param alphabet: set of characters to use
    :type alphabet: str
    :return: number's base36 string value
    :rtype: str
    """
    if not isinstance(number, int):
        raise TypeError('number must be an integer')

    # Special case for zero
    if number == 0:
        return '0'

    base36 = ''

    sign = ''
    if number < 0:
        sign = '-'
        number = - number

    while number > 0:
        number, i = divmod(number, len(alphabet))
        # noinspection PyAugmentAssignment
        base36 = alphabet[i] + base36

    return sign + base36.rjust(4, '0')


def base36decode(number):
    """
    Converts a base36 string to a positive integer

    :param number: base36 string to convert
    :type number: str
    :return: number's integer value
    :rtype: int
    """
    return int(number, 36)
