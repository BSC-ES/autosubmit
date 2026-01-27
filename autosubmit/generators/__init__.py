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

"""This module provides generators to produce workflow configurations for different backend engines."""

from abc import ABC, abstractmethod
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    import argparse
    from autosubmit.config.configcommon import AutosubmitConfig
    from autosubmit.job.job_list import JobList


class Engine(Enum):
    """Workflow Manager engine flavors."""
    AIIDA = 'aiida'

    def __str__(self) -> str:
        """
        Returns the string representation of the engine.

        :return: The value of the engine enum.
        :rtype: str
        """
        return self.value


class AbstractGenerator(ABC):
    """Generator of workflow for an engine."""

    @staticmethod
    @abstractmethod
    def get_engine_name() -> str:
        """
        The engine name used for the help text.

        :return: The name of the engine.
        :rtype: str
        """
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def add_parse_args(parser: "argparse.ArgumentParser") -> None:
        """
        Adds arguments to the parser that are needed for a specific engine implementation.

        :param parser: The argparse parser object.
        :type parser: argparse.ArgumentParser
        """
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def generate(cls, job_list: "JobList", as_conf: "AutosubmitConfig", **arg_options) -> None:
        """
        Generates the workflow from the created autosubmit workflow.

        :param cls: The class itself.
        :type cls: type
        :param job_list: The list of jobs.
        :type job_list: JobList
        :param as_conf: The autosubmit configuration.
        :type as_conf: AutosubmitConfig
        :param arg_options: Keyword arguments for options.
        :type arg_options: dict
        """
        raise NotImplementedError


def get_engine_generator(engine: Engine) -> AbstractGenerator:
    """
    Returns the generator for the given engine.

    :param engine: The engine for which to get the generator.
    :type engine: Engine
    :return: An instance of AbstractGenerator for the specified engine.
    :rtype: AbstractGenerator
    """
    return import_module(f'autosubmit.generators.{engine.value}').Generator


__all__ = [
    'AbstractGenerator',
    'Engine',
    'get_engine_generator'
]
