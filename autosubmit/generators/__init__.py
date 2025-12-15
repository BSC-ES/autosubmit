"""This module provides generators to produce workflow configurations for different backend engines."""
from enum import Enum
from importlib import import_module
from typing import TYPE_CHECKING
from abc import ABC, abstractmethod
if TYPE_CHECKING:
    from autosubmitconfigparser.config.configcommon import AutosubmitConfig
    from autosubmit.job.job_list import JobList


class Engine(Enum):
    """Workflow Manager engine flavors."""
    AIIDA = 'aiida'

    def __str__(self):
        return self.value


class AbstractGenerator(ABC):
    """Generator of workflow for an engine."""

    @staticmethod
    @abstractmethod
    def get_engine_name() -> str:
        """The engine name used for the help text."""
        raise NotImplementedError

    @staticmethod
    @abstractmethod
    def add_parse_args(parser) -> None:
        """Adds arguments to the parser that are needed for a specific engine implementation."""
        raise NotImplementedError

    @classmethod
    @abstractmethod
    def generate(cls, job_list: JobList, as_conf: AutosubmitConfig, **arg_options) -> None:
        """Generates the workflow from the created autosubmit workflow."""
        raise NotImplementedError


def get_engine_generator(engine: Engine) -> AbstractGenerator:
    """Returns the generator for the given engine."""
    return import_module(f'autosubmit.generators.{engine.value}').Generator

__all__ = [
    'Engine',
    'get_engine_generator'
]
