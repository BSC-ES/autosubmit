from enum import Enum
from importlib import import_module
from typing import AbstractSet, Callable, cast
from abc import ABC, abstractmethod


"""This module provides generators to produce workflow configurations for different backend engines."""

class Engine(Enum):
    """Workflow Manager engine flavors."""
    aiida = 'aiida'

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
    def generate(cls, job_list, as_conf, **arg_options) -> None:
        """Generates the workflow from the created autosubmit workflow."""
        raise NotImplementedError


def get_engine_generator(engine: Engine) -> AbstractGenerator:
    return import_module(f'autosubmit.generators.{engine.value}').Generator

__all__ = [
    'Engine',
    'get_engine_generator'
]
