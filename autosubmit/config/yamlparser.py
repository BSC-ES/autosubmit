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


from ruamel.yaml import YAML
from yaml_provenance import (
    load_yaml,
    configure,
    ProvenanceConfig,
    register_pickle_reducers,
    register_yaml_representers,
)

# ---------------------------------------------------------------------------
# yaml-provenance integration
# ---------------------------------------------------------------------------
# Every value loaded from a YAML file becomes a ``WithProvenance`` subclass of
# its native type (str, int, …).  This means any downstream code can inspect
# *which* file, line and column a value originated from without any changes to
# the rest of Autosubmit.
#
# Enable full provenance history so merges across multiple YAML files preserve
# a complete chain of origin information.
# ---------------------------------------------------------------------------
configure(ProvenanceConfig(track_history=True))


class YAMLParserFactory:
    def __init__(self):
        pass

    def create_parser(self):
        return YAMLParser()


class YAMLParser(YAML):

    def __init__(self):
        self.data = []
        super(YAMLParser, self).__init__(typ="safe")

    def load(self, stream):
        """Load YAML from *stream*, attaching provenance metadata.

        The returned mapping is a ``DictWithProvenance`` instance where every
        leaf value carries its source ``yaml_file``, ``line`` and ``col``.
        These survive through subsequent ``dict`` operations because
        ``WithProvenance`` objects are transparent subclasses of their native
        Python types.

        After loading, pickle and YAML-dump compatibility is ensured by
        calling ``register_pickle_reducers()`` and
        ``register_yaml_representers()``.  These must be called after *every*
        load because the internal wrapper registry is populated lazily — new
        types are registered on first encounter.

        :param stream: An open file-like object (with ``.name`` attribute),
            or a ``str``/``pathlib.Path`` pointing to the YAML file.
        :return: Parsed mapping (``DictWithProvenance`` or plain ``dict``).
        """
        result = load_yaml(stream)
        register_pickle_reducers()
        register_yaml_representers()
        return result if result is not None else {}
