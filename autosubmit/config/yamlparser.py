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

# ---------------------------------------------------------------------------
# Optional yaml-provenance integration
# ---------------------------------------------------------------------------
# When the ``yaml-provenance`` library is installed every value loaded from a
# YAML file becomes a ``WithProvenance`` subclass of its native type (str, int,
# …).  This means any downstream code can inspect *which* file, line and column
# a value originated from without any changes to the rest of Autosubmit.
#
# Install (from the feature branch until merged to main):
#   pip install "yaml-provenance @ git+https://github.com/esm-tools/yaml-provenance.git@feat/yaml_dumper"
#
# If the library is not installed, loading falls back silently to the standard
# ruamel.yaml behaviour so nothing breaks.
# ---------------------------------------------------------------------------
try:
    from yaml_provenance import (
        load_yaml,
        configure,
        ProvenanceConfig,
        register_pickle_reducers,
        register_yaml_representers,
    )

    # Enable full provenance history so merges across multiple YAML files
    # preserve a complete chain of origin information.
    configure(ProvenanceConfig(track_history=True))

    _HAS_YAML_PROVENANCE = True
except ImportError:
    _HAS_YAML_PROVENANCE = False


class YAMLParserFactory:
    def __init__(self):
        pass

    def create_parser(self):
        return YAMLParser()


class YAMLParser(YAML):

    def __init__(self):
        self.data = []
        super(YAMLParser, self).__init__(typ="rt")
