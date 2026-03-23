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
register_pickle_reducers()


class YAMLParserFactory:
    def __init__(self):
        pass

    def create_parser(self):
        return YAMLParser()


class YAMLParser(YAML):

    def __init__(self):
        self.data = []
        super(YAMLParser, self).__init__(typ="rt")
