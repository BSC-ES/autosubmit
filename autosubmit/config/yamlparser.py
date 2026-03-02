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
        super(YAMLParser, self).__init__(typ="safe")

    def load(self, stream):
        """Load YAML from *stream*, attaching provenance metadata when available.

        If ``yaml-provenance`` is installed the returned mapping is a
        ``DictWithProvenance`` instance where every leaf value carries its
        source ``yaml_file``, ``line`` and ``col``.  These survive through
        subsequent ``dict`` operations because ``WithProvenance`` objects are
        transparent subclasses of their native Python types.

        After loading, pickle and YAML-dump compatibility is ensured by
        calling ``register_pickle_reducers()`` and
        ``register_yaml_representers()``.  These must be called after *every*
        load because the internal wrapper registry is populated lazily — new
        types are registered on first encounter.

        If ``yaml-provenance`` is **not** installed (or loading via it fails
        for any reason) the method falls back to the standard ``ruamel.yaml``
        loader transparently.

        :param stream: An open file-like object (with ``.name`` attribute),
            or a ``str``/``pathlib.Path`` pointing to the YAML file.
        :return: Parsed mapping (``DictWithProvenance`` or plain ``dict``).
        """
        if _HAS_YAML_PROVENANCE:
            try:
                result = load_yaml(stream)
                register_pickle_reducers()
                register_yaml_representers()
                return result if result is not None else {}
            except Exception:
                # Any error (e.g. file not found, parse error) falls
                # through to the ruamel.yaml loader below so that the
                # existing exception-handling in get_parser() still works.
                pass

        # ------------------------------------------------------------------ #
        # Fallback: standard ruamel.yaml load (no provenance tracking).       #
        # ------------------------------------------------------------------ #
        return super().load(stream)
