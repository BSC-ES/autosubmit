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

"""Tests for the yaml-provenance integration in ``yamlparser.py`` and the
module-level helper functions in ``configcommon.py``.
"""

import json
from pathlib import Path

import pytest

from autosubmit.config.configcommon import (
    _preserve_prov,
    _ProvenanceJSONEncoder,
    _wrap_dict_with_source,
    _wrap_with_source,
)
from autosubmit.config.yamlparser import YAMLParser


# ---------------------------------------------------------------------------
# YAMLParser.load() — provenance path
# ---------------------------------------------------------------------------


class TestYAMLParserProvenance:
    """Verify that ``YAMLParser.load()`` returns provenance-bearing values."""

    @staticmethod
    def _write_yaml(tmp_path: Path, content: str) -> Path:
        """Write *content* to a temporary YAML file and return its path."""
        p = tmp_path / "sample.yml"
        p.write_text(content)
        return p

    def test_load_returns_dict_with_provenance(self, tmp_path):
        yaml_file = self._write_yaml(tmp_path, "FOO: bar\nNUM: 42\n")
        parser = YAMLParser()
        data = parser.load(yaml_file)

        # Should be a dict (or DictWithProvenance subclass of dict)
        assert isinstance(data, dict)

        # Leaf values must carry provenance
        foo = data["FOO"]
        assert hasattr(foo, "provenance"), "Expected FOO to be a WithProvenance type"
        prov = foo.provenance
        assert len(prov) >= 1
        last = prov[-1] if isinstance(prov, list) else prov
        assert "yaml_file" in last
        assert "line" in last
        assert "col" in last
        assert str(yaml_file) in str(last["yaml_file"])

    def test_load_values_are_transparent_subtypes(self, tmp_path):
        yaml_file = self._write_yaml(tmp_path, "NAME: hello\nCOUNT: 7\n")
        parser = YAMLParser()
        data = parser.load(yaml_file)

        # StrWithProvenance is a str, IntWithProvenance is an int
        assert isinstance(data["NAME"], str)
        assert isinstance(data["COUNT"], int)
        assert data["NAME"] == "hello"
        assert data["COUNT"] == 7

    def test_load_nested_dict(self, tmp_path):
        yaml_file = self._write_yaml(
            tmp_path, "SECTION:\n  KEY: value\n  NESTED:\n    DEEP: 99\n"
        )
        parser = YAMLParser()
        data = parser.load(yaml_file)

        deep_val = data["SECTION"]["NESTED"]["DEEP"]
        assert deep_val == 99
        assert hasattr(deep_val, "provenance")


# ---------------------------------------------------------------------------
# _wrap_with_source()
# ---------------------------------------------------------------------------


class TestWrapWithSource:
    def test_wraps_string(self):
        result = _wrap_with_source("hello", "computed:FOO")
        assert isinstance(result, str)
        assert result == "hello"
        assert hasattr(result, "provenance")
        last = result.provenance[-1]
        assert last["yaml_file"] == "computed:FOO"
        assert last["line"] == 0
        assert last["col"] == 0

    def test_wraps_int(self):
        result = _wrap_with_source(42, "computed:BAR")
        assert isinstance(result, int)
        assert result == 42
        assert result.provenance[-1]["yaml_file"] == "computed:BAR"


# ---------------------------------------------------------------------------
# _preserve_prov()
# ---------------------------------------------------------------------------


class TestPreserveProv:
    def test_transfers_metadata_on_upper(self):
        original = _wrap_with_source("hello", "src:file.yml")
        result = _preserve_prov(original, original.upper())
        assert result == "HELLO"
        assert hasattr(result, "provenance")
        assert result.provenance[-1]["yaml_file"] == "src:file.yml"

    def test_transfers_metadata_on_strip(self):
        original = _wrap_with_source("  padded  ", "src:file.yml")
        result = _preserve_prov(original, original.strip())
        assert result == "padded"
        assert hasattr(result, "provenance")

    def test_noop_for_plain_values(self):
        result = _preserve_prov("plain", "PLAIN")
        assert result == "PLAIN"
        assert not hasattr(result, "provenance")


# ---------------------------------------------------------------------------
# _wrap_dict_with_source()
# ---------------------------------------------------------------------------


class TestWrapDictWithSource:
    def test_wraps_flat_dict(self):
        d = {"A": 1, "B": "two"}
        result = _wrap_dict_with_source(d, "prefix")

        assert result["A"] == 1
        assert hasattr(result["A"], "provenance")
        assert result["A"].provenance[-1]["yaml_file"] == "prefix.A"

        assert result["B"] == "two"
        assert result["B"].provenance[-1]["yaml_file"] == "prefix.B"

    def test_wraps_nested_dict(self):
        d = {"OUTER": {"INNER": "val"}}
        result = _wrap_dict_with_source(d, "root")
        inner_val = result["OUTER"]["INNER"]
        assert inner_val == "val"
        assert hasattr(inner_val, "provenance")


# ---------------------------------------------------------------------------
# _ProvenanceJSONEncoder
# ---------------------------------------------------------------------------


class TestProvenanceJSONEncoder:
    def test_encodes_with_provenance_value(self):
        wrapped = _wrap_with_source("hello", "computed:TEST")
        text = json.dumps({"key": wrapped}, cls=_ProvenanceJSONEncoder)
        parsed = json.loads(text)
        assert parsed["key"] == "hello"

    def test_encodes_plain_values(self):
        text = json.dumps({"key": "plain", "num": 42}, cls=_ProvenanceJSONEncoder)
        parsed = json.loads(text)
        assert parsed["key"] == "plain"
        assert parsed["num"] == 42
