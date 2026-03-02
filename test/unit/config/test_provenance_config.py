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

"""Tests for provenance preservation in ``AutosubmitConfig`` methods.

These tests verify that ``yaml-provenance`` metadata survives through config
normalization, variable substitution, serialization, and the public
``get_value_provenance()`` API.
"""

import os
from pathlib import Path

import pytest

from autosubmit.config.configcommon import (
    AutosubmitConfig,
    _preserve_prov,
    _wrap_with_source,
)
from test.unit.config.conftest import requires_yaml_provenance


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def _has_provenance(value, expected_source: str = None) -> bool:
    """Return True when *value* carries provenance metadata.

    If *expected_source* is given, also checks that the last provenance entry's
    ``yaml_file`` contains *expected_source*.
    """
    prov = getattr(value, "provenance", None)
    if prov is None:
        return False
    last = prov[-1] if isinstance(prov, list) else prov
    if expected_source is not None:
        return expected_source in str(last.get("yaml_file", ""))
    return True


# ---------------------------------------------------------------------------
# get_value_provenance()
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestGetValueProvenance:
    """Tests for ``AutosubmitConfig.get_value_provenance()``."""

    def test_returns_provenance_for_wrapped_value(self, autosubmit_config):
        """Wrapped values must return a non-empty provenance list."""
        experiment_data = {
            "EXPERIMENT": {
                "DATELIST": _wrap_with_source("20000101", "expdef_a001.yml"),
            }
        }
        as_conf = autosubmit_config(expid="t000", experiment_data=experiment_data)
        prov = as_conf.get_value_provenance(["EXPERIMENT", "DATELIST"])

        assert isinstance(prov, list)
        assert len(prov) >= 1
        last = prov[-1]
        assert "yaml_file" in last
        assert "expdef_a001.yml" in str(last["yaml_file"])

    def test_returns_empty_for_plain_value(self, autosubmit_config):
        """Plain Python values (no provenance) must return ``[]``."""
        experiment_data = {"EXPERIMENT": {"DATELIST": "20000101"}}
        as_conf = autosubmit_config(expid="t000", experiment_data=experiment_data)
        prov = as_conf.get_value_provenance(["EXPERIMENT", "DATELIST"])
        assert prov == []

    def test_returns_empty_for_missing_key(self, autosubmit_config):
        as_conf = autosubmit_config(expid="t000", experiment_data={})
        prov = as_conf.get_value_provenance(["NONEXISTENT"])
        assert prov == []

    def test_returns_empty_without_library(self, autosubmit_config, mocker):
        mocker.patch("autosubmit.config.configcommon._HAS_YAML_PROVENANCE", False)
        experiment_data = {
            "EXPERIMENT": {
                "DATELIST": _wrap_with_source("20000101", "expdef.yml"),
            }
        }
        as_conf = autosubmit_config(expid="t000", experiment_data=experiment_data)
        assert as_conf.get_value_provenance(["EXPERIMENT", "DATELIST"]) == []


# ---------------------------------------------------------------------------
# deep_normalize() — provenance preservation
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestDeepNormalizeProvenance:
    """Verify that ``deep_normalize()`` preserves provenance on keys and values."""

    def test_uppercased_keys_carry_provenance(self, autosubmit_config):
        key = _wrap_with_source("jobs", "src:expdef.yml")
        inner_key = _wrap_with_source("ini", "src:jobs.yml")
        data = {key: {inner_key: {"wallclock": "00:05"}}}

        as_conf = autosubmit_config(expid="t000", experiment_data={})
        result = as_conf.deep_normalize(data)

        # The normalized keys should be uppercase and carry provenance
        upper_keys = list(result.keys())
        assert len(upper_keys) == 1
        assert str(upper_keys[0]) == "JOBS"
        assert _has_provenance(upper_keys[0])

        inner_keys = list(result["JOBS"].keys())
        assert str(inner_keys[0]) == "INI"
        assert _has_provenance(inner_keys[0])

    def test_leaf_values_unchanged(self, autosubmit_config):
        wrapped_val = _wrap_with_source("00:05", "src:jobs.yml")
        data = {"JOBS": {"INI": {"WALLCLOCK": wrapped_val}}}

        as_conf = autosubmit_config(expid="t000", experiment_data={})
        result = as_conf.deep_normalize(data)
        assert result["JOBS"]["INI"]["WALLCLOCK"] == "00:05"
        assert _has_provenance(result["JOBS"]["INI"]["WALLCLOCK"], "src:jobs.yml")


# ---------------------------------------------------------------------------
# dict_replace_value() — provenance preservation
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestDictReplaceValueProvenance:
    """Verify that ``dict_replace_value()`` preserves provenance."""

    def test_whole_value_replacement_preserves_provenance(self, autosubmit_config):
        wrapped = _wrap_with_source("00:05", "src:jobs.yml")
        data = {"JOBS": {"INI": {"WALLCLOCK": wrapped}}}
        as_conf = autosubmit_config(expid="t000", experiment_data=data)

        as_conf.dict_replace_value(
            data, "00:05", "01:00", 0, ["WALLCLOCK", "INI", "JOBS"]
        )

        result = data["JOBS"]["INI"]["WALLCLOCK"]
        assert result == "01:00"
        assert _has_provenance(result, "src:jobs.yml")

    def test_partial_replacement_preserves_provenance(self, autosubmit_config):
        wrapped = _wrap_with_source("/home/%EXPID%/output", "src:expdef.yml")
        data = {"DEFAULT": {"OUTPUT": wrapped}}
        as_conf = autosubmit_config(expid="t000", experiment_data=data)

        as_conf.dict_replace_value(
            data, "%EXPID%", "a001", 0, ["OUTPUT", "DEFAULT"]
        )

        result = data["DEFAULT"]["OUTPUT"]
        assert result == "/home/a001/output"
        assert _has_provenance(result, "src:expdef.yml")

    def test_list_item_replacement_preserves_provenance(self, autosubmit_config):
        item = _wrap_with_source("file_a.sh", "src:jobs.yml")
        data = {"JOBS": {"SIM": {"ADDITIONAL_FILES": [item]}}}
        as_conf = autosubmit_config(expid="t000", experiment_data=data)

        as_conf.dict_replace_value(
            data, "file_a.sh", "file_b.sh", 0, ["ADDITIONAL_FILES", "SIM", "JOBS"]
        )

        result = data["JOBS"]["SIM"]["ADDITIONAL_FILES"][0]
        assert result == "file_b.sh"
        assert _has_provenance(result, "src:jobs.yml")


# ---------------------------------------------------------------------------
# _normalize_notify_on() — provenance preservation
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestNormalizeNotifyOnProvenance:
    def test_split_tokens_carry_provenance(self, autosubmit_config):
        notify_str = _wrap_with_source("completed, failed", "src:jobs.yml")
        data = {"JOBS": {"SIM": {"NOTIFY_ON": notify_str}}}
        as_conf = autosubmit_config(expid="t000", experiment_data=data)

        AutosubmitConfig._normalize_notify_on(data, "SIM")

        tokens = data["JOBS"]["SIM"]["NOTIFY_ON"]
        assert len(tokens) == 2
        assert str(tokens[0]) == "COMPLETED"
        assert str(tokens[1]) == "FAILED"
        for tok in tokens:
            assert _has_provenance(tok, "src:jobs.yml")


# ---------------------------------------------------------------------------
# _normalize_dependencies() — provenance preservation
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestNormalizeDependenciesProvenance:
    def test_string_deps_uppercased_with_provenance(self, autosubmit_config):
        deps = _wrap_with_source("ini sim", "src:jobs.yml")
        result = AutosubmitConfig._normalize_dependencies(deps)

        keys = list(result.keys())
        assert len(keys) == 2
        assert str(keys[0]) == "INI"
        assert str(keys[1]) == "SIM"
        for k in keys:
            assert _has_provenance(k, "src:jobs.yml")

    def test_dict_deps_uppercased_with_provenance(self, autosubmit_config):
        dep_key = _wrap_with_source("sim", "src:jobs.yml")
        status_val = _wrap_with_source("running", "src:jobs.yml")
        deps = {dep_key: {"STATUS": status_val}}
        result = AutosubmitConfig._normalize_dependencies(deps)

        upper_key = list(result.keys())[0]
        assert str(upper_key) == "SIM"
        assert _has_provenance(upper_key, "src:jobs.yml")

        status = result[upper_key]["STATUS"]
        assert str(status) == "RUNNING"
        assert _has_provenance(status, "src:jobs.yml")


# ---------------------------------------------------------------------------
# _normalize_jobs_section() — provenance on FILE / CUSTOM_DIRECTIVES
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestNormalizeJobsSectionProvenance:
    def test_file_preserves_provenance(self, autosubmit_config):
        file_val = _wrap_with_source("templates/sim.sh", "src:jobs.yml")
        data = {
            "JOBS": {
                "SIM": {
                    "FILE": file_val,
                    "DEPENDENCIES": {},
                }
            }
        }
        as_conf = autosubmit_config(expid="t000", experiment_data=data)
        as_conf._normalize_jobs_section(data, must_exists=False)

        result = data["JOBS"]["SIM"]["FILE"]
        assert str(result) == "templates/sim.sh"
        assert _has_provenance(result, "src:jobs.yml")

    def test_custom_directives_preserves_provenance(self, autosubmit_config):
        cd = _wrap_with_source("#SBATCH --exclusive", "src:platforms.yml")
        data = {
            "JOBS": {
                "SIM": {
                    "CUSTOM_DIRECTIVES": cd,
                    "DEPENDENCIES": {},
                }
            }
        }
        as_conf = autosubmit_config(expid="t000", experiment_data=data)
        as_conf._normalize_jobs_section(data, must_exists=False)

        result = data["JOBS"]["SIM"]["CUSTOM_DIRECTIVES"]
        assert str(result) == "#SBATCH --exclusive"
        assert _has_provenance(result, "src:platforms.yml")


# ---------------------------------------------------------------------------
# load_as_env_variables() — wraps env vars with provenance
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestLoadEnvVariablesProvenance:
    def test_env_vars_carry_provenance(self, monkeypatch):
        monkeypatch.setenv("AS_ENV_TEST_VAR", "my_value")
        params = {}
        result = AutosubmitConfig.load_as_env_variables(params)

        var = result["AS_ENV_TEST_VAR"]
        assert var == "my_value"
        assert _has_provenance(var, "environment:$AS_ENV_TEST_VAR")

    def test_current_user_carries_provenance(self, monkeypatch):
        monkeypatch.setenv("USER", "testuser")
        monkeypatch.delenv("SUDO_USER", raising=False)
        params = {}
        result = AutosubmitConfig.load_as_env_variables(params)

        user = result["AS_ENV_CURRENT_USER"]
        assert user == "testuser"
        assert _has_provenance(user, "environment:$USER")


# ---------------------------------------------------------------------------
# save() — provenance comments in output YAML
# ---------------------------------------------------------------------------


@requires_yaml_provenance
class TestSaveWithProvenance:
    def test_save_writes_provenance_comments(self, autosubmit_config, tmp_path, monkeypatch):
        """When yaml-provenance is installed, ``save()`` should write inline
        provenance comments into ``experiment_data.yml``."""
        # Ensure ownership check passes (same pattern as test_save.py).
        monkeypatch.setenv("USER", Path(tmp_path).owner())
        wrapped = _wrap_with_source("20000101", "expdef_t000.yml")
        experiment_data = {
            "DEFAULT": {"HPCARCH": "LOCAL"},
            "EXPERIMENT": {"DATELIST": wrapped},
        }
        as_conf = autosubmit_config(
            expid="t000", experiment_data=experiment_data, include_basic_config=False
        )
        as_conf.experiment_data["ROOTDIR"] = str(
            Path(as_conf.basic_config.LOCAL_ROOT_DIR) / as_conf.expid
        )
        as_conf.save()

        yml_file = Path(as_conf.metadata_folder) / "experiment_data.yml"
        assert yml_file.exists()
        content = yml_file.read_text()
        # dump_yaml writes provenance as end-of-line comments
        assert "expdef_t000.yml" in content

    def test_save_fallback_without_library(self, autosubmit_config, tmp_path, mocker, monkeypatch):
        """When yaml-provenance is NOT installed, ``save()`` should still write
        a valid YAML file (without provenance comments)."""
        monkeypatch.setenv("USER", Path(tmp_path).owner())
        mocker.patch("autosubmit.config.configcommon._HAS_YAML_PROVENANCE", False)
        experiment_data = {"DEFAULT": {"HPCARCH": "LOCAL"}}
        as_conf = autosubmit_config(
            expid="t000", experiment_data=experiment_data, include_basic_config=False
        )
        as_conf.experiment_data["ROOTDIR"] = str(
            Path(as_conf.basic_config.LOCAL_ROOT_DIR) / as_conf.expid
        )
        as_conf.save()

        yml_file = Path(as_conf.metadata_folder) / "experiment_data.yml"
        assert yml_file.exists()
        content = yml_file.read_text()
        # Should contain config values but NOT provenance comments
        assert "LOCAL" in content
