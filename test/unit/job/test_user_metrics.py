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

"""Unit tests for ``autosubmit.job.user_metrics``."""

from pathlib import Path
from typing import Any

import pytest
from pytest_mock import MockerFixture

from autosubmit.job.job import Job
from autosubmit.job.metrics_processor import (
    MAX_FILE_SIZE_MB,
    MetricSpecSelector,
    MetricSpecSelectorType,
    MetricSpec,
    UserMetricProcessor,
)
from autosubmit.platforms.locplatform import LocalPlatform

_EXPID = "t123"


@pytest.fixture
def disable_metric_repository(mocker):
    mock = mocker.patch("autosubmit.job.metrics_processor.UserMetricRepository")
    mock.return_value = mocker.MagicMock()
    yield mock


@pytest.mark.parametrize(
    "metric_selector_spec, expected",
    [
        (
            None,
            MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
        ),
        (
            {"TYPE": "TEXT"},
            MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
        ),
        (
            {"TYPE": "TEXT", "KEY": None},
            MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
        ),
        (
            {"TYPE": "TEXT", "KEY": "any"},
            MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
        ),
        (
            {"TYPE": "JSON", "KEY": "key1.key2.key3"},
            MetricSpecSelector(
                type=MetricSpecSelectorType.JSON, key=["key1", "key2", "key3"]
            ),
        ),
        (
            {"TYPE": "JSON", "KEY": ["key1", "key2", "key3", "key4"]},
            MetricSpecSelector(
                type=MetricSpecSelectorType.JSON, key=["key1", "key2", "key3", "key4"]
            ),
        ),
    ],
)
def test_spec_selector_load_valid(
    metric_selector_spec: Any, expected: MetricSpecSelector
):
    selector = MetricSpecSelector.load(metric_selector_spec)

    assert selector.type == expected.type
    assert selector.key == expected.key


@pytest.mark.parametrize(
    "metric_selector_spec",
    [
        "invalid",
        123,
        {"TYPE": "INVALID"},
        {"TYPE": "JSON"},
        {"TYPE": "JSON", "KEY": 123},
        {"TYPE": "JSON", "KEY": {"key1": "value1"}},
    ],
)
def test_spec_selector_load_invalid(metric_selector_spec: Any):
    with pytest.raises(Exception):
        MetricSpecSelector.load(metric_selector_spec)


@pytest.mark.parametrize(
    "metric_specs, expected",
    [
        (
            {
                "NAME": "metric1",
                "FILENAME": "file1",
                "MAX_READ_SIZE_MB": MAX_FILE_SIZE_MB,
            },
            MetricSpec(
                name="metric1",
                filename="file1",
                selector=MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
            ),
        ),
        (
            {
                "NAME": "metric2",
                "FILENAME": "file2",
                "MAX_READ_SIZE_MB": 10,
                "SELECTOR": {"TYPE": "TEXT"},
            },
            MetricSpec(
                name="metric2",
                filename="file2",
                max_read_size_mb=10,
                selector=MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
            ),
        ),
        (
            {
                "NAME": "metric3",
                "FILENAME": "file3",
                "MAX_READ_SIZE_MB": 1,
                "SELECTOR": {"TYPE": "JSON", "KEY": "key1.key2.key3"},
            },
            MetricSpec(
                name="metric3",
                filename="file3",
                max_read_size_mb=1,
                selector=MetricSpecSelector(
                    type=MetricSpecSelectorType.JSON, key=["key1", "key2", "key3"]
                ),
            ),
        ),
        (
            {
                "NAME": "metric4",
                "FILENAME": "file4",
                "SELECTOR": {"TYPE": "JSON", "KEY": ["key1", "key2", "key3", "key4"]},
            },
            MetricSpec(
                name="metric4",
                filename="file4",
                selector=MetricSpecSelector(
                    type=MetricSpecSelectorType.JSON,
                    key=["key1", "key2", "key3", "key4"],
                ),
            ),
        ),
    ],
)
def test_metric_spec_load_valid(metric_specs: Any, expected: MetricSpec):
    metric_spec = MetricSpec.load(metric_specs)

    assert metric_spec.name == expected.name
    assert metric_spec.selector.type == expected.selector.type
    assert metric_spec.filename == expected.filename
    assert metric_spec.max_read_size_mb == expected.max_read_size_mb
    assert metric_spec.selector.key == expected.selector.key


@pytest.mark.parametrize(
    "metric_specs",
    [
        {},
        "invalid",
        None,
        {"NAME": "metric1"},
        {"FILENAME": "file1"},
        {
            "NAME": "metric2",
            "FILENAME": "file2",
            "SELECTOR": "invalid",
        },
    ],
)
def test_metric_spec_load_invalid(metric_specs: Any):
    with pytest.raises(Exception):
        MetricSpec.load(metric_specs)


def test_read_metrics_specs_as_conf_exception(disable_metric_repository, mocker):
    # Mocking the AutosubmitConfig and Job objects
    as_conf = mocker.MagicMock()
    as_conf.get_section.side_effect = ValueError
    job = mocker.MagicMock()

    # Do the read test
    user_metric_processor = UserMetricProcessor(as_conf, job)

    with pytest.raises(ValueError) as cm:
        user_metric_processor.read_metrics_specs()

    assert 'Invalid or missing metrics section' in str(cm.value)


def test_process_metrics(disable_metric_repository, mocker: MockerFixture):
    # Mocking the AutosubmitConfig and Job objects
    as_conf = mocker.MagicMock()
    job = mocker.MagicMock()
    job.name = "test_job"
    job.platform = mocker.MagicMock()
    job.platform.read_file = mocker.MagicMock()
    job.platform.read_file.return_value = b'{"key1": "value1", "key2": "value2"}'

    mock_read_metrics_specs =  mocker.patch("autosubmit.job.metrics_processor.UserMetricProcessor.read_metrics_specs")
    mock_read_metrics_specs.return_value = [
        # The first metric is a text file
        MetricSpec(
            name="metric1",
            filename="file1",
            selector=MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
        ),
        # The second metric is a JSON file
        MetricSpec(
            name="metric2",
            filename="file2",
            selector=MetricSpecSelector(
                type=MetricSpecSelectorType.JSON, key=["key2"]
            ),
        ),
    ]

    # Mocking the repository
    mock_store_metric = mocker.MagicMock()
    mock_repo = mocker.MagicMock()
    mock_repo.store_metric = mock_store_metric

    user_metric_processor = UserMetricProcessor(as_conf, job)
    user_metric_processor.user_metric_repository = mock_repo
    user_metric_processor.process_metrics()

    assert mock_read_metrics_specs.call_count == 1

    assert job.platform.read_file.call_count == 2

    assert mock_store_metric.call_count == 2

    assert mock_store_metric.call_args_list[0][0][1] == "test_job"
    assert mock_store_metric.call_args_list[0][0][2] == "metric1"
    assert (
        mock_store_metric.call_args_list[0][0][3]
        == '{"key1": "value1", "key2": "value2"}'
    )

    assert mock_store_metric.call_args_list[1][0][1] == "test_job"
    assert mock_store_metric.call_args_list[1][0][2] == "metric2"
    assert mock_store_metric.call_args_list[1][0][3] == "value2"


def test_get_current_metric_folder(autosubmit_config):
    as_conf = autosubmit_config(
        _EXPID,
        {
            "CONFIG": {
                "METRIC_FOLDER": "/foo/bar",
            },
            "JOBS": {
                "DUMMY_SECTION": {},
            },
        },
    )

    job_name = f'{_EXPID}_DUMMY_SECTION'

    job = Job(job_name, "1", 0, 1)
    job.section = "DUMMY_SECTION"

    parameters = job.update_parameters(as_conf)

    assert parameters["CURRENT_METRIC_FOLDER"] == str(Path("/foo/bar", job_name))


def test_get_current_metric_folder_placeholder(autosubmit_config, local: LocalPlatform):
    as_conf = autosubmit_config(
        _EXPID,
        {
            "CONFIG": {
                "METRIC_FOLDER": "%CURRENT_ROOTDIR%/my_metrics_folder",
            },
            "JOBS": {
                "DUMMY_SECTION": {},
            },
        },
    )

    job_name = f'{_EXPID}_DUMMY_SECTION'

    job = Job(job_name, "1", 0, 1)
    job.section = "DUMMY_SECTION"
    job.platform = local

    parameters = job.update_parameters(as_conf)

    assert (
        isinstance(parameters["CURRENT_ROOTDIR"], str)
        and len(parameters["CURRENT_ROOTDIR"]) > 0
    )

    assert parameters["CURRENT_METRIC_FOLDER"] == str(
        Path(parameters["CURRENT_ROOTDIR"]).joinpath("my_metrics_folder", job_name)
    )
