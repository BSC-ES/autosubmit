import sqlite3
from typing import Any
import pytest
from pathlib import Path
from autosubmit.job.job import Job
from autosubmit.job.metrics_processor import (
    MAX_FILE_SIZE_MB,
    MetricSpecSelector,
    MetricSpecSelectorType,
    MetricSpec,
    UserMetricProcessor,
    UserMetricRepository,
)
from unittest.mock import MagicMock, patch

from autosubmit.platforms.locplatform import LocalPlatform
from autosubmitconfigparser.config.basicconfig import BasicConfig


@pytest.fixture
def disable_metric_repository():
    with patch("autosubmit.job.metrics_processor.UserMetricRepository") as mock:
        mock.return_value = MagicMock()
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


def test_read_metrics_specs(disable_metric_repository):
    # Mocking the AutosubmitConfig and Job objects
    as_conf = MagicMock()
    job = MagicMock()

    as_conf.get_section.return_value = [
        {"NAME": "metric1", "FILENAME": "file1"},
        {
            "NAME": "invalid metric",
        },
        {
            "NAME": "metric2",
            "FILENAME": "file2",
            "SELECTOR": {"TYPE": "JSON", "KEY": "key1.key2.key3"},
        },
    ]
    as_conf.deep_normalize = lambda x: x

    # Do the read test
    user_metric_processor = UserMetricProcessor(as_conf, job)
    metric_specs = user_metric_processor.read_metrics_specs()

    assert len(metric_specs) == 2

    assert metric_specs[0].name == "metric1"
    assert metric_specs[0].filename == "file1"
    assert metric_specs[0].selector.type == MetricSpecSelectorType.TEXT
    assert metric_specs[0].selector.key is None

    assert metric_specs[1].name == "metric2"
    assert metric_specs[1].filename == "file2"
    assert metric_specs[1].selector.type == MetricSpecSelectorType.JSON
    assert metric_specs[1].selector.key == ["key1", "key2", "key3"]


def test_process_metrics(disable_metric_repository):
    # Mocking the AutosubmitConfig and Job objects
    as_conf = MagicMock()
    job = MagicMock()
    job.name = "test_job"
    job.platform = MagicMock()
    job.platform.read_file = MagicMock()
    job.platform.read_file.return_value = b'{"key1": "value1", "key2": "value2"}'

    with patch(
        "autosubmit.job.metrics_processor.UserMetricProcessor.read_metrics_specs"
    ) as mock_read_metrics_specs:
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
        mock_store_metric = MagicMock()
        mock_repo = MagicMock()
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
    JOB_NAME = "t123"
    as_conf = autosubmit_config(
        JOB_NAME,
        {
            "CONFIG": {
                "METRIC_FOLDER": "/foo/bar",
            },
            "JOBS": {
                "DUMMY_SECTION": {},
            },
        },
    )

    job = Job(JOB_NAME, "1", 0, 1)
    job.section = "DUMMY_SECTION"

    parameters = job.update_parameters(as_conf)

    assert parameters["CURRENT_METRIC_FOLDER"] == str(
        Path("/foo/bar").joinpath(JOB_NAME)
    )


def test_get_current_metric_folder_placeholder(autosubmit_config, local: LocalPlatform):
    JOB_NAME = "t123"
    as_conf = autosubmit_config(
        JOB_NAME,
        {
            "CONFIG": {
                "METRIC_FOLDER": "%CURRENT_ROOTDIR%/my_metrics_folder",
            },
            "JOBS": {
                "DUMMY_SECTION": {},
            },
        },
    )

    job = Job(JOB_NAME, "1", 0, 1)
    job.section = "DUMMY_SECTION"
    job.platform = local

    parameters = job.update_parameters(as_conf)

    assert (
        isinstance(parameters["CURRENT_ROOTDIR"], str)
        and len(parameters["CURRENT_ROOTDIR"]) > 0
    )

    assert parameters["CURRENT_METRIC_FOLDER"] == str(
        Path(parameters["CURRENT_ROOTDIR"]).joinpath("my_metrics_folder", JOB_NAME)
    )


def test_store_metric(tmp_path):
    EXPID = "t123"
    with patch("autosubmit.job.metrics_processor.BasicConfig.LOCAL_ROOT_DIR", tmp_path):
        Path(tmp_path).joinpath(EXPID, BasicConfig.LOCAL_TMP_DIR).mkdir(
            parents=True, exist_ok=True
        )

        user_metric_repository = UserMetricRepository(EXPID)
        user_metric_repository.store_metric(
            run_id=1,
            job_name="test_job",
            metric_name="test_metric",
            metric_value="test_value",
        )

        # Check if the metric is stored in the database
        with sqlite3.connect(user_metric_repository.db_path) as conn:
            cursor = conn.cursor()
            cursor.execute(
                """
                SELECT metric_value FROM user_metrics
                WHERE run_id = ? AND job_name = ? AND metric_name = ?;
                """,
                (1, "test_job", "test_metric"),
            )
            result = cursor.fetchone()
            assert result is not None
            assert result[0] == "test_value"
