from typing import Any
import pytest
from autosubmit.job.metrics_processor import (
    MetricSpecSelector,
    MetricSpecSelectorType,
    MetricSpec,
    UserMetricProcessor,
)
from unittest.mock import MagicMock, patch


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
            {"NAME": "metric1", "PATH": "/path/to/", "FILENAME": "file1"},
            MetricSpec(
                name="metric1",
                path="/path/to/",
                filename="file1",
                selector=MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
            ),
        ),
        (
            {
                "NAME": "metric2",
                "PATH": "/path/to/",
                "FILENAME": "file2",
                "SELECTOR": {"TYPE": "TEXT"},
            },
            MetricSpec(
                name="metric2",
                path="/path/to/",
                filename="file2",
                selector=MetricSpecSelector(type=MetricSpecSelectorType.TEXT, key=None),
            ),
        ),
        (
            {
                "NAME": "metric3",
                "PATH": "/path/to/",
                "FILENAME": "file3",
                "SELECTOR": {"TYPE": "JSON", "KEY": "key1.key2.key3"},
            },
            MetricSpec(
                name="metric3",
                path="/path/to/",
                filename="file3",
                selector=MetricSpecSelector(
                    type=MetricSpecSelectorType.JSON, key=["key1", "key2", "key3"]
                ),
            ),
        ),
        (
            {
                "NAME": "metric4",
                "PATH": "/path/to/",
                "FILENAME": "file4",
                "SELECTOR": {"TYPE": "JSON", "KEY": ["key1", "key2", "key3", "key4"]},
            },
            MetricSpec(
                name="metric4",
                path="/path/to/",
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
    assert metric_spec.path == expected.path
    assert metric_spec.selector.type == expected.selector.type
    assert metric_spec.selector.key == expected.selector.key


@pytest.mark.parametrize(
    "metric_specs",
    [
        {},
        "invalid",
        None,
        {"NAME": "metric1"},
        {"PATH": "/path/to/"},
        {"FILENAME": "file1"},
        {
            "NAME": "metric2",
            "PATH": "/path/to/",
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
        {"NAME": "metric1", "PATH": "/path/to/", "FILENAME": "file1"},
        {
            "NAME": "invalid metric",
        },
        {
            "NAME": "metric2",
            "PATH": "/path/to/",
            "FILENAME": "file2",
            "SELECTOR": {"TYPE": "JSON", "KEY": "key1.key2.key3"},
        },
    ]
    as_conf.normalize_parameters_keys = lambda x: x

    # Do the read test
    user_metric_processor = UserMetricProcessor(as_conf, job)
    metric_specs = user_metric_processor.read_metrics_specs()

    assert len(metric_specs) == 2

    assert metric_specs[0].name == "metric1"
    assert metric_specs[0].path == "/path/to/"
    assert metric_specs[0].filename == "file1"
    assert metric_specs[0].selector.type == MetricSpecSelectorType.TEXT
    assert metric_specs[0].selector.key is None

    assert metric_specs[1].name == "metric2"
    assert metric_specs[1].path == "/path/to/"
    assert metric_specs[1].filename == "file2"
    assert metric_specs[1].selector.type == MetricSpecSelectorType.JSON
    assert metric_specs[1].selector.key == ["key1", "key2", "key3"]


@pytest.mark.parametrize(
    "metrics_specs, job_name, expected",
    [
        (
            [
                MetricSpec(
                    name="metric1",
                    path="/path/to/",
                    filename="file1",
                    selector=MetricSpecSelector(
                        type=MetricSpecSelectorType.TEXT, key=None
                    ),
                ),
                MetricSpec(
                    name="metric2",
                    path="/path/to/",
                    filename="file1",
                    selector=MetricSpecSelector(
                        type=MetricSpecSelectorType.JSON, key=["key1"]
                    ),
                ),
                MetricSpec(
                    name="metric3",
                    path="/path/to/",
                    filename="file2",
                    selector=MetricSpecSelector(
                        type=MetricSpecSelectorType.TEXT, key=None
                    ),
                ),
            ],
            "job1",
            {
                "/path/to/job1/file1": {
                    "TEXT": [
                        MetricSpec(
                            name="metric1",
                            path="/path/to/",
                            filename="file1",
                            selector=MetricSpecSelector(
                                type=MetricSpecSelectorType.TEXT, key=None
                            ),
                        )
                    ],
                    "JSON": [
                        MetricSpec(
                            name="metric2",
                            path="/path/to/",
                            filename="file1",
                            selector=MetricSpecSelector(
                                type=MetricSpecSelectorType.JSON, key=["key1"]
                            ),
                        )
                    ],
                },
                "/path/to/job1/file2": {
                    "TEXT": [
                        MetricSpec(
                            name="metric3",
                            path="/path/to/",
                            filename="file2",
                            selector=MetricSpecSelector(
                                type=MetricSpecSelectorType.TEXT, key=None
                            ),
                        )
                    ],
                },
            },
        ),
        (
            [],
            "job2",
            {},
        ),
    ],
)
def test_group_metrics_by_path_selector_type(
    disable_metric_repository, metrics_specs: dict, job_name: str, expected: dict
):
    as_conf = MagicMock()
    job = MagicMock()
    job.name = job_name
    user_metric_processor = UserMetricProcessor(as_conf, job)

    result = user_metric_processor._group_metrics_by_path_selector_type(metrics_specs)

    assert result == expected
