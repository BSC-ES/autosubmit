from dataclasses import dataclass
from enum import Enum
import json
import copy
from typing import Any, Dict, List, Optional
from autosubmit.job.job import Job
from autosubmitconfigparser.config.configcommon import AutosubmitConfig


class MetricSpecSelectorType(Enum):
    TEXT = "TEXT"
    JSON = "JSON"


@dataclass
class MetricSpecSelector:
    type: MetricSpecSelectorType
    key: Optional[List[str]]


@dataclass
class MetricSpec:
    name: str
    path: str
    selector: MetricSpecSelector


class MetricProcessor:
    def __init__(self, as_conf: AutosubmitConfig, job: Job):
        self.as_conf = as_conf
        self.job = job

    def read_metrics_specs(self) -> List[MetricSpec]:
        raw_metrics: List[Dict[str, Any]] = self.as_conf.normalize_parameters_keys(
            self.as_conf.get_section([self.job.section, "METRICS"])
        )

        metrics_specs: List[MetricSpec] = []
        for raw_metric in raw_metrics:
            """
            Read the metrics specs of the job
            """
            _name = raw_metric["NAME"]
            _path = raw_metric["PATH"]

            if not _name or not _path:
                raise ValueError("Name and path must be provided")

            _selector = raw_metric.get("SELECTOR", {})

            if not isinstance(_selector, dict):
                raise ValueError("Invalid selector")

            _selector_type = str(_selector.get("TYPE", "TEXT")).upper()
            for type in MetricSpecSelectorType:
                if type.value == _selector_type:
                    _selector_type = type
                    break

            selector = MetricSpecSelector(
                type=_selector_type, key=_selector.get("KEY", None)
            )

            metrics_specs.append(MetricSpec(name=_name, path=_path, selector=selector))

        return metrics_specs

    def _group_metrics_by_path_selector_type(
        self,
        metrics_specs: List[MetricSpec],
    ) -> Dict[str, Dict[str, List[MetricSpec]]]:
        """
        Group all metrics by file path and selector type
        """

        metrics_by_path_selector_type: Dict[str, Dict[str, List[MetricSpec]]] = {}
        for metric_spec in metrics_specs:
            if metric_spec.path not in metrics_by_path_selector_type:
                metrics_by_path_selector_type[metric_spec.path] = {}
            if (
                metric_spec.selector.type
                not in metrics_by_path_selector_type[metric_spec.path]
            ):
                metrics_by_path_selector_type[metric_spec.path][
                    metric_spec.selector.type
                ] = []

            metrics_by_path_selector_type[metric_spec.path][
                metric_spec.selector.type.value
            ].append(metric_spec)

        return metrics_by_path_selector_type

    def store_metric(self, metric_name: str, metric_value: Any):
        """
        Store the metric value in the database
        """
        self.job.name
        raise NotImplementedError("store_metric method must be implemented")

    def process_metrics_specs(self, metrics_specs: List[MetricSpec]):
        """ """

        metrics_by_path_selector_type = self._group_metrics_by_path_selector_type(
            metrics_specs
        )

        # For each file path, read the content of the file
        for path, metrics_by_selector_type in metrics_by_path_selector_type.items():
            with open(path, "r") as f:
                content = f.read()

            # Process the content based on the selector type

            # Text selector metrics
            text_selector_metrics = metrics_by_selector_type.get(
                MetricSpecSelectorType.TEXT.value, []
            )
            if text_selector_metrics:
                for metric in text_selector_metrics:
                    self.store_metric(metric.name, content)

            # JSON selector metrics
            json_selector_metrics = metrics_by_selector_type.get(
                MetricSpecSelectorType.JSON.value, []
            )
            if json_selector_metrics:
                try:
                    json_content = json.loads(content)
                    for metric in json_selector_metrics:
                        # Get the value based on the key
                        try:
                            key = metric.selector.key
                            value = copy.deepcopy(json_content)
                            if key:
                                for k in key:
                                    value = value[k]
                            self.store_metric(metric.name, value)
                        except Exception:
                            print(
                                f"Error processing JSON content in file {path} for metric {metric.name}"
                            )
                except json.JSONDecodeError:
                    print(f"Invalid JSON content in file {path}")
                except Exception:
                    print(f"Error processing JSON content in file {path}")
