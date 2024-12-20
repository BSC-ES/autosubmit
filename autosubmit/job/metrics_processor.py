from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import copy
from pathlib import Path
import sqlite3
import traceback
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import Log

if TYPE_CHECKING:
    # Avoid circular imports
    from autosubmit.job.job import Job

MAX_FILE_SIZE = 4 * 1024 * 1024  # Default 4MB max file size


class MetricSpecSelectorType(Enum):
    TEXT = "TEXT"
    JSON = "JSON"


@dataclass
class MetricSpecSelector:
    type: MetricSpecSelectorType
    key: Optional[List[str]]

    @staticmethod
    def load(data: Optional[Dict[str, Any]]) -> "MetricSpecSelector":
        if data is None:
            _type = MetricSpecSelectorType.TEXT
            return MetricSpecSelector(type=_type, key=None)

        if not isinstance(data, dict):
            raise ValueError("Invalid metric spec selector")

        # Read the selector type
        _type = str(data.get("TYPE", MetricSpecSelectorType.TEXT.value)).upper()
        try:
            selector_type = MetricSpecSelectorType(_type)
        except Exception:
            raise ValueError(f"Invalid metric spec selector type: {_type}")

        # If selector type is TEXT, key is not required and is set to None
        if selector_type == MetricSpecSelectorType.TEXT:
            return MetricSpecSelector(type=selector_type, key=None)

        # If selector type is JSON, key must be a list or string
        elif selector_type == MetricSpecSelectorType.JSON:
            key = data.get("KEY", None)
            if isinstance(key, str):
                key = key.split(".")
            elif isinstance(key, list):
                key = key
            else:
                raise ValueError("Invalid key for JSON selector")
            return MetricSpecSelector(type=selector_type, key=key)

        return MetricSpecSelector(type=selector_type, key=None)


@dataclass
class MetricSpec:
    name: str
    path: str
    filename: str
    selector: MetricSpecSelector

    @staticmethod
    def load(data: Dict[str, Any]) -> "MetricSpec":
        if not isinstance(data, dict):
            raise ValueError("Invalid metric spec")

        if not data.get("NAME") or not data.get("PATH") or not data.get("FILENAME"):
            raise ValueError("Name, path, and filename are required in metric spec")

        _name = data["NAME"]
        _path = data["PATH"]
        _filename = data["FILENAME"]

        _selector = data.get("SELECTOR", None)
        selector = MetricSpecSelector.load(_selector)

        return MetricSpec(name=_name, path=_path, filename=_filename, selector=selector)


class UserMetricRepository:
    def __init__(self, expid: str):
        exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(expid)
        tmp_path = Path(exp_path).joinpath(BasicConfig.LOCAL_TMP_DIR)
        self.db_path = tmp_path.joinpath(f"metrics_{expid}.db")

        with sqlite3.connect(self.db_path) as conn:
            # Create the metrics table if it does not exist
            conn.execute(
                """
                CREATE TABLE IF NOT EXISTS user_metrics (
                    user_metric_id INTEGER PRIMARY KEY AUTOINCREMENT,
                    job_name TEXT,
                    metric_name TEXT,
                    metric_value TEXT,
                    modified TEXT
                );
                """
            )
            conn.commit()

    def store_metric(self, job_name: str, metric_name: str, metric_value: Any):
        """
        Store the metric value in the database. Will overwrite the value if it already exists.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                DELETE FROM user_metrics
                WHERE job_name = ? AND metric_name = ?;
                """,
                (job_name, metric_name),
            )
            conn.execute(
                """
                INSERT INTO user_metrics (job_name, metric_name, metric_value, modified)
                VALUES (?, ?, ?, ?);
                """,
                (
                    job_name,
                    metric_name,
                    str(metric_value),
                    datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
                ),
            )
            conn.commit()


class UserMetricProcessor:
    def __init__(self, as_conf: AutosubmitConfig, job: "Job"):
        self.as_conf = as_conf
        self.job = job
        self.user_metric_repository = UserMetricRepository(job.expid)

    def read_metrics_specs(self) -> List[MetricSpec]:
        try:
            raw_metrics: List[Dict[str, Any]] = self.as_conf.get_section(
                ["JOBS", self.job.section, "METRICS"]
            )

            # Normalize the parameters keys
            raw_metrics = [
                self.as_conf.normalize_parameters_keys(metric) for metric in raw_metrics
            ]
        except Exception:
            Log.debug(traceback.format_exc())
            raise ValueError("Invalid or missing metrics section")

        metrics_specs: List[MetricSpec] = []
        for raw_metric in raw_metrics:
            """
            Read the metrics specs of the job
            """
            try:
                spec = MetricSpec.load(raw_metric)
                metrics_specs.append(spec)
            except Exception:
                Log.warning("Invalid metric spec: {}", str(raw_metric))

        return metrics_specs

    def _group_metrics_by_path_selector_type(
        self,
        metrics_specs: List[MetricSpec],
    ) -> Dict[str, Dict[str, List[MetricSpec]]]:
        """
        Group all metrics by file path and selector type.
        First level key is the file path, second level key is the selector type.
        """
        metrics_by_path_selector_type: Dict[str, Dict[str, List[MetricSpec]]] = {}
        for metric_spec in metrics_specs:
            spec_path = str(
                Path(metric_spec.path).joinpath(self.job.name, metric_spec.filename)
            )

            # If the path is not in the dictionary, add it
            if spec_path not in metrics_by_path_selector_type:
                metrics_by_path_selector_type[spec_path] = {}

            # If the selector type is not in the dictionary, add it
            if (
                metric_spec.selector.type.value
                not in metrics_by_path_selector_type[spec_path]
            ):
                metrics_by_path_selector_type[spec_path][
                    metric_spec.selector.type.value
                ] = []

            metrics_by_path_selector_type[spec_path][
                metric_spec.selector.type.value
            ].append(metric_spec)

        return metrics_by_path_selector_type

    def store_metric(self, metric_name: str, metric_value: Any):
        """
        Store the metric value in the database
        """
        self.user_metric_repository.store_metric(
            self.job.name, metric_name, metric_value
        )

    def process_metrics_specs(self, metrics_specs: List[MetricSpec]):
        """
        Process the metrics specs of the job
        """

        metrics_by_path_selector_type = self._group_metrics_by_path_selector_type(
            metrics_specs
        )

        # For each file path, read the content of the file
        for path, metrics_by_selector_type in metrics_by_path_selector_type.items():
            # Read the file from remote platform, it will replace the decoding errors.
            try:
                content = self.job.platform.read_file(path, max_size=MAX_FILE_SIZE)
                Log.debug(f"Read file {path}")
                content = content.decode(errors="replace").strip()
            except Exception as exc:
                Log.debug(traceback.format_exc())
                Log.warning(f"Error reading metric file at {path}: {str(exc)}")
                continue

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
                            Log.warning(
                                f"Error processing JSON content in file {path} for metric {metric.name}"
                            )
                except json.JSONDecodeError:
                    Log.warning(f"Invalid JSON content in file {path}")
                except Exception:
                    Log.warning(f"Error processing JSON content in file {path}")
