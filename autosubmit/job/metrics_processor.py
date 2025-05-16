from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
import json
import copy
import locale
from pathlib import Path
import sqlite3
from typing import TYPE_CHECKING, Any, Dict, List, Optional
from autosubmitconfigparser.config.configcommon import AutosubmitConfig
from autosubmitconfigparser.config.basicconfig import BasicConfig
from log.log import Log

if TYPE_CHECKING:
    # Avoid circular imports
    from autosubmit.job.job import Job

# Default 16MB max file size
MAX_FILE_SIZE_MB = 16


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
    filename: str
    selector: MetricSpecSelector
    max_read_size_mb: int = MAX_FILE_SIZE_MB

    @staticmethod
    def load(data: Dict[str, Any]) -> "MetricSpec":
        if not isinstance(data, dict):
            raise ValueError("Invalid metric spec")

        if not data.get("NAME") or not data.get("FILENAME"):
            raise ValueError("Name and filename are required in metric spec")

        _name = data["NAME"]
        _filename = data["FILENAME"]

        _max_read_size = data.get("MAX_READ_SIZE_MB", MAX_FILE_SIZE_MB)

        _selector = data.get("SELECTOR", None)
        selector = MetricSpecSelector.load(_selector)

        return MetricSpec(
            name=_name,
            filename=_filename,
            max_read_size_mb=_max_read_size,
            selector=selector,
        )


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
                    run_id INTEGER,
                    job_name TEXT,
                    metric_name TEXT,
                    metric_value TEXT,
                    modified TEXT
                );
                """
            )
            conn.commit()

    def store_metric(
        self, run_id: int, job_name: str, metric_name: str, metric_value: Any
    ):
        """
        Store the metric value in the database. Will overwrite the value if it already exists.
        """
        with sqlite3.connect(self.db_path) as conn:
            conn.execute(
                """
                DELETE FROM user_metrics
                WHERE run_id = ? AND job_name = ? AND metric_name = ?;
                """,
                (run_id, job_name, metric_name),
            )
            conn.execute(
                """
                INSERT INTO user_metrics (run_id, job_name, metric_name, metric_value, modified)
                VALUES (?, ?, ?, ?, ?);
                """,
                (
                    run_id,
                    job_name,
                    metric_name,
                    str(metric_value),
                    datetime.now(tz=timezone.utc).isoformat(timespec="seconds"),
                ),
            )
            conn.commit()


class UserMetricProcessor:
    def __init__(
        self, as_conf: AutosubmitConfig, job: "Job", run_id: Optional[int] = None
    ):
        self.as_conf = as_conf
        self.job = job
        self.run_id = run_id
        self.user_metric_repository = UserMetricRepository(job.expid)
        self._processed_metrics = {}

    def read_metrics_specs(self) -> List[MetricSpec]:
        try:
            raw_metrics: List[Dict[str, Any]] = self.as_conf.get_section(
                ["JOBS", self.job.section, "METRICS"]
            )

            # Normalize the parameters keys
            raw_metrics = [
                self.as_conf.deep_normalize(metric) for metric in raw_metrics
            ]
        except Exception as exc:
            Log.printlog("Invalid or missing metrics section", code=6019)
            raise ValueError(f"Invalid or missing metrics section: {str(exc)}")

        metrics_specs: List[MetricSpec] = []
        for raw_metric in raw_metrics:
            """
            Read the metrics specs of the job
            """
            try:
                spec = MetricSpec.load(raw_metric)
                metrics_specs.append(spec)
            except Exception:
                Log.printlog(f"Invalid metric spec: {str(raw_metric)}", code=6019)

        return metrics_specs

    def store_metric(self, metric_name: str, metric_value: Any):
        """
        Store the metric value in the database
        """
        self.user_metric_repository.store_metric(
            self.run_id, self.job.name, metric_name, metric_value
        )
        self._processed_metrics[metric_name] = metric_value

    def get_metric_path(self, metric_spec: MetricSpec) -> str:
        """
        Get the path to the metric file
        """
        parameters = self.job.update_parameters(self.as_conf)
        metric_folder = parameters.get("CURRENT_METRIC_FOLDER")
        return str(Path(metric_folder).joinpath(metric_spec.filename))

    def process_metrics(self):
        """
        Process the metrics of the job
        """
        # Read the metrics specs from the config
        metrics_specs = self.read_metrics_specs()

        # Process the metrics specs
        for metric_spec in metrics_specs:
            # Path to the metric file
            spec_path = self.get_metric_path(metric_spec)

            # Read the file from remote platform, it will replace the decoding errors.
            try:
                content = self.job.platform.read_file(
                    spec_path, max_size=(metric_spec.max_read_size_mb * 1024 * 1024)
                )
                Log.debug(f"Read file {spec_path}")
                content = content.decode(
                    encoding=locale.getlocale()[1], errors="replace"
                ).strip()
            except Exception as exc:
                Log.printlog(
                    f"Error reading metric file at {spec_path}: {str(exc)}", code=6018
                )
                continue

            # Process the content based on the selector type
            if metric_spec.selector.type == MetricSpecSelectorType.TEXT:
                # Store the content as a metric
                self.store_metric(metric_spec.name, content)
            elif metric_spec.selector.type == MetricSpecSelectorType.JSON:
                # Parse the JSON content and store the metrics
                try:
                    json_content = json.loads(content)
                    # Get the value based on the key
                    key = metric_spec.selector.key
                    value = copy.deepcopy(json_content)
                    if key:
                        for k in key:
                            value = value[k]
                    self.store_metric(metric_spec.name, value)
                except Exception:
                    Log.printlog(
                        f"Error processing JSON content in file {spec_path}", code=6018
                    )
            else:
                Log.printlog(
                    f"Invalid Metric Spec: Unsupported selector type {metric_spec.selector.type} for metric {metric_spec.name}",
                    code=6019,
                )

        return self._processed_metrics
