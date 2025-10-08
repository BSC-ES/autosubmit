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

import copy
import json
import locale
from dataclasses import dataclass
from datetime import datetime, timezone
from enum import Enum
from pathlib import Path
from typing import TYPE_CHECKING, Any, Optional

from sqlalchemy.schema import CreateTable, CreateSchema

from autosubmit.config.basicconfig import BasicConfig
from autosubmit.config.configcommon import AutosubmitConfig
from autosubmit.database import session, tables
from autosubmit.log.log import Log

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
    key: Optional[list[str]]

    @staticmethod
    def load(data: Optional[dict[str, Any]]) -> "MetricSpecSelector":
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
    def load(data: dict[str, Any]) -> "MetricSpec":
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
        self.expid = expid

        if BasicConfig.DATABASE_BACKEND == "postgres":
            # Postgres backend
            self.connection_url = BasicConfig.DATABASE_CONN_URL
            self.schema = self.expid
        else:
            # SQLite backend
            exp_path = Path(BasicConfig.LOCAL_ROOT_DIR).joinpath(expid)
            tmp_path = Path(exp_path).joinpath(BasicConfig.LOCAL_TMP_DIR)
            db_path = tmp_path.joinpath(f"metrics_{expid}.db")
            self.connection_url = f"sqlite:///{db_path}"
            self.schema = None

        self.table = tables.get_table_from_name(
            schema=self.schema, table_name="user_metrics"
        )
        self.engine = session.create_engine(self.connection_url)

        with self.engine.connect() as conn:
            if self.schema:
                conn.execute(CreateSchema(self.schema, if_not_exists=True))
            conn.execute(CreateTable(self.table, if_not_exists=True))
            conn.commit()

    def store_metric(
        self, run_id: int, job_name: str, metric_name: str, metric_value: Any
    ):
        """
        Store the metric value in the database. Will overwrite the value if it already exists.
        """
        with self.engine.connect() as conn:
            # Delete the existing metric
            conn.execute(
                self.table.delete().where(
                    self.table.c.run_id == run_id,
                    self.table.c.job_name == job_name,
                    self.table.c.metric_name == metric_name,
                )
            )

            # Insert the new metric
            conn.execute(
                self.table.insert().values(
                    run_id=run_id,
                    job_name=job_name,
                    metric_name=metric_name,
                    metric_value=str(metric_value),
                    modified=datetime.now(tz=timezone.utc).isoformat(
                        timespec="seconds"
                    ),
                )
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

    def read_metrics_specs(self) -> list[MetricSpec]:
        try:
            # TODO: Remove ignored type once AS config parser has been moved back and types have been added there.
            raw_metrics = self.as_conf.get_section(  # type: ignore
                ["JOBS", self.job.section, "METRICS"]
            )

            # Normalize the parameters keys
            raw_metrics: list[dict[str, Any]] = [
                self.as_conf.deep_normalize(metric) for metric in raw_metrics
            ]
        except Exception as exc:
            Log.printlog("Invalid or missing metrics section", code=6019)
            raise ValueError(f"Invalid or missing metrics section: {str(exc)}")

        metrics_specs: list[MetricSpec] = []
        for raw_metric in raw_metrics:
            """
            Read the metrics specs of the job
            """
            try:
                spec = MetricSpec.load(raw_metric)
                metrics_specs.append(spec)
            except Exception as e:
                Log.printlog(f"Invalid metric spec: {str(raw_metric)}: {str(e)}", code=6019)

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
                except Exception as e:
                    Log.printlog(
                        f"Error processing JSON content in file {spec_path}: {str(e)}", code=6018
                    )
            else:
                Log.printlog(
                    f"Invalid Metric Spec: Unsupported selector type {metric_spec.selector.type} "
                    f"for metric {metric_spec.name}", code=6019,
                )

        return self._processed_metrics
