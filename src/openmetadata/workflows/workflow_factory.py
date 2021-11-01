import os
import pathlib
from typing import Any, Dict, Optional, Union, List

import yaml
from airflow.models import DAG

from openmetadata.workflows.workflow_builder import WorkflowBuilder

# these are params that cannot be a dag name
from openmetadata.workflows.config import load_config_file

SYSTEM_PARAMS: List[str] = ["default", "task_groups"]


class WorkflowFactory:
    """
    :param config: workflow config dictionary.
    :type config: dict
    """

    def __init__(
            self, config: dict
    ) -> None:
        self.config = config
        self.dags = {}

    @classmethod
    def create(cls, config: str):
        config_file = pathlib.Path(config)
        workflow_config_dict = load_config_file(config_file)
        return cls(workflow_config_dict)

    def get_dag_configs(self) -> Dict[str, Dict[str, Any]]:
        """
        Returns configuration for each the DAG in factory
        :returns: dict with configuration for dags
        """
        return {
            dag: self.config[dag]
            for dag in self.config.keys()
            if dag not in SYSTEM_PARAMS
        }

    def get_default_config(self) -> Dict[str, Any]:
        """
        Returns defaults for the DAG factory. If no defaults exist, returns empty dict.
        :returns: dict with default configuration
        """
        return self.config.get("default", {})

    def build_dags(self) -> Dict[str, DAG]:
        """Build Workflow using the configuration"""

        dag_configs: Dict[str, Dict[str, Any]] = self.get_dag_configs()
        default_config: Dict[str, Any] = self.get_default_config()

        dags: Dict[str, Any] = {}

        for dag_name, dag_config in dag_configs.items():
            dag_config["task_groups"] = dag_config.get("task_groups", {})
            dag_builder: WorkflowBuilder = WorkflowBuilder(
                dag_name=dag_name,
                dag_config=dag_config,
                default_config=default_config,
            )
            try:
                dag: Dict[str, Union[str, DAG]] = dag_builder.build()
                dags[dag["dag_id"]]: DAG = dag["dag"]
            except Exception as err:
                raise Exception(
                    f"Failed to generate dag {dag_name}. verify config is correct"
                ) from err

        return dags

    @staticmethod
    def register_dags(dags: Dict[str, DAG], globals: Dict[str, Any]) -> None:
        for dag_id, dag in dags.items():
            globals[dag_id]: DAG = dag

    def generate_dags(self, globals: Dict[str, Any]) -> None:
        dags: Dict[str, Any] = self.build_dags()
        self.dags = dags
        self.register_dags(dags, globals)

    def get_dag(self, dag_id) -> DAG:
        return self.dags[dag_id] if dag_id in self.dags.keys() else None

    # pylint: enable=redefined-builtin
