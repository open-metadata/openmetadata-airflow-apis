import logging
import pathlib
from typing import Any, Dict, List

import yaml
from airflow.models import DAG

from openmetadata.generated.api.workflows.operations.workflow import WorkflowConfig

# these are params that cannot be a dag name
from openmetadata.workflows.config import load_config_file
from openmetadata.workflows.workflow_builder import WorkflowBuilder

SYSTEM_PARAMS: List[str] = ["default", "task_groups"]
logger = logging.getLogger(__name__)


class WorkflowFactory:
    """
    :param config: workflow config dictionary.
    :type config: dict
    """

    def __init__(self, workflow_config: WorkflowConfig) -> None:
        self.dag = None
        self.workflow_config = workflow_config

    @classmethod
    def create(cls, config: str):
        config_file = pathlib.Path(config)
        workflow_config_dict = load_config_file(config_file)
        workflow_config = WorkflowConfig(**workflow_config_dict)
        return cls(workflow_config)

    def build_dag(self) -> DAG:
        """Build Workflow using the configuration"""

        workflow_builder: WorkflowBuilder = WorkflowBuilder(self.workflow_config)
        try:
            workflow = workflow_builder.build()
        except Exception as err:
            raise Exception(
                f"Failed to generate workflow {self.workflow_config.name}. verify config is correct"
            ) from err
        return workflow

    def register_dag(self, dag: DAG, globals: Dict[str, Any]) -> None:
        globals[dag.dag_id]: DAG = dag

    def generate_dag(self, globals: Dict[str, Any]) -> None:
        dag = self.build_dag()
        self.dag = dag
        self.register_dag(dag, globals)
        logger.info("registered the dag")

    def get_dag(self) -> DAG:
        return self.dag
