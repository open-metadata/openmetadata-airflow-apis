#  Copyright 2021 Collate
#  Licensed under the Apache License, Version 2.0 (the "License");
#  you may not use this file except in compliance with the License.
#  You may obtain a copy of the License at
#  http://www.apache.org/licenses/LICENSE-2.0
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
#  limitations under the License.
"""
Metadata DAG common functions
"""
from typing import Any, Dict

from airflow import DAG

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

from metadata.generated.schema.operations.pipelines.airflowPipeline import (
    AirflowPipeline,
)
from metadata.ingestion.api.workflow import Workflow


def metadata_ingestion_workflow(workflow_config: Dict[str, Any]):
    """
    Task that creates and runs the ingestion workflow.

    The workflow_config gets cooked form the incoming
    airflow_pipeline.

    This will be wrapped in a PythonOperator
    """
    workflow = Workflow.create(workflow_config)
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()


def build_ingestion_dag(
    task_name: str, airflow_pipeline: AirflowPipeline, workflow_config: Dict[str, Any]
) -> DAG:
    """
    Build a simple metadata workflow DAG
    """

    with DAG(
        dag_id=airflow_pipeline.name,
        default_args=...,  # prepare common default
        description=airflow_pipeline.description,
        start_date=...,  # pick it up from airflow_pipeline and make sure it is properly in UTC
        is_paused_upon_creation=airflow_pipeline.airflowConfig.pausePipeline,
        catchup=airflow_pipeline.airflowConfig.pipelineCatchup or False,
    ) as dag:

        PythonOperator(
            task_id=task_name,
            python_callable=metadata_ingestion_workflow,
            op_kwargs={"workflow_config": workflow_config},
        )

        return dag
