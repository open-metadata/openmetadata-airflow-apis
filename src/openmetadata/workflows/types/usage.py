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
Metadata DAG function builder
"""

import logging
from typing import Any, Dict

from airflow import DAG

from openmetadata.workflows.types.common import metadata_ingestion_workflow

try:
    from airflow.operators.python import PythonOperator
except ModuleNotFoundError:
    from airflow.operators.python_operator import PythonOperator

from metadata.generated.schema.operations.pipelines.airflowPipeline import (
    AirflowPipeline,
)


def build_usage_workflow_config(airflow_pipeline: AirflowPipeline) -> Dict[str, Any]:
    """
    Given an airflow_pipeline, prepare the workflow config JSON
    """
    ...


def build_usage_dag(airflow_pipeline: AirflowPipeline) -> DAG:
    """
    Build a simple metadata workflow DAG
    """
    logging.info(f"Building metadata dag {airflow_pipeline.name}")

    with DAG(
        dag_id=airflow_pipeline.name,
        default_args=...,  # prepare common default
        description=airflow_pipeline.description,
        start_date=...,  # pick it up from airflow_pipeline and make sure it is properly in UTC
        is_paused_upon_creation=airflow_pipeline.airflowConfig.pausePipeline,
        catchup=airflow_pipeline.airflowConfig.pipelineCatchup or False,
    ) as dag:
        workflow_config = build_usage_workflow_config(airflow_pipeline)

        PythonOperator(
            task_id="metadata_usage",
            python_callable=metadata_ingestion_workflow,
            op_kwargs={"workflow_config": workflow_config},
        )

        return dag
