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
