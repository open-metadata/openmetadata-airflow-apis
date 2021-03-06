# generated by datamodel-codegen:
#   filename:  api/workflows/operations/workflow.json
#   timestamp: 2021-11-02T17:34:54+00:00

from __future__ import annotations

from typing import Any, Dict, List, Optional, Union

from pydantic import BaseModel, Field, constr

from ..type import basic


class Task(BaseModel):
    name: constr(min_length=1, max_length=64) = Field(
        ..., description='Name that identifies this task instance uniquely.'
    )
    operator: str = Field(
        ..., description='Python classname of the operator used in this Task.'
    )
    dependencies: Optional[List[str]] = Field(
        None, description='All the tasks that are downstream of this task.'
    )
    config: Optional[Dict[str, Union[str, float, Dict[str, Any]]]] = Field(
        None, description='Task Config.'
    )


class WorkflowConfig(BaseModel):
    name: constr(min_length=1, max_length=256) = Field(
        ..., description='Name that identifies this workflow instance uniquely.'
    )
    forceDeploy: Optional[bool] = Field(
        'false',
        description='Deploy the workflow by overwriting existing workflow with the same name.',
    )
    pauseWorkflow: Optional[bool] = Field(
        'false',
        description='pause the workflow from running once the deploy is finished succesfully.',
    )
    description: Optional[str] = Field(None, description='Description of the workflow.')
    concurrency: Optional[int] = Field(1, description='Concurrency of the Pipeline.')
    startDate: str = Field(..., description='Start date of the workflow')
    endDate: Optional[str] = Field(None, description='End Date of the workflow')
    workflowTimezone: Optional[str] = Field(
        'UTC', description='Timezone in which workflow going to be scheduled.'
    )
    retries: Optional[int] = Field(1, description='Retry workflow in case of failure')
    retryDelay: Optional[int] = Field(
        300, description='Delay between retries in seconds.'
    )
    workflowCatchup: Optional[bool] = Field(
        'false', description='Workflow catchup for past executions.'
    )
    scheduleInterval: Optional[str] = Field(
        None, description='Scheduler Interval for the Workflow in cron format.'
    )
    maxActiveRuns: Optional[int] = Field(
        1, description='Maximum Number of active runs.'
    )
    workflowTimeout: Optional[int] = Field(
        60, description='Timeout for the workflow in seconds.'
    )
    workflowDefaultView: Optional[str] = Field(
        'tree', description='Default view in Airflow.'
    )
    workflowDefaultViewOrientation: Optional[str] = Field(
        'LR', description='Default view Orientation in Airflow.'
    )
    pythonOperatorLocation: Optional[str] = Field(
        None,
        description='File system directory path where managed python operator files are stored.',
    )
    slaMissCallback: Optional[str] = Field(
        None, description='python method call back on SLA miss.'
    )
    onSuccessCallback: Optional[str] = Field(
        None, description='callback method on successful execution of the pipeline.'
    )
    onFailureCallback: Optional[str] = Field(
        None, description='callback method on pipeline execution failure.'
    )
    onSuccessCallbackName: Optional[str] = Field(
        None,
        description='python method name to call when the pipeline successfully executes.',
    )
    onSuccessCallbackFile: Optional[str] = Field(
        None, description='python file where the successful callback method is stored.'
    )
    onFailureCallbackName: Optional[str] = Field(
        None,
        description='python method name to call when the pipeline failed executes.',
    )
    onFailureCallbackFile: Optional[str] = Field(
        None, description='python file where the failure callback method is stored'
    )
    tasks: List[Task] = Field(
        ..., description='All the tasks that are part of pipeline.'
    )
    owner: str = Field(..., description='Owner of this database')
    email: Optional[basic.Email] = Field(
        None, description='Email to notify workflow status.'
    )
