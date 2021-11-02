import logging
from datetime import timedelta, datetime
from typing import Any, Callable, Dict, List, Union

import os
from copy import deepcopy

from airflow import DAG, configuration
from airflow.models import Variable
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import BaseOperator
from airflow.operators.python_operator import PythonOperator, BranchPythonOperator
from airflow.sensors.http_sensor import HttpSensor
from airflow.sensors.sql_sensor import SqlSensor
from airflow.utils.module_loading import import_string
from airflow import __version__ as AIRFLOW_VERSION

from openmetadata.generated.api.workflows.operations.workflow import WorkflowConfig, Task

try:
    from airflow.kubernetes.secret import Secret
    from airflow.kubernetes.pod import Port
    from airflow.kubernetes.volume_mount import VolumeMount
    from airflow.kubernetes.volume import Volume
    from airflow.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv
except ImportError:
    from airflow.contrib.kubernetes.secret import Secret
    from airflow.contrib.kubernetes.pod import Port
    from airflow.contrib.kubernetes.volume_mount import VolumeMount
    from airflow.contrib.kubernetes.volume import Volume
    from airflow.contrib.kubernetes.pod_runtime_info_env import PodRuntimeInfoEnv

from kubernetes.client.models import V1Pod, V1Container
from packaging import version

from openmetadata.workflows import workflow_utils

# pylint: disable=ungrouped-imports,invalid-name
# Disabling pylint's ungrouped-imports warning because this is a
# conditional import and cannot be done within the import group above
# TaskGroup is introduced in Airflow 2.0.0
if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
    from airflow.utils.task_group import TaskGroup
else:
    TaskGroup = None
# pylint: disable=ungrouped-imports,invalid-name

# these are params only used in the DAG factory, not in the tasks
SYSTEM_PARAMS: List[str] = ["operator", "dependencies", "task_group_name"]
logger = logging.getLogger(__name__)


class WorkflowBuilder:
    """
    Generates tasks and a DAG from a config.
    :param workflow_config:  configuration for the DAG
    """

    def __init__(
            self, workflow_config: WorkflowConfig
    ) -> None:
        self.dag_config = workflow_config
        self.dag_name: str = self.dag_config.name

    # pylint: disable=too-many-branches
    def get_dag_params(self) -> Dict[str, Any]:
        """
        Merges default config with dag config, sets dag_id, and extropolates dag_start_date

        :returns: dict of dag parameters
        """

        dag_params = {}
        dag_params["dag_id"]: str = self.dag_config.name
        dag_params["description"]: str = self.dag_config.description
        dag_params["retries"]: int = self.dag_config.retries
        dag_params["max_active_runs"]: int = self.dag_config.maxActiveRuns
        dag_params["owner"]: str = self.dag_config.owner
        dag_params["concurrency"] = self.dag_config.concurrency
        dag_params["max_active_runs"] = self.dag_config.maxActiveRuns
        dag_params["default_view"] = self.dag_config.workflowDefaultView
        dag_params["orientation"] = self.dag_config.workflowDefaultViewOrientation
        dag_params["default_args"] = {}
        if self.dag_config.scheduleInterval is None:
            dag_params["schedule_interval"] = None
        else:
            dag_params["schedule_interval"] = self.dag_config.scheduleInterval

        if self.dag_config.workflowTimeout is not None:
            dag_params["dagrun_timeout"]: timedelta = timedelta(
                seconds=self.dag_config.workflowTimeout
            )

        # Convert from 'end_date: Union[str, datetime, date]' to 'end_date: datetime'
        if self.dag_config.endDate is not None:
            dag_params["default_args"]["end_date"]: datetime = workflow_utils.get_datetime(
                date_value=self.dag_config.endDate,
                timezone=self.dag_config.workflowTimezone,
            )

        if self.dag_config.retryDelay is not None:
            dag_params["default_args"]["retry_delay"]: timedelta = timedelta(
                seconds=self.dag_config.retryDelay
            )

        if self.dag_config.slaMissCallback is not None:
            if isinstance(self.dag_config.slaMissCallback, str):
                dag_params["default_args"][
                    "sla_miss_callback"
                ]: Callable = import_string(
                    self.dag_config.slaMissCallback
                )
                dag_params["sla_miss_callback"]: Callable = import_string(
                    self.dag_config.slaMissCallback
                )

        if self.dag_config.onSuccessCallback is not None:
            if isinstance(self.dag_config.onSuccessCallback, str):
                dag_params["default_args"][
                    "on_success_callback"
                ]: Callable = import_string(
                    self.dag_config.onSuccessCallback
                )
                dag_params["on_success_callback"]: Callable = import_string(
                    self.dag_config.onSuccessCallback
                )

        if self.dag_config.onFailureCallback is not None:
            if isinstance(self.dag_config.onFailureCallback, str):
                dag_params["default_args"][
                    "on_failure_callback"
                ]: Callable = import_string(
                    self.dag_config.onFailureCallback
                )
                dag_params["on_failure_callback"]: Callable = import_string(
                    self.dag_config.onFailureCallback
                )

        if self.dag_config.onSuccessCallbackName and self.dag_config.onSuccessCallbackFile:
            dag_params["on_success_callback"]: Callable = workflow_utils.get_python_callable(
                self.dag_config.onSuccessCallbackName,
                self.dag_config.onSuccessCallbackFile,
            )

        if self.dag_config.onFailureCallbackName and self.dag_config.onFailureCallbackFile:
            dag_params["on_failure_callback"]: Callable = workflow_utils.get_python_callable(
                self.dag_config.onFailureCallbackName,
                self.dag_config.onFailureCallbackFile,
            )

        try:
            dag_params["default_args"]["start_date"]: datetime = workflow_utils.get_datetime(
                date_value=self.dag_config.startDate,
                timezone=self.dag_config.workflowTimezone,
            )
        except KeyError as err:
            raise Exception(f"{self.dag_name} config is missing start_date") from err
        return dag_params

    # pylint: disable=too-many-branches
    # pylint: disable=too-many-statements
    @staticmethod
    def make_task(operator: str, task_params: Dict[str, Any]) -> BaseOperator:
        """
        Takes an operator and params and creates an instance of that operator.
        :returns: instance of operator object
        """
        try:
            # class is a Callable https://stackoverflow.com/a/34578836/3679900
            operator_obj: Callable[..., BaseOperator] = import_string(operator)
        except Exception as err:
            raise Exception(f"Failed to import operator: {operator}") from err
        try:
            if operator_obj in [PythonOperator, BranchPythonOperator]:
                if not task_params.get("python_callable_name") and not task_params.get(
                        "python_callable_file"
                ):
                    raise Exception(
                        "Failed to create task. PythonOperator and BranchPythonOperator requires \
                        `python_callable_name` and `python_callable_file` parameters."
                    )
                task_params["python_callable"]: Callable = workflow_utils.get_python_callable(
                    task_params["python_callable_name"],
                    task_params["python_callable_file"],
                )
                # remove dag-factory specific parameters
                # Airflow 2.0 doesn't allow these to be passed to operator
                del task_params["python_callable_name"]
                del task_params["python_callable_file"]

            # Check for the custom success and failure callables in SqlSensor. These are considered
            # optional, so no failures in case they aren't found. Note: there's no reason to
            # declare both a callable file and a lambda function for success/failure parameter.
            # If both are found the object will not throw and error, instead callable file will
            # take precedence over the lambda function
            if operator_obj in [SqlSensor]:
                # Success checks
                if task_params.get("success_check_file") and task_params.get(
                        "success_check_name"
                ):
                    task_params["success"]: Callable = workflow_utils.get_python_callable(
                        task_params["success_check_name"],
                        task_params["success_check_file"],
                    )
                    del task_params["success_check_name"]
                    del task_params["success_check_file"]
                elif task_params.get("success_check_lambda"):
                    task_params["success"]: Callable = workflow_utils.get_python_callable_lambda(
                        task_params["success_check_lambda"]
                    )
                    del task_params["success_check_lambda"]
                # Failure checks
                if task_params.get("failure_check_file") and task_params.get(
                        "failure_check_name"
                ):
                    task_params["failure"]: Callable = workflow_utils.get_python_callable(
                        task_params["failure_check_name"],
                        task_params["failure_check_file"],
                    )
                    del task_params["failure_check_name"]
                    del task_params["failure_check_file"]
                elif task_params.get("failure_check_lambda"):
                    task_params["failure"]: Callable = workflow_utils.get_python_callable_lambda(
                        task_params["failure_check_lambda"]
                    )
                    del task_params["failure_check_lambda"]

            if operator_obj in [HttpSensor]:
                if not (
                        task_params.get("response_check_name")
                        and task_params.get("response_check_file")
                ) and not task_params.get("response_check_lambda"):
                    raise Exception(
                        "Failed to create task. HttpSensor requires \
                        `response_check_name` and `response_check_file` parameters \
                        or `response_check_lambda` parameter."
                    )
                if task_params.get("response_check_file"):
                    task_params["response_check"]: Callable = workflow_utils.get_python_callable(
                        task_params["response_check_name"],
                        task_params["response_check_file"],
                    )
                    # remove dag-factory specific parameters
                    # Airflow 2.0 doesn't allow these to be passed to operator
                    del task_params["response_check_name"]
                    del task_params["response_check_file"]
                else:
                    task_params[
                        "response_check"
                    ]: Callable = workflow_utils.get_python_callable_lambda(
                        task_params["response_check_lambda"]
                    )
                    # remove dag-factory specific parameters
                    # Airflow 2.0 doesn't allow these to be passed to operator
                    del task_params["response_check_lambda"]

            # KubernetesPodOperator
            if operator_obj == KubernetesPodOperator:
                task_params["secrets"] = (
                    [Secret(**v) for v in task_params.get("secrets")]
                    if task_params.get("secrets") is not None
                    else None
                )

                task_params["ports"] = (
                    [Port(**v) for v in task_params.get("ports")]
                    if task_params.get("ports") is not None
                    else None
                )
                task_params["volume_mounts"] = (
                    [VolumeMount(**v) for v in task_params.get("volume_mounts")]
                    if task_params.get("volume_mounts") is not None
                    else None
                )
                task_params["volumes"] = (
                    [Volume(**v) for v in task_params.get("volumes")]
                    if task_params.get("volumes") is not None
                    else None
                )
                task_params["pod_runtime_info_envs"] = (
                    [
                        PodRuntimeInfoEnv(**v)
                        for v in task_params.get("pod_runtime_info_envs")
                    ]
                    if task_params.get("pod_runtime_info_envs") is not None
                    else None
                )
                task_params["full_pod_spec"] = (
                    V1Pod(**task_params.get("full_pod_spec"))
                    if task_params.get("full_pod_spec") is not None
                    else None
                )
                task_params["init_containers"] = (
                    [V1Container(**v) for v in task_params.get("init_containers")]
                    if task_params.get("init_containers") is not None
                    else None
                )

            if workflow_utils.check_dict_key(task_params, "execution_timeout_secs"):
                task_params["execution_timeout"]: timedelta = timedelta(
                    seconds=task_params["execution_timeout_secs"]
                )
                del task_params["execution_timeout_secs"]

            if workflow_utils.check_dict_key(task_params, "sla_secs"):
                task_params["sla"]: timedelta = timedelta(
                    seconds=task_params["sla_secs"]
                )
                del task_params["sla_secs"]

            if workflow_utils.check_dict_key(task_params, "execution_delta_secs"):
                task_params["execution_delta"]: timedelta = timedelta(
                    seconds=task_params["execution_delta_secs"]
                )
                del task_params["execution_delta_secs"]

            if workflow_utils.check_dict_key(
                    task_params, "execution_date_fn_name"
            ) and workflow_utils.check_dict_key(task_params, "execution_date_fn_file"):
                task_params["execution_date_fn"]: Callable = workflow_utils.get_python_callable(
                    task_params["execution_date_fn_name"],
                    task_params["execution_date_fn_file"],
                )
                del task_params["execution_date_fn_name"]
                del task_params["execution_date_fn_file"]

            # on_execute_callback is an Airflow 2.0 feature
            if workflow_utils.check_dict_key(
                    task_params, "on_execute_callback"
            ) and version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
                task_params["on_execute_callback"]: Callable = import_string(
                    task_params["on_execute_callback"]
                )

            if workflow_utils.check_dict_key(task_params, "on_failure_callback"):
                task_params["on_failure_callback"]: Callable = import_string(
                    task_params["on_failure_callback"]
                )

            if workflow_utils.check_dict_key(task_params, "on_success_callback"):
                task_params["on_success_callback"]: Callable = import_string(
                    task_params["on_success_callback"]
                )

            if workflow_utils.check_dict_key(task_params, "on_retry_callback"):
                task_params["on_retry_callback"]: Callable = import_string(
                    task_params["on_retry_callback"]
                )

            # use variables as arguments on operator
            if workflow_utils.check_dict_key(task_params, "variables_as_arguments"):
                variables: List[Dict[str, str]] = task_params.get(
                    "variables_as_arguments"
                )
                for variable in variables:
                    if Variable.get(variable["variable"], default_var=None) is not None:
                        task_params[variable["attribute"]] = Variable.get(
                            variable["variable"], default_var=None
                        )
                del task_params["variables_as_arguments"]

            task: BaseOperator = operator_obj(**task_params)
        except Exception as err:
            raise Exception(f"Failed to create {operator_obj} task") from err
        return task

    @staticmethod
    def make_task_groups(
            task_groups: Dict[str, Any], dag: DAG
    ) -> Dict[str, "TaskGroup"]:
        """Takes a DAG and task group configurations. Creates TaskGroup instances.

        :param task_groups: Task group configuration from the YAML configuration file.
        :param dag: DAG instance that task groups to be added.
        """
        task_groups_dict: Dict[str, "TaskGroup"] = {}
        if version.parse(AIRFLOW_VERSION) >= version.parse("2.0.0"):
            for task_group_name, task_group_conf in task_groups.items():
                task_group_conf["group_id"] = task_group_name
                task_group_conf["dag"] = dag
                task_group = TaskGroup(
                    **{
                        k: v
                        for k, v in task_group_conf.items()
                        if k not in SYSTEM_PARAMS
                    }
                )
                task_groups_dict[task_group.group_id] = task_group
        return task_groups_dict

    @staticmethod
    def set_dependencies(
            tasks_config: List[Task],
            operators_dict: Dict[str, BaseOperator],
            task_groups_config: Dict[str, Dict[str, Any]],
            task_groups_dict: Dict[str, "TaskGroup"],
    ):
        """Take the task configurations in YAML file and operator
        instances, then set the dependencies between tasks.

        :param tasks_config: Raw task configuration
        :param operators_dict: Dictionary for operator instances
        :param task_groups_config: Raw task group configuration
        :param task_groups_dict: Dictionary for task group instances
        """
        tasks_and_task_groups_config = {**tasks_config, **task_groups_config}
        tasks_and_task_groups_instances = {**operators_dict, **task_groups_dict}
        for name, conf in tasks_and_task_groups_config.items():
            # if task is in a task group, group_id is prepended to its name
            if conf.get("task_group"):
                group_id = conf["task_group"].group_id
                name = f"{group_id}.{name}"
            if conf.get("dependencies"):
                source: Union[
                    BaseOperator, "TaskGroup"
                ] = tasks_and_task_groups_instances[name]
                for dep in conf["dependencies"]:
                    if tasks_and_task_groups_config[dep].get("task_group"):
                        group_id = tasks_and_task_groups_config[dep][
                            "task_group"
                        ].group_id
                        dep = f"{group_id}.{dep}"
                    dep: Union[
                        BaseOperator, "TaskGroup"
                    ] = tasks_and_task_groups_instances[dep]
                    source.set_upstream(dep)

    def build(self) -> DAG:
        """
        Generates a DAG from the DAG parameters.

        :returns: dict with dag_id and DAG object
        :type: Dict[str, Union[str, DAG]]
        """
        dag_params: Dict[str, Any] = self.get_dag_params()
        dag_kwargs: Dict[str, Any] = {}

        dag_kwargs["dag_id"] = dag_params["dag_id"]

        dag_kwargs["schedule_interval"] = dag_params.get(
            "schedule_interval", timedelta(days=1)
        )

        if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.11"):
            dag_kwargs["description"] = dag_params.get("description", None)
        else:
            dag_kwargs["description"] = dag_params.get("description", "")

        if version.parse(AIRFLOW_VERSION) >= version.parse("2.2.0"):
            dag_kwargs["max_active_tasks"] = dag_params.get(
                "concurrency",
                configuration.conf.getint("core", "max_active_tasks_per_dag"),
            )
        else:
            dag_kwargs["concurrency"] = dag_params.get(
                "concurrency", configuration.conf.getint("core", "dag_concurrency")
            )

        dag_kwargs["catchup"] = dag_params.get(
            "catchup", configuration.conf.getboolean("scheduler", "catchup_by_default")
        )

        dag_kwargs["max_active_runs"] = dag_params.get(
            "max_active_runs",
            configuration.conf.getint("core", "max_active_runs_per_dag"),
        )

        dag_kwargs["dagrun_timeout"] = dag_params.get("dagrun_timeout", None)

        dag_kwargs["default_view"] = dag_params.get(
            "default_view", configuration.conf.get("webserver", "dag_default_view")
        )

        dag_kwargs["orientation"] = dag_params.get(
            "orientation", configuration.conf.get("webserver", "dag_orientation")
        )

        dag_kwargs["sla_miss_callback"] = dag_params.get("sla_miss_callback", None)
        dag_kwargs["on_success_callback"] = dag_params.get("on_success_callback", None)
        dag_kwargs["on_failure_callback"] = dag_params.get("on_failure_callback", None)
        dag_kwargs["default_args"] = dag_params.get("default_args", None)
        dag_kwargs["doc_md"] = dag_params.get("doc_md", None)

        dag: DAG = DAG(**dag_kwargs)

        # tags parameter introduced in Airflow 1.10.8
        if version.parse(AIRFLOW_VERSION) >= version.parse("1.10.8"):
            dag.tags = dag_params.get("tags", None)


        task_groups_dict: Dict[str, "TaskGroup"] = self.make_task_groups(
            dag_params.get("task_groups", {}), dag
        )

        # create dictionary to track tasks and set dependencies
        tasks_dict: Dict[str, BaseOperator] = {}
        for task_c in self.dag_config.tasks:
            task_conf = {"task_id": task_c.name}
            operator: str = task_c.operator
            task_conf["dag"]: DAG = dag

            # add task to task_group
            if task_groups_dict and task_conf.get("task_group_name"):
                task_conf["task_group"] = task_groups_dict[
                    task_conf.get("task_group_name")
                ]
            params: Dict[str, Any] = {
                k: v for k, v in task_c.config.items() if k not in SYSTEM_PARAMS
            }
            params["task_id"] = task_c.name
            params["dag"]: DAG = dag
            logger.info(params)
            task: BaseOperator = WorkflowBuilder.make_task(
                operator=operator, task_params=params
            )
            tasks_dict[task.task_id]: BaseOperator = task

        # set task dependencies after creating tasks
        #self.set_dependencies(
         #   self.dag_config.tasks, tasks_dict, dag_params.get("task_groups", {}), task_groups_dict
        #)

        return dag
