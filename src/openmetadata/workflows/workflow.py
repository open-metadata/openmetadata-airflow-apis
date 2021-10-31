from datetime import date
from typing import Any, Dict, List, Optional

from pydantic import BaseModel


class TaskConfig(BaseModel):
    """OpenMetadata Airflow Task Configs"""
    bash_command: str = None
    python_callable_command: str = None
    python_callable_file: str = None


class Task(BaseModel):
    """OpenMetadata Airflow Task Definition"""
    task_id: str
    task_name: str
    task_class: str
    dependencies: List[str] = None
    task_config: str


class WorkflowConfig(BaseModel):
    """OpenMetadata Airflow DAG Definition"""
    dag_id: str
    dag_name: str
    dag_description: Optional[str] = None
    owner: str
    tags: List[str] = None
    email: Optional[str] = None
    auto_enable: bool = True
    start_date: date = None
    end_date: date = None
    retries: int = 1
    retry_delay_sec: int = 300
    concurrency: int = 1
    execution_timeout_secs: int = 600
    default_view: str = "tree"
    view_orientation: str = "LR"
    tasks: List[Task]
    task_groups: List




