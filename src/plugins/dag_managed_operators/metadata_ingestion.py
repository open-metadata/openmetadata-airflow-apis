#!/usr/bin/env python
from metadata.ingestion.api.workflow import Workflow

def metadata_ingestion_workflow(**kwargs):
    workflow = Workflow.create(kwargs['workflow_config'])
    workflow.execute()
    workflow.raise_from_status()
    workflow.print_status()
    workflow.stop()
