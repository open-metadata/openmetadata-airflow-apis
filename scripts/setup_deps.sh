# Helper to spin up a standalone local airflow for testing purposes
set -ex

cd && python -m virtualenv airflow_venv && source airflow_venv/bin/activate

cd ~/projects/openmetadata-airflow-apis && make install
cd ~/projects/OpenMetadata && python -m pip install "./ingestion[airflow-container]"

cp -r ~/projects/openmetadata-airflow-apis/src/plugins ~/airflow/plugins/
cp -r ~/projects/openmetadata-airflow-apis/src/plugins/dag_templates ~/airflow/
mkdir -p ~/airflow/dag_generated_configs
cp -r ~/projects/openmetadata-airflow-apis/src/plugins/dag_managed_operators ~/airflow/

airflow standalone
