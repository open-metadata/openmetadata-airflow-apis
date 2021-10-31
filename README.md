# OpenMetadata Airflow Managed DAGS Api

This is a plugin for Apache Airflow >= 1.10 and Airflow >=2.x that exposes REST APIs to deploy a workflow definition and manage DAGS and tasks.


## Requirements
Install following packages in your scheduler and webserver python env.

```
pip install openmetadata-airflow-apis
```

## Configuration
Add the following section to airflow.cfg

```
[openmetadata_airflow_apis]
dag_runner_template = {AIRFLOW_HOME}/dag_templates/dag_runner.j2
dag_generated_configs = {AIRFLOW_HOME}/dag_generated_configs
dag_managed_operators = {AIRFLOW_HOME}/dag_managed_operators
```
substitute AIRFLOW_HOME with your airflow installation home




## Deploy 

1. Download the latest release
2. Create plugins folder in your scheduler and webserver if its not exist already
3. cp -r src/plugins/* ${AIRFLOW_HOME}/plugins
4. cp -r src/plugins/dag_templates {AIRFLOW_HOME}
5. mkdir -p {AIRFLOW_HOME}/dag_generated_configs
6. cp -r src/plugins/dag_managed_operators {AIRFLOW_HOME}
7. (re)start the airflow webserver and scheduler

 ```
 airflow webserver
 airflow scheduler
 ```
 
 
## APIs

#### Enable JWT Auth tokens
Plugin enables JWT Token based authentication for Airflow versions 1.10.4 or higher when RBAC support is enabled.

##### Generating the JWT access token

```bash
curl -XPOST http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/api/v1/security/login -H "Content-Type: application/json" -d '{"username":"username", "password":"password", "refresh":true, "provider": "db"}'
```
##### Examples:

```bash
curl -X POST http://localhost:8080/api/v1/security/login -H "Content-Type: application/json" -d '{"username":"admin", "password":"admin", "refresh":true, "provider": "db"}'
```

##### Sample response which includes access_token and refresh_token.
```json
{
 "access_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MDQyMTc4MzgsIm5iZiI6MTYwNDIxNzgzOCwianRpIjoiMTI4ZDE2OGQtMTZiOC00NzU0LWJiY2EtMTEyN2E2ZTNmZWRlIiwiZXhwIjoxNjA0MjE4NzM4LCJpZGVudGl0eSI6MSwiZnJlc2giOnRydWUsInR5cGUiOiJhY2Nlc3MifQ.xSWIE4lR-_0Qcu58OiSy-X0XBxuCd_59ic-9TB7cP9Y",
 "refresh_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MDQyMTc4MzgsIm5iZiI6MTYwNDIxNzgzOCwianRpIjoiZjA5NTNkODEtNWY4Ni00YjY0LThkMzAtYzg5NTYzMmFkMTkyIiwiZXhwIjoxNjA2ODA5ODM4LCJpZGVudGl0eSI6MSwidHlwZSI6InJlZnJlc2gifQ.VsiRr8_ulCoQ-3eAbcFz4dQm-y6732QR6OmYXsy4HLk"
}
```
By default, JWT access token is valid for 15 mins and refresh token is valid for 30 days. You can renew the access token with the help of refresh token as shown below.

##### Renewing the Access Token
```bash
curl -X POST "http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/api/v1/security/refresh" -H 'Authorization: Bearer <refresh_token>'
```
##### Examples:
```bash
curl -X POST "http://localhost:8080/api/v1/security/refresh" -H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MDQyMTc4MzgsIm5iZiI6MTYwNDIxNzgzOCwianRpIjoiZjA5NTNkODEtNWY4Ni00YjY0LThkMzAtYzg5NTYzMmFkMTkyIiwiZXhwIjoxNjA2ODA5ODM4LCJpZGVudGl0eSI6MSwidHlwZSI6InJlZnJlc2gifQ.VsiRr8_ulCoQ-3eAbcFz4dQm-y6732QR6OmYXsy4HLk'
```
##### sample response returns the renewed access token as shown below.
```json
{
 "access_token":"eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MDQyODQ2OTksIm5iZiI6MTYwNDI4NDY5OSwianRpIjoiZDhhN2IzMmYtMWE5Zi00Y2E5LWFhM2ItNDEwMmU3ZmMyMzliIiwiZXhwIjoxNjA0Mjg1NTk5LCJpZGVudGl0eSI6MSwiZnJlc2giOmZhbHNlLCJ0eXBlIjoiYWNjZXNzIn0.qY2e-bNSgOY-YboinOoGqLfKX9aQkdRjo025mZwBadA"
}
```


#### Enable API request with JWT
##### If the Authorization header is not added in the api request，response error:
```json
{"msg":"Missing Authorization Header"}
```
##### Pass the additional Authorization:Bearer <access_token> header in the rest API request.
Examples:
```bash
curl -X GET -H 'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MDQyODQ2OTksIm5iZiI6MTYwNDI4NDY5OSwianRpIjoiZDhhN2IzMmYtMWE5Zi00Y2E5LWFhM2ItNDEwMmU3ZmMyMzliIiwiZXhwIjoxNjA0Mjg1NTk5LCJpZGVudGl0eSI6MSwiZnJlc2giOmZhbHNlLCJ0eXBlIjoiYWNjZXNzIn0.qY2e-bNSgOY-YboinOoGqLfKX9aQkdRjo025mZwBadA' http://localhost:8080/rest_api/api\?api\=dag_state\&dag_id\=dag_test\&run_id\=manual__2020-10-28T17%3A36%3A28.838356%2B00%3A00
```

## Using the API
Once you deploy the plugin and restart the webserver, you can start to use the REST API. Bellow you will see the endpoints that are supported. 

**Note:** If enable RBAC, `http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/rest_api/`<br>
This web page will show the Endpoints supported and provide a form for you to test submitting to them.

- [deploy_dag](#deploy_dag)
- [refresh_all_dags](#refresh_all_dags)
- [delete_dag](#delete_dag)
- [dag_state](#dag_state)
- [task_instance_detail](#task_instance_detail)
- [restart_failed_task](#restart_failed_task)
- [kill_running_tasks](#kill_running_tasks)
- [run_task_instance](#run_task_instance)
- [skip_task_instance](#skip_task_instance)

### ***<span id="deploy_dag">deploy_dag</span>***
##### Description:
- Deploy a new dag, and refresh dag to session.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/rest_api/api?api=deploy_dag
```
##### Method:
- POST
##### POST request Arguments:
```json
{
	"workflow": {
		"name": "test_ingestion_x_35",
		"force": "true",
		"pause": "false",
		"unpause": "true",
		"dag_config": {
			"test_ingestion_x_35": {
				"default_args": {
					"owner": "harsha",
					"start_date": "2021-10-29T00:00:00.000Z",
					"end_date": "2021-11-05T00:00:00.000Z",
					"retries": 1,
					"retry_delay_sec": 300
				},
				"schedule_interval": "0 3 * * *",
				"concurrency": 1,
				"max_active_runs": 1,
				"dagrun_timeout_sec": 60,
				"default_view": "tree",
				"orientation": "LR",
				"description": "this is an example dag!",
				"tasks": {
					"task_1": {
						"operator": "airflow.operators.python_operator.PythonOperator",
						"python_callable_name": "metadata_ingestion_workflow",
						"python_callable_file": "metadata_ingestion.py",
						"op_kwargs": {
							"workflow_config": {
								"metadata_server": {
									"config": {
										"api_endpoint": "http://localhost:8585/api",
										"auth_provider_type": "no-auth"
									},
									"type": "metadata-server"
								},
								"sink": {
									"config": {
										"es_host": "localhost",
										"es_port": 9200,
										"index_dashboards": "true",
										"index_tables": "true",
										"index_topics": "true"
									},
									"type": "elasticsearch"
								},
								"source": {
									"config": {
										"include_dashboards": "true",
										"include_tables": "true",
										"include_topics": "true",
										"limit_records": 10
									},
									"type": "metadata"
								}
							}
						}
					}
				}
			}
		}
	}
}
```

##### Examples:
```bash
curl -H  'Authorization: Bearer eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJpYXQiOjE2MzU2NTE1MDAsIm5iZiI6MTYzNTY1MTUwMCwianRpIjoiNWQyZTM3ZDYtNjdiYS00NGZmLThjOWYtMDM0ZTQyNGE3MTZiIiwiZXhwIjoxNjM1NjUyNDAwLCJpZGVudGl0eSI6MSwiZnJlc2giOnRydWUsInR5cGUiOiJhY2Nlc3MifQ.DRUYCAiMh5h2pk1MZZJ4asyVFC20pu35DuAANQ5GxGw' -H 'Content-Type: application/json' -d "@test_ingestion_config.json" -X POST http://localhost:8080/rest_api/api\?api\=deploy_dag```
##### response:
```json
{"message": "Workflow [test_ingestion_x_35] has been created", "status": "success"}
```
### ***<span id="refresh_all_dags">refresh_all_dags</span>***
##### Description:
- Get all dags from dag_floder, refresh the dags to the session.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=refresh_all_dags
```
##### Method:
- GET
##### GET request Arguments:
- None
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=refresh_all_dags
```
##### response:
```json
{
  "message": "All DAGs are now up to date",
  "status": "success"
}
```
### ***<span id="delete_dag">delete_dag</span>***
##### Description:
- Delete dag based on dag_id.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=delete_dag&dag_id=value
```
##### Method:
- GET
##### GET request Arguments:
- dag_id - string - The id of dag.
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=delete_dag&dag_id=dag_test
```
##### response:
```json
{
  "message": "DAG [dag_test] deleted",
  "status": "success"
}
```
### ***<span id="dag_state">dag_state</span>***
##### Description:
- Get the status of a dag run.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=dag_state&dag_id=value&run_id=value
```
##### Method:
- GET
##### GET request Arguments:
- dag_id - string - The id of dag.
- run_id - string - The id of the dagRun.
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=dag_state&dag_id=dag_test&run_id=manual__2020-10-28T16%3A15%3A19.427214%2B00%3A00
```
##### response:
```json
{
  "state": "success",
  "startDate": "2020-10-28T16:15:19.436693+0000",
  "endDate": "2020-10-28T16:21:36.245696+0000",
  "status": "success"
}
```
### ***<span id="task_instance_detail">task_instance_detail</span>***
##### Description:
- Get the detail info of a task instance.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=task_instance_detail&dag_id=value&run_id=value&task_id=value
```
##### Method:
- GET
##### GET request Arguments:
- dag_id - string - The id of dag.
- run_id - string - The id of the dagRun.
- task_id - string - The id of the task.
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=task_instance_detail&dag_id=dag_test&run_id=manual__2020-10-28T16%3A31%3A17.247035%2B00%3A00&task_id=task_test
```
##### response:
```json
{
  "taskId": "task_test",
  "dagId": "dag_test",
  "state": "success",
  "tryNumber": null,
  "maxTries": null,
  "startDate": "2020-10-28T16:31:57.882329+0000",
  "endDate": "2020-10-28T16:31:57.882329+0000",
  "duration": null,
  "status": "success"
}
```
### ***<span id="restart_failed_task">restart_failed_task</span>***
##### Description:
- Restart failed tasks with downstream.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=restart_failed_task&dag_id=value&run_id=value
```
##### Method:
- GET
##### GET request Arguments:
- dag_id - string - The id of dag.
- run_id - string - The id of the dagRun.
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=restart_failed_task&dag_id=dag_test&run_id=manual__2020-10-28T16%3A31%3A17.247035%2B00%3A00
```
##### response:
```json
{
  "failed_task_count": 2,
  "clear_task_count": 6,
  "status": "success"
}
```
### ***<span id="kill_running_tasks">kill_running_tasks</span>***
##### Description:
- Kill running tasks that status in ['none', 'running'].
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=kill_running_tasks&dag_id=value&run_id=value&task_id=value
```
##### Method:
- GET
##### GET request Arguments:
- dag_id - string - The id of dag.
- run_id - string - The id of the dagRun.
- task_id - string - If task_id is none, kill all tasks, else kill one task.
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=kill_running_tasks&dag_id=dag_test&run_id=manual__2020-10-28T16%3A31%3A17.247035%2B00%3A00&task_id=task_test
```
##### response:
```json
{
  "status": "success"
}
```
### ***<span id="run_task_instance">run_task_instance</span>***
##### Description:
- Create dagRun, and run some tasks, other task skip.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=run_task_instance
```
##### Method:
- POST
##### POST request Arguments:
- dag_id - string - The id of dag.
- run_id - string - The id of the dagRun.
- tasks - string - The id of the tasks, Multiple tasks are split by comma.
- conf - string - Conf of creating dagRun.
##### Examples:
```bash
curl -X POST -F 'dag_id=dag_test' -F 'run_id=manual__2020-10-28T17:36:28.838356+00:00' -F 'tasks=task_test_3,task_test_4,task_test_6' http://localhost:8080/admin/rest_api/api?api=run_task_instance
```
##### response:
```json
{
  "execution_date": "2020-10-28T17:39:14.941060+0000",
  "status": "success"
}
```
### ***<span id="skip_task_instance">skip_task_instance</span>***
##### Description:
- Skip one task instance and downstream task.
##### Endpoint:
```text
http://{AIRFLOW_HOST}:{AIRFLOW_PORT}/admin/rest_api/api?api=skip_task_instance&dag_id=value&run_id=value&task_id=value
```
##### Method:
- GET
##### GET request Arguments:
- dag_id - string - The id of dag.
- run_id - string - The id of the dagRun.
- task_id - string - The id of the task.
##### Examples:
```bash
curl -X GET http://localhost:8080/admin/rest_api/api?api=skip_task_instance&dag_id=dag_test&run_id=manual__2020-10-28T17%3A43%3A10.053716%2B00%3A00&task_id=task_test_2
```
##### response:
```json
{
  "status": "success"
}
```

