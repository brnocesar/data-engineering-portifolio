# google_cloud_functions

> This directory represents its own Git repository: each subfolder is the source code of an independent Cloud Function, with automated deployment via Cloud Build on every push (see each function's `cloudbuild.yaml`).

## Cloud Functions in this repository

- **`gcf_1_insert_raw_table`**: reads the content available in the BigQuery external table and inserts it into the corresponding raw table. Can be triggered via HTTP or by a Cloud Storage upload event (two _entry points_ in the same `main.py`).
- **`gcf_2_scd_versioning`**: applies versioning (SCD) of the most recent, qualified load from the raw table into the respective versioned table.
- **`trigger_airflow_dag`**: receives an HTTP request with a `dag_id` and triggers the corresponding DAG in Airflow (Cloud Composer).

Check each function's `readme.md` for details about parameters, authentication and how to add the mapping for a new base.

## Mapping conventions

The bases processed by `gcf_1_insert_raw_table` and `gcf_2_scd_versioning` are defined by Python dictionaries (`table_definitions_manual.py` and `table_definitions_auto.py`, merged into `table_definitions.py`), with no fixed SQL per base. See the "Base mappings" section in the repository root's `readme.md` to understand how these mappings are structured and generated (manual vs. automatic).

## Deploy

Each function has its own `cloudbuild.yaml`, tied to a Cloud Build _trigger_: every push to this repository's main branch automatically triggers the `gcloud functions deploy` of the corresponding function, publishing the new revision already with any mappings that were added.

## Triggering the Cloud Function

Let's consider the scenario where the cloud function only accepts authenticated requests. If the code calling the cloud function is running inside the cloud environment, then the authentication token can be obtained through a Google Cloud package specific for that.

First, make sure the service account running the service that calls this _function_ has the `cloudfunctions.invoker` role. From there, just use the code below, which can live in the `main.py` file or be imported from a module you define; what matters is calling the `make_authorized_request()` function:

```python
import requests as r # pip install requests
from google.auth.transport.requests import Request # pip install google-auth
from google.oauth2.id_token import fetch_id_token

def make_authorized_request(dataset, table, ts_dt=None):
    url      = "https://region-id-gcp-project-id.cloudfunctions.net/gcf_2_scd_versioning"
    id_token = fetch_id_token(Request(), url)
    header   = {'Authorization': f"Bearer {id_token}"}
    body     = {'dataset': dataset, 'table': table, 'ts_dt': None}
    response = r.post(url=url, headers=header, json=body, verify=False)

    return response.content, response.status_code
```

| Status Code | Meaning |
| :-: | -- |
| 200 | Process ran with no errors, but no new records were inserted into the RAW table |
| 201 | New records were added to the RAW table |
| 400 | Something went wrong! |
