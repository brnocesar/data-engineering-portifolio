"""
Trigger a DAG in a Cloud Composer 2 environment in response to an event, using Cloud Functions.
"""

from typing import Any
from google.auth.transport.requests import AuthorizedSession
import re

# Following GCP best practices, these credentials should be constructed at start-up time and used throughout
AUTH_SCOPE     = "https://www.googleapis.com/auth/cloud-platform"
CREDENTIALS, _ = google.auth.default(scopes=[AUTH_SCOPE])


def make_request_to_api(url: str, **kwargs: Any) -> google.auth.transport.Response:
    """
    Make a request to Cloud Composer 2 environment's web server.
    Args:
        url: The URL to fetch.
        **kwargs: Any of the parameters defined for the request function. If no timeout is provided, it is set to 90 by default.
    """

    authorized_session = AuthorizedSession(CREDENTIALS)
    kwargs["timeout"]  = 90 if "timeout" not in kwargs else kwargs["timeout"] # set the default timeout, if missing

    return authorized_session.request('POST', url, **kwargs)


def trigger_dag(web_server_url: str, dag_id: str, data: dict) -> str:
    """
    Make a request to trigger a dag using the stable Airflow 2 REST API.

    Args:
        web_server_url: The URL of the Airflow 2 web server.
        dag_id: The DAG ID.
        data: Additional configuration parameters for the DAG run (json).
    """

    endpoint    = f"api/v1/dags/{dag_id}/dagRuns"
    request_url = f"{web_server_url}/{endpoint}"
    json_data   = {"conf": data}
    response    = make_request_to_api(request_url, json=json_data)

    if response.status_code != 200:
        raise Exception(f"{response.headers} - {response.text}")
    else:
        # PLACE HOLDER FOR CODE: cria registro de log para 'FIM' do acionamento do DAG
        return {'status': 'success', 'message': response.text}, 200


def entry_point(request):
    web_server_url = "airflow-instance-url" # replace with your actual Airflow instance URL
    dag_id         = request.json['dag_id']
    
    # PLACE HOLDER FOR CODE: cria registro de log para 'INICIO' do acionamento do DAG
    
    try:
        return trigger_dag(web_server_url, dag_id, {})

    except Exception as e:
        erro_capturado = re.sub(r'\s+', ' ', str(e)).strip()
        
        # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' do acionamento do DAG indicando o `erro capturado`
        
        return {'status': 'failure', 'message': erro_capturado}, 500
