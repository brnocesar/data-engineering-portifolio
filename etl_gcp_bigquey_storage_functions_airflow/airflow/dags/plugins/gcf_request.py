import requests as r
from google.auth.transport.requests import Request
from google.oauth2.id_token import fetch_id_token


def make_authorized_request(url, body):
    id_token = fetch_id_token(Request(), url)
    header   = {'Authorization': f"Bearer {id_token}"}
    response = r.post(url=url, headers=header, json=body, verify=False)
    if response.status_code not in [200,201]:
        raise ValueError("ERRO NA FUNCTION (retorno 400): " + str(response.status_code)+ "  " + str(response.content))
    return response.status_code

def data_quality(dataset, table, ts_dt=None, file_name=None):
    url  = "https://region-id-gcp-project-id.cloudfunctions.net/data_quality_process_run"
    body = {'dataset': dataset, 'table': table, 'ts_dt': ts_dt,  'file_name': file_name}
    return make_authorized_request(url, body)

def gcf_1_insert_raw_table(dataset, table, date, special_daylie_frequency=None):
    url  = "https://region-id-gcp-project-id.cloudfunctions.net/gcf_1_insert_raw_table"
    body = {'dataset': dataset, 'table': table, 'date': date, 'special_daylie_frequency': special_daylie_frequency}
    return make_authorized_request(url, body)

def gcf_2_scd_versioning(dataset, table, ts_dt=None):
    url  = "https://region-id-gcp-project-id.cloudfunctions.net/gcf_2_scd_versioning"
    body = {'dataset': dataset, 'table': table, 'ts_dt': ts_dt}
    return make_authorized_request(url, body)
