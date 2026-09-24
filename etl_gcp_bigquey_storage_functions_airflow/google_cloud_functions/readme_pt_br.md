# google_cloud_functions

> Este diretório representa um repositório Git próprio: cada subpasta é o código-fonte de uma Cloud Function independente, com deploy automatizado via Cloud Build a cada push (ver `cloudbuild.yaml` de cada função).

## Cloud Functions neste repositório

- **`gcf_1_insert_raw_table`**: lê o conteúdo disponível na tabela externa do BigQuery e insere na tabela raw correspondente. Pode ser acionada via HTTP ou por um evento de upload no Cloud Storage (dois _entry points_ no mesmo `main.py`).
- **`gcf_2_scd_versioning`**: aplica o versionamento (SCD) da carga mais recente e qualificada da tabela raw na respectiva tabela versionada.
- **`trigger_airflow_dag`**: recebe uma requisição HTTP com um `dag_id` e aciona a DAG correspondente no Airflow (Cloud Composer).

Consulte o `readme.md` de cada função para detalhes sobre parâmetros, autenticação e como adicionar o mapeamento de uma nova base.

## Convenções de mapeamento

As bases processadas por `gcf_1_insert_raw_table` e `gcf_2_scd_versioning` são definidas por dicionários Python (`table_definitions_manual.py` e `table_definitions_auto.py`, unidos em `table_definitions.py`), sem SQL fixo por base. Veja a seção "Mapeamentos das bases" no `readme.md` da raiz do repositório para entender como esses mapeamentos são estruturados e gerados (manual x automático).

## Deploy

Cada função possui seu próprio `cloudbuild.yaml`, associado a um _trigger_ do Cloud Build: todo push na branch principal deste repositório dispara automaticamente o `gcloud functions deploy` da função correspondente, publicando a nova revisão já com eventuais mapeamentos adicionados.

## Acionando a Cloud Function

Vamos considerar o cenário em que a cloud function aceita apenas requisições autenticadas. Se o código que chamar a cloud function estiver rodando dentro do ambiente cloud, então o token para autenticação pode ser obtido através de um pacote do Google Cloud específico para isso.

Primeiro, é necessário se certificar que a conta de serviço rodando o serviço que chamar a function_ possua o role `cloudfunctions.invoker`. A parir disso, basta utilizar os códigos abaixo, que pode ficar no arquivo `main.py` ou ser importado de algum módulo que você defina, o importante é chamar a função `make_authorized_request()`:

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

| Status Code | Significado |
| :-: | -- |
| 200 | Processo rodou sem erros, mas não foram inseridos novos registros na tabela RAW |
| 201 | Foram adicionados novos registros na tabela RAW |
| 400 | Deu ruim! |
