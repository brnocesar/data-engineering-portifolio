# airflow

> DAGs e _plugins_ do Cloud Composer responsáveis por orquestrar as chamadas às _Cloud Functions_ (`gcf_1_insert_raw_table` e `gcf_2_scd_versioning`) que fazem, respectivamente, o insert na tabela RAW e o versionamento (SCD) de cada base.

## Como as DAGs decidem o que processar

Nenhuma DAG tem uma lista fixa de bases: cada uma implementa uma função `table_querys_ref_job()` que consulta a tabela de controle `gcp-project-id.control_etl_process.tb_ref_tables`, filtrando por `update_frequency_min` (`1440`, `60` ou `0`) e por `integration_bq = 1`. O resultado dessa consulta (dataset, tabela e a flag `versioned`) é usado para criar, dinamicamente, uma cadeia de _tasks_ por base.

## Cadeia de tasks por base

Para cada base retornada por `table_querys_ref_job()` é montada a mesma sequência:

1. **`gcf_1_insert_raw_table_<tabela>`**: aciona a _Cloud Function_ que insere o conteúdo disponível na tabela externa na tabela RAW.
2. **`data_quality_<tabela>`**: é uma task que roda entre o insert na RAW e o versionamento. Não foi implementada nesse projeto, seria uma checagem na qualidade dos dados (de acordo com o critério que você leitor julgar necessário) que marcaria a carga como "apta a ser usada no SCD".
3. **`gcf_2_scd_versioning_<tabela>`**: só é criada se a base estiver marcada como `versioned` na tabela de controle; aplica o versionamento (SCD) usando a carga que acabou de ser inserida.

Essas chamadas são feitas via HTTP autenticado, através das funções auxiliares em `dags/plugins/gcf_request.py` (`gcf_1_insert_raw_table`, `data_quality`, `gcf_2_scd_versioning`).

## DAGs

- **`dag_dm_raw_1440min.py`** (`dm_raw_tables_1440min`): roda uma vez ao dia, processa as bases com `update_frequency_min = 1440` e passa `special_daylie_frequency='daily'` para a cloud function de insert (`gcf_1_insert_raw_table`).
- **`dag_dm_raw_60min.py`** (`dm_raw_tables_60min`): roda a cada hora, processa as bases com `update_frequency_min = 60`.
- **`dag_dm_raw_instantly.py`** (`dm_raw_tables_instantly`): não possui agendamento (`schedule=None`), é disparada sob demanda pela cloud function `trigger_airflow_dag`. Antes de montar as tasks, `table_querys_ref_job()` também cruza a tabela externa de cada base (`update_frequency_min = 0`) com a tabela de logs de arquivos carregados e com a própria tabela RAW, processando somente as bases que realmente têm arquivo novo pendente.
