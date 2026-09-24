"""DAG disparada pela cloud function `trigger_airflow_dag`; processa apenas as bases de frequencia instantanea (0) que possuam arquivos novos ainda nao carregados."""
from datetime import timedelta
import pendulum
import pandas_gbq
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.gcf_request import gcf_1_insert_raw_table, data_quality, gcf_2_scd_versioning


def table_querys_ref_job():
    # tabela de controle com as bases de frequencia instantanea habilitadas para integracao
    ref_tables = pandas_gbq.read_gbq("""
        select dataset, raw_table as table_name, versioned
        from `gcp-project-id.control_etl_process.tb_ref_tables`
        where integration_bq = 1
            and update_frequency_min = 0;
    """)

    if ref_tables.empty:
        return ref_tables

    # para cada base candidata, cruza a tabela de logs e a propria tabela RAW com a externa para achar arquivo ainda nao carregado
    pending_query = "\nunion all\n".join(
        f"""
        select '{row.dataset}' as dataset, '{row.table_name}' as table_name
        from `{row.dataset}.ext_{row.table_name}` ext
        where not exists (
                select 1
                from `gcp-project-id.logs_dataset.logs_table` log
                where log.dataset = '{row.dataset}' and log.table_name = '{row.table_name}'
                    and log.event_desc = 'insert' and log.event_status = 'success'
                    and log.file_name = REGEXP_EXTRACT(ext._FILE_NAME, r'([^/]+)$')
            )
            and not exists (
                select 1
                from `{row.dataset}.{row.table_name}` raw_tbl
                where raw_tbl.arquivo = REGEXP_EXTRACT(ext._FILE_NAME, r'([^/]+)$')
            )
        group by 1, 2
        """
        for row in ref_tables.itertuples()
    )
    pending_tables = pandas_gbq.read_gbq(pending_query)

    return ref_tables.merge(pending_tables, on=["dataset", "table_name"])


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=90),
}

with models.DAG(
    dag_id="dm_raw_tables_instantly",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 24, 1, 15, tz="America/Sao_Paulo"),
    schedule=None,  # disparada sob demanda pela cloud function `trigger_airflow_dag`
    catchup=False,
    max_active_runs=5,
) as dag:
    start = EmptyOperator(task_id="start")

    ref_tables = table_querys_ref_job()

    for _, row in ref_tables.iterrows():
        dataset, table, versioned = row["dataset"], row["table_name"], bool(row["versioned"])

        job_gcf_1_insert_raw_table = PythonOperator(
            task_id=f"gcf_1_insert_raw_table_{table}",
            python_callable=gcf_1_insert_raw_table,
            op_kwargs={"dataset": dataset, "table": table, "date": None},
        )
        job_data_quality = PythonOperator(
            task_id=f"data_quality_{table}",
            python_callable=data_quality,
            op_kwargs={"dataset": dataset, "table": table, "ts_dt": None},
        )

        start >> job_gcf_1_insert_raw_table >> job_data_quality

        if versioned:
            job_gcf_2_scd_versioning = PythonOperator(
                task_id=f"gcf_2_scd_versioning_{table}",
                python_callable=gcf_2_scd_versioning,
                op_kwargs={"dataset": dataset, "table": table, "ts_dt": None},
            )
            job_data_quality >> job_gcf_2_scd_versioning

