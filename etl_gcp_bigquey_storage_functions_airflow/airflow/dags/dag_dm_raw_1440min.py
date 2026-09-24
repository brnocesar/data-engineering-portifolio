

"""DAG que orquestra, diariamente, a carga nas tabelas RAW e o versionamento (SCD) das bases de atualizacao diaria."""
from datetime import timedelta
import pendulum
import pandas_gbq
from airflow import models
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from plugins.gcf_request import gcf_1_insert_raw_table, data_quality, gcf_2_scd_versioning


def table_querys_ref_job():
    # tabela de controle (espelho no BQ) que indica quais bases devem ser processadas, com qual frequencia e se devem ser versionadas
    query = """
        select dataset, raw_table as table_name, versioned
        from `gcp-project-id.control_etl_process.tb_ref_tables`
        where integration_bq = 1
            and update_frequency_min = 1440;
    """
    return pandas_gbq.read_gbq(query)


default_args = {
    "retries": 3,
    "retry_delay": timedelta(minutes=2),
    "execution_timeout": timedelta(minutes=90),
}

with models.DAG(
    dag_id="dm_raw_tables_1440min",
    default_args=default_args,
    start_date=pendulum.datetime(2026, 9, 24, 1, 15, tz="America/Sao_Paulo"),
    schedule="22 1 * * *",  # todos os dias as 22:22 (GMT-3, Brasil)
    catchup=False,
    max_active_runs=3,
) as dag:
    start = EmptyOperator(task_id="start")

    ref_tables = table_querys_ref_job()

    for _, row in ref_tables.iterrows():
        dataset, table, versioned = row["dataset"], row["table_name"], bool(row["versioned"])

        job_gcf_1_insert_raw_table = PythonOperator(
            task_id=f"gcf_1_insert_raw_table_{table}",
            python_callable=gcf_1_insert_raw_table,
            op_kwargs={"dataset": dataset, "table": table, "date": None, "special_daylie_frequency": "daily"},
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
