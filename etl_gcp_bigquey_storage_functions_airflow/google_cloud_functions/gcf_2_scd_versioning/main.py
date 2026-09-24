from google.cloud import bigquery
import build_sql_instructions as sql
from table_definitions import query_string_parameters, unmapped_base_error
import pandas_gbq
import re

bq_client  = bigquery.Client()


def get_sql_error(query_job_id) -> None:
    output_select    = []
    select_type_jobs = [job.result() for job in bq_client.list_jobs(parent_job=query_job_id) if job.statement_type == "SELECT"]
    
    for rows in select_type_jobs:
        output_select.append([row[0] for row in rows if len(row)])
    
    output_select = [output[0] for output in output_select if 'Query error:' in str(output[0])]
    
    if len(output_select):
        raise Exception(output_select[0])


def build_queries(dataset, table, ts_dt):
    query_build = query_string_parameters()
    
    if table not in query_build[dataset].keys():
        raise Exception(unmapped_base_error)
    
    last_load, event_dates = sql.declare_var_last_date_instruction(
                                source_dataset=query_build[dataset][table]['source_dataset'], 
                                source_table=query_build[dataset][table]['source_table'], 
                                ts_dt=ts_dt)
    tmp_source_string      = sql.create_tmp_source_instruction(
                                source_dataset=query_build[dataset][table]['source_dataset'], 
                                source_table=query_build[dataset][table]['source_table'], 
                                fields=query_build[dataset][table]['source_fields'], 
                                array_type_fields=query_build[dataset][table]['array_type_fields'], 
                                partition_clause=query_build[dataset][table]['partition_clause_tmp'], 
                                grouping=query_build[dataset][table]['grouping_tmp'], 
                                event_date_values=event_dates)
    vars_delete_string     = sql.declare_vars_delete_instruction(
                                fields=query_build[dataset][table]['fields_to_delete'], 
                                special_clause=query_build[dataset][table]['specific_clause_del'])
    merge_string           = sql.merge_instruction(
                                target_dataset=query_build[dataset][table]['target_dataset'], 
                                target_table=query_build[dataset][table]['target_table'], 
                                fields_to_match=query_build[dataset][table]['fields_to_match'])
    update_string          = sql.update_instruction(
                                target_fields=query_build[dataset][table]['target_fields'], 
                                source_fields=query_build[dataset][table]['source_fields'])
    delete_string          = sql.delete_instruction(
                                fields=query_build[dataset][table]['fields_to_delete'], 
                                special_clause=query_build[dataset][table]['specific_clause_del'])
    insert_string          = sql.insert_instruction(
                                target_fields=query_build[dataset][table]['target_fields'], 
                                source_fields=query_build[dataset][table]['source_fields'])
    
    ts_dt_query = f"""
        BEGIN
            {last_load}
            select replace(cast(ts_dt as string), '+00', ' UTC') as ts_dt_update;
        END;
    """
    log_files_query = f"""
        BEGIN
            {last_load}
            select REGEXP_REPLACE(arquivo, r'^gs:\/\/\w+\/(\w+\/)?', '') as available_files
            from `gcp-project-id.{dataset}.{table}`
            where ts_dt_update = ts_dt {event_dates}
            group by arquivo;
        END;
    """
    scd_query = f"""
        BEGIN
            {last_load}
            {tmp_source_string}
            BEGIN
                {vars_delete_string}
                BEGIN TRANSACTION;
                    {merge_string} 
                    {update_string} 
                    {delete_string} 
                    {insert_string} 
                    ;
                    
                COMMIT TRANSACTION;
                DROP TABLE tmp_source;

                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message as query_error_message;
                ROLLBACK TRANSACTION;
            END;
        END;
    """
    
    return log_files_query, ts_dt_query, scd_query


def insert_new_records(request):
    bq_dataset = request.json['dataset']
    table      = request.json['table']
    ts_dt      = request.json['ts_dt']

    try:
        # monta codigos SQL a partir dos mapeamentos definidos em table_definitions
        files_query, ts_dt_query, scd_query = build_queries(bq_dataset, table, ts_dt)
        
        files        = bq_client.query(files_query).to_dataframe().available_files.to_list()
        ts_dt_update = bq_client.query(ts_dt_query).to_dataframe().ts_dt_update.to_list()[0]
        # PLACE HOLDER FOR CODE: cria registro de log para 'INICIO' da operacao de SCD na tabela `table` com o conteudo dos arquivos `files` inseridos na data `ts_dt_update`
        
        # verifica se ja foi feito SCD com essa carga (remendo para nao mexer nas DAGs do Airflow, que é a forma ideal)
        scd_runned_df = pandas_gbq.read_gbq(f"""select dataset, table_name, flag_scd, ts_dt_update 
                                                FROM `gcp-project-id.logs_dataset.log_load_data_process`
                                                where flag_scd is not null 
                                                    and dataset = '{bq_dataset}' and table_name = '{table}' 
                                                    and ts_dt_update = cast('{ts_dt_update}' as timestamp) ;""") 
        scd_runned = bool(len(scd_runned_df))
        
        if not scd_runned:
            # executa processo de SCD
            query_job = bq_client.query(scd_query)
            query_job.result()
        
            # verifica se teve erro na execucao do SQL do SCD, se sim levanta excecao
            get_sql_error(query_job.job_id)
        
        # PLACE HOLDER FOR CODE: cria registro de log para 'FIM' da operacao de SCD na tabela `table` com o conteudo dos arquivos `files` inseridos na data `ts_dt_update`
        
        return {'status': 'success', 'dataset': bq_dataset, 'table': table, 'message': 'versionamento dos dados realizado'}, 201

    except Exception as e:
        erro_capturado              = re.sub(r'\s+', ' ', str(e)).strip()
        status_message, status_code = ('success', 200) if erro_capturado == unmapped_base_error else ('fail', 400)
        
        # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' da operacao de SCD na tabela `table` indicando o erro capturado `erro_capturado`
        
        return {'status': status_message, 'dataset': bq_dataset, 'table': table, 'message': erro_capturado}, status_code
