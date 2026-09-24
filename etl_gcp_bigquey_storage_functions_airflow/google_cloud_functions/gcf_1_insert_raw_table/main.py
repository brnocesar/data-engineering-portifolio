from google.cloud import bigquery, storage
import pandas_gbq
from datetime import datetime, timedelta
from zoneinfo import ZoneInfo
from table_definitions import query_string_parameters
import re

bq_client = bigquery.Client()


def get_sql_error(query_job_id) -> None:
    output_select    = []
    select_type_jobs = [job.result() for job in bq_client.list_jobs(parent_job=query_job_id) if job.statement_type == "SELECT"]
    
    if len(select_type_jobs) == 0:
        return

    for rows in select_type_jobs:
        output_select = [row[0] for row in rows if len(row)]

    raise Exception(output_select[0])


def get_files_pattern(dataset: str, raw_table: str, target_datetime: str, special_daylie_frequency: str) -> tuple[str, str]:
    query_build = query_string_parameters()
    if raw_table not in query_build[dataset].keys():
        raise Exception("arquivo nao mapeado no dicionario de definicoes") # trocar por defaultdict???
    
    storage_prefix = query_build[dataset][raw_table]['file_pattern']
    load_frequency = query_build[dataset][raw_table]['load_frequency']
    if load_frequency not in ['1440', '60', '0']:
        raise Exception("frequencia de atualizacao mapeada nao existe")
    
    underline_dt_format, dot_dt_format = ('%d_%m_%Y', '%d.%m.%Y')
    inter_date_format                  = f"{target_datetime.strftime('%Y-%m-%d')}"
    day_pattern_query                  = f"({target_datetime.strftime(underline_dt_format)}|{target_datetime.strftime(dot_dt_format)})"
    
    if load_frequency == '1440' or special_daylie_frequency:
        underline_dt_format, dot_dt_format = (('%Y_%m_%d', '%Y.%m.%d') if dataset == 'raw_api' else ('%d_%m_%Y', '%d.%m.%Y'))
        return f"^gs://[\w-]+/{storage_prefix}_({target_datetime.strftime(underline_dt_format)}|{target_datetime.strftime(dot_dt_format)})", inter_date_format
    
    if load_frequency == '60':
        underline_dt_format, dot_dt_format = ('%Y_%m_%d_%H', '%Y.%m.%d.%H') if dataset == 'raw_api' else ('%d_%m_%Y_%H', '%d.%m.%Y.%H')
        return f"^gs://[\w-]+/{storage_prefix}_({target_datetime.strftime(underline_dt_format)}|{target_datetime.strftime(dot_dt_format)})", inter_date_format
    
    if load_frequency == '0':
        # recupera nome dos arquivos do dia atual que ainda nao foram inseridos
        query = f"""
            select e.file_name
            from (
                select regexp_replace(_file_name, r'^gs://[\w-]+/\w+/', '') as file_name 
                FROM {dataset}.ext_{raw_table}
                where REGEXP_CONTAINS(_file_name, r'^gs://[\w-]+/{storage_prefix}_{day_pattern_query}')
                group by regexp_replace(_file_name, r'^gs://[\w-]+/\w+/', '')
            ) e
                left join (
                    select file_name  
                    from `gcp-project-id.logs_dataset.logs_table`
                    where event_desc = 'insert' 
                        and event_status = 'success' 
                        and dt_ref = '{inter_date_format}' 
                        and dataset = '{dataset}' 
                        and table_name = '{raw_table}'
                    group by file_name
                ) a on a.file_name = e.file_name
            where a.file_name is null
        """
        files_df        = pandas_gbq.read_gbq(query, progress_bar_type=None)
        available_files = files_df.file_name.to_list() if len(files_df) else ['batatinha_quando_nasce_escolhe_uma_string_que_nao_vai_dar_match_com_nenhum_nome_de_arquivo']
        return f"({'|'.join(available_files)})", inter_date_format


def build_query(dataset: str, raw_table: str, target_datetime: str, ts_dt_updated: str, special_daylie_frequency: str) -> str:
    query_build        = query_string_parameters()
    files_pattern_query, inter_date_format = get_files_pattern(dataset, raw_table, target_datetime, special_daylie_frequency)
    
    no_subquery = ""
    if query_build[dataset][raw_table]['has_subquery'] == '0':
        no_subquery = f"""
                ,_FILE_NAME as arquivo, ts_dt as ts_dt_update
            FROM {dataset}.ext_{raw_table}
        """
    
    query = f"""
        BEGIN
            declare ts_dt timestamp default "{ts_dt_updated}";
            
            BEGIN
                BEGIN TRANSACTION;
                    insert into `gcp-project-id.{dataset}.{raw_table}` 
                    (
                        {query_build[dataset][raw_table]['insert_fields']}
                        ,arquivo, ts_dt_update 
                    )
                    select 
                        {query_build[dataset][raw_table]['select_fields']}
                        {no_subquery} 
                    where REGEXP_CONTAINS(_FILE_NAME, r'{files_pattern_query}')
                        {query_build[dataset][raw_table]['conditions']};
                        
                COMMIT TRANSACTION;

                EXCEPTION WHEN ERROR THEN
                    SELECT @@error.message;
                    ROLLBACK TRANSACTION;
            END;
            
            -- PLACE HOLDER FOR SQL: insert em tabela de logs para registrar operacao
        END;
    """
    return query


def get_files_from_ext_table(dataset: str, raw_table: str, target_datetime: str, special_daylie_frequency: str) -> list:
    query_build            = query_string_parameters()
    files_pattern_query, _ = get_files_pattern(dataset, raw_table, target_datetime, special_daylie_frequency)
    load_frequency         = query_build[dataset][raw_table]['load_frequency']
    storage_prefix         = query_build[dataset][raw_table]['file_pattern']
    external_table         = f"{dataset}.ext_{raw_table}"
    
    query = f"""
        select distinct REGEXP_REPLACE(_FILE_NAME, r'^gs://[\w-]+/\w+/', '') as available_files
        FROM {external_table}
        where REGEXP_CONTAINS(_FILE_NAME, r'{files_pattern_query}');
    """
    
    try:
        files_df = pandas_gbq.read_gbq(query)
        return files_df.available_files.to_list()
    except Exception as e:
        AVAILABLE_BUCKETS_TO_UPLOAD_FILES = {
            '1440': 'temp_local_files_bucket_daily',
            '60':   'temp_local_files_bucket_hourly',
            '0':    'temp_local_files_bucket_instantly'
        }
        storage_client = storage.Client()
        blobs          = storage_client.list_blobs(AVAILABLE_BUCKETS_TO_UPLOAD_FILES[load_frequency], prefix=storage_prefix)
        all_files      = [str(blob.name).split('/')[1] for blob in blobs if len(str(blob.name).split('/')) == 2]
        p              = re.compile('^(.\w)+'+files_pattern_query)
        return [file for file in all_files if p.match(file)]


def insert_in_raw_table(request):
    dataset                  = request.json['dataset']                  # dataset RAW de destino no BQ
    table                    = request.json['table']                    # tabela RAW de destino no BQ
    date                     = request.json['date']                     # data dos arquivos a serem inseridos
    special_daylie_frequency = request.json['special_daylie_frequency']
    files                    = []
    
    # 1) define valor das variaveis temporais
    target_datetime = (datetime.now(ZoneInfo("America/Sao_Paulo")) if date is None else datetime.strptime(date, '%Y-%m-%d %H')) - timedelta(hours=1)
    dt_ref          = target_datetime.strftime('%Y-%m-%d')
    ts_dt_updated   = datetime.now().strftime("%Y-%m-%d %H:%M:%S.%f UTC")
    
    try:
        # 2) cria registro de log dos arquivos sendo carregados: INICIO do evento 'insert_raw'
        files = get_files_from_ext_table(dataset, table, target_datetime, special_daylie_frequency)
        # PLACE HOLDER FOR CODE: cria registro de log para 'INICIO' da operacao de insert do conteudo dos arquivos `files` da data `dt_ref` na tabela RAW `table`
        
        # 3) recupera num de linhas na tabela antes de inserir e executa SQL para inserção
        ini_rows  = bq_client.get_table(f"{dataset}.{table}").num_rows
        query_str = build_query(dataset, table, target_datetime, ts_dt_updated, special_daylie_frequency) 
        query_job = bq_client.query(query_str)
        query_job.result()
        
        # 4) verifica se teve erro na execucao do SQL (se sim, levanta excecao) e recupera num de linhas na tabela após inserção
        get_sql_error(query_job.job_id)
        final_rows  = bq_client.get_table(f"{dataset}.{table}").num_rows # conta numero de linhas apos execucao, para saber se teve insert
        message     = 'Insercao de NOVOS registros na tabela RAW finalizada' if final_rows > ini_rows else  'Nao foram encontrados novos registros para inserir na tabela RAW'
        status_code = 201 if final_rows > ini_rows else 200
        
        # 5) cria registro de log do arquivo FIM
        # PLACE HOLDER FOR CODE: cria registro de log para 'FIM' da operacao de insert do conteudo dos arquivos `files` da data `dt_ref` na tabela RAW `table`
        
        return {'status': 'success', 'message': message, 'dataset': dataset, 'table': table}, status_code

    except Exception as e:
        erro_capturado = re.sub(r'\s+', ' ', str(e)).strip()
        # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' da operacao de insert do conteudo dos arquivos `files` da data `dt_ref` na tabela RAW `table`
        
        return {'status': 'fail', 'dataset': dataset, 'table': table, 'message': erro_capturado}, 400


def http_trigger_entry_point(request):
    return insert_in_raw_table(request)


# o mesmo codigo poderia ser usado por duas cloud functions: uma HTTP trigger e uma Storage trigger
def storage_trigger_entry_point(data, context=None):
    # converte o evento do Storage em um request compativel com a funcao insert_in_raw_table
    request = {
        'dataset': data['dataset'],
        'table': data['table'],
        'date': data.get('date'),
        'special_daylie_frequency': data.get('special_daylie_frequency')
    }
    
    return insert_in_raw_table(request)
