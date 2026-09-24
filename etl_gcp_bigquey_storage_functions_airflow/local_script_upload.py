import os
import sys
from local_sql_packege import database # out of the scope of this project
from google.cloud import storage
from google.cloud import bigquery
import pandas as pd
import pandas_gbq
import shutil
import ast
import json
from collections import defaultdict
import subprocess
from os.path import isdir, isfile, join as join_path

conn                              = database.Database()       # conector para o banco SQL local
storage_client                    = storage.Client()          # client para o Google Cloud Storage
bq_client                         = bigquery.Client()         # client para o BigQuery
FILES_PATH                        = "local_files_to_upload/"  # path for local files to be uploaded to BigQuery
GIT_REPOSITORY_PATH               = "google_cloud_functions/" # path for git repository with serverless functions
AVAILABLE_BUCKETS_TO_UPLOAD_FILES = {
    '1440': 'temp_local_files_bucket_daily',
    '60':   'temp_local_files_bucket_hourly',
    '0':    'temp_local_files_bucket_instantly'
}

os.chdir(FILES_PATH) # define o diretorio de trabalho para o caminho dos arquivos locais


def get_folders_to_upload(frequency: str = '1440') -> list:
    
    if frequency not in ('1440', '60', '0'):
        # PLACE HOLDER FOR CODE: cria registro de log para 'WARNING' da frequencia informada nao ser um valor esperado
        frequency = '1440'
    
    complement_filter = f" and update_frequency_min = {frequency} " if frequency != '1440' else ""
    tables = conn.query(f"""
            select raw_table 
            from tb_ref_tables 
            where integration_bq = 1 
                and update_frequency_min in (1440, 60, 0) 
                {complement_filter};
        """)
    
    return tables.raw_table.to_list()


def move_files_to_backup(folder: str, files: list[str]) -> None:
    for file in files:
        shutil.move(join_path(folder, file), join_path(folder, 'backup', file))


def table_must_be_created_in_bq(table_name: str, dataset: str, last_file_uploaded: str) -> bool:
    query = f"""
        select table_name
        from `gcp-project-id.{dataset}.INFORMATION_SCHEMA.TABLES`
        where table_name = '{table_name}'
        union all select table_name
        from `gcp-project-id.raw_{dataset}.INFORMATION_SCHEMA.TABLES`
        where table_name = '{table_name}';
    """
    bq_table_exists = pandas_gbq.read_gbq(query).shape[0]
    
    return bq_table_exists == 0 and last_file_uploaded is not None


def generate_ddl_codes(sql_db, 
                       sql_table: str, 
                       bq_dataset: str, 
                       bq_table_name: str, 
                       load_frequency_min: int, 
                       example_file: str = None, 
                       verify_pk: bool = True
    ) -> tuple[str] | tuple[str, dict]:
    # 1) faz conexao com a devida database
    sql = database.Database(database_name=sql_db)
    
    # 2) monta dataset com schema da tabela
    # 2.1) recupera colunas do arquivo que eh carregado
    try:
        fields_file = pd.read_csv(f"{FILES_PATH}\\{sql_table}\\{example_file}", sep=';', nrows=0, encoding='latin1').columns.str.lower()
        fields_file = [i.lstrip('[').rstrip(']') for i in fields_file]
        fields_file = pd.DataFrame(fields_file, columns=['column_name'])
    except Exception as e:
        return [str(e)]
    
    # 2.2) recupera campos da tabela no SQL SERVER
    sql_schema = sql.query(f"""
        select lower(c.column_name) as column_name 
            ,case 
                when c.data_type in ('varchar', 'nvarchar', 'ntext', 'char') then 'string' 
                when c.data_type in ('bigint', 'bit') then 'int' 
                when c.data_type in ('decimal', 'float') then 'numeric' 
                when c.data_type in ('smalldatetime', 'datetime2') then 'datetime'
                else c.data_type 
            end as data_type 
            ,case when p.column_name is not null then 1 else 0 end is_pk 
        from information_schema.columns c 
            left join ( 
                select a.table_name, a.column_name 
                from information_schema.constraint_column_usage a
                    inner join information_schema.table_constraints b on a.table_name = b.table_name and a.constraint_name = b.constraint_name
                where a.table_name = '{sql_table}'
                    and b.constraint_type = 'primary key'
            ) p 
                on c.table_name = p.table_name and c.column_name = p.column_name
        where c.table_name = '{sql_table}';
    """)
    
    # 2.3) monta df com schema final (mantendo apenas campos existentes no arquivo) e trata nome das colunas
    fields_base                = pd.merge(fields_file, sql_schema, on="column_name")
    pattern_to_replace         = r'(\s)|(\\)|(/)'
    fields_base['column_name'] = fields_base['column_name'].str.normalize('NFKD').str.encode('ascii', errors='ignore').str.decode('utf-8').str.replace(pattern_to_replace, '_', regex=True)
    fields_base['column_name'] = [(f"_{i}" if i in ('arquivo', 'ts_dt_update') else i) for i in fields_base.column_name]
    
    # 2.4) avalia PK na tabela do banco SQL local
    # 2.4.1) se tabela nao possui campos na PK
    num_fields_pk = fields_base.is_pk.sum()
    if verify_pk and num_fields_pk == 0:
        return ["Tabela no banco SQL local NAO possui PK"]

    # 2.4.2) se tabela possui mais de 3 campos na PK, trunca lista por conta do limite de campos na clusterizacao da tabela no BQ
    campos_pk      = fields_base.query(" is_pk == 1 ").column_name.tolist() # [f"{row['column_name']}" for index, row in fields_base.iterrows() if row['is_pk'] == 1]
    campos_cluster = campos_pk[0:2] if num_fields_pk > 3 else campos_pk
    
    # 3) gera codigos SQL e dicionarios Python de mapeamento
    # 3.1.1) gera SQL para tabela externa no BQ (sem definir schema)
    schema_fields_ext = ", ".join([f"{row['column_name']} string" for index, row in fields_base.iterrows()])
    sql_create_ext    = f"""
        create or replace external table `gcp-project-id.raw_{bq_dataset}.ext_{bq_table_name}` (
            {schema_fields_ext}
        )
        options (
            format = 'csv',
            encoding = 'utf-8',
            preserve_ascii_control_characters=true,
            uris = ['gs://temp_local_digital_magic/{sql_table}/*.csv', 'gs://temp_local_digital_magic_instantly/{sql_table}/*.csv'],
            field_delimiter = ';',
            skip_leading_rows = 1
        );
    """
    #encoding = 'ISO_8859_1',
    # 3.1.2) gera SQL para tabela RAW no BQ (especificando schema)
    campos_create_raw  = ", ".join([f"{row['column_name']} {row['data_type']}" for index, row in fields_base.iterrows()])
    campos_for_cluster = ", ".join(campos_cluster + ['ts_dt_update'])
    cluster_clause     = f" cluster by {campos_for_cluster}"
    sql_create_raw_all_records = f"""
        create or replace table `gcp-project-id.raw_{bq_dataset}.{bq_table_name}` (
            {campos_create_raw}
            ,arquivo string
            ,ts_dt_update timestamp default current_timestamp()
        )
        partition by date(ts_dt_update)
        {cluster_clause};
    """
    
    
    # 3.1.3) define mapeamento para insert na RAW a partir da EXT
    def value_for_insert(row, for_where=True):
        value   = f"trim({row['column_name']})"
        alias   = f" as {row['column_name']} " if for_where else ' is not null '
        
        if row['data_type'] == 'int':
            casting = f"safe_cast(safe_cast({value} as numeric) as int)" 
        elif row['data_type'] == 'time':
            casting = f"safe_cast(replace({value}, '.0000000', '') as time)"
        elif row['data_type'] == 'datetime':
            casting = f"ifnull(safe_cast({value} as datetime), safe.PARSE_DATE('%d/%m/%Y', {value}))" 
        else:
            casting = f"safe_cast({value} as {row['data_type']})"
        
        return f"{casting} {alias}"
    
    if load_frequency_min not in ('0', '60', '1440'):
        return ["O intervalo entre as cargas (frequencia) e invalido"]

    td_dict_raw_all = {
        bq_table_name: {
            'file_pattern': f"{sql_table}/{sql_table}", 
            'insert_fields': " " + ", ".join(fields_base.column_name.to_list()) + " ", 
            'select_fields': " " + ", ".join([value_for_insert(row) for index, row in fields_base.iterrows()]) + " ",
            'conditions': (" and " if num_fields_pk else "") + " and ".join([value_for_insert(row, False) for index, row in fields_base.iterrows() if row['is_pk'] == 1]),
            'has_subquery': "0",
            'load_frequency': load_frequency_min
        },
    }
    
    
    # 3.2) se a tabela no SQL SERVER nao tiver PK, os codigos devem ser gerados de forma MANUAL
    if verify_pk == False or num_fields_pk:
        # 3.2.1) tabela RAW versionada
        sql_create_raw_versioned = f"""
            create or replace table `gcp-project-id.{bq_dataset}.{bq_table_name}` (
                {campos_create_raw}
                ,ts_dt_update timestamp 
                ,ts_dt_update_previous timestamp 
                ,im_fingerprint integer 
            )
            partition by date(ts_dt_update)
            {cluster_clause};
        """
    
        # 3.2.2) define mapeamento para SCD (insert na RAW versionada)
        versioned_fields = fields_base.column_name.to_list() + ['ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint']
        td_dict_raw_version = {
            bq_table_name: {
                'target_dataset': bq_dataset,
                'target_table': bq_table_name,
                'target_fields': versioned_fields,
                'source_dataset': f"raw_{bq_dataset}",
                'source_table': bq_table_name,
                'source_fields': versioned_fields,
                'array_type_fields': [],
                'fields_to_match': [(row['column_name'], row['column_name']) for index, row in fields_base.iterrows() if row['is_pk'] == 1],
                'fields_to_delete': (),
                'specific_clause_del': '',
                'partition_clause_tmp': (", ".join([row['column_name'] for index, row in fields_base.iterrows() if row['is_pk'] == 1]), 'ts_dt_update desc, arquivo desc'),
                'grouping_tmp': ()
            }
        }
        
        # 4) agrupa codigos DDL para rodarem juntos e agrupa dicionarios de mapeamento
        ddls = f"""
            BEGIN
                {sql_create_ext}
                {sql_create_raw_all_records}
                {sql_create_raw_versioned}
            END;
        """
        dicts = {'gcf_1_insert_raw_table': td_dict_raw_all, 'gcf_2_scd_versioning': td_dict_raw_version}
        
    else:
        # 4) agrupa codigos DDL para rodarem juntos
        ddls = f"""
            BEGIN
                {sql_create_ext}
                {sql_create_raw_all_records}
            END;
        """
        dicts = {'gcf_1_insert_raw_table': td_dict_raw_all}
    
    return ddls, dicts
    

def update_local_git_repository() -> bool:
    os.chdir(GIT_REPOSITORY_PATH)
    commands = [
        "import os", 
        "os.system('git pull origin main --quiet')"
    ]
    result = subprocess.run([sys.executable, "-c", "\n".join(commands)], capture_output=True, text=True)
    os.chdir(files_path)
    
    if len(result.stderr):
        raise Exception(f"Ocorreu um problema ao atualizar o repositorio local: {result.stderr}")
    
    return True


def update_auto_table_definitions(bq_dataset: str, table_mappings: dict) -> None:
    bq_dataset = f"raw_{bq_dataset}"
    
    for function in table_mappings.keys():
        # 1) recupera conteudo do arquivo para um dicionario
        file_name = f"{GIT_REPOSITORY_PATH}\\load_data_process_functions\\{function}\\table_definitions_auto.py"
        with open(file_name, 'r') as f: # , encoding='utf-8'
            current_str = f.read().replace('auto_td = ', '')
        all_datasets_dict = defaultdict(dict, ast.literal_eval(current_str))
        
        # 2) adiciona mapeamento de nova base no devido dataset e atualiza dicionario geral
        new_datset_item = all_datasets_dict[bq_dataset] | table_mappings[function]
        all_datasets_dict[bq_dataset] = new_datset_item
        
        # 3) passa dicionario para string e sobreescreve conteudo do arquivo
        new_str = 'auto_td = ' + json.dumps(all_datasets_dict) # talvez tenha que ver melhor isso aqui depois
        with open(file_name, 'w') as f: # , encoding='utf-8'
            f.write(new_str)


def stage_changes_git_repository(bq_dataset: str, mapped_table: str) -> None:
    os.chdir(GIT_REPOSITORY_PATH)
    commands = [
        "import os", 
        "os.system('git add .')",
        f"os.system('git commit -m \"Adiciona mapeamento de {mapped_table} no dataset {bq_dataset}\"')"
    ]
    result = subprocess.run([sys.executable, "-c", "\n".join(commands)], capture_output=True, text=True)
    os.chdir(files_path)
    
    if len(result.stderr):
        raise Exception(f"Ocorreu um problema ao commitar alteracoes: {result.stderr}")


def drop_tables_statement(bq_dataset: str, table_name: str) -> str:
    return f"""
        BEGIN
            DROP TABLE IF EXISTS `gcp-project-id.raw_{bq_dataset}.ext_{table_name}`;
            DROP TABLE IF EXISTS `gcp-project-id.raw_{bq_dataset}.{table_name}`;
            DROP TABLE IF EXISTS `gcp-project-id.{bq_dataset}.{table_name}`;
        END;
    """


def discard_changes_git_repository() -> None:
    os.chdir(GIT_REPOSITORY_PATH)
    commands = [
        "import os", 
        "os.system('git restore .')"
    ]
    result = subprocess.run([sys.executable, "-c", "\n".join(commands)], capture_output=True, text=True)
    os.chdir(files_path)


def push_to_remote_git_repository() -> None:
    os.chdir(GIT_REPOSITORY_PATH)
    commands = [
        "import os", 
        "os.system('git push origin main')"
    ]
    subprocess.run([sys.executable, "-c", "\n".join(commands)], capture_output=True, text=True)
    os.chdir(files_path)
    

def upload_bases_to_bq(
    frequency_to_upload: str    = '1440', 
    specific_folders: list      = [], 
    excluded_folders_plus: list = [], 
    debug_mode: bool            = True, 
    verify_pk: bool             = True
) -> None:
    
    bucket_name          = AVAILABLE_BUCKETS_TO_UPLOAD_FILES.get(frequency_to_upload, 'temp_local_files_bucket_daily')
    bucket               = storage_client.bucket(bucket_name)
    has_created_new_base = False
    starting_folder      = 0
    excluded_folders     = ['TB_DATA_1', 'TB_DATA_2', 'TB_DATA_3'] + excluded_folders_plus
    folders_to_upload    = specific_folders if len(specific_folders) > 0 else [i for i in get_folders_to_upload(frequency_to_upload) if isdir(i) and i not in excluded_folders]
    
    # PLACE HOLDER FOR CODE: cria registro de log para 'INICIO' do processo de upload
    
    # tabelas que nao seguem o padrao de nomenclatura e precisam de mapeamento
    de_para_bases_bq = {
        'nome_que_nao_segue_padrao': 'nome_da_tabela_seguindo_padrao',
    }
    
    for folder in folders_to_upload[starting_folder:]:
        try:            
            if 'backup' not in os.listdir(folder):
                os.mkdir(os.path.join(folder, 'backup'))
            
            # 1) upload dos arquivos dessa base/tabela
            bq_table_name = folder.replace('TB_', '').replace('EXT_', '').replace('RAW_', '').replace('TMP_', '').replace('ARQUIVO_', '').lower()
            bq_table_name = de_para_bases_bq[bq_table_name] if bq_table_name in list(de_para_bases_bq.keys()) else bq_table_name
            file          = None
            files         = [j for j in os.listdir(folder) if isfile(join_path(folder, j))]
            
            # 1.1) envia cada um dos arquivos e cria registro de log
            # PLACE HOLDER FOR CODE: cria registro de log para 'INICIO' de upload dos arquivos em `files`
            try:
                uploaded_files = []
                for file in files:
                    blob = bucket.blob(f"{folder}/{file}")
                    blob.upload_from_filename(join_path(folder, file))
                    uploaded_files.append(file)
            except Exception as e:
                failed_files = [ff for ff in files if ff not in uploaded_files]
                # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' de upload dos arquivos em `failed_files`
                continue
            
            # PLACE HOLDER FOR CODE: cria registro de log para 'FIM' de upload dos arquivos em `uploaded_files`
        
        
            
            # 2) verifica se a base existe no BQ
            # 2.1) consulta tabela de referencia no banco SQL local
            ref_table  = conn.query(f"""
                select top 1 table_catalog, bq_dataset, load_frequency_min
                from tb_ref_tables 
                where integration_bq = 1 and raw_table_name = '{folder}'
                order by dt_update desc; 
            """)
            
            if ref_table.shape[0] == 0:
                # base nao esta na tabela de referencia do banco SQL local. Teoricamente, se cair nesse IF eh uma pasta perdida
                move_files_to_backup(folder, files)
                # PLACE HOLDER FOR CODE: cria registro de log para 'WARNING' de base nao existente no banco SQL local para arquivos em `files`
                continue
            
            table_catalog      = ref_table['table_catalog'].to_string(index=False)
            bq_dataset         = ref_table['bq_dataset'].to_string(index=False)
            load_frequency_min = ref_table['load_frequency_min'].to_string(index=False)
            must_create_table  = table_must_be_created_in_bq(bq_table_name, bq_dataset, file)
            
            
            # 2.2) SE base NAO existe no BQ, executa processo automatico para criar a base
            if must_create_table:
                generated_codes = generate_ddl_codes(sql_db=table_catalog, 
                                                     sql_table=folder, 
                                                     bq_dataset=bq_dataset, 
                                                     bq_table_name=bq_table_name, 
                                                     load_frequency_min=load_frequency_min, 
                                                     example_file=file, 
                                                     verify_pk=verify_pk)
                
                if len(generated_codes) == 1:
                    # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' na criacao bases no BQ e/ou codigos de mapeamento
                    move_files_to_backup(folder, files)
                    continue
                
                generated_ddls, generated_mappings = generated_codes[0], generated_codes[1]
                
                try:
                    # atualiza repositorio Git local das cloud functions
                    git_repository_updated = False
                    git_repository_updated = update_local_git_repository()
                    
                    bq_client.query(generated_ddls).result() # cria tabelas no BQ
                    
                    # atualiza arquivo de definicoes automaticas de tabelas nas cloud functions e coloca alteracoes em staging no repositorio Git local
                    update_auto_table_definitions(bq_dataset, generated_mappings) 
                    stage_changes_git_repository(bq_dataset, bq_table_name)
                    has_created_new_base = True
                    
                except Exception as e:
                    bq_client.query(drop_tables_statement(bq_dataset, bq_table_name)).result()
                    discard_changes_git_repository()
                    
                    if git_repository_updated:
                        # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' na atualizacao do mapeamento das bases no codigo da funcao serverless
                        pass
                    else:
                        # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' na atualizacao do repositorio Git local
                        pass
            
            # 3) move arquivos que ja foram carregados para pasta de backup
            move_files_to_backup(folder, files)
        
        except Exception as e:
            # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' na operacao de upload das bases no BQ
            continue
    
    if has_created_new_base:
        try:
            # realiza o push das alteracoes para o repositorio Git remoto das cloud functions
            push_to_remote_git_repository()
            # PLACE HOLDER FOR CODE: cria registro de log para 'SUCESSO' do push para repositorio Git remoto
            pass
            
        except Exception as e:
            # PLACE HOLDER FOR CODE: cria registro de log para 'FALHA' do push para repositorio Git remoto
            pass
    
    # PLACE HOLDER FOR CODE: cria registro de log para 'FIM' do processo de upload


upload_bases_to_bq()
# upload_bases_to_bq(folders_to_run=['TB_EXEMPLO_DADOS_A'])
# upload_bases_to_bq(folders_to_run=['TB_EXEMPLO_DADOS_A'], verify_pk=False)