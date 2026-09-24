manual_td = {
    'hogwarts': {
        'titulo_bruxo_ref': {
            # registros depara de titulos magicos entre o Ministerio da Magia e Hogwarts
            'file_pattern': 'TITULO_BRUXO/TITULO_DE', 
            'insert_fields': 'tipo_registro, dt_referencia, id_ministerio, id_hogwarts, descricao',
            'select_fields': """
                TRIM(tipo_registro) as tipo_registro 
                ,SAFE.PARSE_DATE('%d.%m.%Y', TRIM(dt_fim)) AS dt_referencia 
                ,TRIM(id_ministerio) as id_ministerio 
                ,TRIM(id_hogwarts) as id_hogwarts 
                ,TRIM(descricao) as descricao
            """,
            'conditions': " AND SAFE.PARSE_DATE('%d.%m.%Y', TRIM(dt_fim)) IS NOT NULL AND SAFE_CAST(TRIM(id_hogwarts) AS INT64) IS NOT NULL AND SUBSTR(UPPER(TRIM(id_ministerio)), 1, 2) = 'JC' ",
            'has_subquery': "0",
            'load_frequency': "1440"
        },
        'estrutura_casas': {
            'file_pattern': 'CASAS_HOGWARTS/CASA_DE', 
            'insert_fields': 'dt_ano_mes, tipo_registro, cod_casa, cod_salao, descricao',
            'select_fields': """
                DATE_TRUNC(SAFE.PARSE_DATE('%d_%m_%Y', REGEXP_EXTRACT(_FILE_NAME, r'\d{2}_\d{2}_\d{4}')), MONTH) AS dt_ano_mes,
                TRIM(tipo_registro) AS tipo_registro 
                ,TRIM(cod_casa) AS cod_casa 
                ,TRIM(cod_salao) AS cod_salao 
                ,TRIM(descricao) AS descricao
            """,
            'conditions': " AND SAFE_CAST(TRIM(cod_salao) AS INT64) IS NOT NULL AND SUBSTR(UPPER(TRIM(cod_casa)), 1, 2) NOT IN ('PO', 'JC') ",
            'has_subquery': "0",
            'load_frequency': "1440"
        },
    },
    'ministerio_magia': {
        'cargos_curandeiros': {
            'file_pattern': 'TB_TMP_CURANDEIROS/TB_TMP_CURANDEIROS', 
            'insert_fields': " vassoura_cedida, id_hogwarts, id_ministerio, cota_aprendiz_magico, elegivel_po_de_flu, grupo_exposicao_magica, adicional_risco_magico, periodo ",
            'select_fields': """
                trim(vassoura_cedida) as vassoura_cedida
                ,safe_cast(id_hogwarts as int) as id_hogwarts
                ,trim(id_ministerio) as id_ministerio
                ,trim(cota_aprendiz_magico) as cota_aprendiz_magico
                ,trim(elegivel_po_de_flu) as elegivel_po_de_flu
                ,trim(grupo_exposicao_magica) as grupo_exposicao_magica
                ,trim(adicional_risco_magico) as adicional_risco_magico
                ,safe.PARSE_DATE('%d/%m/%Y', trim(periodo)) as periodo
            """,
            'conditions': " and safe.PARSE_DATE('%d/%m/%Y', trim(periodo)) IS NOT NULL and safe_cast(id_hogwarts as int) is not null ",
            'has_subquery': "0",
            'load_frequency': "1440"
        },
    },
    'pergaminhos': {
        'pergaminho_ponto': {
            'file_pattern': 'PERGAMINHO_PONTO/registro_ponto',
            'insert_fields': ' matricula_bruxo, periodo_referencia, lancamento ',
            'select_fields': """
                    * 
                from (
                    select matricula_bruxo, periodo_referencia, ARRAY_AGG(lancamento) as lancamento, _FILE_NAME, ts_dt_update
                    from (
                        SELECT 
                            safe_cast(trim(string_field_1) as int) as matricula_bruxo 
                            ,safe.PARSE_DATE('%d.%m.%Y', trim(string_field_14)) as periodo_referencia 
                            ,struct(
                                ,safe_cast(trim(string_field_3) as string) as nome_bruxo 
                                ,nullif(safe_cast(trim(string_field_4) as string), '') as num_identificacao 
                                ,safe_cast(trim(string_field_5) as string) as departamento 
                                ,CASE WHEN RIGHT(TRIM(string_field_28), 1) = '-' 
                                    THEN (-1)*SAFE_CAST(REPLACE(REPLACE(REPLACE(TRIM(string_field_28), '.', ''), ',', '.'), '-', '') as numeric) 
                                    ELSE SAFE_CAST(REPLACE(REPLACE(TRIM(string_field_28), '.', ''), ',', '.') as numeric) 
                                END as valor 
                                ,safe_cast(trim(string_field_30) as string) as moeda 
                            ) as lancamento
                            ,_FILE_NAME
                            ,ts_dt as ts_dt_update
                        FROM `gcp-project-id.raw_rpa.ext_pergaminho_ponto` 
                        where safe.PARSE_DATE('%d.%m.%Y', trim(string_field_14)) is not null
                    )
                    group by matricula_bruxo, periodo_referencia, _FILE_NAME, ts_dt_update
                )
            """,
            'conditions': "  ",
            'has_subquery': "1",
            'load_frequency': "1440"
        },
        'pergaminho_ausencias': {
            # registro de todas as ausencias de bruxos em vigor (que terminam) do mes anterior em diante
            'file_pattern': 'AUSENCIAS_BRUXOS/EXTRATO_AUSENCIAS',
            'insert_fields': 'id_bruxo,nome_bruxo,dt_inicio,dt_fim,nome_licenca,qtd_horas',
            'select_fields': """
                id_bruxo
                ,UPPER(TRIM(nome_bruxo)) AS nome_bruxo
                ,SAFE.PARSE_DATE('%d.%m.%Y', TRIM(dt_inicio)) AS dt_inicio
                ,SAFE.PARSE_DATE('%d.%m.%Y', TRIM(dt_fim)) AS dt_fim
                ,TRIM(nome_licenca) AS nome_licenca
                ,CASE 
                    WHEN RIGHT(TRIM(qtd_horas),1) = '-' THEN (-1) * SAFE_CAST(REPLACE(REPLACE(TRIM(qtd_horas),',','.'),'-','') AS numeric)
                    ELSE SAFE_CAST(REPLACE(TRIM(qtd_horas),',','.') AS numeric)
                END AS qtd_horas 
            """,
            'conditions': " AND id_bruxo IS NOT NULL ",
            'has_subquery': "0",
            'load_frequency': "1440"
        },
        'pergaminho_ferias': {
            # registros de ferias gozadas pelos bruxos no ano
            'file_pattern': 'FERIAS_BRUXOS/XFERIAS',
            'insert_fields': 'id_bruxo ,nome_bruxo ,dt_inicio ,dt_fim ,tipo_licenca ,qtd_dias ,perc_adiantamento ,flag_pagto_gratificacao_natalina',
            'select_fields': """
                id_bruxo
                ,UPPER(TRIM(nome_bruxo)) as nome_bruxo
                ,SAFE.PARSE_DATE('%d.%m.%Y', TRIM(dt_inicio)) as dt_inicio
                ,SAFE.PARSE_DATE('%d.%m.%Y', TRIM(dt_fim)) as dt_fim
                ,TRIM(tipo_licenca) as tipo_licenca
                ,SAFE_CAST(REPLACE(TRIM(qtd_dias),',','.') as numeric) as qtd_dias
                ,IF(TRIM(perc_adiantamento) <> '', TRIM(perc_adiantamento), NULL) AS perc_adiantamento
                ,IF(TRIM(flag_pagto_gratificacao_natalina) = 'X', True, False) AS flag_pagto_gratificacao_natalina
            """,
            'conditions': " AND id_bruxo IS NOT NULL ",
            'has_subquery': "0",
            'load_frequency': "1440"
        },
    },
}
