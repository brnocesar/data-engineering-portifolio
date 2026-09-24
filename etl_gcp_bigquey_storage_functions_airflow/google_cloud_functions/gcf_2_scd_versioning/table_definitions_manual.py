manual_td = {
    'hogwarts': {
        'titulo_bruxo_ref': {
            # registros depara de titulos magicos entre o Ministerio da Magia e Hogwarts
            'target_dataset': 'support',
            'target_table': 'ref_titulo_bruxo',
            'target_fields': ['tipo_registro', 'id_ministerio', 'id_hogwarts', 'descricao', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': 'raw_rpa',
            'source_table': 'titulo_bruxo_ref',
            'source_fields': ['tipo_registro', 'id_ministerio', 'id_hogwarts', 'descricao', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': [],
            'fields_to_match': [('id_ministerio', 'id_ministerio'), ('id_hogwarts', 'id_hogwarts')],
            'fields_to_delete': (),
            'specific_clause_del': '',
            'partition_clause_tmp': ('id_ministerio, id_hogwarts', 'ts_dt_update desc'),
            'grouping_tmp': ()
        },
        'estrutura_casas': {
            'target_dataset': 'departamentos_hogwarts',
            'target_table': 'estrutura_casas_de_para',
            'target_fields': ['tipo_registro', 'cod_casa', 'cod_salao', 'descricao', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': 'hogwarts',
            'source_table': 'estrutura_casas',
            'source_fields': ['tipo_registro', 'cod_casa', 'cod_salao', 'descricao', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': [],
            'fields_to_match': [('cod_casa', 'cod_casa'), ('cod_salao', 'cod_salao')], 
            'fields_to_delete': (),
            'specific_clause_del': '',
            'partition_clause_tmp': ('cod_casa, cod_salao', 'ts_dt_update desc'),
            'grouping_tmp': () 
        },
    },
    'ministerio_magia': {
        'cargos_curandeiros': {
            'target_dataset': 'setor_hospitalar',
            'target_table': 'cargos_curandeiros',
            'target_fields': ['vassoura_cedida', 'id_hogwarts', 'id_ministerio', 'cota_aprendiz_magico', 'elegivel_po_de_flu', 'grupo_exposicao_magica', 
                                'adicional_risco_magico', 'periodo', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': 'ministerio_magia',
            'source_table': 'cargos_curandeiros',
            'source_fields': ['vassoura_cedida', 'id_hogwarts', 'id_ministerio', 'cota_aprendiz_magico', 'elegivel_po_de_flu', 'grupo_exposicao_magica', 
                                'adicional_risco_magico', 'periodo', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': [],
            'fields_to_match': [('periodo', 'periodo'), ('id_hogwarts', 'id_hogwarts'), ('id_ministerio', 'id_ministerio')],
            'fields_to_delete': ('periodo', 'periodo'),
            'specific_clause_del': '',
            'partition_clause_tmp': ('periodo, id_hogwarts, id_ministerio', 'ts_dt_update desc'),
            'grouping_tmp': ()
        },
    },
    'raw_pergaminhos': {
        'pergaminho_ponto': {
            'target_dataset': 'frequencia',
            'target_table': 'pergaminho_ponto',
            'target_fields': ['matricula_bruxo', 'periodo_referencia', 'lancamento', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': 'raw_pergaminhos',
            'source_table': 'pergaminho_ponto',
            'source_fields': ['matricula_bruxo', 'periodo_referencia', 'lancamento', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': ['lancamento'],
            'fields_to_match': [('matricula_bruxo', 'matricula_bruxo'), ('periodo_referencia', 'periodo_referencia')],
            'fields_to_delete': ('periodo_referencia', 'periodo_referencia'),
            'specific_clause_del': '',
            'partition_clause_tmp': ('matricula_bruxo, periodo_referencia', 'ts_dt_update desc'),
            'grouping_tmp': ()
        },
        'pergaminho_ausencias': {
            # registro de todas as ausencias de bruxos em vigor (que terminam) do mes anterior em diante
            'target_dataset': 'frequencia',
            'target_table': 'ausencias_bruxos',
            'target_fields': ['id_bruxo', 'nome_bruxo', 'dt_inicio', 'dt_fim', 'nome_licenca', 'qtd_dias', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': 'raw_pergaminhos',
            'source_table': 'pergaminho_ausencias',
            'source_fields': ['id_bruxo', 'nome_bruxo', 'dt_inicio', 'dt_fim', 'nome_licenca', 'qtd_horas', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': [],
            'fields_to_match': [('id_bruxo', 'id_bruxo'), ('dt_inicio', 'dt_inicio'), ('nome_licenca', 'nome_licenca')],
            'fields_to_delete': ("dt_fim",),
            'specific_clause_del': "select min(dt_fim) from `gcp-project-id.raw_pergaminhos.pergaminho_ausencias` where ts_dt_update = ts_dt",
            'partition_clause_tmp': (),
            'grouping_tmp': ('id_bruxo, nome_bruxo, dt_inicio, dt_fim, nome_licenca, ts_dt_update', 'sum', 'qtd_horas')
        },
        'pergaminho_ferias': {
            # registros de ferias gozadas pelos bruxos no ano
            'target_dataset': 'frequencia',
            'target_table': 'ferias_bruxos',
            'target_fields': ['id_bruxo', 'nome_bruxo', 'dt_inicio', 'dt_fim', 'tipo_licenca', 'qtd_dias', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': 'raw_pergaminhos',
            'source_table': 'pergaminho_ferias',
            'source_fields': ['id_bruxo', 'nome_bruxo', 'dt_inicio', 'dt_fim', 'tipo_licenca', 'qtd_dias', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': [],
            'fields_to_match': [('id_bruxo', 'id_bruxo'), ('dt_inicio', 'dt_inicio'), ('dt_fim', 'dt_fim')],
            'fields_to_delete': ('dt_fim', 'dt_fim'),
            'specific_clause_del': '',
            'partition_clause_tmp': ('id_bruxo, dt_inicio, dt_fim', 'dt_fim desc'),
            'grouping_tmp': ()
        },
    },
}