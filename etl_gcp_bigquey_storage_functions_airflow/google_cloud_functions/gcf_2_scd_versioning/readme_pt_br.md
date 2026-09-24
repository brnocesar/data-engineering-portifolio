# `gcf_2_scd_versioning`

> Realiza o processo de SCD nas tabelas versionadas usando a carga mais recente e adequada que existe nas tabelas raw (denominada "carga atual qualificada").
>
> Todos os parâmetros são obrigatórios, sendo eles: `dataset` (nome do dataset raw), `table` (nome da tabela raw) e `ts_dt` (_timestamp_ da carga na tabela raw ). Se for passado o valor `None` para o instante da carga que deve ser utilizada na atualização, será recuperado o instante da carga atual qualificada.

## Mapeamento das bases

Essa _function_ também não sabe nada sobre as bases que processa: o `MERGE` de versionamento (SCD) é montado dinamicamente a partir do dicionário definido em `table_definitions_manual.py` (ou gerado automaticamente em `table_definitions_auto.py`). A estrutura é:

```python
manual_td = {
    '<dataset-fonte>': {
        '<tabela-fonte>': {
            'target_dataset': '',
            'target_table': '',
            'target_fields': ['id_bruxo', '...', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': '<dataset-fonte>',
            'source_table': '<tabela-fonte>',
            'source_fields': ['id_bruxo', '...', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'array_type_fields': [],
            'fields_to_match': [('id_bruxo', 'id_bruxo'), ('dt_ini', 'dt_ini'), ('dt_fim', 'dt_fim')],
            'fields_to_delete': ('dt_ini', 'dt_fim'),
            'specific_clause_del': '',
            'partition_clause_tmp': ('id_bruxo, dt_ini, dt_fim', 'dt_inclusao desc, dt_modificacao desc, dt_ini_periodo_aquisitivo desc'),
            'grouping_tmp': ()
        }
    }
}
```

- `<dataset-fonte>`: é o _dataset_ em que a tabela RAW está
- `<tabela-fonte>`: nome da tabela RAW
- `'target_dataset'` e `'target_table'`: _dataset_ onde deve ficar a tabela versionada e o nome dessa tabela, respectivamente
- `'target_fields'` e `'source_fields'`: é a lista de campos das respectivas tabelas. Em geral essas duas listas são iguais e para determiná-la de forma simples basta:
  - (i) pegar todos os campos da tabela RAW (_source_), com exceção do campo `'arquivo'`, e
  - (ii) adicionar os campos `'ts_dt_update_previous'` e `'im_fingerprint'`  
A tabela RAW (_source_) não tem os campos `'ts_dt_update_previous' e 'im_fingerprint'`, mas isso é devido a uma "decisão de implementação nos estágios iniciais do projeto".  
Caso o nome dos respectivos campos não sejam iguais nas duas tabelas (RAW e versionada) é necessário fazer o devido ajuste.  
- `'array_type_fields'`: lista de campos do tipo _array_ na tabela versionada. Se não tiver nenhum, basta manter a lista vazia
- `'fields_to_match'`: lista de pares (tuplas) dos campos usados para avaliar o match no merge, cada tupla tem o campo da tabela target (primeira posição) e o respectivo campo na RAW (segunda posição)
- `'fields_to_delete'`: campos avaliados no `DELETE` de registros que não devem mais existir. É uma tupla que pode receber **zero**, **um** ou **dois** valores:
  - **Dois valores**: Na primeira posição fica o campo que determina o INICIO do intervalo de tempo, e na segunda, o campo que indica o fim do intervalo.
  - **Um valor**: Passando uma tupla com apenas um valor, ou seja, `'fields_to_delete': ('date_field',)` (não se esqueça da vírgula), serão consideradores para DELETE apenas registros de datas que existam na carga de dados usados no SCD. Essa opção pode ser usada em conjunto com o próximo item de mapeamento que será descrito, `'specific_clause_del'`.
  - **Zero valores**: Se essa operação não for necessária, deve ser passada uma tupla vazia.
- `'specific_clause_del'`: quando a lógica montada para o item acima não contemplar a necessidade da base versionada, especifique aqui o `DELETE` que deve ser utilizado. Mas sinceramente, esse item foi uma gambiarra meio preguiçosa, então se você achar que deve usar isso, da uma pensadinha a mais.
- seleção dos dados na RAW: os itens `'partition_clause_tmp'` e `'grouping_tmp'` são excludentes, ou seja, se uso um não posso usar o outro. Servem para informar como será feita a seleção dos dados da tabela RAW que serão inseridos na versionada
  - `'partition_clause_tmp'`: é uma tupla em que o primeiro item é uma _string_ com os campos do `"partition by"` separado por vírgula e o segundo são os campos do `"order by"`. Este _partition_ é realizado na tabela temporária criada a partir da tabela RAW (_source_), então verifique bem o nome dos campos (isso é importante para o caso em que campos respectivos nas tabelas _target_ e _source_ não possuem o mesmo nome)
  - `'grouping_tmp'`: é uma tupla de três elementos. O primeiro item são os campos do `"group by"`, o segundo é a função de agragação que será aplicada e o último é o campo passado para essa função.

Cada chave existe em função exclusivamente do `MERGE` na tabela versionada; o mapeamento usado no _insert_ da tabela RAW é independente e fica definido nos arquivos `table_definitions_manual.py`/`table_definitions_auto.py` de `gcf_1_insert_raw_table`.
  