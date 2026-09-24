# `gcf_1_insert_raw_table`

> Recupera dados de arquivos no **Cloud STORAGE** e insere nas respectivas tabelas _RAW_.
>
> Todos os parâmetros são obrigatórios, são eles: `dataset` (nome do _dataset_ das tabelas _RAW_), `table` (nome da tabela _RAW_), `date` (data de competência dos arquivos no formato `'aaaa-mm-dd'`, `None` para o dia atual) e `special_daylie_frequency` (usado apenas para forçar uma carga diária fora do agendamento padrão; use `None` caso contrário).

## Mapeamento das bases

Essa _function_ não sabe nada sobre as bases que processa: para cada tabela é montado dinamicamente um `INSERT INTO ... SELECT ... FROM <dataset>.ext_<tabela>` a partir do dicionário definido em `table_definitions_manual.py` (ou gerado automaticamente em `table_definitions_auto.py`). A estrutura é:

```python
manual_td = {
    '<dataset>': {
        '<tabela_raw>': {
            'file_pattern': '<pasta>/<prefixo_do_arquivo>',
            'insert_fields': 'campo_a, campo_b, ...',
            'select_fields': "SAFE_CAST(TRIM(campo_a) AS ...) AS campo_a, ...",
            'conditions': " AND campo_a IS NOT NULL ",
            'has_subquery': "0",
            'load_frequency': "1440"
        }
    }
}
```

- `'file_pattern'`: prefixo (pasta + início do nome do arquivo) usado para montar a expressão regular que filtra, na tabela externa, apenas os arquivos da competência (`date`) e frequência desejadas.
- `'load_frequency'`: `'1440'`, `'60'` ou `'0'`; define o formato do sufixo de data/hora esperado no nome do arquivo e como esse sufixo é combinado com `'file_pattern'` para localizar os arquivos certos.
- `'insert_fields'`: lista de colunas (na ordem do `'select_fields'`) usada no `INSERT INTO` da tabela _RAW_.
- `'select_fields'`: expressões de conversão (`SAFE_CAST`, `TRIM` etc.) aplicadas sobre as colunas `STRING` da tabela externa, produzindo os valores tipados inseridos em `'insert_fields'`.
- `'conditions'`: cláusula `AND ...` adicional aplicada no `WHERE`, normalmente validando que os campos de chave não estejam nulos antes de aceitar a linha.
- `'has_subquery'`: `"0"` quando o `'select_fields'` é aplicado diretamente sobre a tabela externa; qualquer outro valor indica que `'select_fields'` já contém uma subquery completa (com seu próprio `FROM`/`GROUP BY`), usada quando é preciso pré-agregar os dados (por exemplo, para montar campos do tipo _array_/_struct_) antes do insert.

Cada chave existe em função exclusivamente do `INSERT` na tabela _RAW_; o mapeamento usado no versionamento (SCD) é independente e fica definido nos arquivos `table_definitions_manual.py`/`table_definitions_auto.py` de `gcf_2_scd_versioning`.
