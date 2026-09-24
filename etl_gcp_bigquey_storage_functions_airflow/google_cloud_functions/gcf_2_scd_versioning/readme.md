# `gcf_2_scd_versioning`

> Runs the SCD process on the versioned tables using the most recent, adequate load that exists in the raw tables (referred to as the "current qualified load").
>
> All parameters are required: `dataset` (name of the raw dataset), `table` (name of the raw table) and `ts_dt` (_timestamp_ of the load in the raw table). If `None` is passed for the instant of the load to be used in the update, the instant of the current qualified load will be retrieved instead.

## Base mapping

This _function_ also knows nothing about the bases it processes: the versioning (SCD) `MERGE` is dynamically built from the dictionary defined in `table_definitions_manual.py` (or automatically generated in `table_definitions_auto.py`). The structure is:

```python
manual_td = {
    '<source-dataset>': {
        '<source-table>': {
            'target_dataset': '',
            'target_table': '',
            'target_fields': ['id_bruxo', '...', 'ts_dt_update', 'ts_dt_update_previous', 'im_fingerprint'],
            'source_dataset': '<source-dataset>',
            'source_table': '<source-table>',
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

- `<source-dataset>`: the _dataset_ where the RAW table is
- `<source-table>`: name of the RAW table
- `'target_dataset'` and `'target_table'`: dataset where the versioned table should live, and its name, respectively
- `'target_fields'` and `'source_fields'`: the list of fields for the respective tables. In general these two lists are identical, and to determine them in a simple way you just need to:
  - (i) take every field from the RAW table (_source_), except for the `'arquivo'` field, and
  - (ii) add the `'ts_dt_update_previous'` and `'im_fingerprint'` fields  
The RAW table (_source_) doesn't have the `'ts_dt_update_previous'` and `'im_fingerprint'` fields, but that's due to an "implementation decision made in the project's early stages".  
If the respective field names aren't the same in both tables (RAW and versioned), the proper adjustment must be made.  
- `'array_type_fields'`: list of _array_-type fields in the versioned table. If there are none, just keep the list empty
- `'fields_to_match'`: list of field pairs (tuples) used to evaluate the match in the merge; each tuple has the target table's field (first position) and the corresponding field in RAW (second position)
- `'fields_to_delete'`: fields evaluated in the `DELETE` of records that should no longer exist. It's a tuple that can receive **zero**, **one** or **two** values:
  - **Two values**: the first position holds the field that determines the START of the time interval, and the second, the field indicating the end of the interval.
  - **One value**: passing a tuple with a single value, i.e. `'fields_to_delete': ('date_field',)` (don't forget the comma), only records with dates present in the load used for SCD will be considered for DELETE. This option can be used together with the next mapping item described, `'specific_clause_del'`.
  - **Zero values**: if this operation isn't needed, an empty tuple should be passed.
- `'specific_clause_del'`: when the logic built for the item above doesn't cover the versioned base's need, specify here the `DELETE` that should be used. Honestly though, this item was a somewhat lazy workaround, so if you think you need to use it, think it through a bit more.
- selecting data from RAW: the `'partition_clause_tmp'` and `'grouping_tmp'` items are mutually exclusive, meaning if you use one you can't use the other. They're used to define how the data from the RAW table that will be inserted into the versioned one is selected
  - `'partition_clause_tmp'`: a tuple where the first item is a _string_ with the `"partition by"` fields separated by comma, and the second is the `"order by"` fields. This _partition_ is performed on the temporary table created from the RAW table (_source_), so check the field names carefully (this matters when the respective fields in the _target_ and _source_ tables don't share the same name)
  - `'grouping_tmp'`: a tuple of three elements. The first item is the `"group by"` fields, the second is the aggregation function that will be applied, and the last is the field passed to that function.

Every key exists solely for the `MERGE` into the versioned table; the mapping used for the RAW table _insert_ is independent and defined in the `table_definitions_manual.py`/`table_definitions_auto.py` files of `gcf_1_insert_raw_table`.
