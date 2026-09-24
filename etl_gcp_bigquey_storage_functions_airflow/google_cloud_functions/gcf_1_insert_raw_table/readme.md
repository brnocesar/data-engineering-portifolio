# `gcf_1_insert_raw_table`

> Retrieves data from files in **Cloud STORAGE** and inserts it into the respective _RAW_ tables.
>
> All parameters are required: `dataset` (name of the _RAW_ tables' _dataset_), `table` (name of the _RAW_ table), `date` (competency date of the files, formatted `'yyyy-mm-dd'`, `None` for the current day) and `special_daylie_frequency` (used only to force a daily load outside the standard schedule; use `None` otherwise).

## Base mapping

This _function_ knows nothing about the bases it processes: for each table, an `INSERT INTO ... SELECT ... FROM <dataset>.ext_<table>` is dynamically built from the dictionary defined in `table_definitions_manual.py` (or automatically generated in `table_definitions_auto.py`). The structure is:

```python
manual_td = {
    '<dataset>': {
        '<raw_table>': {
            'file_pattern': '<folder>/<file_prefix>',
            'insert_fields': 'field_a, field_b, ...',
            'select_fields': "SAFE_CAST(TRIM(field_a) AS ...) AS field_a, ...",
            'conditions': " AND field_a IS NOT NULL ",
            'has_subquery': "0",
            'load_frequency': "1440"
        }
    }
}
```

- `'file_pattern'`: prefix (folder + start of the file name) used to build the regular expression that filters, in the external table, only the files for the desired competency (`date`) and frequency.
- `'load_frequency'`: `'1440'`, `'60'` or `'0'`; defines the expected date/time suffix format in the file name and how that suffix is combined with `'file_pattern'` to locate the right files.
- `'insert_fields'`: list of columns (in the same order as `'select_fields'`) used in the `INSERT INTO` of the _RAW_ table.
- `'select_fields'`: conversion expressions (`SAFE_CAST`, `TRIM`, etc.) applied to the external table's `STRING` columns, producing the typed values inserted into `'insert_fields'`.
- `'conditions'`: additional `AND ...` clause applied in the `WHERE`, usually validating that key fields aren't null before accepting the row.
- `'has_subquery'`: `"0"` when `'select_fields'` is applied directly over the external table; any other value indicates that `'select_fields'` already contains a full subquery (with its own `FROM`/`GROUP BY`), used when the data needs to be pre-aggregated (for example, to build _array_/_struct_ fields) before the insert.

Every key exists solely for the `INSERT` into the _RAW_ table; the mapping used for versioning (SCD) is independent and defined in the `table_definitions_manual.py`/`table_definitions_auto.py` files of `gcf_2_scd_versioning`.
