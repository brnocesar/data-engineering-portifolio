# airflow

> DAGs and _plugins_ for Cloud Composer responsible for orchestrating calls to the _Cloud Functions_ (`gcf_1_insert_raw_table` and `gcf_2_scd_versioning`) that perform, respectively, the insert into the RAW table and the versioning (SCD) of each base.

## How the DAGs decide what to process

No DAG has a fixed list of bases: each one implements a `table_querys_ref_job()` function that queries the control table `gcp-project-id.control_etl_process.tb_ref_tables`, filtering by `update_frequency_min` (`1440`, `60` or `0`) and by `integration_bq = 1`. The result of this query (dataset, table and the `versioned` flag) is used to dynamically create a chain of _tasks_ per base.

## Task chain per base

For every base returned by `table_querys_ref_job()` the same sequence is built:

1. **`gcf_1_insert_raw_table_<table>`**: triggers the _Cloud Function_ that inserts the content available in the external table into the RAW table.
2. **`data_quality_<table>`**: a task that runs between the insert into RAW and the versioning. It was not implemented in this project; it would be a data quality check (based on whatever criteria the reader deems necessary) that would mark the load as "fit to be used in SCD".
3. **`gcf_2_scd_versioning_<table>`**: only created if the base is marked as `versioned` in the control table; applies versioning (SCD) using the load that was just inserted.

These calls are made via authenticated HTTP, through the helper functions in `dags/plugins/gcf_request.py` (`gcf_1_insert_raw_table`, `data_quality`, `gcf_2_scd_versioning`).

## DAGs

- **`dag_dm_raw_1440min.py`** (`dm_raw_tables_1440min`): runs once a day, processes bases with `update_frequency_min = 1440` and passes `special_daylie_frequency='daily'` to the insert cloud function (`gcf_1_insert_raw_table`).
- **`dag_dm_raw_60min.py`** (`dm_raw_tables_60min`): runs every hour, processes bases with `update_frequency_min = 60`.
- **`dag_dm_raw_instantly.py`** (`dm_raw_tables_instantly`): has no schedule (`schedule=None`), it's triggered on demand by the `trigger_airflow_dag` cloud function. Before building the tasks, `table_querys_ref_job()` also cross-references each base's external table (`update_frequency_min = 0`) with the loaded-files log table and with the RAW table itself, processing only the bases that actually have a new file pending.
