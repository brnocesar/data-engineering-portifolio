import pandas as pd
import pandas_gbq

def declare_var_last_date_instruction(source_dataset: str, source_table: str, ts_dt: str = None) -> tuple[str, str]:
    
    def get_ts_dt_event_values(event_dates_df: pd.DataFrame) -> str:
        if event_dates_df.shape[0] == 0:
            return ""
        event_dates_values = ", ".join([f"'{i}'" for i in event_dates_df.ts_dt_event.to_list()])
        return f" and {event_dates_df.field_name_event[0]} in ({event_dates_values}) "
    
    if ts_dt is None:
        ts_dt_update_query = f"(select ts_dt_update from `gcp-project-id.logs_dataset.vw_table_last_valid_record` where dataset = '{source_dataset}' and table_name = '{source_table}' order by ts_dt_update desc limit 1)"
        view_to_select     = f""" `gcp-project-id.logs_dataset.vw_table_last_valid_record` where """ # estou pegando a carga validada mais recente
    else:
        ts_dt_update_query = f"'{ts_dt}'"
        view_to_select     = f""" `gcp-project-id.logs_dataset.vw_table_valid_records` where ts_dt_update = '{ts_dt}' and """ # estou especificando qual carga deve ser usada
        
    query_event_dates = f"""select cast(ts_dt_event as string) as ts_dt_event, field_name_event 
                            from {view_to_select} 
                                dataset = '{source_dataset}' and table_name = '{source_table}' and ts_dt_event is not null
                            order by ts_dt_event asc; """
    event_dates = pandas_gbq.read_gbq(query_event_dates)
    ts_dt_event_values = get_ts_dt_event_values(event_dates)
    
    return f"declare ts_dt timestamp default {ts_dt_update_query};", ts_dt_event_values


def create_tmp_source_partition(source_dataset: str, 
                                source_table: str, 
                                fields: list[str], 
                                finger_print: str, 
                                partition_by: str, 
                                order_by: str, 
                                event_date_values: str, 
                                pretty_format: str
                                ) -> str:
    select_fields = f",{pretty_format}".join(fields + [f"FARM_FINGERPRINT(ARRAY_TO_STRING([{finger_print}], '')) as im_fingerprint"])
    
    return f"""
        CREATE OR REPLACE TEMPORARY TABLE tmp_source AS
        select * EXCEPT(part), cast(null as timestamp) as ts_dt_update_previous 
        from (
            select {select_fields},
                row_number() over (partition by {partition_by} order by {order_by}) as part
            from `gcp-project-id.{source_dataset}.{source_table}` 
            where ts_dt_update = ts_dt {event_date_values}
        ) a
        where part = 1;
    """


def create_tmp_source_grouping(source_dataset: str, 
                               source_table: str, 
                               fields: list[str], 
                               finger_print: str, 
                               group_by: str, 
                               aggregate_function: str, 
                               aggregated_field: str, 
                               event_date_values: str, 
                               pretty_format: str
                               ) -> str:
    fields        = list(map(lambda i: f"{aggregate_function}({i}) as {i}" if i == aggregated_field else i, fields))
    select_fields = f",{pretty_format}".join(fields)
    
    return f"""
        CREATE OR REPLACE TEMPORARY TABLE tmp_source AS
        select * 
            ,FARM_FINGERPRINT(ARRAY_TO_STRING([{finger_print}], '')) as im_fingerprint
            ,cast(null as timestamp) as ts_dt_update_previous 
        from (
            select {select_fields}
            from `gcp-project-id.{source_dataset}.{source_table}` 
            where ts_dt_update = ts_dt {event_date_values}
            group by {group_by}
        ) a;
    """


def create_tmp_source_instruction(source_dataset: str, 
                                  source_table: str, 
                                  fields: list[str], 
                                  array_type_fields: list[str], 
                                  partition_clause: list[str], 
                                  grouping: list[str], 
                                  event_date_values: str, 
                                  pretty_format: bool = False
                                  ) -> str:
    fields         = [i for i in fields if i not in ('ts_dt_update_previous', 'im_fingerprint')]
    pretty_format  = "\n\t\t" if pretty_format else ""
    regular_fields = [f"cast({i} as string)" for i in fields if i not in array_type_fields + ['ts_dt_update']]
    array_fields   = [f"to_json_string({i})" for i in array_type_fields]
    finger_print   = f",{pretty_format}\t".join(regular_fields + array_fields)
    
    if len(partition_clause) == 2:
        partition_by, order_by = partition_clause 
        return create_tmp_source_partition(source_dataset=source_dataset, 
                                           source_table=source_table, 
                                           fields=fields, 
                                           finger_print=finger_print, 
                                           partition_by=partition_by, 
                                           order_by=order_by, 
                                           event_date_values=event_date_values, 
                                           pretty_format=pretty_format)
    
    if len(grouping) == 3:
        group_by, aggregate_function, aggregated_field = grouping
        return create_tmp_source_grouping(source_dataset=source_dataset,
                                          source_table=source_table,
                                          fields=fields,
                                          finger_print=finger_print,
                                          group_by=group_by,
                                          aggregate_function=aggregate_function,
                                          aggregated_field=aggregated_field,
                                          event_date_values=event_date_values,
                                          pretty_format=pretty_format)
    
    return ""


def declare_vars_delete_instruction(fields: tuple, special_clause: str) -> str:
    if not isinstance(fields, tuple):
        return ""
    
    if len(fields) == 2:
        start_range, end_range = fields
        return f"""
            declare mdt_ini DATE default (SELECT date_trunc(MIN({start_range}), MONTH) FROM tmp_source);
            declare mdt_fim DATE default (SELECT LAST_DAY(max({end_range}), MONTH) FROM tmp_source);
        """
    
    if len(fields) == 1 and isinstance(special_clause, str):
        date_field, = fields
        return f"declare dates_to_delete array <date> default ARRAY(SELECT distinct({date_field}) FROM tmp_source);" if special_clause == '' else f"declare mdt_fim DATE default ({special_clause});"
    
    return ""


def merge_instruction(target_dataset: str, target_table: str, fields_to_match: tuple|list) -> str:
    match_string = " and ".join([f"TARGET.{i[0]} = SOURCE.{i[1]}" for i in fields_to_match])
    return f"""
        MERGE `gcp-project-id.{target_dataset}.{target_table}` as TARGET 
        USING tmp_source as SOURCE 
            on {match_string}
    """


def update_instruction(target_fields: tuple|list, source_fields: tuple|list, pretty_format: str = "") -> str:
    pretty_format   = "\n\t\t" if pretty_format else ""
    set_fields_list = f",{pretty_format}".join([f"TARGET.{i[0]} = SOURCE.{i[1]}" for i in zip(target_fields, source_fields)])
    set_fields_list = set_fields_list.replace(' = SOURCE.ts_dt_update_previous', ' = TARGET.ts_dt_update')
    return f"""
        when matched and TARGET.im_fingerprint != SOURCE.im_fingerprint
            then UPDATE SET {set_fields_list}
    """


def delete_instruction(fields: tuple, special_clause: str) -> str:
    if not (isinstance(fields, tuple) and isinstance(special_clause, str)):
        return ""
    
    if len(fields) == 2:
        start_range, end_range = fields
        return f"when not matched by SOURCE and TARGET.{start_range} >= mdt_ini and TARGET.{end_range} <= mdt_fim then DELETE "
    
    if len(fields) == 1:
        comparison = " in unnest(dates_to_delete) " if special_clause == '' else " >= mdt_fim "
        return f" when not matched by SOURCE and TARGET.{fields[0]} {comparison} then DELETE "
    
    return ""

def insert_instruction(target_fields: tuple|list, source_fields: tuple|list, pretty_format: str = "") -> str:
    pretty_format = "\n\t\t" if pretty_format else ""
    insert_fields = f",{pretty_format}".join(target_fields)
    insert_values = f",{pretty_format}".join([f"SOURCE.{i}" for i in source_fields])
    return f" when not matched then INSERT ({insert_fields}) VALUES ({insert_values}) "
