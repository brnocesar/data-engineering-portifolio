from collections import defaultdict
from table_definitions_manual import manual_td
from table_definitions_auto import auto_td


unmapped_base_error = 'Base nao mapeada em table_definitions'

def query_string_parameters():
    dd = defaultdict(list)
    
    for d in (manual_td, auto_td):
        for key, value in d.items():
            dd[key] = dd[key] | value if dd[key] else value
    
    return dict(dd)
