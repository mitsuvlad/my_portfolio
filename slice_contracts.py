#!/usr/bin/env python
# coding: utf-8


from fastzila_credentials import fastzila_credentials
from pymysql import connect


connection = connect(user=fastzila_credentials['user'],
                     password=fastzila_credentials['db_pass'],
                     database=fastzila_credentials['db_name'],
                     port=fastzila_credentials['port'],
                     host=fastzila_credentials['host'],
                     charset=fastzila_credentials['charset'])

with connection:
    with connection.cursor() as cursor:

        sql = "INSERT INTO slices_contracts " \
              "SELECT id, type, courier_id, entity_id, project_id, region_id, speciality_id, start, stop, " \
              "termin_date, is_active, is_handed_out, created, created_by, changed, changed_by, client_doer_id, " \
              "DATE( NOW() - INTERVAL 1 DAY) as fix_date FROM db_fastzila.contracts " \
              "WHERE is_active=1 OR is_active = FALSE  " \
              "AND date(changed) = DATE(NOW()) - INTERVAL 1 DAY;"
        cursor.execute(sql)
        rows = cursor.rowcount
        connection.commit()
        
        sql_log = f"INSERT INTO script_logs (event_type, event_source, description) " \
                  f"VALUES ('event', 'slice_contracts.py', '{rows} records commited to table slices_contracts');"
        cursor.execute(sql_log)
        connection.commit()
