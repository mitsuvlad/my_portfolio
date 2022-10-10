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

        sql = "INSERT INTO slices_couriers " \
              "SELECT c.id, c.state, c.paycard, c.bank_acc, corr_acc, c.bic, c.bank_name, c.inn, c.snils, " \
              "c.med_book, c.med_book_checked, c.medbook_expired, c.dmr_card, c.dmr_card_expired, " \
              "c.cant_be_selfempl, c.salary_blocked, c.is_in_fzhero, c.qr_code_vac_checked, " \
              "DATE( NOW() - INTERVAL 1 DAY) as fix_date " \
              "FROM db_fastzila.couriers c JOIN db_fastzila.states st " \
              "ON c.state = st.id WHERE st.global_id = 1;"
        cursor.execute(sql)
        rows = cursor.rowcount
        connection.commit()

        sql_log = f"INSERT INTO script_logs (event_type, event_source, description) " \
                  f"VALUES ('event', 'slice_couriers.py', '{rows} records commited to table slices_couriers')"
        cursor.execute(sql_log)
        connection.commit()
