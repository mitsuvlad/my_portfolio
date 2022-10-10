#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
from pymysql import connect, connections
from fastzila_credentials import fastzila_credentials
from sys import exit
from datetime import datetime
from traceback import format_exc
from requests import get


LENTA_COLUMNS = ('id', 'courier_id', 'employee_id', 'tk_id', 'division_id', 'date_',
                 'transport_id', 'courier_name', 'organisation_id', 'active_time_hours',
                 'first_activation', 'last_deactivation', 'orders_delivered', 
                 'daily_distance', 'orders_over_twenty_kg', 
                 'orders_twenty_forty_kg', 'orders_over_forty_kg', 
                 'SLA_delivery', 'filename', 'file_date', 'created_date', 
                 'mandatory_client', 'mandatory_doer', 'hour_client', 'hour_doer', 
                 'order_client', 'order_doer', 'orders_over_twenty_kg_client', 
                 'orders_over_twenty_kg_doer', 'orders_over_forty_kg_client', 
                 'orders_over_forty_kg_doer', 'mileage_client', 'mileage_doer', 
                 'full_payment_client', 'full_payment_doer')
TARIFFS_COLUMNS = ('date_', 'project_id', 'region_id', 'speciality_id', 'tariff_id', 
                   'field_id', 'cost', 'amount')


TOKEN = fastzila_credentials['tg_bot_token']
CHAT_ID = "-632497144"
TELEGRAM_API_LINK = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&parse_mode=HTML&text="


def create_connection() -> connections.Connection:
    """Create a constant connection, must be closed in the end."""

    return connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                   database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                   host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])


def execute_select(sql_query: str, conn: connections.Connection) -> tuple:
    """Any SELECT-query execution function"""
    
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        return cursor.fetchall()


def executemany_update(sql_query: str, data: tuple, conn: connections.Connection) -> int:
    """Universal database table change function, return number of rows"""
    
    with conn.cursor() as cursor:
        cursor.executemany(sql_query, data)
        conn.commit()
        return cursor.rowcount


def execute_update(sql_query: str, conn: connections.Connection) -> None:
    """Commit logs to script_logs table"""
    
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        conn.commit()
    

def main():
    
    conn = create_connection()

    # Take dates with NULL field full_payment_client from table OR exit if not found
    sql = "SELECT date_ FROM db_analytics.lenta_recieved_couriers WHERE full_payment_client IS NULL GROUP BY date_;"
    data = execute_select(sql, conn)
    
    # Make string with dates for queries. Or exit if no dates recieved.
    if len(data) > 1:
        sql_dates = f"IN {tuple([datetime.strftime(i[0], '%y-%m-%d') for i in data])}"
        get(TELEGRAM_API_LINK + f"Идет заполнение выручки от курьеров Ленты на: {', '.join([datetime.strftime(i[0], '%y-%m-%d') for i in data])}")
    elif len(data) == 1:
        sql_dates = f"= {datetime.strftime(data[0])}"
        get(TELEGRAM_API_LINK + f"Идет заполнение выручки от курьеров Ленты на дату: {data[0]}")        
    else:
        get(TELEGRAM_API_LINK + "Нет незаполненных данных по выручке по курьерам Ленты")
        sql = f"INSERT INTO db_analytics.script_logs (event_type, event_source, description) " \
              f"VALUES ('event', 'Lenta_couriers_revenues_calculate.py', 'No new data')";
        execute_update(sql, conn)
        exit()

    # Take tariffs with query
    sql = f"SELECT data, project_id, region_id, speciality_id, tariff_id, field_id, cost, amount \
          FROM (SELECT t.*, tp.field_id, tp.cost, tp.amount \
            FROM (SELECT q.data, q.project_id, q.region_id, q.speciality_id, MAX(q.id) AS tariff_id \
                FROM (SELECT dh.data, t.* \
                    FROM (SELECT t.id, t.speciality_id, t.project_id, t.region_id, t.state_id, t.state_date, t.date_start, if(t.state_id = 4, t.state_date, t.date_stop) AS date_stop FROM db_fastzila.tariffs t \
                    WHERE (t.state_id = 3) or (t.state_id = 4 and date(t.created) <> date(t.state_date)) ) t \
                JOIN db_analytics.date_hierarchy dh ON dh.data >= t.date_start AND (dh.data <= t.date_stop OR t.date_stop IS NULL) \
                WHERE dh.data {sql_dates}) q \
            GROUP BY q.data, q.project_id, q.region_id, q.speciality_id) t \
            JOIN db_fastzila.tariff_prices tp ON tp.tariff_id = t.tariff_id) b;"
    data = execute_select(sql, conn)
    tariffs_df = pd.DataFrame(data, columns=TARIFFS_COLUMNS)

    # Take regions from database
    sql = "SELECT id, region_id FROM db_analytics.lenta_tks;"
    data = execute_select(sql, conn)
    data = (i for i in data if i[1] != None)
    regions_df = pd.DataFrame(data, columns=('id', 'region_id'))
    regions_df['region_id'] = regions_df.region_id.astype('int')
    regions_df.columns = ('lenta_region_id', 'region_id')
    
    # Take Lenta courier records with recieved dates
    sql = f"SELECT * FROM db_analytics.lenta_recieved_couriers WHERE date_ {sql_dates};"
    data = execute_select(sql, conn)
    lenta_df = pd.DataFrame(data, columns=LENTA_COLUMNS)
    lenta_df['speciality_id'] = 1
    lenta_df['speciality_id'] = np.where(lenta_df.transport_id == 0, 0, lenta_df.speciality_id)
    lenta_df['speciality_id'] = np.where((lenta_df.transport_id == 1) | (lenta_df.transport_id == 4), 2, lenta_df.speciality_id)
    lenta_df['project_id'] = 20

    # Lenta with regions ids
    lenta_df = lenta_df.merge(regions_df, left_on='tk_id', right_on='lenta_region_id')

    # Lenta with regions ids & tariffs
    lenta_tariffs_df = lenta_df.merge(tariffs_df, how='left', on=('region_id', 'project_id', 'speciality_id', 'date_'))
    lenta_tariffs_df['cost'] = lenta_tariffs_df.cost.astype('int')
    lenta_tariffs_df['amount'] = lenta_tariffs_df.amount.astype('int')

    # Pivots for unwrap costs & amounts per Lenta record
    pvt_cost = lenta_tariffs_df.pivot(index='id', columns='field_id', values='cost').reset_index()
    pvt_amount = lenta_tariffs_df.pivot(index='id', columns='field_id', values='amount').reset_index()
    pvt_cost.columns = ['cost_' + str(i) for i in pvt_cost.columns]
    pvt_amount.columns = ['amount_' + str(i) for i in pvt_amount.columns]
    pvt = pvt_cost.merge(pvt_amount, left_on='cost_id', right_on='amount_id')
    df = lenta_df.merge(pvt, left_on='id', right_on='cost_id')

    # Calculate payments
    df['mandatory_client'] = np.where((df.active_time_hours > 11) & (df.orders_delivered < 11) & (df.amount_58 > (df.active_time_hours * df.amount_1 + df.orders_delivered * df.amount_2)), df.amount_58, 0)
    df['mandatory_doer'] = np.where((df.active_time_hours > 11) & (df.orders_delivered < 11) & (df.cost_58 > (df.active_time_hours * df.cost_1 + df.orders_delivered * df.cost_2)), df.cost_58, 0)
    df['hour_client'] = np.where(df['mandatory_client'] == 0, df.active_time_hours * df.amount_1, 0)
    df['hour_doer'] = np.where(df['mandatory_doer'] == 0, df.active_time_hours * df.cost_1, 0)
    df['order_client'] = np.where(df['mandatory_client'] == 0, df.orders_delivered * df.amount_2, 0)
    df['order_doer'] = np.where(df['mandatory_doer'] == 0, df.orders_delivered * df.cost_2, 0)
    df['orders_over_twenty_kg_client'] = df.orders_over_twenty_kg * df.amount_59
    df['orders_over_twenty_kg_doer'] = df.orders_over_twenty_kg * df.cost_59
    df['orders_over_forty_kg_client'] = df.orders_over_forty_kg * df.amount_82
    df['orders_over_forty_kg_doer'] = df.orders_over_forty_kg * df.cost_82
    df['mileage_client'] = np.where(df.daily_distance - 8 * df.orders_delivered > 0, (df.daily_distance - 8 * df.orders_delivered) * df.amount_60, 0)
    df['mileage_doer'] = np.where(df.daily_distance - 8 * df.orders_delivered > 0, (df.daily_distance - 8 * df.orders_delivered) * df.cost_60, 0)
    df['full_payment_client'] = df.mandatory_client + df.hour_client + df.order_client + df.orders_over_twenty_kg_client + df.orders_over_forty_kg_client + df.mileage_client
    df['full_payment_doer'] = df.mandatory_doer + df.hour_doer + df.order_doer + df.orders_over_twenty_kg_doer + df.orders_over_forty_kg_doer + df.mileage_doer

    # Make update query
    to_sql_df = df[['mandatory_client', 'mandatory_doer', 'hour_client', 'hour_doer', 
                    'order_client', 'order_doer', 'orders_over_twenty_kg_client', 
                    'orders_over_twenty_kg_doer', 'orders_over_forty_kg_client', 
                    'orders_over_forty_kg_doer', 'mileage_client', 'mileage_doer', 
                    'full_payment_client', 'full_payment_doer', 'id']].fillna(0)
    response_data = tuple(to_sql_df.itertuples(index=False, name=None))
    sql = "UPDATE db_analytics.lenta_recieved_couriers " \
          "SET mandatory_client = %s, mandatory_doer = %s, hour_client = %s, hour_doer = %s, order_client = %s, " \
          "order_doer = %s, orders_over_twenty_kg_client = %s, orders_over_twenty_kg_doer = %s, " \
          "orders_over_forty_kg_client = %s, orders_over_forty_kg_doer = %s, mileage_client = %s, " \
          "mileage_doer = %s, full_payment_client = %s, full_payment_doer = %s " \
          "WHERE id = %s;"

    cnt = executemany_update(sql, response_data, conn)
    
    # Messages & logs
    get(TELEGRAM_API_LINK + f"Успешно выполнено обновление выручки в таблице курьеров. Изменено записей: {cnt}")
    sql = f"INSERT INTO db_analytics.script_logs (event_type, event_source, description) " \
          f"VALUES ('event', 'Lenta_couriers_revenues_calculate.py', 'Updated {cnt} records')";
    execute_update(sql, conn)
    
    conn.close()


if __name__ == '__main__':
        
    try:
        main()
    except:
        if 'Systemexit' not in format_exc().title():    
            with open('python_scripts/script_messages.log', 'a', encoding = 'utf-8') as f:
                f.write('\n *** ' + datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + 
                        ' - Lenta_couriers_revenues_calculate.py\n' + format_exc())
            get(TELEGRAM_API_LINK + 'Ошибка, сообщение записано в журнале script_messages.log')
            