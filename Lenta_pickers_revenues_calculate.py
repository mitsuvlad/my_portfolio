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


LENTA_COLUMNS = ('id', 'picker_id', 'employee_id', 'tk_id', 'division_id', 'date_',
                 'picker_name', 'organisation_id', 'active_time_hours',
                 'first_activation', 'last_deactivation', 'orders_picked', 
                 'ordered_units', 'picked_units', 'ordered_SKU', 'picked_SKU', 
                 'changed_SKU', 'pick_fullness', 'SLA_reaction', 'SLA_pick', 
                 'filename', 'file_date', 'created_date', 
                 'hour_day_client', 'hour_day_doer', 'hour_night_client', 'hour_night_doer', 
                 'picked_SKU_client', 'picked_SKU_doer',
                 'full_payment_client', 'full_payment_doer')
TARIFFS_COLUMNS = ('date_', 'project_id', 'region_id', 'speciality_id', 'tariff_id', 
                   'field_id', 'cost', 'amount')
                   

# FOLDER = 'python_scripts'
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


def execute_update(sql_query: str, data: tuple, conn: connections.Connection) -> int:
    """Universal database table change function, return number of rows"""
    
    with conn.cursor() as cursor:
        cursor.executemany(sql_query, data)
        conn.commit()
        return cursor.rowcount


def log_to_sql(sql_query: str, conn: connections.Connection) -> None:
    """Commit logs to script_logs table"""
    
    with conn.cursor() as cursor:
        cursor.execute(sql_query)
        conn.commit()
            

def main():
    
    conn = create_connection()
    
    # Get dates with NULL field full_payment_doer from database table
    sql = "SELECT date_ FROM db_analytics.lenta_recieved_pickers WHERE full_payment_doer IS NULL AND file_date > '2022,5,11' GROUP BY date_;"
    dates = execute_select(sql, conn)

    # Make string with dates for queries. Or exit if no dates recieved.
    if len(dates) > 1:
        sql_dates = f"IN {tuple([datetime.strftime(i[0], '%y-%m-%d') for i in dates])}"
        get(TELEGRAM_API_LINK + f"Идет заполнение выручки от сборщиков Ленты на: {', '.join([datetime.strftime(i[0], '%y-%m-%d') for i in dates])}")
    elif len(dates) == 1:
        sql_dates = f"= {datetime.strftime(dates[0][0], '%y-%m-%d')}"
        get(TELEGRAM_API_LINK + f"Идет заполнение выручки от сборщиков Ленты на дату: {dates[0]}")
    else:
        get(TELEGRAM_API_LINK + "Нет незаполненных данных по выручке по сборщикам Ленты")
        sql = f"INSERT INTO db_analytics.script_logs (event_type, event_source, description) " \
              f"VALUES ('event', 'Lenta_pickers_revenues_calculate.py', 'No new data')";
        log_to_sql(sql, conn)
        exit()
    
    # Get tariffs with query
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
    
    # Get regions from database table
    sql = "SELECT id, region_id FROM db_analytics.lenta_tks;"
    data = execute_select(sql, conn)
    data = (i for i in data if i[1] != None)
    regions_df = pd.DataFrame(data, columns=('id', 'region_id'))
    regions_df['region_id'] = regions_df.region_id.astype('int')
    regions_df.columns = ('lenta_region_id', 'region_id')
    
    # Get Lenta pickers with NULL full_payment_doer field & first_activation NOT NULL
    sql = f"SELECT * FROM db_analytics.lenta_recieved_pickers WHERE date_ {sql_dates} AND first_activation IS NOT NULL;"
    data = execute_select(sql, conn)
    lenta_df = pd.DataFrame(data, columns=LENTA_COLUMNS)
    
    # Speciality = picker, project = Lenta
    lenta_df['speciality_id'] = 3
    lenta_df['project_id'] = 20
    
    # Lenta with regions ids & tariff fields
    lenta_df = lenta_df.merge(regions_df, left_on='tk_id', right_on='lenta_region_id')
    lenta_df = lenta_df.merge(tariffs_df, how='left', on=('region_id', 'project_id', 'speciality_id', 'date_'))
    lenta_df = lenta_df[lenta_df.field_id.notna()]
    lenta_df['field_id'] = lenta_df.field_id.astype('int')
    
    # Pivots for calculating clients & doers payments
    pvt_cost = lenta_df.pivot(index='id', columns='field_id', values='cost').reset_index()
    pvt_amount = lenta_df.pivot(index='id', columns='field_id', values='amount').reset_index()
    pvt_cost.columns = ['cost_' + str(i) for i in pvt_cost.columns]
    pvt_amount.columns = ['amount_' + str(i) for i in pvt_amount.columns]
    pvt = pvt_cost.merge(pvt_amount, left_on='cost_id', right_on='amount_id')
    df = lenta_df.merge(pvt, left_on='id', right_on='cost_id')
    
    # Payments calculations
    df['start_time'] = df.first_activation.dt.hour + df.first_activation.dt.minute * 5 / 300
    df['finish_time'] = df.last_deactivation.dt.hour + df.last_deactivation.dt.minute * 5 / 300
    df['max_start_morning'] = np.where(df['start_time'] > 0, df['start_time'], 0)
    df['min_finish_morning'] = np.where(df['finish_time'] < 6, df['finish_time'], 6)
    df['max_start_day'] = np.where(df['start_time'] > 6, df['start_time'], 6)
    df['min_finish_day'] = np.where(df['finish_time'] < 22, df['finish_time'], 22)
    df['max_start_evening'] = np.where(df['start_time'] > 22, df['start_time'], 22)
    df['min_finish_evening'] = np.where(df['finish_time'] < 24, df['finish_time'], 24)
    df['morning'] = np.where((df.max_start_morning <= df.start_time) & (df.start_time <= df.min_finish_morning), df.min_finish_morning - df.max_start_morning, 0)
    df['hours_day'] = np.where((df.max_start_day <= df.start_time) & (df.start_time <= df.min_finish_day), df.min_finish_day - df.max_start_day, 0)
    df['preevening'] = np.where((df.max_start_evening <= df.start_time) & (df.start_time <= df.min_finish_evening), df.min_finish_evening - df.max_start_evening, 0)
    df['evening'] = np.where(df.preevening > 0, df.preevening, 0)
    df['hours_night'] = df.morning + df.evening
    df['hour_day_client'] = df.hours_day * df.amount_1.astype('float')
    df['hour_day_doer'] = df.hours_day * df.cost_1.astype('float')
    df['hour_night_client'] = df.hours_night * df.amount_86.astype('float')
    df['hour_night_doer'] = df.hours_night * df.cost_86.astype('float')
    df['picked_SKU_client'] = df.picked_SKU / df.active_time_hours * df.hours_day * df.amount_92.astype('float')
    df['picked_SKU_doer'] = df.picked_SKU / df.active_time_hours * df.hours_day * df.cost_92.astype('float')
    df['hour_day_client'] = df.hour_day_client.replace(np.nan, 0)
    df['hour_day_doer'] = df.hour_day_doer.replace(np.nan, 0)
    df['hour_night_client'] = df.hour_night_client.replace(np.nan, 0)
    df['hour_night_doer'] = df.hour_night_doer.replace(np.nan, 0)
    df['picked_SKU_client'] = df.picked_SKU_client.replace([np.inf, np.nan], 0)
    df['picked_SKU_doer'] = df.picked_SKU_doer.replace([np.inf, np.nan], 0)
    df['full_payment_client'] = df.hour_day_client + df.hour_night_client + df.picked_SKU_client
    df['full_payment_doer'] = df.hour_day_doer + df.hour_night_doer + df.picked_SKU_doer
    
    # Make tuple & commit data to database
    to_sql_df = df[['hour_day_client', 'hour_day_doer', 'hour_night_client', 'hour_night_doer', 
                'picked_SKU_client', 'picked_SKU_doer', 'full_payment_client', 
                'full_payment_doer', 'id']]
    response_data = tuple(to_sql_df.itertuples(index=False, name=None))
    sql = "UPDATE db_analytics.lenta_recieved_pickers " \
          "SET hour_day_client = %s, hour_day_doer = %s, hour_night_client = %s, hour_night_doer = %s, " \
          "picked_SKU_client = %s, picked_SKU_doer = %s, full_payment_client = %s, full_payment_doer = %s " \
          "WHERE id = %s;"
    cnt = execute_update(sql, response_data, conn)

    # Messages & logs
    get(TELEGRAM_API_LINK + f"Успешно выполнено обновление выручки в таблице сборщиков. Изменено записей: {cnt}")
    sql = f"INSERT INTO db_analytics.script_logs (event_type, event_source, description) " \
          f"VALUES ('event', 'Lenta_pickers_revenues_calculate.py', 'Updated {cnt} records')";
    log_to_sql(sql, conn)
    
    conn.close()
    
    
if __name__ == '__main__':
        
    try:
        main()
    except:
        if 'Systemexit' not in format_exc().title():
            with open('python_scripts/script_messages.log', 'a', encoding = 'utf-8') as f:
                f.write('\n *** ' + datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + 
                        ' - Lenta_pickers_revenues_calculate.py\n' + format_exc())
            get(TELEGRAM_API_LINK + 'Ошибка, сообщение записано в журнале script_messages.log')
            