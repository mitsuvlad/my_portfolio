#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
from datetime import datetime
from pymysql import connect
from imap_tools import MailBox
from os import path, scandir, remove
from fastzila_credentials import fastzila_credentials
from traceback import format_exc
from pandas import Timestamp
from requests import get


FOLDER = 'python_scripts/temp/'
TOKEN = fastzila_credentials['tg_bot_token']
CHAT_ID = "-632497144"
TELEGRAM_API_LINK = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&parse_mode=HTML&text="


def files_from_email(imap=fastzila_credentials['imap'], 
                     email=fastzila_credentials['email'], 
                     password=fastzila_credentials['mail_pass'],
                     from_folder='INBOX', folder_path=FOLDER) -> None:
    """Copy email attachments to folder_path"""
    
    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        for message in mailbox.fetch():
            if (message.from_ == 'romanova_tat@fastzila.ru') | (message.from_ == 'vlasiuk@fastzila.ru'):
                for attachment in message.attachments:
                    file_prefix = '.' + str(message.date.date()) + '.' + message.from_ + '.'
                    with open(path.join(folder_path, str(len(file_prefix) + 2) + file_prefix +
                                                     attachment.filename), 'wb') as f:
                        f.write(attachment.payload)


def move_emails_inside_mailbox(imap=fastzila_credentials['imap'],
                               email=fastzila_credentials['email'],
                               password=fastzila_credentials['mail_pass'],
                               from_folder='INBOX', to_folder='Archive') -> None:
    """Move all mails from INBOX folder to Archive after processing its files"""
    
    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        messages_list = []
        for message in mailbox.fetch():
            if (message.from_ == 'romanova_tat@fastzila.ru') | (message.from_ == 'vlasiuk@fastzila.ru'):
                messages_list.append(message)
        for message in messages_list:
            mailbox.move(message.uid, to_folder)


def files_list_in_folder(folder_path=FOLDER) -> list:
    """Recieve files_list in defined folder_path"""
    
    folder_files_list = []
    with scandir(folder_path) as entries:
        for entry in entries:
            if entry.is_file():
                folder_files_list.append(entry.name)
                
    return folder_files_list


def sort_files_by_types(folder_files_list: list, folder=FOLDER) -> list:
    """Return three lists of files by types of documents"""


    LENTA_FIELDS_PICKERS = ['КПП Комплектовщика', 'employee_id', 'ТК', 'Дивизион', 'Город', 'Дата',
                            'Имя комплектовщика', 'Фамилия', 'Организация', 'Активное время, часов', 'Кол-во включений',
                            'Кол-во выключений', 'Первое включение', 'Последнее выключение', 'Плановое начало смены',
                            'Плановый конец смены', 'Время последнего заказа', 'Заказов собрано', 'Заказано штук',
                            'Собрано штук', 'Заказано SKU', 'Собрано SKU', 'Заменено SKU', 'Полнота сборки',
                            'Утилизовано', 'Заказов в час', 'SLA, реакция', 'SLA, сборка']
    LENTA_FIELDS_COURIERS = ['КПП Курьера', 'employee_id', 'ТК', 'Дивизион', 'Город', 'Дата', 'Транспорт',
                             'Имя Курьера', 'Фамилия', 'Организация', 'Активное время, часов', 'Кол-во включений',
                             'Кол-во выключений', 'Первое включение', 'Последнее выключение', 'Плановое начало смены',
                             'Плановый конец смены', 'Время последнего заказа', 'Заказов доставлено', 'Утилизовано',
                             'Пробег за день, км', 'Заказов больше 20 кг', 'Заказов 20-40 кг', 'Заказов больше 40 кг',
                             'Заказов в час', 'SLA, доставка']
    def_xfive_list = []
    
    for filename in folder_files_list:
        try:
            dataframe = pd.read_excel(path.join(folder, filename))
            if dataframe.columns.to_list()[:28] == LENTA_FIELDS_PICKERS:
                def_xfive_list.append('pickers.' + filename)
                get(TELEGRAM_API_LINK + f"Обнаружен файл <b>'{filename[int(filename.split('.')[0]):]}'</b>, идет обработка ...")
            if dataframe.columns.to_list()[:26] == LENTA_FIELDS_COURIERS:
                def_xfive_list.append('couriers.' + filename)
        except (ValueError, KeyError):
            pass
        
    return def_xfive_list


def get_table_from_sql(table_name: str) -> list:
    """Get table from database"""
    
    sql = f"SELECT * FROM {table_name};"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            list_sql = cursor.fetchall()
    return list_sql


def add_elements_to_dictionary_to_sql(table_name: str, field_name: str, elements_set: set) -> None:
    """Insert new found elements to database dictionary-tables"""
    
    sql = f"INSERT INTO {table_name} ({field_name}) VALUES (%s)"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, tuple(elements_set))
            connection.commit()
            
    log_to_sql('event', f"{','.join(elements_set)} added to dictionary-table {table_name}")


def dataframe_to_tuple(dataframe: pd.DataFrame) -> tuple:
    """Make tuples from dataframes and update or insert them into the database table"""
    
    return tuple(dataframe.itertuples(index=False, name=None))


def delete_objects_in_folder(def_files_list: list, folder_path=FOLDER) -> None:
    """Delete all Lenta's files from working directory"""
    
    for file in def_files_list:
        remove(path.join(folder_path, file))


def log_to_sql(def_event, def_log: str) -> None:
    """Commit logs to script_logs table"""

    sql_log = f"INSERT INTO script_logs (event_type, event_source, description) " \
              f"VALUES ('{def_event}', 'email_excels_Lenta_parser.py', '{def_log}');"

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_log)
            connection.commit()


def fill_first_activation(pd_obj):
    start = pd_obj['Последнее выключение']
    hours_delta = pd_obj['Активное время, часов']
    return start - pd.Timedelta(hours=hours_delta)


def fill_last_deactivation(pd_obj):
    start = pd_obj['Первое включение']
    hours_delta = pd_obj['Активное время, часов']
    return start + pd.Timedelta(hours=hours_delta)
    

def main_pickers(df: pd.DataFrame, file: str) -> None:
    """Main function that parse & process pickers data
    Write data & new TK-fields to database
    Log all actions"""

    # Control incoming values
    df['ТК'] = np.where(df['ТК'].astype(str).str.contains('ТК'),  df['ТК'], 'ТК' + df['ТК'].astype(str))
    df['employee_id'] = np.where(df.employee_id.notna(),
                                 pd.to_numeric(df['employee_id'].str.findall(r'\d+').str[0]), None)
    df['employee_id'] = df.employee_id.fillna(0)
    df['Дата'] = pd.to_datetime(df['Дата'], format='%Y/%m/%d')
    docs = file[int(file.split('.')[0]):]
    dates = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()

    # To avoid writing NULLS in the database
    df['SLA, реакция'] = df['SLA, реакция'].fillna(0)
    df['SLA, сборка'] = df['SLA, сборка'].fillna(0)
    df['Полнота сборки'] = df['Полнота сборки'].fillna(0)
    df['Заменено SKU'] = df['Заменено SKU'].fillna(0)
    df['Собрано SKU'] = df['Собрано SKU'].fillna(0)
    df['Заказано SKU'] = df['Заказано SKU'].fillna(0)
    df['Собрано штук'] = df['Собрано штук'].fillna(0)
    df['Заказано штук'] = df['Заказано штук'].fillna(0)
    df['Заказов собрано'] = df['Заказов собрано'].fillna(0)
    df['Активное время, часов'] = df['Активное время, часов'].fillna(0)
    df['SLA, реакция'] = df['SLA, реакция'].round(4)
    df['SLA, сборка'] = df['SLA, сборка'].round(4)
    
    # Filling NULLS in the first_activation & last_deactivation columns
    df['Первое включение'] = np.where(df['Первое включение'].isna() & df['Последнее выключение'].isna(), df['Дата'] + pd.DateOffset(hours=6), df['Первое включение'])
    df['Первое включение'] = np.where(df['Первое включение'].isna() & df['Последнее выключение'].notna(), df.apply(fill_first_activation, axis=1), df['Первое включение'])
    df['Последнее выключение'] = np.where(df['Последнее выключение'].isna(), df.apply(fill_last_deactivation, axis=1), df['Последнее выключение'])

    # Create 'division_id' column, fill it and log if it has irregular values
    df['division_id'] = 0
    df['division_id'] = np.where(df['Дивизион'] == 'Волга', 1, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Санкт-Петербург', 2, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Сибирь', 3, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Урал', 4, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Центр', 5, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Юг', 6, df.division_id)
    df['division_id'] = np.where(df['Дивизион'].isna(), 7, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'ДМФ', 8, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'ММФ', 9, df.division_id)
    if df[df['division_id'] == 0].shape[0] != 0:
        log_to_sql('warning', f"Field 'division_id' has {df[df['division_id'] == 0].shape[0]} zero values")
   
    # Search absent tks and organisations in database, add it if absent
    tk_df = pd.DataFrame(get_table_from_sql('lenta_tks'))
    organisation_df = pd.DataFrame(get_table_from_sql('lenta_organisations'))
    update_set = set(df['ТК']) - set([i[0] for i in dataframe_to_tuple(tk_df[[1, 2]])])
   
    if update_set != set():
        add_elements_to_dictionary_to_sql('lenta_tks', 'tk_name', tuple(update_set))
        tk_df = pd.DataFrame(get_table_from_sql('lenta_tks'))
       
    update_set = set(df['Организация']) - set(organisation_df[1].tolist())
    if update_set != set():
        add_elements_to_dictionary_to_sql('lenta_organisations', 'organisation_name', tuple(update_set))
        organisation_df = pd.DataFrame(get_table_from_sql('lenta_organisations'))
   
    # Make identifiers fields in DataFrame
    df = df.merge(tk_df, left_on='ТК', right_on=1)
    df = df.merge(organisation_df, left_on='Организация', right_on=1)
   
    # Make final DataFrame
    df = df[['КПП Комплектовщика', 'employee_id', '0_x', 'division_id', 'Дата', 'Имя комплектовщика', '0_y',
             'Активное время, часов', 'Первое включение', 'Последнее выключение', 'Заказов собрано', 
             'Заказано штук', 'Собрано штук', 'Заказано SKU', 'Собрано SKU', 'Заменено SKU', 
             'Полнота сборки', 'SLA, реакция', 'SLA, сборка']]
    df['filename'] = docs
    df['file_date'] = dates
    df.replace({np.nan: None}, inplace=True)
    df['employee_id'] = np.where(df.employee_id.isna(), None, df.employee_id)

    # Write data and log it
    rows = insert_update_pickers_to_sql(dataframe_to_tuple(df))
    if rows > 0:
        log_to_sql('event', f'{docs} {rows} records changed in table lenta_recieved_pickers')
        get(TELEGRAM_API_LINK + f"Обработка файла завершена, загружено {rows} строк сборщиков")
        

def main_couriers(df: pd.DataFrame, file: str) -> None:
    """Main function that parse & process couriers data
    Write data & new TK- & organisation fields to database
    Log all actions"""
    
    # Control incoming values
    df['ТК'] = np.where(df['ТК'].astype(str).str.contains('ТК'), df['ТК'], 'ТК' + df['ТК'].astype(str))
    df['employee_id'] = np.where(~df.employee_id.isna(), 
                                 pd.to_numeric(df['employee_id'].str.findall(r'\d+').str[0]), 
                                 None)
    df['employee_id'] = df.employee_id.fillna(0)
    df['Дата'] = pd.to_datetime(df['Дата'], format='%Y/%m/%d')
    docs = file[int(file.split('.')[0]):]
    dates = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()

    # To avoid writing NULLS in the database
    df['SLA, доставка'] = df['SLA, доставка'].fillna(0)
    df['Заказов больше 40 кг'] = df['Заказов больше 40 кг'].fillna(0)
    df['Заказов 20-40 кг'] = df['Заказов 20-40 кг'].fillna(0)
    df['Заказов больше 20 кг'] = df['Заказов больше 20 кг'].fillna(0)
    df['Пробег за день, км'] = df['Пробег за день, км'].fillna(0)
    df['Заказов доставлено'] = df['Заказов доставлено'].fillna(0)
    df['Активное время, часов'] = df['Активное время, часов'].fillna(0)
    
    # Filling NULLS in the first_activation & last_deactivation columns
    df['Первое включение'] = np.where(df['Первое включение'].isna() & df['Последнее выключение'].isna(), df['Дата'] + pd.DateOffset(hours=6), df['Первое включение'])
    df['Первое включение'] = np.where(df['Первое включение'].isna() & df['Последнее выключение'].notna(), df.apply(fill_first_activation, axis=1), df['Первое включение'])
    df['Последнее выключение'] = np.where(df['Последнее выключение'].isna(), df.apply(fill_last_deactivation, axis=1), df['Последнее выключение'])
    
    # Create 'transport_id' column, fill it and log if it has irregular values
    df['transport_id'] = 0
    df['transport_id'] = np.where(df['Транспорт'] == 'Велосипед', 1, df.transport_id)
    df['transport_id'] = np.where(df['Транспорт'] == 'Авто', 2, df.transport_id)
    df['transport_id'] = np.where(df['Транспорт'] == 'Мото', 3, df.transport_id)
    df['transport_id'] = np.where(df['Транспорт'] == 'Пеший', 4, df.transport_id)
    if df[df['transport_id'] == 0].shape[0] != 0:
        log_to_sql('warning', f"Field transport_id has {df[df['transport_id'] == 0].shape[0]} zero values")
    
    # Create 'division_id' column, fill it and log if it has irregular values
    df['division_id'] = 0
    df['division_id'] = np.where(df['Дивизион'] == 'Волга', 1, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Санкт-Петербург', 2, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Сибирь', 3, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Урал', 4, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Центр', 5, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'Юг', 6, df.division_id)
    df['division_id'] = np.where(df['Дивизион'].isna(), 7, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'ДМФ', 8, df.division_id)
    df['division_id'] = np.where(df['Дивизион'] == 'ММФ', 9, df.division_id)
    if df[df['division_id'] == 0].shape[0] != 0:
        log_to_sql('warning', f"Field 'division_id' has {df[df['division_id'] == 0].shape[0]} zero values")    
    
    # Search absent tks and organisations in database, add it if absent
    tk_df = pd.DataFrame(get_table_from_sql('lenta_tks'))
    organisation_df = pd.DataFrame(get_table_from_sql('lenta_organisations'))
    update_set = set(df['ТК']) - set([i[0] for i in dataframe_to_tuple(tk_df[[1, 2]])])
    
    if update_set != set():
        add_elements_to_dictionary_to_sql('lenta_tks', 'tk_name', tuple(update_set))
        tk_df = pd.DataFrame(get_table_from_sql('lenta_tks'))
        
    update_set = set(df['Организация']) - set(organisation_df[1].tolist())
    if update_set != set():
        add_elements_to_dictionary_to_sql('lenta_organisations', 'organisation_name', tuple(update_set))
        organisation_df = pd.DataFrame(get_table_from_sql('lenta_organisations'))
    
    # Make identifiers fields in DataFrame
    df = df.merge(tk_df, left_on='ТК', right_on=1)
    df = df.merge(organisation_df, left_on='Организация', right_on=1)
    
    # Make final DataFrame
    df = df[['КПП Курьера', 'employee_id', '0_x', 'division_id', 'Дата', 'transport_id', 'Имя Курьера',
             '0_y', 'Активное время, часов', 'Первое включение', 'Последнее выключение', 
             'Заказов доставлено', 'Пробег за день, км', 'Заказов больше 20 кг', 'Заказов 20-40 кг', 
             'Заказов больше 40 кг', 'SLA, доставка']]
    df['filename'] = docs
    df['file_date'] = dates
    df.replace({np.nan: None}, inplace=True)
    df['employee_id'] = np.where(df.employee_id.isna(), None, df.employee_id)

    # Write data and log it
    rows = insert_update_couriers_to_sql(dataframe_to_tuple(df))
    if rows > 0:
        log_to_sql('event', f'{docs} {rows} records changed in table lenta_recieved_couriers')
        get(TELEGRAM_API_LINK + f"Обработка файла завершена, загружено {rows} строк курьеров")


def insert_update_pickers_to_sql(data_tuple: tuple) -> int:
    """Insert or update table with lenta data and return the number of changed rows"""

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "INSERT INTO lenta_recieved_pickers (picker_id, employee_id, tk_id, division_id, date_, picker_name, " \
          "organisation_id, active_time_hours, first_activation, last_deactivation, orders_picked, ordered_units, " \
		  "picked_units, ordered_SKU, picked_SKU, " \
          "changed_SKU, pick_fullness, SLA_reaction, SLA_pick, filename, file_date) " \
          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)" \
          "ON DUPLICATE KEY UPDATE tk_id = VALUES(tk_id), division_id = VALUES(division_id), " \
          "picker_name = VALUES(picker_name), organisation_id = VALUES(organisation_id), " \
          "active_time_hours = VALUES(active_time_hours), first_activation = VALUES(first_activation), " \
		  "last_deactivation = VALUES(last_deactivation), orders_picked = VALUES(orders_picked), " \
          "ordered_units = VALUES(ordered_units), picked_units = VALUES(picked_units), " \
          "ordered_SKU = VALUES(ordered_SKU), picked_SKU = VALUES(picked_SKU), changed_SKU = VALUES(changed_SKU), " \
          "pick_fullness = VALUES(pick_fullness), SLA_reaction = VALUES(SLA_reaction), SLA_pick = VALUES(SLA_pick), " \
          "filename = VALUES(filename), file_date = VALUES(file_date);"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
            connection.commit()
            
    return def_rows


def insert_update_couriers_to_sql(data_tuple: tuple) -> int:
    """Insert or update table with lenta data and return the number of changed rows"""
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "INSERT INTO lenta_recieved_couriers (courier_id, employee_id, tk_id, division_id, date_, transport_id, " \
          "courier_name, organisation_id, active_time_hours, first_activation, last_deactivation, orders_delivered, " \
          "daily_distance, " \
          "orders_over_twenty_kg, orders_twenty_forty_kg, orders_over_forty_kg, SLA_delivery, filename, file_date) " \
          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
          "ON DUPLICATE KEY UPDATE tk_id = VALUES(tk_id), division_id = VALUES(division_id), " \
          "transport_id = VALUES(transport_id), courier_name = VALUES(courier_name), " \
          "organisation_id = VALUES(organisation_id), active_time_hours = VALUES(active_time_hours), " \
          "first_activation = VALUES(first_activation), last_deactivation = VALUES(last_deactivation), " \
          "orders_delivered = VALUES(orders_delivered), daily_distance = VALUES(daily_distance), " \
          "orders_over_twenty_kg = VALUES(orders_over_twenty_kg), " \
          "orders_twenty_forty_kg = VALUES(orders_twenty_forty_kg), " \
          "orders_over_forty_kg = VALUES(orders_over_forty_kg), SLA_delivery = VALUES(SLA_delivery), " \
          "filename = VALUES(filename), file_date = VALUES(file_date);"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
            connection.commit()
            
    return def_rows


if __name__ == '__main__':

    try:
        files_from_email()
        move_emails_inside_mailbox()
        files_list = sort_files_by_types(files_list_in_folder())

        for one_file in files_list:
            if one_file.split('.')[0] == 'couriers':
                couriers_df = pd.read_excel(path.join(FOLDER, '.'.join(one_file.split('.')[1:])))
                pickers_df = pd.read_excel(path.join(FOLDER, '.'.join(one_file.split('.')[1:])), sheet_name=1)
                main_couriers(couriers_df, one_file[9:])
                main_pickers(pickers_df, one_file[9:])
            elif one_file.split('.')[0] == 'pickers':
                pickers_df = pd.read_excel(path.join(FOLDER, '.'.join(one_file.split('.')[1:])))
                couriers_df = pd.read_excel(path.join(FOLDER, '.'.join(one_file.split('.')[1:])), sheet_name=1)
                main_couriers(couriers_df, one_file[8:])
                main_pickers(pickers_df, one_file[8:])

        files_list = ['.'.join(i.split('.')[1:]) for i in files_list]
        delete_objects_in_folder(files_list)
        
    except:
        with open(path.join('python_scripts', 'script_messages.log'), 'a', encoding = 'utf-8') as f:
            f.write('\n *** ' + datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + 
                    ' - email_excels_Lenta_parser.py\n' + format_exc())
        get(TELEGRAM_API_LINK + 'Ошибка, сообщение записано в журнале script_messages.log')
        