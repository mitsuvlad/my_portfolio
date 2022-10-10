#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from pymysql import connect
from imap_tools import MailBox
from os import path, scandir, remove
from fastzila_credentials import fastzila_credentials
from traceback import format_exc


FOLDER = 'python_scripts/temp/'


def files_from_email(imap=fastzila_credentials['imap'],
                     email=fastzila_credentials['email'],
                     password=fastzila_credentials['mail_pass'],
                     from_folder='INBOX',
                     folder_path=FOLDER):
    """Copy email attachments to folder_path
    
    Parameters: 
               imap, email, password, from_folder, folder_path - str
    No return
    """
    
    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        for message in mailbox.fetch():
            if '@corp.mail.ru' in message.from_:
                for attachment in message.attachments:
                    if attachment.filename[::-1][:4] != 'gnp.':
                        file_prefix = '.' + str(message.date.date()) + '.' + message.from_ + '.'
                        with open(path.join(folder_path, str(len(file_prefix) + 2) + file_prefix +
                                                         attachment.filename), 'wb') as f:
                            f.write(attachment.payload)


def files_list_in_folder(folder_path=FOLDER):
    """Recieve files_list in defined folder_path
    
    Parameters:  
                folder_path - str
    Return:
                list of str
    """
    
    folder_files_list = []
    with scandir(folder_path) as entries:
        for entry in entries:
            if entry.is_file():
                folder_files_list.append(entry.name)
    return folder_files_list


def sort_files_in_folder_by_types(folder_files_list, folder=FOLDER):
    """Return three lists of files by types of documents
    
    Parameters:
                folder_files_list - list of str
                folder - str
    Return:
                list of 3 lists of str
    """

    PREFERS_FIELDS_PHONE = ['courier_id', 'first_name', 'last_name', 'full_name', 'phone', 'type', 'fraud',
                            'delivery_provider_un_id', 'delivery_provider_name', 'delivery_service', 'zones',
                            'zone_ids', 'couriers_zones_last_update', 'day_of_week_number', 'day_of_week',
                            'time_start', 'time_end', 'time_interval', 'couriers_intervals_last_update']
    PREFERS_FIELDS_NOPHONE = ['courier_id', 'first_name', 'last_name', 'full_name', 'type', 'fraud',
                              'delivery_provider_un_id', 'delivery_provider_name', 'delivery_service', 'zones',
                              'zone_ids', 'couriers_zones_last_update', 'day_of_week_number', 'day_of_week',
                              'time_start', 'time_end', 'time_interval', 'couriers_intervals_last_update']
    OUTPUTS_FIELDS = ['city', 'delivery_provider', 'delivery_service', 'starting_point_id', 'starting_point_name',
                      'delivery_zone_id', 'delivery_zone_name', 'courier_un_id', 'rider_name', 'schedule_date',
                      'start_time', 'end_time', 'horario_reparto_id', 'free_float']

    def_prefers_list, def_outputs_list = [], []

    for filename in folder_files_list:
        try:
            dataframe = pd.read_excel(path.join(folder, filename))
            if (dataframe.columns.to_list() == PREFERS_FIELDS_PHONE) | (dataframe.columns.to_list() == PREFERS_FIELDS_NOPHONE):
                def_prefers_list.append(filename)
            elif dataframe.columns.to_list() == OUTPUTS_FIELDS:
                def_outputs_list.append(filename)
        except (ValueError, KeyError):
            pass

    return [def_prefers_list, def_outputs_list]


def sort_lists_by_date(unsorted_list):
    """Return sorted list of files by dates of recieving
    
    Parameters:
                unsorted_list - list of str
    Return:
                list of str
    """

    unsorted_list.sort(key=lambda x: datetime.strptime(x.split('.')[1], '%Y-%m-%d'))
    return unsorted_list


def make_daily_dataframe(dataframe, doc_date):
    """Return dataframe with added field preferred_day and selected by weekdays
    
    Parameters:
                dataframe - pandas.DataFrame
                doc_date - datetime
    Return:
                pandas.DataFrame
    """
    
    dataframe['preferred_date'] = doc_date
    return dataframe[dataframe.day_of_week_number == doc_date.isoweekday()].copy()


def get_last_date_from_sql(date):
    """Process query for get last date preferred_date in table dc_prefers_recieved or input date
    
    Parameters:
                date - datetime
    Return:
                date - datetime
    """
    
    sql = "SELECT preferred_date     FROM dc_prefers_recieved     ORDER BY preferred_date DESC     LIMIT 1;"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            get_date = cursor.fetchall()
    
    if get_date != ():
        date = get_date[0][0]
    return date


def delete_files_in_folder(folder_files_list, folder=FOLDER):
    """Clean work folder from files
    Parameters:
                folder_files_list - list of str
                folder - str
    No return
    """

    for filename in folder_files_list:
        remove(path.join(folder, filename))


def dataframe_to_tuple(dataframe):
    """Make tuples from dataframes and update or insert them into the database table
    
    Parameters:
                dataframe - pandas.DataFrame
    Return:
                tuple of tuples
    """
    
    return tuple(dataframe.itertuples(index=False, name=None))


def move_emails_inside_mailbox(imap=fastzila_credentials['imap'],
                               email=fastzila_credentials['email'],
                               password=fastzila_credentials['mail_pass'],
                               from_folder='INBOX', to_folder='Archive'):
    """Move all mails INBOX -> Archive after processing its files
    Parameters:
                email, password, imap, from_folder, to_folder - str
    No return
    """

    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        messages_list = []
        for message in mailbox.fetch():
            if '@corp.mail.ru' in message.from_:
                messages_list.append(message)
        for message in messages_list:
            mailbox.move(message.uid, to_folder)


def make_daily_dataframe_without_preferences(def_excluded_couriers, doc, date):
    """Make dataframe with couriers ids, document parameters and empty columns for replace absent couriers records
    
    Parameters:
                def_excluded_couriers - array of int,
                day, date - datetime.day
                doc - str
    Return:
                pandas.DataFrame
    """
    
    # Make update_dataframe
    default_tuple = (None,) * len(def_excluded_couriers)
    output_df = pd.DataFrame({'zones': default_tuple,
                              'zone_ids': default_tuple,
                              'couriers_zones_last_update': default_tuple,
                              'day_of_week_number': default_tuple,
                              'day_of_week': default_tuple,
                              'time_start': default_tuple,
                              'time_end': default_tuple,
                              'time_interval': default_tuple,
                              'couriers_intervals_last_update': default_tuple,
                              'filename': doc,
                              'file_date': date,
                              'courier_id': def_excluded_couriers})
    
    return output_df


def get_excluded_couriers_on_date_from_sql(excluded_couriers_str, date):
    """Process query for get last date preferred_date in table dc_prefers_recieved or input date
    
    Parameters:
                def_excluded_couriers - str
                date - datetime.date
    Return:
                list of int
    """
    
    sql = f"SELECT courier_id FROM dc_prefers_recieved WHERE preferred_date = {date} AND courier_id IN " \
          f"{excluded_couriers_str};"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            output_list = cursor.fetchall()
    
    return output_list


def insert_update_prefers(dataframe, day, def_docs):
    """Make dataframe with couriers ids, document parameters and empty columns for replace absent couriers records
    Calls insert_update_prefers_to_sql to insert/update data
    Check courier_id & type fields for NULL values
    Parameters:
                def_excluded_couriers - array of int,
                day, def_dates - datetime.day
                def_docs - str
    Return:
                pandas.DataFrame
    """
    
    dataframe = make_daily_dataframe(dataframe, day)
    def_rows = insert_update_prefers_to_sql(dataframe_to_tuple(dataframe))
    if def_rows > 0:
        log_to_sql('event', f"{def_docs[int(def_docs.split('.')[0]):]} document inserted/updated {def_rows} records on date {day} "
                   f"by working employee")


def update_without_prefers(def_excluded_couriers, day, def_docs, def_dates):
    """Make dataframe with couriers ids, document parameters and empty columns for replace absent couriers records.
    Calls function get_excluded_couriers_on_date_from_sql to collect list of antendant couriers in table to fill there
    records with None fields.
    Calls update_prefers_to_sql to commit updates to database
    
    Parameters:
                def_excluded_couriers - array of int,
                day, def_dates - datetime.day
                def_docs - str
    No return
    """
    
    if len(def_excluded_couriers) >= 1:
        if len(def_excluded_couriers) > 1:
            excluded_couriers_str = str(tuple(def_excluded_couriers))
        else:
            excluded_couriers_str = f'({def_excluded_couriers[0]})'
            
        excluded_couriers_list = get_excluded_couriers_on_date_from_sql(excluded_couriers_str, day)
            
        without_preferes_df = make_daily_dataframe_without_preferences(excluded_couriers_list, def_docs, def_dates)
        def_rows = update_prefers_to_sql(dataframe_to_tuple(without_preferes_df))

        if def_rows > 0:
            log_to_sql('event', f'{def_docs} of {def_dates} document updated {def_rows} records on date {day} '
                       f'by absent employee')


def main_prefers(def_prefers_list):
    """main_1 fills table dc_prefers_recieved with workers prefers
    
    Parameters:
                def_prefers_list - list of str
    No return
    """
    
    for file in def_prefers_list:

        # Preparation any constants for data manipulations
        docs = file[int(file.split('.')[0]):]
        dates = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()
        last_preferred_date = get_last_date_from_sql(dates)
        third_day = (dates + timedelta(days=3))
        fourth_day = (dates + timedelta(days=4))
        fifth_day = (dates + timedelta(days=5))

        # Get dataframe & preliminary preparations
        df = pd.read_excel(path.join(FOLDER, file))
        if 'phone' in df.columns.to_list():
            df = df.drop('phone', axis=1)

        if df[df.courier_id.isna()].shape[0] != 0:
            log_to_sql('warning', f'File {docs} has field "courier_id" without value')
        if df[df.type.isna()].shape[0] != 0:
            log_to_sql('warning', f'File {docs} has field "type" without value')
            
        df = df.drop_duplicates(subset=['courier_id', 'day_of_week_number'], keep='last')
        excluded_couriers = df[df.couriers_intervals_last_update.isna()].courier_id.unique()
        df = df[df.couriers_intervals_last_update.notna()]
        df['filename'] = docs
        df['file_date'] = dates

        # Make dataframe for insert
        df.zones.replace({np.nan: None}, inplace=True)
        df.zone_ids.replace({np.nan: None}, inplace=True)
        df.couriers_zones_last_update = df.couriers_zones_last_update.astype('datetime64[us]')
        df.couriers_zones_last_update.replace({np.nan: None}, inplace=True)
        df.couriers_intervals_last_update = df.couriers_intervals_last_update.astype('datetime64[us]')
        df.couriers_intervals_last_update.replace({np.nan: None}, inplace=True)

        if (last_preferred_date - dates).days > 4:
            update_without_prefers(excluded_couriers, third_day, file, dates)
            update_without_prefers(excluded_couriers, fourth_day, file, dates)
            update_without_prefers(excluded_couriers, fifth_day, file, dates)
            insert_update_prefers(df, third_day, file)
            insert_update_prefers(df, fourth_day, file)
            insert_update_prefers(df, fifth_day, file)

        elif (last_preferred_date - dates).days == 4:
            update_without_prefers(excluded_couriers, third_day, file, dates)
            update_without_prefers(excluded_couriers, fourth_day, file, dates)
            insert_update_prefers(df, third_day, file)
            insert_update_prefers(df, fourth_day, file)
            insert_update_prefers(df, fifth_day, file)

        elif (last_preferred_date - dates).days == 3:
            update_without_prefers(excluded_couriers, third_day, file, dates)
            insert_update_prefers(df, third_day, file)
            insert_update_prefers(df, fourth_day, file)
            insert_update_prefers(df, fifth_day, file)

        elif (last_preferred_date - dates).days < 3:
            insert_update_prefers(df, third_day, file)
            insert_update_prefers(df, fourth_day, file)
            insert_update_prefers(df, fifth_day, file)


def main_outputs(def_outputs_list):
    """main_2 forms table dc_outputs_recieved with outputs
    
    Parameters:
                def_outputs_list - list of str
    No return
    """

    for file in def_outputs_list:

        # Preparation any constants for data manipulations
        docs = file[int(file.split('.')[0]):]
        dates = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()

        # Get dataframe & preliminary preparation
        df = pd.read_excel(path.join(FOLDER, file))
        df['filename'] = docs
        df['fix_date'] = dates
        df.replace({np.nan: None}, inplace=True)

        # Insert data in to the database
        rows = insert_outputs_to_sql(dataframe_to_tuple(df))
        if rows > 0:
            log_to_sql('event', f'{docs} of {dates} document inserted {rows} records on date {dates}')
            if df[df.city.isna()].shape[0] != 0:
                log_to_sql('warning', 'Table dc_outputs_recieved recieved city(ies) without value')
            if df[df.courier_un_id.isna()].shape[0] != 0:
                log_to_sql('warning', 'Table dc_outputs_recieved recieved type(s) without value')
        else:
            log_to_sql('warning', f'{docs} of {dates} document inserted 0 records on date {dates}')


def update_prefers_to_sql(data_tuple):
    """Update query to table with preferred data and return the number of updated rows
    
    Parameters:
                data_tuple: - tuple of tuples
    No return
    """

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "UPDATE dc_prefers_recieved " \
          "SET zones = %s, zone_ids = %s, couriers_zones_last_update = %s, day_of_week_number = %s, " \
          "day_of_week = %s, time_start = %s, time_end = %s, time_interval = %s, couriers_intervals_last_update = %s," \
          "filename = %s, file_date = %s " \
          "WHERE courier_id = %s AND preferred_date = %s;"
    
    with connection:
        with connection.cursor() as cursor:            
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
        connection.commit()
    return def_rows


def insert_update_prefers_to_sql(data_tuple):
    """Insert to table with preferred data and return the number of inserted rows
    
    Parameters:
                data_tuple - tuple of tuples
    Return:
                int
    """

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "INSERT INTO dc_prefers_recieved (courier_id, first_name, last_name, full_name, type, fraud, " \
          "delivery_provider_un_id, delivery_provider_name, delivery_service, zones, zone_ids, " \
          "couriers_zones_last_update, day_of_week_number, day_of_week, time_start, time_end, time_interval, " \
          "couriers_intervals_last_update, filename, file_date, preferred_date)" \
          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
          "ON DUPLICATE KEY UPDATE courier_id = VALUES(courier_id), first_name = VALUES(first_name), " \
          "last_name = VALUES(last_name), full_name = VALUES(full_name), " \
          "type = VALUES(type), fraud = VALUES(fraud), delivery_provider_un_id = VALUES(delivery_provider_un_id), " \
          "delivery_provider_name = VALUES(delivery_provider_name), delivery_service = VALUES(delivery_service), " \
          "zones = VALUES(zones), zone_ids = VALUES(zone_ids), " \
          "couriers_zones_last_update = VALUES(couriers_zones_last_update), " \
          "day_of_week_number = VALUES(day_of_week_number), day_of_week = VALUES(day_of_week), " \
          "time_start = VALUES(time_start), time_end = VALUES(time_end), time_interval = VALUES(time_interval), " \
          "couriers_intervals_last_update = VALUES(couriers_intervals_last_update), " \
          "filename = IF(first_name <> VALUES(first_name) OR last_name <> VALUES(last_name) OR " \
          "full_name <> VALUES(full_name) OR type <> VALUES(type) OR " \
          "fraud <> VALUES(fraud) OR delivery_provider_un_id <> VALUES(delivery_provider_un_id) OR " \
          "delivery_provider_name <> VALUES(delivery_provider_name) OR " \
          "delivery_service <> VALUES(delivery_service) OR zones <> VALUES(zones) OR zone_ids <> VALUES(zone_ids) OR " \
          "couriers_zones_last_update <> VALUES(couriers_zones_last_update) OR " \
          "day_of_week_number <> VALUES(day_of_week_number) OR day_of_week <> VALUES(day_of_week) OR " \
          "time_start <> VALUES(time_start) OR time_end <> VALUES(time_end) OR " \
          "time_interval <> VALUES(time_interval) OR " \
          "couriers_intervals_last_update <> VALUES(couriers_intervals_last_update), VALUES(filename), filename), " \
          "file_date = IF(first_name <> VALUES(first_name) OR last_name <> VALUES(last_name) OR " \
          "full_name <> VALUES(full_name) OR type <> VALUES(type) OR " \
          "fraud <> VALUES(fraud) OR delivery_provider_un_id <> VALUES(delivery_provider_un_id) OR " \
          "delivery_provider_name <> VALUES(delivery_provider_name) OR " \
          "delivery_service <> VALUES(delivery_service) OR zones <> VALUES(zones) OR " \
          "zone_ids <> VALUES(zone_ids) OR couriers_zones_last_update <> VALUES(couriers_zones_last_update) OR " \
          "day_of_week_number <> VALUES(day_of_week_number) OR day_of_week <> VALUES(day_of_week) OR " \
          "time_start <> VALUES(time_start) OR time_end <> VALUES(time_end) OR " \
          "time_interval <> VALUES(time_interval) OR " \
          "couriers_intervals_last_update <> VALUES(couriers_intervals_last_update), VALUES(file_date), file_date);"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
        connection.commit()
    return def_rows


def insert_outputs_to_sql(data_tuple):
    """Insert to table with outputs data and return the number of inserted rows
    
    Parameters:
                data_tuple - tuple of tuples
    Return:
                int
    """

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "INSERT INTO dc_outputs_recieved (city, delivery_provider, delivery_service, " \
          "starting_point_id, starting_point_name, delivery_zone_id, delivery_zone_name, courier_un_id, rider_name, " \
          "schedule_date, start_time, end_time, horario_reparto_id, free_float, filename, fix_date) " \
          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s) " \
          "ON DUPLICATE KEY UPDATE city = VALUES(city), delivery_provider = VALUES(delivery_provider), " \
          "delivery_service = VALUES(delivery_service), starting_point_id = VALUES(starting_point_id), " \
          "starting_point_name = VALUES(starting_point_name), delivery_zone_id = VALUES(delivery_zone_id), " \
          "delivery_zone_name = VALUES(delivery_zone_name), courier_un_id = VALUES(courier_un_id), " \
          "rider_name = VALUES(rider_name), schedule_date = VALUES(schedule_date), start_time = VALUES(start_time), " \
          "end_time = VALUES(end_time), horario_reparto_id = VALUES(horario_reparto_id), " \
          "free_float = VALUES(free_float), filename = VALUES(filename), fix_date = VALUES(fix_date);"

    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
            connection.commit()
    return def_rows


def log_to_sql(def_event, def_log):
    """Commit logs to script_logs table
    
    Parameters:
                def_event, def_log - str
    No return
    """

    sql_log = f"INSERT INTO script_logs (event_type, event_source, description) "\
              f"VALUES ('{def_event}', 'email_excels_DeliveryClub_parser.py', '{def_log}');"

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_log)
            connection.commit()


if __name__ == '__main__':
    
    try:
        files_from_email()
        move_emails_inside_mailbox()

        files_list = files_list_in_folder()
        prefers_list, outputs_list = sort_files_in_folder_by_types(files_list)
        prefers_list = sort_lists_by_date(prefers_list)
        outputs_list = sort_lists_by_date(outputs_list)
        main_prefers(prefers_list)
        main_outputs(outputs_list)

    # Cleaning after work
        delete_files_in_folder(prefers_list)
        delete_files_in_folder(outputs_list)

    except Exception as err:
        with open(path.join('python_scripts', 'script_messages.log'), 'a', encoding = 'utf-8') as f:
            f.write('\n *** ' + datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + 
                    ' - email_excels_DeliveryClub_parser.py\n' + format_exc())