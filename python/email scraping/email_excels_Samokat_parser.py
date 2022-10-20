#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
from os import path, scandir, remove
from pymysql import connect
from datetime import datetime
from imap_tools import MailBox
from fastzila_credentials import fastzila_credentials
from traceback import format_exc
from requests import get


FOLDER = 'python_scripts/temp/'
TOKEN = fastzila_credentials['tg_bot_token']
CHAT_ID = "-632497144"
TELEGRAM_API_LINK = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&parse_mode=HTML&text="


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
            if '@fastzila.ru' in message.from_:
                for attachment in message.attachments:
                    file_prefix = '.' + str(message.date.date()) + '.' + message.from_ + '.'
                    with open(path.join(folder_path, str(len(file_prefix) + 2) + file_prefix +
                                                     attachment.filename), 'wb') as f:
                        f.write(attachment.payload)


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
            if '@fastzila.ru' in message.from_:
                messages_list.append(message)
        for message in messages_list:
            mailbox.move(message.uid, to_folder)


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


def log_to_sql(def_event, def_log):
    """Commit logs to script_logs table
    
    Parameters:
                def_event, def_log - str
    No return
    """

    sql_log = f"INSERT INTO script_logs (event_type, event_source, description) " \
              f"VALUES ('{def_event}', 'email_excels_Samokat_parser.py', '{def_log}');"

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_log)
            connection.commit()


def sort_files_by_types(folder_files_list, folder=FOLDER):
    """Return list of files contained Samokat data
    
    Parameters:
                folder_files_list - list of str
                folder - str
    Return:
                list of str
    """

    SAMOKAT_FIELDS = ['Подразделение', 'Исполнитель', 'Роль', 'ID', 'Город']

    def_files_list = []
    for filename in folder_files_list:
        try:
            dataframe = pd.read_excel(path.join(folder, filename))
            if dataframe.columns[:5].to_list() == SAMOKAT_FIELDS:
                def_files_list.append(filename)
                get(TELEGRAM_API_LINK + f"Обнаружен файл <b>'{filename[int(filename.split('.')[0]):]}'</b>, идет обработка ...")
        except (ValueError, KeyError):
            pass
    
    return def_files_list


def get_table_from_sql(table_name):
    """Get table from database by input table_name. 
    
    Parameters:
                table_name - str
    Return:
                list of lists
    """
    
    sql = f"SELECT * FROM {table_name};"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            list_sql = cursor.fetchall()
    return list_sql


def dataframe_to_tuple(dataframe):
    """Make tuples from dataframes and update or insert them into the database table
    
    Parameters:
                dataframe - pandas.DataFrame
    Return:
                tuple of tuples
    """
    
    return tuple(dataframe.itertuples(index=False, name=None))


def add_elements_list_to_dictionary_to_sql(elements_list):
    """Insert new found elements to dictionary-tables
    
    Parameters:
                elements_list - set
    No return
    """
    
    sql = f"INSERT INTO samokat_tks (city_id, address) VALUES (%s, %s)"
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])

    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, tuple(elements_list))
            connection.commit()
    log_to_sql('event', f"{len(elements_list)} elements added to dictionary table samokat_tks")


def get_dates_list_from_sql():
    """Get table from database by input table_name. 
    
    No parameters
    Return:
                list of dates
    """
    
    sql = 'SELECT report_date FROM samokat_doers_recieved GROUP BY report_date;'
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            tuple_sql = cursor.fetchall()
    return [i[0] for i in tuple_sql]


def delete_records_with_repeating_dates_in_sql(dates_tuple):
    """In case of incoming data with consisted dates, delete all records on existed date & replace with the new records
    
    Parameters:
                    dates_tuple - tuple of dates
    No return
    """
    
    if len(dates_tuple) == 1:
        sql = f"DELETE FROM samokat_doers_recieved WHERE report_date = '{dates_tuple[0]}';"
    else:
        sql = f"DELETE FROM samokat_doers_recieved WHERE report_date IN {dates_tuple};"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            def_rows = cursor.rowcount
            connection.commit()
            
    log_to_sql('event', f"Dates: {', '.join(dates_tuple)} intersecting in db & email-file. {def_rows} "
                        f"records was deleted & inserted")


def insert_data_to_sql(data_tuple):
    """Insert to table with preferred data and return the number of inserted rows
    
    Parameters:
                data_tuple - tuple of tuples
    Return:
                int
    """
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "INSERT INTO samokat_doers_recieved "\
          "(doer_name, doer_id, tks_id, speciality_id, worked_hours, report_date, file_date, filename) "\
          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s);"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
            connection.commit()
    return def_rows


def delete_objects_in_folder(def_files_list, folder_path=FOLDER):
    """Delete all Samokat files from working directory
    
    Parameters:  
                folder_path - str
    Return:
                list of str
    """
    
    for file in def_files_list:
        remove(path.join(folder_path, file))


def main(file):
    """Prepare the data, commit it, change samokat_tks dictionary if it needs & log operations
    
    Parameters:
                file - str
    No return
    """
    
    docs = file[int(file.split('.')[0]):]
    dates = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()
    
    # Receive and prepare data
    df = pd.read_excel(path.join(FOLDER, file))
    df = df.iloc[1:df.shape[0]-1]
    df['Город'] = df['Город'].str.strip()
    df['ID'] = pd.to_numeric(df.ID, errors='coerce', downcast='integer')
    
    regions_df = pd.DataFrame(get_table_from_sql('db_fastzila.regions'), 
                              columns=('id', 'cluster_id', 'fin_cluster_id',
                                       'name', 'legal_name', 'amber_id', 'has_hr'))
    regions_df = regions_df[['id', 'legal_name']]
    df = df.merge(regions_df, how='left', left_on='Город', right_on='legal_name')
    df[df.columns[5:-3]] = df[df.columns[5:-3]].fillna(0)
    
    # Create speciality identifier & commit message if got zero identifier
    df['speciality_id'] = 0
    df['speciality_id'] = np.where(df['Роль'] == 'Вело курьер', 4, df.speciality_id)
    df['speciality_id'] = np.where(df['Роль'] == 'Курьер-электровело', 4, df.speciality_id)
    df['speciality_id'] = np.where(df['Роль'] == 'Авто-курьер', 1, df.speciality_id)
    df['speciality_id'] = np.where(df['Роль'] == 'Сборщик', 3, df.speciality_id)
    df['speciality_id'] = np.where(df['Роль'] == 'Мото-курьер', 7, df.speciality_id)
    if df[df['speciality_id'] == 0].shape[0] > 0:
        log_to_sql('warning', 'Zero values if the "speciality_id" field')
    
    # Create TK identifier, add record to database if TK absent & send report about new TKs
    tks_df = pd.DataFrame(get_table_from_sql('samokat_tks'), columns=('id', 'city_id', 'address'))
    database_tk_set = set(dataframe_to_tuple(tks_df[['city_id', 'address']]))
    email_tk_set = set(dataframe_to_tuple(df[['id', 'Подразделение']]))
    difference_tks_names_set = email_tk_set - database_tk_set

    if difference_tks_names_set != set():
        add_elements_list_to_dictionary_to_sql(tuple(difference_tks_names_set))
#        log_to_sql('event', f"{len(difference_tks_names_set)} new TKs found in Samokat report")
        tks_df = pd.DataFrame(get_table_from_sql('samokat_tks'), columns=('id', 'city_id', 'address'))
        
    df = df.merge(tks_df, how='left', left_on=['Подразделение', 'id'], right_on=['address', 'city_id'])
    
    df = pd.concat([df['Исполнитель'], df['ID'], df.id_y, df.speciality_id, df.iloc[:, 5:-7]], axis=1)
    
    # Commit only original data - find & delete intersecting report_dates
    email_dates_list = df.iloc[:, 4:].columns.to_list()

    try:
        email_dates_list = [datetime.strptime(i, '%d.%m.%y').date() for i in email_dates_list]
    except TypeError:
        email_dates_list = [i.date for i in email_dates_list]

    db_dates_list = get_dates_list_from_sql()
    date_columns_set = set(db_dates_list) & set(email_dates_list)
    if date_columns_set != set():
        delete_records_with_repeating_dates_in_sql(tuple([i.strftime('%y.%m.%d') for i in date_columns_set]))
    
    # Commit all data to database & log the count of new records commited    
    counter = 0
    for i in df.iloc[:, 4:].columns.to_list():

        output_df = pd.concat([df.iloc[:, :4], df[i]], axis=1)
        
        try:
            output_df['report_date'] = datetime.strptime(i, "%d.%m.%y")
        except TypeError:
            output_df['report_date'] = i
        
        output_df['file_date'] = dates
        output_df['filename'] = docs
#        output_df['ID'] = output_df['ID'].str.findall(r'\d+').str[0]
        output_df = output_df[output_df[i] != 0]
        insert_data_to_sql(dataframe_to_tuple(output_df))
        counter += output_df.shape[0]
    
    log_to_sql('event', f"File {docs} added new {counter} records")
    get(TELEGRAM_API_LINK + f"Обработка файла завершена, загружено {counter} строк")


if __name__ == '__main__':
    
    try:
        files_from_email()
        move_emails_inside_mailbox()
    
        files_list = sort_files_by_types(files_list_in_folder())
        for one_file in files_list:
            main(one_file)
        
        delete_objects_in_folder(files_list)
    except:
        with open(path.join('python_scripts', 'script_messages.log'), 'a', encoding = 'utf-8') as f:
            f.write('\n *** ' + datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + 
                    ' - email_excels_Samokat_parser.py\n' + format_exc())
        get(TELEGRAM_API_LINK + 'Ошибка, сообщение записано в журнале script_messages.log')
        