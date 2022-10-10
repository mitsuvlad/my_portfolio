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
from requests import get


FOLDER = 'python_scripts/temp/'
TOKEN = fastzila_credentials['tg_bot_token']
CHAT_ID = "-632497144"
TELEGRAM_API_LINK = f"https://api.telegram.org/bot{TOKEN}/sendMessage?chat_id={CHAT_ID}&parse_mode=HTML&text="


def files_from_email(imap=fastzila_credentials['imap'], 
                     email=fastzila_credentials['email'], 
                     password=fastzila_credentials['mail_pass'],
                     from_folder='INBOX', folder_path=FOLDER):
    """Copy email attachments to folder_path
    
    Parameters: 
               imap, email, password, from_folder, folder_path - str
    No return
    """
    
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
                               from_folder='INBOX', to_folder='Archive'):
    """Move all mails from INBOX folder to Archive after processing its files
    
    Parameters:
                email, password, imap, from_folder, to_folder - str
    No return
    """
    
    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        messages_list = []
        for message in mailbox.fetch():
            if (message.from_ == 'romanova_tat@fastzila.ru') | (message.from_ == 'vlasiuk@fastzila.ru'):
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


def sort_files_by_types(folder_files_list, folder=FOLDER):
    """Return list of files contained Samokat data
    
    Parameters:
                folder_files_list - list of str
                folder - str
    Return:
                list of str
    """

    LENTA_REQUESTS_FIELDS_1 = ['ТК', 'Тип ресурса']
    LENTA_REQUESTS_FIELDS_2 = ['Ночь', 'Доп потребность']

    def_files_list = []
    for filename in folder_files_list:
        try:
            dataframe = pd.read_excel(path.join(folder, filename))

            if (dataframe.columns[:2].to_list() == LENTA_REQUESTS_FIELDS_1) & \
                (LENTA_REQUESTS_FIELDS_2[0] in dataframe.columns.to_list()) & \
                (LENTA_REQUESTS_FIELDS_2[1] in dataframe.columns.to_list()):

                def_files_list.append(filename)
                get(TELEGRAM_API_LINK + f"Обнаружен файл <b>'{filename[int(filename.split('.')[0]):]}'</b>, идет обработка ...")
        except (ValueError, KeyError):
            pass
    
    return def_files_list


def get_table_from_sql(table_name):
    """Get table from database
    
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


def log_to_sql(def_event, def_log):
    """Commit logs to script_logs table
    
    Parameters:
                def_event, def_log - str
    No return
    """

    sql_log = f"INSERT INTO script_logs (event_type, event_source, description) " \
              f"VALUES ('{def_event}', 'email_excels_Lenta_requests_parser.py', '{def_log}');"

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_log)
            connection.commit()


def add_elements_to_dictionary_to_sql(elements_list):
    """Insert new found elements to database dictionary-tables
    
    Parameters:
                elements_list - set
    No return
    """
    
    sql = f"INSERT INTO lenta_tks (tk_name) VALUES (%s)"
    
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, tuple(elements_list))
            connection.commit()
            
    log_to_sql('event', f"{','.join(elements_list)} added to dictionary-table lenta_tks")


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
    
    sql = "INSERT INTO lenta_requests " \
          "(tk_id, resource_id, is_night, add_requirement, doer_count, requested_date, file_date, \
          filename) " \
          "VALUES(%s, %s, %s, %s, %s, %s, %s, %s);"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
            connection.commit()
    return def_rows


def delete_objects_in_folder(files, folder_path=FOLDER):
    """Delete all Lenta's files from working directory

    Parameters:
                folder_path - str
    Return:
                list of str
    """

    for file in files:
        remove(path.join(folder_path, file))


def main(file, folder=FOLDER):

    docs = file[int(file.split('.')[0]):]
    dates = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()
    df0 = pd.read_excel(path.join(folder, file))

    # patched - delete Unnamed columns in couriers DataFrame
    columns_list = []
    for i in df0.columns.to_list():
        if isinstance(i, str):
            if 'Unnamed:' not in i:
                columns_list.append(i)
        else:
            columns_list.append(i)
    df0 = df0[columns_list]
    
    df1 = pd.read_excel(path.join(folder, file), sheet_name=1)
    df1 = df1.rename(columns={'Unnamed: 0': 'ТК'})
    
    # patched - delete Unnamed columns in pickers DataFrame
    columns_list = []
    for i in df1.columns.to_list():
        if isinstance(i, str):
            if 'Unnamed:' not in i:
                columns_list.append(i)
        else:
            columns_list.append(i)
    df1 = df1[columns_list]
    
    df = pd.concat([df0, df1])
    
    df['ТК'] = np.where(df['ТК'].astype(str).str.contains('ТК'),  df['ТК'], 'ТК' + df['ТК'].astype(int).astype(str))

    # Take TKs from database, compare it with available TKs in email-file & append absent to database
    tk_df = pd.DataFrame(get_table_from_sql('lenta_tks'), columns=('tk_id', 'tk_name', 'cities', 'region_id'))[['tk_id', 'tk_name']]
    email_tk_set = set(df['ТК'])
    db_tk_set = set(tk_df.tk_name)
    update_set = email_tk_set - db_tk_set
    if update_set != set():
        add_elements_to_dictionary_to_sql(tuple(update_set))
        tk_df = pd.DataFrame(get_table_from_sql('lenta_tks'), columns=('tk_id', 'tk_name', 'cities', 'region_id'))[['tk_id', 'tk_name']]

    # Make tk_id column in dataframe
    df = df.merge(tk_df, how='left', left_on='ТК', right_on='tk_name')

    # Create 'resource_id column, fill it & log if it has irregular values
    df['resource_id'] = 0
    df['resource_id'] = np.where(df['Тип ресурса'] == 'Курьер (кол-во)', 1, df.resource_id)
    df['resource_id'] = np.where(df['Тип ресурса'] == 'Пикер (кол-во)', 2, df.resource_id)

    if df[df['resource_id'] == 0].shape[0] != 0:
        log_to_sql('warning', f"Field 'resource_id' has {df[df['resource_id'] == 0].shape[0]} zero values")

    # Mofify the dataframe to data commit
    data_df = df[['tk_id', 'resource_id', 'Ночь', 'Доп потребность']]
    dates_df = df.loc[:, 'Тип ресурса':'Ночь'].drop(['Тип ресурса', 'Ночь'], axis=1)
    data_dates_df = pd.concat([data_df, dates_df], axis=1)

    data_dates_df.replace({np.nan: None}, inplace=True)

    # This cycle create everyday dataframe & commits there to database
    counter = 0
    for i in data_dates_df.loc[:, 'Доп потребность':].drop('Доп потребность', axis=1).columns.to_list():

        output_df = data_dates_df.loc[:, :'Доп потребность']
        output_df['doer_count'] = pd.to_numeric(data_dates_df[i], errors='coerce')
        output_df.doer_count.replace({np.nan: None}, inplace=True)
        output_df['requested_date'] = i
        output_df['file_date'] = dates
        output_df['filename'] = docs
        insert_data_to_sql(dataframe_to_tuple(output_df))
        counter += output_df.shape[0]

    log_to_sql('event', f"File {file} added new {counter} records")
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
                    ' - email_excels_Lenta_requests_parser.py\n' + format_exc())
        get(TELEGRAM_API_LINK + 'Ошибка, сообщение записано в журнале script_messages.log')
        