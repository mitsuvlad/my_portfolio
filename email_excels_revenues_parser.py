#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
from pymysql import connect
from imap_tools import MailBox
from os import path, scandir, remove
from fastzila_credentials import fastzila_credentials
from sys import exit
from datetime import datetime
from traceback import format_exc
from requests import get


FOLDER = 'python_scripts/temp'
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
            if message.from_ == 'pugachev@fastzila.ru':
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
            if message.from_ == 'pugachev@fastzila.ru':
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

    REVENUE_FIELDS = ['ID', 'Дата', 'Сумма Выручки', 'Статья', 'Проект', 'Регион', 'Специальность', 'Заметка']
    def_files_list = []

    for filename in folder_files_list:
        try:
            dataframe = pd.read_excel(path.join(folder, filename))
            if dataframe.columns.to_list() == REVENUE_FIELDS:
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


def delete_objects_in_folder(def_files_list, folder_path=FOLDER):
    """Delete all Lenta's files from working directory
    
    Parameters:  
                folder_path - str
    Return:
                list of str
    """
    
    for file in def_files_list:
        remove(path.join(folder_path, file))


def log_to_sql(def_event, def_log):
    """Commit logs to script_logs table
    
    Parameters:
                def_event, def_log - str
    No return
    """

    sql_log = f"INSERT INTO script_logs (event_type, event_source, description) " \
              f"VALUES ('{def_event}', 'email_excels_revenue_parser.py', '{def_log}');"

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_log)
            connection.commit()


def insert_table_to_sql(data_tuple):
    """Insert or update table with lenta data and return the number of changed rows
    
    Parameters:
                data_tuple - tuple of tuples
    Return:
                int
    """

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    sql = "INSERT INTO fin_revenues (doer_id, date, amount, article_id, created_by, project_id, region_id, " \
          "speciality_id, notes, file_date, filename) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s);"
    
    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            def_rows = cursor.rowcount
            connection.commit()
            
    return def_rows


def get_author_id_from_sql(email):
    """Get author id from table db_fastzila.users

    Parameters:
                email - str
    Return:
                int
    """

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])

    sql = f"SELECT id FROM db_fastzila.users WHERE email = '{email}';"

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
            author_id = cursor.fetchall()[0][0]

    return author_id


def main(file):
    """Prepare the data, cancel or commit it in any cases, log operations.
    
    Parameters:
                file - str
    No return
    """

    author = '.'.join(file[:int(file.split('.')[0])-1].split('.')[2:])
    df = pd.read_excel(path.join(FOLDER, file))
    df.replace({np.nan: None}, inplace=True)
    df['ID'] = np.where(df.ID.isna(), 0, df.ID)
    df['Заметка'] = np.where(df['Заметка'].isna(), '', df['Заметка'])
    
    unfilled_columns_set = set(df.columns[df.isna().any()].tolist()) - {'ID', 'Заметка'}
    if unfilled_columns_set != set():
        try:
            exit()
        except SystemExit:
            log_to_sql('error', f"{', '.join(unfilled_columns_set)} column(s) has empty values!")

    df['Заметка'].replace({np.nan: None}, inplace=True)

    try:
        author_id = get_author_id_from_sql(author)
    except IndexError:
        author_id = 0
        log_to_sql('warning', f"User '{author}' is unknown. Field 'created_by' sets in 0")

    project_df = pd.DataFrame(get_table_from_sql('db_fastzila.projects'))[[0, 1]]
    regions_df = pd.DataFrame(get_table_from_sql('db_fastzila.regions'))[[0, 4]]
    specialities_df = pd.DataFrame(get_table_from_sql('db_fastzila.specialities'))[[0, 1]]

    merged_df = df.merge(project_df, how='left', left_on='Проект', right_on=1)
    merged_df = merged_df.merge(regions_df, how='left', left_on='Регион', right_on=4)
    merged_df = merged_df.merge(specialities_df, how='left', left_on='Специальность', right_on=1)

    merged_df['file_date'] = datetime.strptime(file.split('.')[1], '%Y-%m-%d').date()
    merged_df['filename'] = file[int(file.split('.')[0]):]
    merged_df['created_by'] = author_id
    output_df = merged_df[['ID', 'Дата', 'Сумма Выручки', 'Статья', 'created_by', '0_x', '0_y', 0, 'Заметка',
                           'file_date', 'filename']]

    row_count = insert_table_to_sql(dataframe_to_tuple(output_df))
    log_to_sql('event', f"{file[int(file.split('.')[0]):]} inserted {row_count} rows to table fin_revenues")
    get(TELEGRAM_API_LINK + f"Обработка файла завершена, загружено {row_count} строк")


if __name__ == '__main__':

    try:
        files_from_email()
        move_emails_inside_mailbox()
        files_list = sort_files_by_types(files_list_in_folder())

        for i in files_list:
            main(i)
        delete_objects_in_folder(files_list)
    except:
        with open(path.join('python_scripts', 'script_messages.log'), 'a', encoding = 'utf-8') as f:
            f.write('\n *** ' + datetime.now().strftime('%Y-%m-%d:%H-%M-%S') + 
                    ' - email_excels_revenues_parser.py\n' + format_exc())
        get(TELEGRAM_API_LINK + 'Ошибка, сообщение записано в журнале script_messages.log')
        