#!/usr/bin/env python
# coding: utf-8


from imap_tools import MailBox
from fastzila_credentials import fastzila_credentials
from os import path, scandir, remove
from pymysql import connect
import pandas as pd


FOLDER = 'python_scripts/temp/'


def files_from_email(from_email = [],
                     imap=fastzila_credentials['imap'],
                     email=fastzila_credentials['email'],
                     password=fastzila_credentials['mail_pass'],
                     from_folder='INBOX',
                     folder_path=FOLDER) -> None:
    """Copy attachments to folder_path from specified emails except embedded png-files"""
    
    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        for message in mailbox.fetch():
            if ([i in message.from_ for i in from_email].count(True) > 0) | (from_email == []):
                for attachment in message.attachments:
                    if attachment.filename[::-1][:4] != 'gnp.':
                        file_prefix = '.' + str(message.date.date()) + '.' + message.from_ + '.'
                        with open(path.join(folder_path, str(len(file_prefix) + 2) + file_prefix +
                                                         attachment.filename), 'wb') as f:
                            f.write(attachment.payload)


def move_emails_inside_mailbox(from_email = [],
                               imap=fastzila_credentials['imap'], 
                               email=fastzila_credentials['email'], 
                               password=fastzila_credentials['mail_pass'],
                               from_folder='INBOX', to_folder='Archive') -> list:
    """Move letters within folders inside a mailbox.
    from_email contains masks adresses of senders"""
    
    with MailBox(imap).login(email, password, initial_folder=from_folder) as mailbox:
        messages_list = []
        for message in mailbox.fetch():
            if ([i in message.from_ for i in from_email].count(True) > 0) | (from_email == []):
                messages_list.append(message)
        for message in messages_list:
            mailbox.move(message.uid, to_folder)
    
    # return ['FROM: ' + i.from_ + ', Date: ' + i.date.strftime('%Y-%m-%d:%H-%M-%S') for i in messages_list]


def files_list_in_folder(folder_path=FOLDER) -> list:
    """Recieve files_list in defined folder_path"""
    
    folder_files_list = []
    with scandir(folder_path) as entries:
        for entry in entries:
            if entry.is_file():
                folder_files_list.append(entry.name)
                
    return folder_files_list


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


def dataframe_to_tuple(dataframe: pd.DataFrame) -> tuple:
    """Make tuples from dataframes and update or insert them into the database table"""
    
    return tuple(dataframe.itertuples(index=False, name=None))


def delete_files_in_folder(files_list: list, folder_path=FOLDER) -> None:
    """Delete all files in the working directory"""
    
    for file in files_list:
        remove(path.join(folder_path, file))


def log_to_sql(def_event: str, def_filename: str, def_log: str) -> None:
    """Commit logs to script_logs table"""

    sql_log = f"INSERT INTO script_logs (event_type, event_source, description) "\
              f"VALUES ('{def_event}', '{def_filename}', '{def_log}');"

    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_log)
            connection.commit()
