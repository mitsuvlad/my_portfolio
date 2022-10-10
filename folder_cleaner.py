#!/usr/bin/env python
# coding: utf-8


from os import path, scandir, remove, listdir
from fastzila_credentials import fastzila_credentials
from pymysql import connect
from shutil import rmtree
from fz_module import files_list_in_folder, delete_files_in_folder, move_emails_inside_mailbox


FOLDER = 'python_scripts/temp/'


if __name__ == '__main__':
    
    # Search & delete all files in the working folder
    files_list = files_list_in_folder()
    delete_files_in_folder(files_list)

    # Create a query for logging of the list of deleted files
    sql_files = f"INSERT INTO script_logs (event_type, event_source, description) " \
                f"VALUES ('cleaner', 'folder_cleaner.py', " \
                f"'Files deleted: {', '.join(files_list)}');"

    # Search & delete all folders in the working folder
    folders_list = []
    with scandir(FOLDER) as entries:
        for entry in entries:
            if entry.is_dir():
                folders_list.append(entry)
                rmtree(path.join(FOLDER, entry))
    folders_list = [i.name for i in folders_list]
    
    # Create a query for logging of the list of deleted folders
    sql_folders = f"INSERT INTO script_logs (event_type, event_source, description) " \
                  f"VALUES ('cleaner', 'folder_cleaner.py', " \
                  f"Folders deleted: {', '.join(folders_list)}');"
    
    # Search & move all letters from 'INBOX' to 'No_useful_data' folder
    emails_list = move_emails_inside_mailbox(to_folder='No_useful_data')
    sql_emails = f"INSERT INTO script_logs (event_type, event_source, description) " \
                 f"VALUES ('cleaner', 'folder_cleaner.py', " \
                 f"Emails moved: {', '.join(emails_list)}');"    
    
    # Commit all queries
    connection = connect(user=fastzila_credentials['user'], password=fastzila_credentials['db_pass'],
                         database=fastzila_credentials['db_name'], port=fastzila_credentials['port'],
                         host=fastzila_credentials['host'], charset=fastzila_credentials['charset'])
    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql_files)
            if folders_list != []:
                cursor.execute(sql_folders)
            if emails_list != []:
                cursor.execute(sql_emails)
        connection.commit()
