#!/usr/bin/env python
# coding: utf-8


import pandas as pd
import numpy as np
import re
import pymysql
from requests import Session
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
from bs4 import BeautifulSoup
from time import sleep, gmtime, strftime
from random import uniform
from random import shuffle
from sys import exit
from fake_user_agent import recieve_random_useragent
from traceback import format_exc
from fastzila_credentials import fastzila_credentials


REGION_IDS = (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19, 20, 21, 22, 23, 24, 25, 26, 28, 29, 30,
              31, 32, 33, 34, 64, 65, 66, 67, 69, 70, 71, 72, 73, 74, 75, 77, 78, 80, 81, 82, 83, 84, 85, 86, 88, 89,
              90, 91, 92, 93, 96, 98, 99, 102, 103, 104, 105, 106, 108, 111, 112, 113, 114, 115, 122, 124, 127, 128,
              129, 131, 135, 138, 139, 143, 154)

REGION_NAMES = ('moskva', 'moskovskaya_oblast', 'ekaterinburg', 'samara', 'sankt-peterburg', 'nizhniy_novgorod',
                'krasnodar', 'novosibirsk', 'ryazan', 'sochi', 'krasnoyarsk', 'ufa', 'perm', 'saratov', 'voronezh',
                'orenburg', 'volgograd', 'chelyabinsk', 'rostov-na-donu', 'omsk', 'tver', 'belgorod', 'ivanovo',
                'tyumen', 'ulyanovsk', 'vladimir', 'orel', 'tolyatti', 'tula', 'kursk', 'yaroslavl', 'kaliningrad',
                'smolensk', 'kazan', 'kemerovo', 'magnitogorsk', 'kaluga', 'stavropol', 'penza', 'tambov',
                'cheboksary', 'novorossiysk', 'naberezhnye_chelny', 'nizhnevartovsk', 'arhangelsk',
                'kirovskaya_oblast_kirov', 'tomsk', 'astrahan', 'velikiy_novgorod', 'murmansk', 'cherepovets',
                'izhevsk', 'kostroma', 'petrozavodsk', 'surgut', 'bryansk', 'vologda', 'lipetsk', 'novokuznetsk',
                'barnaul', 'syktyvkar', 'balakovo', 'irkutsk', 'anapa', 'armavir', 'vladivostok', 'vyborg', 'kingisepp',
                'severodvinsk', 'taganrog', 'shahty', 'habarovsk', 'sterlitamak', 'nalchik', 'leningradskaya_oblast',
                'ulan-ude', 'chita', 'yuzhno-sahalinsk', 'nizhnekamsk', 'elabuga', 'pskov', 'engels',
                'volgogradskaya_oblast_volzhskiy', 'nizhniy_tagil')

REGIONS_LIST = list(zip(REGION_NAMES, REGION_IDS))

LINKS_SPECIALITIES_LIST = [('/tag/kurer-na-avtomobile', ''), ('/tag/peshij-kurer', ''), ('', 'сборщик+заказов'),
                           ('', 'водитель+экспедитор')]

# Randomise it. Hard template makes script catchable simply
shuffle(REGIONS_LIST)
shuffle(LINKS_SPECIALITIES_LIST)
DBCONNECT = fastzila_credentials['host'], fastzila_credentials['port'], fastzila_credentials['user'], \
            fastzila_credentials['db_pass'], fastzila_credentials['db_name'], \
            fastzila_credentials['charset']

headers_ = {
    'Accept': 'text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,*/*;q=0.8',
    'Accept-Encoding': 'gzip, deflate, br',
    'Accept-Language': 'ru-RU,ru;q=0.8,en-US;q=0.5,en;q=0.3',
    'Cache-Control': 'max-age=0',
    'Connection': 'keep-alive',
}

RE_IVA_ITEM_DESCRIPTION = re.compile('iva-item-description-')
ua = recieve_random_useragent()


def randomize():
    """Sleep random seconds to lull server defences.
    Here calculate moments of session rebuilding to imitate great number of site users
    
    No parameters
    Return:
                int
    """
      
    sleep(int(uniform(9, 12)))
    return int(uniform(1, 13))


def find_pages_quantity(beautifulsoup):
    """Know how many pages needs for parsing
    
    Parameters:
                beautifulsoup - BeautifulSoap4
    Return:
                int
    """
    
    pages_list = []
    def_items_soups_list = beautifulsoup.select('span[class^="pagination-item"]')
    if def_items_soups_list == []:
        def_pages_number = 0
    else:
        for item in def_items_soups_list:
            try:
                pages_list.append(int(item.text))
            except ValueError:
                continue
        def_pages_number = max(pages_list)

    # If pages number = 100, verify this from quantity of records
    # If verify failed - take only one page
    # It is not accurate verify. Make it approximately    
    if def_pages_number == 100:
        vacancies_count = int(beautifulsoup.select('span[class^="page-title-count"]')[0].get_text().replace('\xa0', ''))
        if vacancies_count < 4000:
            def_pages_number = 0
    
    return def_pages_number


def data_processing(items_list):
    """Process data and preparation data for commit in database.
    There is rules for classificating vacancies for categories (speciality_id)
    
    Parameters:
                items_list - list of list of values
    Return:
                pandas.DataFrame
    """
    
    df = pd.DataFrame(items_list, columns=('vacancy_id', 'vacancy_name', 'region', 'speciality_id',
                                           'employer_name', 'salary', 'description'))
    df = df.drop_duplicates()
    df.vacancy_name = df.vacancy_name.str.lower()
    df.employer_name = df.employer_name.fillna('')
    df.description = df.description.fillna('')
    
    # Rules for classificate workers for categories:

    # automobile drivers
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер на авто'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('водитель курьер'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер водитель'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('водитель-курьер'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер-водитель'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('автокурьер'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('водитель - курьер'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('водителем курьером'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name == 'курьер-почтальон (на авто)', 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('водители курьеры'), 1, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('на авто'),
                                1, df.speciality_id)

    # walking couriers
    df.speciality_id = np.where(df.vacancy_name.str.contains('пеший курьер'), 2, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер пеший'), 2, df.speciality_id)

    # orders pickers and assemblers
    df.speciality_id = np.where(df.vacancy_name == 'сборщик заказов', 3, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('сбор') & df.vacancy_name.str.contains('заказов'),
                                3, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('сбор') & df.vacancy_name.str.contains('товаров'),
                                3, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('комплектовщик'), 3, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('комплектовка заказов'), 3, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('сборщик продуктов'), 3, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('водитель на доставку'), 3, df.speciality_id)

    # velocouriers
    df.speciality_id = np.where(df.vacancy_name.str.contains('велокурьер'), 4, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name == 'курьер (вело)', 4, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name == 'курьер на велосипеде', 4, df.speciality_id)

    # expeditors-drivers
    df.speciality_id = np.where(df.vacancy_name.str.contains('водител') & df.vacancy_name.str.contains('экспедитор'),
                                9, df.speciality_id)

    # couriers-drivers on company autos (or own autos) move into the other categories
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер с авто'), 10, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('со своим авто'),
                                10, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('с личным авто'),
                                10, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('на личном авто'),
                                10, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('на своем авто'),
                                10, df.speciality_id)
    df.speciality_id = np.where(df.vacancy_name.str.contains('курьер на авто компани'), 11, df.speciality_id)
    
    return df


def parsed_page_to_sql(dataframe, dbconnect=DBCONNECT):
    """Commit logs to script_logs table
    
    Parameters:
                def_log - str
                dbconnect - list of 1xstr, 1xint, 4xstr
    No return
    """
    
    dataframe = pd.concat([dataframe, dataframe], axis=1)
    data_tuple = tuple(dataframe.itertuples(index=False, name=None))

    sql = "INSERT INTO vacancies_avito " \
          "(vacancy_id, vacancy_name, region, speciality_id, employer_name, salary, description) " \
          "SELECT %s, %s, %s, %s, %s, %s, %s " \
          "WHERE NOT EXISTS " \
          "(SELECT vacancy_id, vacancy_name, region, speciality_id, employer_name, salary, description " \
          "FROM vacancies_avito " \
          "WHERE vacancy_id = %s AND vacancy_name = %s AND region = %s AND speciality_id = %s AND " \
          "employer_name = %s AND salary = %s AND description = %s);"
    
    connection = pymysql.connect(host=dbconnect[0], port=int(dbconnect[1]), user=dbconnect[2], passwd=dbconnect[3],
                                 db=dbconnect[4], charset=dbconnect[5])

    with connection:
        with connection.cursor() as cursor:
            cursor.executemany(sql, data_tuple)
            rows = cursor.rowcount
        connection.commit()

    return rows


def log_to_sql(def_log, dbconnect=DBCONNECT):
    """Commit logs to script_logs table
    
    Parameters:
                def_log - str
                dbconnect - list of 1xstr, 1xint, 4xstr
    No return
    """
    
    # A hour trying to send data to database
    sql = f"INSERT INTO script_logs (event_type, event_source, description) " \
          f"VALUES ('event', 'avito_vacancies_parser.py', '{def_log}');"

    connection = pymysql.connect(host=dbconnect[0], port=int(dbconnect[1]), user=dbconnect[2], passwd=dbconnect[3],
                                 db=dbconnect[4], charset=dbconnect[5])

    with connection:
        with connection.cursor() as cursor:
            cursor.execute(sql)
        connection.commit()
            

if __name__ == '__main__':
    
    ua = recieve_random_useragent()
    counter_commited, counter_parsed = 0, 0
    headers_['User-Agent'] = ua
    req = Session()
    retry = Retry(connect=3, backoff_factor=0.5)
    adapter = HTTPAdapter(max_retries=retry)
    req.mount('http://', adapter)
    req.mount('https://', adapter)

    for regions in REGIONS_LIST:
        
        # Errors counter
        errors_counter = 0
        
        # Create links for parsing
        link = 'https://www.avito.ru/' + regions[0] + '/vakansii' 
        for links_specialities in LINKS_SPECIALITIES_LIST:
            pages_counter, pages_number = 1, 0
            work_link = link + links_specialities[0]
            parameters = {}
            sleep(5)

            while True:
                
                # Answer randomiser, about pause of session switching and headers
                if randomize() > 11:
                    headers_['User-Agent'] = ua
                    req = Session()
                    retry = Retry(connect=3, backoff_factor=0.5)
                    adapter = HTTPAdapter(max_retries=retry)
                    req.mount('http://', adapter)
                    req.mount('https://', adapter)

                if pages_counter == 1:
                    if links_specialities[1] != '':
                        parameters['q'] = links_specialities[1]
                    response = req.get(work_link, params=parameters, headers=headers_)
                else:
                    parameters = {'p': pages_counter}
                    if links_specialities[1] != '':
                        parameters['q'] = links_specialities[1]
                    response = req.get(work_link, params=parameters, headers=headers_)
                soup = BeautifulSoup(response.text, 'lxml')
                print(response.url)
                # Errors control. If we collects 5 errors - break script to avoid permanent cycle
                if soup == '<html><body><p>upstream connect error or disconnect/reset before headers. reset reason: ' \
                           'connection failure</p></body></html>':
                    sleep(1)
                    errors_counter += 1
                    log_to_sql(f'{errors_counter} time(s) error: "upstream connect error or disconnect/reset before '
                               f'headers. reset reason: connection failure"')
                    if errors_counter == 5:
                        try:
                            exit()
                        except SystemExit:
                            print('Exit by reason: "upstream connect error or disconnect/reset before headers. reset '
                                  'reason: connection failure"')
                    continue
                
                # Raise message and go out if blocked
                elif soup.title.string == 'Доступ временно заблокирован': 
                    try:
                        exit()
                    except SystemExit:
                        log_to_sql(f'Exit by ban on Avito. {counter_parsed} records were parsed and {counter_commited} '
                                   f'commited')
                    break

                if pages_counter == 1:
                    pages_number = find_pages_quantity(soup)

                # Fill data for parsing results
                items_soups_list = soup.select('div[class^="iva-item-body"]')
                for i in items_soups_list:

                    href = i.find('a')['href']
                    vacancy_name = i.find('h3').get_text()    
                    try:
                        description = i.find('div', RE_IVA_ITEM_DESCRIPTION).get_text()
                    except AttributeError:
                        description = None

                    salary = i.find('meta', itemprop='price').attrs['content']

                    try:
                        employer_name = i.find(attrs={'data-marker': 'item-link'}).get_text()
                    except AttributeError:
                        employer_name = None
                        
                    batch_df = data_processing([[href, vacancy_name, regions[1], 0, employer_name, salary,
                                                 description]])
                    counter_commited += parsed_page_to_sql(batch_df)
                    counter_parsed += 1
                    
                pages_counter += 1
                if pages_counter > pages_number:
                    break
                    
    log_to_sql(f'Finished successfully. {counter_parsed} records were parsed and {counter_commited} commited')
