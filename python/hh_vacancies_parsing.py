#!/usr/bin/env python
# coding: utf-8


from requests import get
from json import loads
import pandas as pd
import numpy as np
from pymysql import connect
from time import gmtime, strftime, sleep
from fastzila_credentials import fastzila_credentials


LINK = 'https://api.hh.ru/vacancies'
LINK_AREAS = 'https://api.hh.ru/areas'
SPECIALISATIONS_TUPLE = ('4.127', '21.482', '21.506')

vacancies_list = []
areas_list = []

# Fill the list of region identifiers

response = get(LINK_AREAS)
parsed_dict = loads(response.text)

for area in parsed_dict[0]['areas']:
    areas_list.append(int(area['id']))

# Parsing

for specialisation in SPECIALISATIONS_TUPLE:
    for area in areas_list:
        counter = 0
        while True:
            vacancies = []
            parameters = {'clusters': 'true',
                          'enable_snippets': 'true',
                          'st': 'searchVacancy',
                          'only_with_salary': 'true',
                          'specialization': specialisation,
                          'per_page': 100,
                          'page': counter,
                          'area': area}

            # Filter on expeditors. It lowers records count by all records shall be expeditors

            if specialisation == '21.482':
                parameters['search_field'] = 'name'
                parameters['text'] = 'экспедитор'

            response = get(LINK, params=parameters)
            parsed_dict = loads(response.text)

            for i in parsed_dict['items']:

                # Not all employers have identifiers. If absent, return None

                try:
                    employer_id = i['employer']['id']
                except (KeyError, TypeError):
                    employer_id = None

                vacancies.append([i['id'],
                                  i['name'],
                                  i['area']['id'],
                                  0,
                                  employer_id,
                                  i['employer']['name'],
                                  i['salary']['from'],
                                  i['salary']['to'],
                                  i['salary']['gross'],
                                  i['published_at'],
                                  i['type']['id'],
                                  i['snippet']['requirement']])

            vacancies_list.extend(vacancies)
            counter += 1

            if counter == parsed_dict['pages']:
                break

# Pandas DataFrame is most convenience for a work - make it

df = pd.DataFrame(vacancies_list, columns=('vacancy_id', 'vacancy_name', 'region', 'speciality_id', 'employer_id',
                                           'employer_name', 'salary_min', 'salary_max', 'gross', 'published_at',
                                           'type_vacancy', 'requirements'))
df = df.drop_duplicates()
df.replace({np.nan: None}, inplace=True)
df.vacancy_name = df.vacancy_name.str.lower()  # Преобразуем все в нижний регистр, чтобы дважды не повторять операции
df.region = df.region.astype('int64')
df.gross = df.gross.astype('bool')
df.published_at = df.published_at.astype('datetime64')
df['requirements'] = df['requirements'].fillna('')
df['employer_name'] = df['employer_name'].fillna('')
df['speciality_id'] = 0

# Divide all workers on groups:

# auto drivers
df.speciality_id = np.where(df.vacancy_name.str.contains('водитель-курьер'), 1, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер-водитель'), 1, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('водитель - курьер'), 1, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('водитель курьер'), 1, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('водитель на доставку'), 1, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('авто-курьер'), 1, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('автокурьер'), 1, df.speciality_id)

# velocyclers, motorcyclers and walkers
df.speciality_id = np.where(df.vacancy_name.str.contains('велокурьер'), 4, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('вело-курьер'), 4, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('на велосипеде'), 4, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('мотокурьер'), 7, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('пеший'), 2, df.speciality_id)

# orders pickers
df.speciality_id = np.where(df.vacancy_name.str.contains('сборщик заказов'), 3, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('сборщик-курьер'), 3, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('комплектовщик заказов'), 3, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('сборщик (комплектовщик) заказов', regex=False),
                            3, df.speciality_id)

# drivers-expeditors
df.speciality_id = np.where(df.vacancy_name.str.contains('водитель') & df.vacancy_name.str.contains('экспедитор'),
                            9, df.speciality_id)

# courier-drivers with company auto or own auto transfer in special groups
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('личн'),
                            10, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('собственн'),
                            10, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('своем'),
                            10, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('с автомобил'),
                            10, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('на авто') &
                            df.vacancy_name.str.contains('компани'), 11, df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер на служебном автомобиле'), 11, df.speciality_id)

# undefined and truck driver turn into None
df.speciality_id = np.where(df.vacancy_name.str.contains('курьер') & df.vacancy_name.str.contains('сборщик'), 0,
                            df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('пеший') & df.vacancy_name.str.contains('сборщик'), 0,
                            df.speciality_id)
df.speciality_id = np.where(df.vacancy_name.str.contains('водитель на автомобиль компании / личном автомобиле'),
                            0, df.speciality_id)
df.speciality_id = np.where(df.speciality_id == 1 & df.vacancy_name.str.contains('грузовом'), 0, df.speciality_id)

# Connect to database for write parsed data

df = pd.concat([df, df], axis=1)
data_tuple = tuple(df.itertuples(index=False, name=None))

sql = "INSERT INTO vacancies_hh \
(vacancy_id, vacancy_name, region, speciality_id, employer_id, employer_name, salary_min, salary_max, gross, \
published_at, type_vacancy, requirements) \
SELECT %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s \
WHERE NOT EXISTS \
(SELECT vacancy_id, vacancy_name, region, speciality_id, employer_id, employer_name, salary_min, salary_max, \
gross, published_at, type_vacancy, requirements \
FROM vacancies_hh \
WHERE vacancy_id = %s AND vacancy_name = %s AND \
region = %s AND speciality_id = %s AND \
employer_id = %s AND employer_name = %s AND \
salary_min = %s AND salary_max = %s AND gross = %s AND \
published_at = %s AND type_vacancy = %s AND requirements = %s);"

connection = connect(user=fastzila_credentials['user'],
                     password=fastzila_credentials['db_pass'],
                     database=fastzila_credentials['db_name'],
                     port=fastzila_credentials['port'],
                     host=fastzila_credentials['host'],
                     charset=fastzila_credentials['charset'])

with connection:
    with connection.cursor() as cursor:
        cursor.executemany(sql, data_tuple)
        rows = cursor.rowcount
        connection.commit()
        sql_log = f"INSERT INTO script_logs (event_type, event_source, description) VALUES " \
                  f"('event', 'hh_vacancies_parser.py', '{rows} strings commited to table vacancies_hh');"
        cursor.execute(sql_log)
        connection.commit()
