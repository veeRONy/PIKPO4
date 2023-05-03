from typing import List

from .connector import StoreConnector
from pandas import DataFrame, Series
from datetime import datetime

"""
    В данном модуле реализуется API (Application Programming Interface)
    для взаимодействия с БД с помощью объектов-коннекторов.
    
    ВАЖНО! Методы должны быть названы таким образом, чтобы по названию
    можно было понять выполняемые действия.
"""


def select_all_from_source_files(connector: StoreConnector) -> List[tuple]:
    """ Вывод списка обработанных файлов с сортировкой по дате в порядке убывания (DESCENDING) """
    query = f'SELECT * FROM source_files ORDER BY processed_date DESC'
    result = connector.execute(query).fetchall()
    return result


def insert_into_source_files(connector: StoreConnector, filename: str):
    """ Вставка в таблицу обработанных файлов """
    now = datetime.now()        # текущая дата и время
    date_time = now.strftime("%Y-%m-%d %H:%M:%S")   # преобразуем дату в формат SQL, например, '2022-11-15 22:03:16'
    query = f'INSERT INTO source_files (filename, processed_date) VALUES (\'{filename}\', \'{date_time}\')'
    result = connector.execute(query)
    return result


def insert_rows_into_processed_data(connector: StoreConnector, dataframe: DataFrame):
    """ Вставка строк из DataFrame в БД с привязкой данных к последнему обработанному файлу (по дате) """
    rows = dataframe.to_dict('records')
    files_list = select_all_from_source_files(connector)  # получаем список обработанных файлов
    # т.к. строка БД после выполнения SELECT возвращается в виде объекта tuple, например:
    # row = (1, 'seeds_dataset.csv', '2022-11-15 22:03:16'),
    # то значение соответствующей колонки можно получить по индексу, например id = row[0]
    last_file_id = files_list[0][0]  # получаем индекс последней записи из таблицы с файлами
    if len(files_list) > 0:
        for row in rows:
           # print(row)
            connector.execute(f"INSERT INTO matches (tourney_id, tourney_name, tourney_date, match_num, "
                              f"winner_id, winner_name, winner_hand, winner_ht, winner_ioc, winner_age, "
                              f"loser_id, loser_name, loser_hand, loser_ht, loser_ioc, loser_age, score, "
                              f"minutes, source_file) VALUES("
                              f"\'{row['tourney_id']}\', \'{row['tourney_name']}\', \'{row['tourney_date']}\',"
                              f"\'{row['match_num']}\', \'{row['winner_id']}\', \'{row['winner_name']}\', "
                              f"\'{row['winner_hand']}\', \'{row['winner_ht']}\', \'{row['winner_ioc']}\', "
                              f"\'{row['winner_age']}\', \'{row['loser_id']}\', \'{row['loser_name']}\', "
                              f"\'{row['loser_hand']}\', \'{row['loser_ht']}\', \'{row['loser_ioc']}\', "
                              f"\'{row['loser_age']}\', "
                              f"\'{row['score']}\', "
                              f"\'{row['minutes']}\', "
                              f"{last_file_id})")
        print('Data was inserted successfully')
    else:
        print('File records not found. Data inserting was canceled.')


def get_rows_by_tourney_name(self, name: str):
    result = self.execute(f"SELECT * FROM matches WHERE tourney_name='{name}'")
    return result.fetchall()


def get_rows_by_winner_name(self, name: str):
    result = self.execute(f"SELECT * FROM matches WHERE winner_name='{name}'")
    return result.fetchall()


def get_rows_by_loser_name(self, name: str):
    result = self.execute(f"SELECT * FROM matches WHERE loser_name='{name}'")
    return result.fetchall()


def get_rows_by_country(self, country: str):
    result = self.execute(f"SELECT * FROM matches WHERE winner_ioc='{country}' OR loser_ioc='{country}'")
    return result.fetchall()


def sort_matches_by_date(self):
    result = self.execute(f"SELECT * FROM matches ORDER BY tourney_date")
    return result.fetchall()


def delete_rows_by_date(self, date: int):
    result = self.execute(f"DELETE FROM matches WHERE date={date}")
    return result


def insert_new_tourney(self, tourney_id: str, tourney_name: str, tourney_date: int):
    result = self.execute(f"INSERT INTO matches (tourney_id, tourney_name, tourney_date) VALUES ('{tourney_id}','{tourney_name}', '{tourney_date}')")
    return result

