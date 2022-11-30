import datetime
import logging
import csv

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool
from typing import Dict, List, Tuple, Optional

from api_client import YandexWeatherAPI
from utils import (
    HOURS_RANGE, GOOD_CONDITIONS, CITIES_DESCRIPTION_MAP, CSV_FILE_RELATIVE_PATH
)


class DataFetchingTask:
    def __init__(self, cities_urls: Dict[str, str]):
        self.cities_urls = cities_urls

    def start_threads(self) -> List[Tuple[str, Dict]]:
        """Запускает пул потоков для получения данных о погоде"""
        with ThreadPoolExecutor(max_workers=8) as pool:
            data_generator = pool.map(
                self.get_data, self.cities_urls.keys(), chunksize=4
            )
        logging.info('Получены данные о погоде')
        return list(data_generator)

    @staticmethod
    def get_data(city: str) -> Tuple[str, Optional[Dict]]:
        """Возвращает название города и сырые данные из YandexWeatherAPI"""
        try:
            return city, YandexWeatherAPI().get_forecasting(city)
        except Exception as er:
            logging.error(
                f'Ошибка при получении данных погоды для города {city}: {er}'
            )
            return city, None


class DataCalculationTask:
    def __init__(self, raw_data: List[Tuple[str, Dict]]) -> None:
        self.raw_data = raw_data

    def calculate_data(self):
        """Обработать данные погоды для всех городов"""
        logging.info('Начало расчетов данных о погоде')
        if not self.raw_data:
            logging.warning('Нет данных для расчета!')
            return []
        with Pool(processes=4) as pool:
            poll_map_iterator = pool.map(self.get_forecast_data, self.raw_data)
        logging.info('Данные расчитаны')
        return list(poll_map_iterator)

    @staticmethod
    def get_data_per_day(
            forecast_day_hours: list) -> Tuple[Optional[int], Optional[int]]:
        """
        Получить среднее значение температуры и количество часов погоды
        без осадков за день
        """
        average_temp = 0
        temp_count = 0
        good_condition_hours = 0
        for hour in forecast_day_hours:
            if HOURS_RANGE[1] >= int(hour['hour']) >= HOURS_RANGE[0]:
                average_temp += hour['temp']
                temp_count += 1
                if hour['condition'] in GOOD_CONDITIONS:
                    good_condition_hours += 1
        if temp_count:
            average_temp /= temp_count
            return int(round(average_temp, 0)), good_condition_hours
        return None, None

    @staticmethod
    def get_average_city_data(
            city_data: Dict[str, Tuple[int, int]]
    ) -> Tuple[Optional[int], Optional[int]]:
        """
        Получить средние значения температуры и часов без осадков за весь период
        """
        if not city_data:
            logging.warning('Невозможно посчитать средние значения. Нет данных')
            return None, None

        good_conditions_hours = 0
        average_temp = 0
        for day in city_data.values():
            average_temp += day[0]
            good_conditions_hours += day[1]
        average_temp /= len(city_data.values())
        average_temp = int(round(average_temp, 0))
        good_conditions_hours /= len(city_data.values())
        good_conditions_hours = int(round(good_conditions_hours, 0))
        return average_temp, good_conditions_hours

    def get_forecast_data(self, raw_city_data: Tuple[str, Dict]) -> Dict:
        """
        Получить среднее значение температуры и количество часов погоды
        без осадков для города за каждый день, а также средние
        значения этих характеристик за весь период
        """
        forecasts_data = {
            'city': raw_city_data[0],
            'data': dict()
        }
        if not raw_city_data[1]:
            logging.warning(f'Нет данных для города {raw_city_data[0]}!')
            return forecasts_data
        for forecast in raw_city_data[1]['forecasts']:
            average_temp, good_condition_hours = self.get_data_per_day(
                forecast['hours']
            )
            if average_temp is not None:
                date = datetime.datetime.strptime(forecast['date'], '%Y-%m-%d')
                forecasts_data['data'][date.strftime('%d-%m')] = (
                    average_temp, good_condition_hours
                )

        forecasts_data['data']['Среднее'] = self.get_average_city_data(
            forecasts_data['data']
        )
        return forecasts_data


class DataAggregationTask:
    def __init__(self, cities_urls: Dict[str, str]):
        self.cities_urls = cities_urls

    def aggregate_data(self) -> Optional[List[Dict]]:
        """Получить данные и обработать их"""
        raw_data = DataFetchingTask(self.cities_urls).start_threads()
        forecasts_data = DataCalculationTask(raw_data).calculate_data()
        if not forecasts_data or len(forecasts_data) == 0:
            logging.warning('Агрегированные данные пусты')
            return None
        return self.replace_city_name(forecasts_data)

    @staticmethod
    def replace_city_name(data: List[Dict]) -> List[Dict]:
        """Заменить названия городов в соответствии с настройками"""
        logging.info('Замена названий городов')
        for forecast in data:
            forecast['city'] = CITIES_DESCRIPTION_MAP.get(
                forecast['city'], forecast['city']
            )
        return data


class DataAnalyzingTask:
    def __init__(self, data: List[Dict]) -> None:
        self.data = data

    def analyze_data(self) -> None:
        """Провести анализ погоды в городах и записать данные в csv-файл"""
        logging.info('Начало анализа данных о погоде')
        if not self.data:
            print('Данные не предоставлены, анализ остановлен')
            return
        self.set_rating_for_city()
        self.create_csv_file()

    def set_rating_for_city(self) -> None:
        """Добавить данные рейтинга городов"""
        logging.info('Формирование рейтинга городов')
        intermediate_data = []
        for city in self.data:
            try:
                average_temp = city['data']['Среднее'][0]
                good_days = city['data']['Среднее'][1]
                intermediate_data.append(
                    (city['city'], average_temp, good_days)
                )
            except KeyError:
                continue
        rating = self.compute_rating(intermediate_data)
        if rating:
            for city_data in self.data:
                city_data['data']['Рейтинг'] = rating[city_data['city']]

    @staticmethod
    def compute_rating(summary_data: list) -> Dict:
        """Составить рейтинг городов"""
        sorted_list = sorted(
            summary_data, key=lambda x: (x[1], x[2]), reverse=True
        )
        return {city[0]: grade+1 for grade, city in enumerate(sorted_list)}

    def create_csv_file(self) -> None:
        """
        Создать csv-файл с данными о температуре, часах без осадков
        и рейтинге городов
        """
        logging.info('Создание CSV-файла с данными анализа')
        head = self.get_csv_head()
        with open(CSV_FILE_RELATIVE_PATH, 'w') as csv_file:
            writer = csv.DictWriter(csv_file, delimiter=',', fieldnames=head)
            writer.writerow({column: column for column in head})
            for city in self.data:
                if not city['data']:
                    logging.warning('Нет данных для записи в файл')
                    continue
                temp_row = {
                    'Город/день': city['city'],
                    '': 'Температура, среднее'
                }
                condition_row = {
                    'Город/день': '',
                    '': 'Без осадков, часов'
                }
                for column in head[2:]:
                    try:
                        temp_row[column] = city['data'][column][0]
                        condition_row[column] = city['data'][column][1]
                    except KeyError:
                        temp_row[column] = ''
                        condition_row[column] = ''
                    except TypeError:
                        temp_row[column] = city['data'][column]
                        condition_row[column] = ''
                writer.writerow(temp_row)
                writer.writerow(condition_row)

    def get_csv_head(self) -> List[str]:
        """Получить шапку для csv-файла"""
        head = ['Город/день', '']
        max_count_date = 0
        index = 0
        for i, city_data in enumerate(self.data):
            if len(city_data['data']) > max_count_date:
                max_count_date = len(city_data['data'])
                index = i
        for column_name in self.data[index]['data'].keys():
            head.append(column_name)
        return head
