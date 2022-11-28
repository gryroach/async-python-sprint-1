import datetime
import csv

from concurrent.futures import ThreadPoolExecutor
from multiprocessing import Pool

from api_client import YandexWeatherAPI
from utils import CITIES, HOURS_RANGE, GOOD_CONDITIONS, CITIES_DESCRIPTION_MAP


class DataFetchingTask:
    def start_thread(self):
        with ThreadPoolExecutor(max_workers=8) as pool:
            data_generator = pool.map(
                self.get_data, CITIES.keys(), chunksize=2
            )
            raw_data = [i for i in data_generator]
        return raw_data

    @staticmethod
    def get_data(city: str):
        return city, YandexWeatherAPI().get_forecasting(city)


class DataCalculationTask:
    def __init__(self, raw_data) -> None:
        self.raw_data = raw_data

    def calculate_data(self):
        with Pool(processes=4) as pool:
            poll_map_iterator = pool.map(self.get_forecasts, self.raw_data)
        return list(poll_map_iterator)

    @staticmethod
    def get_data_per_day(forecast_day_hours: list):
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
    def get_average_city_data(city_data):
        good_conditions_days = 0
        average_temp = 0
        for day in city_data.values():
            average_temp += day[0]
            good_conditions_days += day[1]
        average_temp /= len(city_data.values())
        average_temp = int(round(average_temp, 0))
        return average_temp, good_conditions_days

    def get_forecasts(self, raw_data: dict) -> dict:
        forecasts_data = {
            'city': raw_data[0],
            'data': dict()
        }
        for forecast in raw_data[1]['forecasts']:
            average_temp, good_condition_hours = self.get_data_per_day(
                forecast['hours']
            )
            if average_temp:
                date = datetime.datetime.strptime(forecast['date'], '%Y-%m-%d')
                forecasts_data['data'][date.strftime('%d-%m')] = (
                    average_temp, good_condition_hours
                )

        forecasts_data['data']['Среднее'] = self.get_average_city_data(
            forecasts_data['data']
        )
        return forecasts_data


class DataAggregationTask:
    def aggregate_data(self):
        raw_data = DataFetchingTask().start_thread()
        forecasts_data = DataCalculationTask(raw_data).calculate_data()
        return self.replace_city_name(forecasts_data)

    @staticmethod
    def replace_city_name(data):
        for forecast in data:
            forecast['city'] = CITIES_DESCRIPTION_MAP[forecast['city']]
        return data


class DataAnalyzingTask:
    def __init__(self, data):
        self.data = data

    def analyze_data(self):
        self.set_rating_for_city()
        self.create_csv_file()

    def set_rating_for_city(self):
        intermediate_data = []
        for city in self.data:
            average_temp = city['data']['Среднее'][0]
            good_days = city['data']['Среднее'][1]
            intermediate_data.append(
                (city['city'], average_temp, good_days)
            )
        rating = self.compute_rating(intermediate_data)
        for city_data in self.data:
            city_data['data']['Рейтинг'] = rating[city_data['city']]

    @staticmethod
    def compute_rating(summary_data: list) -> dict:
        sorted_list = sorted(
            summary_data, key=lambda x: (x[1], x[2]), reverse=True
        )
        rating = {city[0]: grade+1 for grade, city in enumerate(sorted_list)}
        return rating

    def create_csv_file(self):
        head = self.get_csv_head()
        with open('result.csv', 'w') as csv_file:
            writer = csv.DictWriter(csv_file, delimiter=',', fieldnames=head)
            writer.writerow({column: column for column in head})
            for city in self.data:
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

    def get_csv_head(self):
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
