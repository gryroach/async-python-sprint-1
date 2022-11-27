# import logging
# import threading
# import subprocess
# import multiprocessing

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES
import datetime

def forecast_weather():
    """
    Анализ погодных условий по городам@
    """
    data = DataFetchingTask().start_thread()
    n = datetime.datetime.now()
    result = DataAggregationTask().aggregate_data()
    print(result)
    DataAnalyzingTask(result).set_rating_for_city()
    print('-->', datetime.datetime.now() - n)


if __name__ == "__main__":
    forecast_weather()
