import logging
# import threading
# import subprocess
# import multiprocessing
import time

from api_client import YandexWeatherAPI
from tasks import (
    DataFetchingTask,
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    start = time.time()
    data = DataAggregationTask().aggregate_data()
    DataAnalyzingTask(data).analyze_data()
    delta = time.time() - start
    logging.info(f'Время выполнения - {delta:.2f}s')


if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG)
    forecast_weather()
