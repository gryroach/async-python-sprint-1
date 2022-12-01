import logging
import time
import pathlib

from tasks import (
    DataAggregationTask,
    DataAnalyzingTask,
)
from utils import CITIES, CSV_FILE_RELATIVE_PATH


def forecast_weather():
    """
    Анализ погодных условий по городам
    """
    logging.info('Start of weather analysis')
    start = time.time()
    data = DataAggregationTask(CITIES).aggregate_data()
    best_weather_cities = DataAnalyzingTask(data).analyze_data()
    delta = time.time() - start
    logging.info(f'Analysis completed. Execution time - {delta:.2f}s')
    print(
        'Анализ погодных условий окончен. '
        'Наиболее благоприятные для поездки города: '
        f'{", ".join(best_weather_cities)}.'
    )
    print(
        'CSV-файл с анализом создан по адресу: '
        f'{pathlib.Path().resolve().joinpath(CSV_FILE_RELATIVE_PATH)}'
    )


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    forecast_weather()
