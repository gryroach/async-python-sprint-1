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
    logging.info('Старт анализа погодных условий')
    start = time.time()
    data = DataAggregationTask(CITIES).aggregate_data()
    DataAnalyzingTask(data).analyze_data()
    delta = time.time() - start
    print(
        f'Анализ погодных условий окончен. CSV-файл создан по адресу: '
        f'{pathlib.Path().resolve().joinpath(CSV_FILE_RELATIVE_PATH)}'
    )
    logging.info(f'Анализ окончен. Время выполнения - {delta:.2f}s')


if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO)
    forecast_weather()
