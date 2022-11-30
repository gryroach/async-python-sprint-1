import unittest
import json

from tasks import (
    DataCalculationTask,
    DataAggregationTask,
    DataAnalyzingTask,
)

with open('examples/response.json', 'r') as json_file:
    test_data = [('MOSCOW', json.load(json_file))]


class DataCalculationTest(unittest.TestCase):
    calculation_task = DataCalculationTask(list(test_data))

    def test_calculate_data(self):
        result = self.calculation_task.calculate_data()
        self.assertEqual(
            [
                {
                    'city': 'MOSCOW',
                    'data': {
                        '18-05': (13, 11),
                        '19-05': (11, 5),
                        '20-05': (11, 11),
                        'Среднее': (12, 9)
                    }
                }
            ],
            result
        )

    def test_get_data_per_day(self):
        data = [
            {'hour': '0', 'temp': 10, 'condition': 'overcast'},
            {'hour': '9', 'temp': 0, 'condition': 'cloudy'},
            {'hour': '15', 'temp': 20, 'condition': 'rain'},
            {'hour': '19', 'temp': 1, 'condition': 'snow'},
        ]
        result = self.calculation_task.get_data_per_day(data)
        self.assertEqual((7, 1), result)

        data = [
            {'hour': '0', 'temp': 10, 'condition': 'overcast'},
            {'hour': '8', 'temp': 0, 'condition': 'cloudy'},
            {'hour': '20', 'temp': 20, 'condition': 'rain'},
        ]
        result = self.calculation_task.get_data_per_day(data)
        self.assertEqual((None, None), result)

        # testing round
        data = [
            {'hour': '10', 'temp': 1, 'condition': 'overcast'},
            {'hour': '16', 'temp': 2, 'condition': 'cloudy'},
        ]
        result = self.calculation_task.get_data_per_day(data)
        self.assertEqual((2, 2), result)

    def test_get_average_city_data(self):
        data = {'18-05': (15, 11), '19-05': (11, 5), '20-05': (10, 11)}
        result = self.calculation_task.get_average_city_data(data)
        self.assertEqual((12, 9), result)

        data = {}
        result = self.calculation_task.get_average_city_data(data)
        self.assertEqual((None, None), result)

    def test_get_forecast_data(self):
        result = self.calculation_task.get_forecast_data(test_data[0])
        self.assertEqual(
            {
                'city': 'MOSCOW',
                'data': {
                    '18-05': (13, 11),
                    '19-05': (11, 5),
                    '20-05': (11, 11),
                    'Среднее': (12, 9)
                }
            },
            result
        )

        result = self.calculation_task.get_forecast_data(('Test', dict()))
        self.assertEqual({'city': 'Test', 'data': {}}, result)


class DataAggregationTest(unittest.TestCase):
    cities = {
        'WrongCity': 'https://code.s3.yandex.net',
    }
    aggregate_task = DataAggregationTask(cities)

    def test_aggregate_data(self):
        aggregated_data = self.aggregate_task.aggregate_data()
        self.assertEqual([{'city': 'WrongCity', 'data': {}}], aggregated_data)

    def test_replace_city_name(self):
        data = [
            {
                'city': 'MOSCOW',
                'data': {'26-05': (18, 7), 'Среднее': (18, 7)}
            }
        ]
        replaced_data = self.aggregate_task.replace_city_name(data)
        self.assertEqual(
            [
                {
                    'city': 'Москва',
                    'data': {'26-05': (18, 7), 'Среднее': (18, 7)}
                }
            ],
            replaced_data
        )


class DataAnalyzingTest(unittest.TestCase):
    data = [
        {
            'city': 'Лондон',
            'data': {
                '18-05': (10, 5),
                'Среднее': (10, 5)
            }
        },
        {
            'city': 'Москва',
            'data': {
                '18-05': (13, 11),
                '19-05': (23, 9),
                'Среднее': (18, 10)
            }
        }
    ]
    analyzing_task = DataAnalyzingTask(data)

    def test_set_rating_for_city(self):
        self.analyzing_task.set_rating_for_city()
        self.assertEqual(
            [
                {
                    'city': 'Лондон',
                    'data': {
                        '18-05': (10, 5),
                        'Среднее': (10, 5),
                        'Рейтинг': 2
                    }
                },
                {
                    'city': 'Москва',
                    'data': {
                        '18-05': (13, 11),
                        '19-05': (23, 9),
                        'Среднее': (18, 10),
                        'Рейтинг': 1
                    }
                }
            ], self.analyzing_task.data
        )

    def test_compute_rating(self):
        summary_data = [('Лондон', 10, 5), ('Москва', 13, 11)]
        self.assertEqual(
            {'Москва': 1, 'Лондон': 2},
            self.analyzing_task.compute_rating(summary_data)
        )

    def test_get_csv_head(self):
        self.assertEqual(
            ['Город/день', '', '18-05', '19-05', 'Среднее'],
            self.analyzing_task.get_csv_head()
        )


if __name__ == "__main__":
    unittest.main()
